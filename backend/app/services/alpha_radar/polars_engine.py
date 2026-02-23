"""Polars-based calculation engine for Alpha Radar.

This module provides high-performance data loading and calculations using Polars.
All scoring calculations are done in-memory for flexibility during the exploration phase.
"""

from datetime import date
from typing import Optional
from decimal import Decimal

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.engine.factors.market_factors import (
    compute_ici,
    compute_sci,
    compute_dispersion,
    SW_INDUSTRY_NAME_TO_INDEX,
)


class PolarsEngine:
    """
    High-performance calculation engine using Polars.

    Loads data from PostgreSQL and performs vectorized calculations
    for scoring and analysis.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_latest_trading_date(self) -> Optional[date]:
        """Get the most recent trading date from market_daily."""
        result = await self.db.execute(text("SELECT MAX(date) FROM market_daily"))
        row = result.fetchone()
        return row[0] if row and row[0] else None

    async def get_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """Get available date range from market_daily."""
        result = await self.db.execute(text("SELECT MIN(date), MAX(date) FROM market_daily"))
        row = result.fetchone()
        if row:
            return row[0], row[1]
        return None, None

    async def load_market_data(
        self,
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        lookback_days: int = 60,
    ) -> pl.DataFrame:
        """
        Load market data from database into Polars DataFrame.

        For snapshot mode: loads data for lookback_days before target_date.
        For period mode: loads data for the entire period plus lookback_days.

        Args:
            target_date: Target date for snapshot mode
            start_date: Start date for period mode
            end_date: End date for period mode
            lookback_days: Days of history needed for calculations (default 60)

        Returns:
            Polars DataFrame with market data
        """
        # Pad lookback for factor shift/rolling windows
        db_lookback = lookback_days

        # Determine date range to load
        if target_date:
            # Snapshot mode - load lookback_days before target
            query = text("""
                SELECT
                    md.code,
                    md.date,
                    md.open,
                    md.high,
                    md.low,
                    md.close,
                    md.preclose,
                    md.volume,
                    md.amount,
                    iv.turnover_rate AS turn,
                    md.pct_chg,
                    am.name,
                    am.asset_type,
                    am.exchange
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                LEFT JOIN indicator_valuation iv ON md.code = iv.code AND md.date = iv.date
                WHERE md.date <= :target_date
                AND md.date >= :start_date
                AND am.asset_type = 'STOCK'
                ORDER BY md.code, md.date
            """)
            # Calculate start date for lookback
            result = await self.db.execute(
                text("""
                    SELECT DISTINCT date FROM market_daily
                    WHERE date <= :target_date
                    ORDER BY date DESC
                    LIMIT :lookback
                """),
                {"target_date": target_date, "lookback": db_lookback},
            )
            dates = [row[0] for row in result.fetchall()]
            calc_start_date = dates[-1] if dates else target_date

            result = await self.db.execute(
                query, {"target_date": target_date, "start_date": calc_start_date}
            )
        else:
            # Period mode - load from start_date minus lookback to end_date
            # Calculate actual start for lookback
            result = await self.db.execute(
                text("""
                    SELECT DISTINCT date FROM market_daily
                    WHERE date <= :start_date
                    ORDER BY date DESC
                    LIMIT :lookback
                """),
                {"start_date": start_date, "lookback": db_lookback},
            )
            dates = [row[0] for row in result.fetchall()]
            calc_start_date = dates[-1] if dates else start_date

            query = text("""
                SELECT
                    md.code,
                    md.date,
                    md.open,
                    md.high,
                    md.low,
                    md.close,
                    md.preclose,
                    md.volume,
                    md.amount,
                    iv.turnover_rate AS turn,
                    md.pct_chg,
                    am.name,
                    am.asset_type,
                    am.exchange
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                LEFT JOIN indicator_valuation iv ON md.code = iv.code AND md.date = iv.date
                WHERE md.date >= :start_date
                AND md.date <= :end_date
                AND am.asset_type = 'STOCK'
                ORDER BY md.code, md.date
            """)
            result = await self.db.execute(
                query, {"start_date": calc_start_date, "end_date": end_date}
            )

        rows = result.fetchall()
        columns = result.keys()

        # Convert to Polars DataFrame
        if not rows:
            return pl.DataFrame()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        df = pl.DataFrame(data)

        # Convert types
        df = df.with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("preclose").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                pl.col("amount").cast(pl.Float64),
                pl.col("turn").cast(pl.Float64),
                pl.col("pct_chg").cast(pl.Float64),
            ]
        )

        return df

    async def load_market_regime(
        self,
        target_date: date,
        lookback_days: int = 20,
    ) -> dict:
        """Compute market regime signal from index + breadth divergence.

        Returns a dict with:
        - index_return_5d: 5-trading-day return of Shanghai Composite (%)
        - index_return_10d: 10-trading-day return (%)
        - index_drawdown_from_high_20d: drawdown from 20d high (%)
        - breadth_5d_avg: avg % of advancing stocks over last 5 days
        - breadth_today: % of advancing stocks today
        - market_regime_score: 0-100, higher = more favorable for new longs

        Design rationale (Iteration 7 v2 — divergence-based regime):

        v1 failed because mapping raw breadth levels to scores compressed all
        regime values to 41-51 (no discriminating power). Low breadth always
        scored low, but 12-15 (breadth_5d_avg=33.7%) was actually an excellent
        capitulation entry that should score HIGH.

        Key insight: the REAL danger signal is INDEX-BREADTH DIVERGENCE.
        - 12-08: Index +1.19%, breadth_5d_avg 42.9% → index MASKS weakness → TRAP
        - 12-15: Index -0.84%, breadth 33.7% → HONEST capitulation → good entry

        New approach:
        - Index component remains the BASE signal (Iter 4 logic, good range 30-85)
        - Breadth acts as a MODIFIER that penalizes divergence and rewards alignment:
          * Index up + breadth weak → penalty (index masking weakness)
          * Index down + breadth down → no penalty (honest capitulation)
          * Breadth strong → slight bonus (broad participation)
        - Final score = index_component + breadth_modifier (clamped 0-100)
        """
        # --- Index data ---
        result = await self.db.execute(
            text("""
                SELECT date, close, pct_chg
                FROM market_daily
                WHERE code = 'sh.000001'
                AND date <= :target_date
                ORDER BY date DESC
                LIMIT :lookback
            """),
            {"target_date": target_date, "lookback": lookback_days + 5},
        )
        rows = result.fetchall()

        if len(rows) < 10:
            return {
                "index_return_5d": 0.0,
                "index_return_10d": 0.0,
                "index_drawdown_from_high_20d": 0.0,
                "breadth_5d_avg": 50.0,
                "breadth_today": 50.0,
                "market_regime_score": 50.0,
            }

        close_today = float(rows[0][1])
        close_5d_ago = float(rows[4][1]) if len(rows) > 4 else close_today
        close_10d_ago = float(rows[9][1]) if len(rows) > 9 else close_today

        ret_5d = (close_today / close_5d_ago - 1) * 100 if close_5d_ago else 0.0
        ret_10d = (close_today / close_10d_ago - 1) * 100 if close_10d_ago else 0.0

        closes = [float(r[1]) for r in rows[:20]]
        high_20d = max(closes) if closes else close_today
        drawdown = (close_today / high_20d - 1) * 100 if high_20d else 0.0

        # Index component (0-100): same as Iter 4
        if ret_5d > 3.0:
            index_component = max(10.0, 30.0 - (ret_5d - 3.0) * 5)
        elif ret_5d > 1.0:
            index_component = 50.0 - (ret_5d - 1.0) * 10
        elif ret_5d > -1.0:
            index_component = 60.0 + (-ret_5d) * 10
        elif ret_5d > -3.0:
            index_component = 70.0 + (-ret_5d - 1.0) * 7.5
        else:
            index_component = max(25.0, 55.0 + ret_5d * 5)
        index_component = max(0.0, min(100.0, index_component))

        # --- Market breadth data ---
        index_dates = [rows[i][0] for i in range(min(5, len(rows)))]

        breadth_result = await self.db.execute(
            text("""
                SELECT
                    date,
                    COUNT(*) FILTER (WHERE pct_chg > 0)::float
                        / NULLIF(COUNT(*), 0) * 100 AS breadth_pct
                FROM market_daily
                WHERE date = ANY(:dates)
                AND code NOT LIKE 'sh.000%%'
                AND code NOT LIKE 'sz.399%%'
                GROUP BY date
                ORDER BY date DESC
            """),
            {"dates": index_dates},
        )
        breadth_rows = breadth_result.fetchall()

        if breadth_rows:
            breadth_values = [float(r[1]) for r in breadth_rows]
            breadth_today = breadth_values[0]
            breadth_5d_avg = sum(breadth_values) / len(breadth_values)
        else:
            breadth_values = []
            breadth_today = 50.0
            breadth_5d_avg = 50.0

        # Breadth MODIFIER (-20 to +10):
        # Breadth acts as an adjustment to the index base, NOT an independent score.
        # This preserves the index component's full range while adding divergence info.
        #
        # Divergence detection:
        #   Index optimistic (ret_5d > 0) + breadth weak (5d_avg < 45):
        #     This is the 12-08 pattern — index masks internal weakness.
        #     Penalty scales with divergence severity.
        #   Index pessimistic (ret_5d < 0) + breadth weak:
        #     Honest capitulation — no penalty (may even be good entry).
        #   Breadth healthy (5d_avg > 55):
        #     Broad participation — slight bonus.
        #
        # Count "panic days" in last 5 days (breadth < 40%)
        panic_days = sum(1 for v in breadth_values if v < 40) if breadth_rows else 0
        # Iter 10: count weak breadth days (< 35%) for fragility detection
        weak_days = sum(1 for v in breadth_values if v < 35) if breadth_rows else 0

        breadth_modifier = 0.0

        if ret_5d > 0 and breadth_5d_avg < 45:
            # INDEX-BREADTH DIVERGENCE: index up but most stocks declining
            # Severity increases with: higher ret_5d, lower breadth, more panic days
            divergence_gap = ret_5d * (45 - breadth_5d_avg) / 45.0
            panic_penalty = panic_days * 3.0
            breadth_modifier = -(divergence_gap * 5.0 + panic_penalty)
            breadth_modifier = max(-25.0, breadth_modifier)
            # Iter 10: attenuate divergence penalty when target-day breadth is strong.
            # Strong today breadth (>60%) means the market is recovering from prior weakness.
            if breadth_today > 60:
                attenuation = min(1.0, (breadth_today - 60) / 20.0)
                breadth_modifier *= 1.0 - attenuation * 0.7

        elif ret_5d <= 0 and breadth_5d_avg < 35:
            # HONEST CAPITULATION: both index and breadth weak
            # This is a potential entry — give a slight boost for deep capitulation
            # Deeper breadth collapse → stronger capitulation signal
            capitulation_depth = (35 - breadth_5d_avg) / 35.0  # 0 to 1
            breadth_modifier = capitulation_depth * 8.0  # 0 to +8

        elif breadth_5d_avg > 55:
            # BROAD PARTICIPATION: healthy market breadth
            breadth_modifier = min(10.0, (breadth_5d_avg - 55) * 0.5)  # 0 to +10

        elif breadth_5d_avg < 45 and panic_days >= 3:
            # PERSISTENT WEAKNESS: not divergence (index also weak) but many panic days
            # Mild penalty — market is genuinely stressed
            breadth_modifier = -panic_days * 2.0  # -6 to -10

        # --- Iter 10: Target-day breadth signal ---
        # Breadth on the target day itself is a strong contemporaneous signal.
        # High breadth (>65%) = broad bullish participation → regime boost.
        # Low breadth (<35%) = widespread selling → regime penalty.
        # Iter 10b: Strengthened breadth_today signal (threshold 60, slope 1.0, cap 20)
        breadth_today_modifier = 0.0
        if breadth_today > 60:
            breadth_today_modifier = min(20.0, (breadth_today - 60) * 1.0)
        elif breadth_today < 35:
            breadth_today_modifier = max(-15.0, (breadth_today - 35) * 0.5)

        # --- Iter 10: Breadth fragility detection ---
        # Penalize when recent breadth swings wildly (e.g. 27%→80%→62%).
        # The 5d average smooths away instability; weak_days count catches it.
        fragility_penalty = 0.0
        if weak_days >= 2:
            fragility_penalty = weak_days * 5.0
        elif weak_days == 1 and panic_days >= 2:
            fragility_penalty = 5.0

        # Iter 10b: Attenuate fragility penalty when target-day breadth is strong (>65%).
        # Historical fragility was real, but strong today breadth = market recovering.
        if fragility_penalty > 0 and breadth_today > 65:
            frag_attenuation = min(1.0, (breadth_today - 65) / 15.0)
            fragility_penalty *= 1.0 - frag_attenuation * 0.7

        # Final composite: index base + breadth modifier + target-day signal - fragility
        regime_score = (
            index_component + breadth_modifier + breadth_today_modifier - fragility_penalty
        )
        regime_score = max(0.0, min(100.0, regime_score))

        # Iter 13: Market-wide moneyflow signals for abstain logic
        mf_result = await self.db.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (WHERE net_mf_amount > 0)::float
                        / NULLIF(COUNT(*), 0) * 100 AS mf_pct_inflow,
                    AVG(net_mf_amount) AS mf_avg_net
                FROM moneyflow_daily
                WHERE date = :target_date
            """),
            {"target_date": target_date},
        )
        mf_row = mf_result.fetchone()
        mf_pct_inflow = float(mf_row[0]) if mf_row and mf_row[0] is not None else 50.0
        mf_avg_net = float(mf_row[1]) if mf_row and mf_row[1] is not None else 0.0

        # Iter 13: Limit-down count for extreme market stress
        limit_result = await self.db.execute(
            text("""
                SELECT COUNT(*) FROM limit_list_daily
                WHERE date = :target_date AND limit_type = 'D'
            """),
            {"target_date": target_date},
        )
        limit_row = limit_result.fetchone()
        limit_down = int(limit_row[0]) if limit_row and limit_row[0] else 0

        # Iter 13: Multi-signal tradeable-day gate
        is_capitulation = breadth_today < 5.0 or limit_down > 500
        hostile_breadth = breadth_today < 35.0
        hostile_moneyflow = mf_pct_inflow < 32.0 or mf_avg_net < -1200
        hostile_weakness = weak_days >= 2
        low_regime = regime_score < 45

        should_abstain = False
        if not is_capitulation:
            hostile_signals = sum(
                [hostile_breadth, hostile_moneyflow, hostile_weakness, low_regime]
            )
            if hostile_breadth and hostile_signals >= 3:
                should_abstain = True
            elif low_regime and weak_days >= 3:
                should_abstain = True

        # --- ICI/SCI/Dispersion (Phase 1) ---
        start_date_limit = rows[-1][0] if rows else target_date

        # 1. Load stock pct_chg data
        stock_pct_result = await self.db.execute(
            text("""
                SELECT md.date, md.code, md.pct_chg
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                WHERE md.date <= :target_date
                AND md.date >= :start_date
                AND am.asset_type = 'STOCK'
            """),
            {"target_date": target_date, "start_date": start_date_limit},
        )
        stock_rows = stock_pct_result.fetchall()
        stock_pct_df = pl.DataFrame(stock_rows, schema=["date", "code", "pct_chg"], orient="row")

        # 2. Load index pct_chg data (already have from rows)
        # rows is [date, close, pct_chg], sorted DESC
        index_pct_df = pl.DataFrame(rows, schema=["date", "close", "pct_chg"], orient="row").select(
            ["date", "pct_chg"]
        )

        # 3. Load industry index data
        ind_pct_result = await self.db.execute(
            text("""
                SELECT date, code, pct_chg
                FROM market_daily
                WHERE date <= :target_date
                AND date >= :start_date
                AND code LIKE 'sh.801%'
            """),
            {"target_date": target_date, "start_date": start_date_limit},
        )
        ind_rows = ind_pct_result.fetchall()
        ind_pct_df = pl.DataFrame(ind_rows, schema=["date", "code", "pct_chg"], orient="row")

        # 4. Load stock-to-industry mapping
        profile_result = await self.db.execute(
            text("SELECT code, sw_industry_l1 FROM stock_profile")
        )
        profile_rows = profile_result.fetchall()
        stock_industry_map = {}
        for r in profile_rows:
            if r[1] and r[1] in SW_INDUSTRY_NAME_TO_INDEX:
                stock_industry_map[r[0]] = SW_INDUSTRY_NAME_TO_INDEX[r[1]]

        # 5. Compute metrics
        ici_20d = compute_ici(stock_pct_df, index_pct_df, target_date)
        sci_by_industry = compute_sci(stock_pct_df, ind_pct_df, stock_industry_map, target_date)
        dispersion_std = compute_dispersion(stock_pct_df, index_pct_df, target_date)

        # 6. Phase 2: ICI → regime_discount enhancement
        # ICI measures market coherence (how much stocks move together with the index).
        # High ICI + index falling = systemic risk (everything drops together) → penalize
        # High ICI + index rising = systemic uptrend (ride the wave) → mild boost
        # Low ICI = stock divergence (alpha stock-picking is most valuable) → boost
        ici_modifier = 0.0
        if ici_20d > 0.5 and ret_5d < -1.0:
            ici_modifier = -15.0  # 系统性风险惩罚
        elif ici_20d > 0.5 and ret_5d > 1.0:
            ici_modifier = 5.0  # 系统性顺势加分
        elif ici_20d < 0.25:
            ici_modifier = 10.0  # 个股分化期，选股价值高
        regime_score = max(0.0, min(100.0, regime_score + ici_modifier))

        # Re-evaluate should_abstain with ICI-adjusted regime_score
        low_regime = regime_score < 45
        should_abstain = False
        abstain_reason = None
        if not is_capitulation:
            hostile_signals = sum(
                [hostile_breadth, hostile_moneyflow, hostile_weakness, low_regime]
            )
            if hostile_breadth and hostile_signals >= 3:
                should_abstain = True
                abstain_reason = "hostile_breadth"
            elif low_regime and weak_days >= 3:
                should_abstain = True
                abstain_reason = "sustained_fragility"
            elif dispersion_std > 4.5 and hostile_signals >= 2:
                should_abstain = True
                abstain_reason = "extreme_dispersion"

        # 7. Determine market regime v2
        market_regime_v2 = "个股分化"
        if ici_20d > 0.5 and ret_5d > 0:
            market_regime_v2 = "系统性主导"
        elif ici_20d > 0.5 and ret_5d < 0:
            market_regime_v2 = "系统性风险"
        elif 0.25 <= ici_20d <= 0.5:
            market_regime_v2 = "行业轮动"

        return {
            "index_return_5d": round(ret_5d, 2),
            "index_return_10d": round(ret_10d, 2),
            "index_drawdown_from_high_20d": round(drawdown, 2),
            "breadth_5d_avg": round(breadth_5d_avg, 1),
            "breadth_today": round(breadth_today, 1),
            "market_regime_score": round(regime_score, 1),
            "should_abstain": should_abstain,
            "abstain_reason": abstain_reason,
            "mf_pct_inflow": round(mf_pct_inflow, 1),
            "mf_avg_net": round(mf_avg_net, 1),
            "limit_down": limit_down,
            "ici_20d": round(ici_20d, 4),
            "ici_modifier": round(ici_modifier, 1),
            "sci_by_industry": sci_by_industry,
            "dispersion_std": round(dispersion_std, 4),
            "market_regime_v2": market_regime_v2,
        }

    async def load_moneyflow_data(self, target_date: date) -> pl.DataFrame:
        """Load per-stock moneyflow data and compute percentiles for a single date.

        Iter 14/15: Per-stock moneyflow percentiles used by trend/dragon scoring.
        Returns DataFrame with columns: code, mf_net_percentile, elg_net_percentile.
        """
        result = await self.db.execute(
            text("""
                SELECT code, net_mf_amount, buy_elg_amount, sell_elg_amount
                FROM moneyflow_daily
                WHERE date = :target_date
            """),
            {"target_date": target_date},
        )
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame(
                schema={
                    "code": pl.Utf8,
                    "mf_net_percentile": pl.Float64,
                    "elg_net_percentile": pl.Float64,
                }
            )

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        df = pl.DataFrame(data)

        df = df.with_columns(
            [
                (pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw"),
            ]
        )
        df = df.with_columns(
            [
                (pl.col("net_mf_amount").rank() / pl.len() * 100).alias("mf_net_percentile"),
                (pl.col("elg_net_raw").rank() / pl.len() * 100).alias("elg_net_percentile"),
            ]
        ).select(["code", "mf_net_percentile", "elg_net_percentile"])

        return df

    async def load_valuation_data(
        self,
        target_date: Optional[date] = None,
    ) -> pl.DataFrame:
        """Load valuation data (PE, PB, market cap) for target date."""
        if target_date is None:
            target_date = await self.get_latest_trading_date()

        query = text("""
            SELECT
                code,
                date,
                pe_ttm,
                pb_mrq,
                ps_ttm,
                total_mv,
                circ_mv,
                is_st
            FROM indicator_valuation
            WHERE date = :target_date
        """)
        result = await self.db.execute(query, {"target_date": target_date})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)

    async def load_style_factors(
        self,
        target_date: Optional[date] = None,
    ) -> pl.DataFrame:
        """Load style factors for target date."""
        if target_date is None:
            target_date = await self.get_latest_trading_date()

        query = text("""
            SELECT
                code,
                date,
                market_cap,
                size_category,
                size_percentile,
                volatility_20d,
                vol_category,
                vol_percentile,
                avg_turnover_20d,
                turnover_category,
                turnover_percentile,
                value_category,
                value_percentile,
                ep_ratio,
                bp_ratio,
                momentum_20d,
                momentum_60d,
                momentum_percentile
            FROM stock_style_exposure
            WHERE date = :target_date
        """)
        result = await self.db.execute(query, {"target_date": target_date})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)

    async def load_stock_profiles(self) -> pl.DataFrame:
        """Load stock profiles (industry classification)."""
        query = text("""
            SELECT
                code,
                sw_industry_l1,
                sw_industry_l2,
                sw_industry_l3,
                em_industry
            FROM stock_profile
        """)
        result = await self.db.execute(query)
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        return pl.DataFrame(data)

    def calculate_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate technical indicators needed for scoring.

        Adds:
        - volume_ratio_5d: Current volume / 5-day average volume
        - turnover_change_20d: (5d avg turnover / 20d avg turnover) - 1
        - price_position_60d: (close - 60d_low) / (60d_high - 60d_low)
        - main_strength_proxy: Composite indicator (0-100)
        """
        if df.is_empty():
            return df

        # Sort by code and date for rolling calculations
        df = df.sort(["code", "date"])

        from app.services.alpha_radar.engine.factors import compute_all_factors

        return compute_all_factors(df)

    @staticmethod
    def compute_sector_momentum(
        market_df: pl.DataFrame,
        profile_df: pl.DataFrame,
        target_date: "date",
        lookback_days: int = 5,
    ) -> pl.DataFrame:
        """Compute sector-level momentum for diversification and penalty scoring.

        Iter 9: Qualitative analysis of 12-08 systemic failure revealed that ALL
        10-11 unique picks came from just 2 sectors (汽车 + 机械设备). Neither sector
        was declining pre-12-08 (+0.94%, +1.11%), but both couldn't survive the
        subsequent market crash. This factor penalizes stocks in sectors with weak
        or fragmented momentum.

        Computes per-sector average daily return over the last `lookback_days`
        trading days, then sums to get cumulative sector momentum.

        Args:
            market_df: Full market data (all dates, all stocks)
            profile_df: Stock profiles with sw_industry_l1
            target_date: The recommendation date
            lookback_days: Number of trading days to look back (default 5)

        Returns:
            DataFrame with columns: sw_industry_l1, sector_momentum_5d, sector_stock_count
        """
        if market_df.is_empty() or profile_df.is_empty():
            return pl.DataFrame(
                schema={
                    "sw_industry_l1": pl.Utf8,
                    "sector_momentum_5d": pl.Float64,
                    "sector_stock_count": pl.UInt32,
                }
            )

        # Exclude target_date: sector momentum should reflect PRIOR momentum
        trading_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") < target_date)
            .sort("date", descending=True)
            .head(lookback_days)
        )

        if trading_dates.height == 0:
            return pl.DataFrame(
                schema={
                    "sw_industry_l1": pl.Utf8,
                    "sector_momentum_5d": pl.Float64,
                    "sector_stock_count": pl.UInt32,
                }
            )

        date_list = trading_dates["date"].to_list()

        sector_data = (
            market_df.filter(
                (pl.col("date").is_in(date_list))
                & (~pl.col("code").str.starts_with("sh.000"))
                & (~pl.col("code").str.starts_with("sz.399"))
            )
            .join(
                profile_df.select(["code", "sw_industry_l1"]),
                on="code",
                how="inner",
            )
            .filter(pl.col("sw_industry_l1").is_not_null())
        )

        if sector_data.is_empty():
            return pl.DataFrame(
                schema={
                    "sw_industry_l1": pl.Utf8,
                    "sector_momentum_5d": pl.Float64,
                    "sector_stock_count": pl.UInt32,
                }
            )

        sector_daily = sector_data.group_by(["sw_industry_l1", "date"]).agg(
            pl.col("pct_chg").fill_null(0.0).mean().alias("avg_pct_chg"),
        )

        sector_momentum = sector_daily.group_by("sw_industry_l1").agg(
            pl.col("avg_pct_chg").sum().alias("sector_momentum_5d"),
            pl.len().alias("sector_stock_count"),
        )

        return sector_momentum

    def calculate_period_metrics(
        self,
        df: pl.DataFrame,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """
        Calculate period aggregation metrics for period mode.

        Returns per-stock metrics:
        - period_return: (end_close / start_close) - 1
        - max_drawdown: Maximum peak-to-trough decline
        - avg_turnover: Average daily turnover
        """
        if df.is_empty():
            return df

        # Filter to the period
        period_df = df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))

        # Calculate per-stock aggregations
        result = period_df.group_by("code").agg(
            [
                # Get first and last close for return calculation
                pl.col("close").first().alias("start_close"),
                pl.col("close").last().alias("end_close"),
                # Average turnover
                pl.col("turn").mean().alias("avg_turnover"),
                # Max drawdown calculation
                # For each stock, compute running max and drawdown
                pl.col("close").alias("close_series"),
                # Keep latest values
                pl.col("name").last(),
                pl.col("asset_type").last(),
                pl.col("exchange").last(),
            ]
        )

        # Calculate period return
        result = result.with_columns(
            [
                ((pl.col("end_close") / pl.col("start_close")) - 1)
                .fill_null(0.0)
                .alias("period_return"),
            ]
        )

        # Calculate max drawdown (simplified - peak-to-trough)
        # Note: Full drawdown calculation would require row-by-row processing
        # For now, use a simplified approximation
        result = result.with_columns(
            [
                pl.lit(0.0).alias("max_drawdown"),  # Placeholder - implement full logic later
            ]
        )

        return result
