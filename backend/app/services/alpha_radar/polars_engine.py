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
                    md.turn,
                    md.pct_chg,
                    am.name,
                    am.asset_type,
                    am.exchange
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
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
                {"target_date": target_date, "lookback": lookback_days},
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
                {"start_date": start_date, "lookback": lookback_days},
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
                    md.turn,
                    md.pct_chg,
                    am.name,
                    am.asset_type,
                    am.exchange
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
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

        # --- Iter 10: Tradeable-day gate (abstain mechanism) ---
        # When market is deeply hostile, ANY stock recommendation has near-zero
        # probability of success. Evidence: 12-08 had only 7/50 winners (14%) in
        # top-50 candidates — no factor reweighting can fix this.
        #
        # Gate requires TWO simultaneous signals (prevents false positives):
        #   1. regime_score < 45  (weak market regime)
        #   2. weak_days >= 3     (3+ of last 5 days had <35% stocks positive)
        #
        # Validation on 8-date backtest:
        #   12-08: regime=39.8, weak_days=3 → GATE (WR was 0%) ✅
        #   12-22: regime=35.8, weak_days=1 → pass (WR=40%)    ✅
        #   12-29: regime=38.4, weak_days=2 → pass (WR=80%)    ✅
        #   All other dates: pass                                ✅
        should_abstain = regime_score < 45 and weak_days >= 3

        return {
            "index_return_5d": round(ret_5d, 2),
            "index_return_10d": round(ret_10d, 2),
            "index_drawdown_from_high_20d": round(drawdown, 2),
            "breadth_5d_avg": round(breadth_5d_avg, 1),
            "breadth_today": round(breadth_today, 1),
            "market_regime_score": round(regime_score, 1),
            "should_abstain": should_abstain,
        }

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

        # Calculate rolling indicators per stock
        df = df.with_columns(
            [
                # 5-day average volume
                pl.col("volume")
                .rolling_mean(window_size=5)
                .over("code", order_by="date")
                .alias("volume_ma5"),
                # 20-day average turnover
                pl.col("turn")
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .alias("turnover_ma20"),
                # 5-day average turnover
                pl.col("turn")
                .rolling_mean(window_size=5)
                .over("code", order_by="date")
                .alias("turnover_ma5"),
                # 60-day high
                pl.col("high")
                .rolling_max(window_size=60)
                .over("code", order_by="date")
                .alias("high_60d"),
                # 60-day low
                pl.col("low")
                .rolling_min(window_size=60)
                .over("code", order_by="date")
                .alias("low_60d"),
            ]
        )

        # Calculate derived indicators
        df = df.with_columns(
            [
                # Volume ratio (current / 5d avg)
                (pl.col("volume") / pl.col("volume_ma5").shift(1).over("code", order_by="date"))
                .fill_null(1.0)
                .alias("volume_ratio_5d"),
                # Turnover change (5d avg / 20d avg - 1)
                ((pl.col("turnover_ma5") / pl.col("turnover_ma20")) - 1)
                .fill_null(0.0)
                .alias("turnover_change_20d"),
                # Price position (0-1)
                ((pl.col("close") - pl.col("low_60d")) / (pl.col("high_60d") - pl.col("low_60d")))
                .fill_null(0.5)
                .clip(0.0, 1.0)
                .alias("price_position_60d"),
            ]
        )

        # -- Additional indicators for improved scoring --

        # Positive day ratio: % of days with positive return in last 20d
        df = df.with_columns(
            [
                (
                    pl.when(pl.col("pct_chg").fill_null(0.0) > 0)
                    .then(1.0)
                    .otherwise(0.0)
                    .rolling_mean(window_size=20)
                    .over("code", order_by="date")
                    .fill_null(0.5)
                ).alias("positive_day_ratio_20d"),
            ]
        )

        # Up-volume ratio: volume on up days / total volume over 20d
        # This measures whether volume concentrates on up moves (accumulation)
        df = df.with_columns(
            [
                (
                    pl.when(pl.col("pct_chg").fill_null(0.0) > 0)
                    .then(pl.col("volume").cast(pl.Float64))
                    .otherwise(pl.lit(0.0))
                    .rolling_sum(window_size=20)
                    .over("code", order_by="date")
                ).alias("up_volume_20d"),
                (
                    pl.col("volume")
                    .cast(pl.Float64)
                    .rolling_sum(window_size=20)
                    .over("code", order_by="date")
                ).alias("total_volume_20d"),
            ]
        )

        df = df.with_columns(
            [
                (pl.col("up_volume_20d") / pl.col("total_volume_20d").clip(lower_bound=1.0))
                .fill_null(0.5)
                .alias("up_volume_ratio"),
            ]
        )

        # 5/10/20-day price moving averages + ATR for normalization
        df = df.with_columns(
            [
                pl.col("close")
                .rolling_mean(window_size=5)
                .over("code", order_by="date")
                .alias("ma_5"),
                pl.col("close")
                .rolling_mean(window_size=10)
                .over("code", order_by="date")
                .alias("ma_10"),
                pl.col("close")
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .alias("ma_20"),
                (
                    pl.max_horizontal(
                        pl.col("high") - pl.col("low"),
                        (pl.col("high") - pl.col("preclose")).abs(),
                        (pl.col("low") - pl.col("preclose")).abs(),
                    )
                    .rolling_mean(window_size=20)
                    .over("code", order_by="date")
                    .fill_null(1.0)
                    .clip(lower_bound=0.01)
                ).alias("atr_20d"),
            ]
        )

        # Continuous ma_alignment_score (Iter 10 de-ceiling)
        # ATR-normalized spacing, slope, and overextension for within-regime differentiation
        _atr = pl.col("atr_20d")

        _z_c10 = (pl.col("close") - pl.col("ma_10")) / _atr
        _z_c20 = (pl.col("close") - pl.col("ma_20")) / _atr
        _z_1020 = (pl.col("ma_10") - pl.col("ma_20")) / _atr
        _z_510 = (pl.col("ma_5") - pl.col("ma_10")) / _atr
        _z_c5 = (pl.col("close") - pl.col("ma_5")) / _atr

        _d_ma10 = (pl.col("ma_10") - pl.col("ma_10").shift(3).over("code", order_by="date")) / _atr
        _d_ma20 = (pl.col("ma_20") - pl.col("ma_20").shift(5).over("code", order_by="date")) / _atr

        def _sig(x: pl.Expr) -> pl.Expr:
            return pl.lit(1.0) / (pl.lit(1.0) + (-x).exp())

        _price_ok = _sig((_z_c10 - 0.05) / 0.25)
        _spacing_ok = _sig((_z_1020 - 0.05) / 0.25)
        _slope10_ok = _sig(_d_ma10.fill_null(0.0) / 0.20)
        _slope20_ok = _sig(_d_ma20.fill_null(0.0) / 0.15)
        _ma5_ok = _sig(_z_510 / 0.20) * _sig(_z_c5 / 0.20)
        _overext = _sig((_z_c10 - 1.8) / 0.35)

        _strength100 = (
            0.30 * _slope10_ok
            + 0.20 * _slope20_ok
            + 0.25 * _spacing_ok
            + 0.15 * _price_ok
            + 0.10 * _ma5_ok
        ) * (pl.lit(1.0) - 0.6 * _overext)

        _strength65 = (
            0.55 * _sig(_z_c20 / 0.35) + 0.30 * _slope10_ok + 0.15 * _sig(_z_1020 / 0.35)
        ) * (pl.lit(1.0) - 0.4 * _overext)

        _strength45 = (0.60 * _sig(_z_c10 / 0.35) + 0.40 * _slope10_ok) * (
            pl.lit(1.0) - 0.4 * _overext
        )

        _strength20 = _sig(_z_c20 / 0.60)

        df = df.with_columns(
            [
                (
                    pl.when(
                        (pl.col("close") > pl.col("ma_10")) & (pl.col("ma_10") > pl.col("ma_20"))
                    )
                    .then(93.0 + 7.0 * _strength100)
                    .when(pl.col("close") > pl.col("ma_20"))
                    .then(60.0 + 5.0 * _strength65)
                    .when(pl.col("close") > pl.col("ma_10"))
                    .then(40.0 + 5.0 * _strength45)
                    .otherwise(17.0 + 3.0 * _strength20)
                )
                .clip(0.0, 100.0)
                .fill_null(50.0)
                .alias("ma_alignment_score"),
            ]
        )

        # Price stability (20d): standard deviation of daily returns / abs(mean return)
        # Lower = more stable. We'll invert for scoring.
        df = df.with_columns(
            [
                pl.col("pct_chg")
                .fill_null(0.0)
                .rolling_std(window_size=20)
                .over("code", order_by="date")
                .fill_null(2.0)
                .alias("return_std_20d"),
                pl.col("pct_chg")
                .fill_null(0.0)
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .fill_null(0.0)
                .alias("return_mean_20d"),
            ]
        )

        # Stability score (0-100): calmer uptrends survive market pullbacks better
        # return_std_20d typical range: 0.5 (very stable) to 5.0+ (volatile)
        # Stocks with std < 1.5% get high stability; std > 4% get near-zero
        df = df.with_columns(
            [
                ((1 - (pl.col("return_std_20d").clip(0.5, 4.5) - 0.5) / 4.0) * 100)
                .fill_null(50.0)
                .alias("stability_score"),
            ]
        )

        # --- Iter 5: Trend quality + volume consistency ---
        # Trend quality (0-100): measures if uptrend was achieved via many small
        # positive days (good, like 恒邦股份) vs volatile swings (bad, like 神宇股份).
        # Formula: positive_day_ratio × (1 - max_drawdown_normalized)
        # max_single_loss_20d: largest single-day loss in last 20 days (absolute)
        df = df.with_columns(
            [
                # Max single-day loss in 20d (absolute, lower = better)
                pl.col("pct_chg")
                .fill_null(0.0)
                .rolling_min(window_size=20)
                .over("code", order_by="date")
                .fill_null(0.0)
                .alias("max_loss_20d"),
            ]
        )

        df = df.with_columns(
            [
                # trend_quality_20d (0-100):
                # High when: many positive days + no severe daily losses
                # 恒邦: pos_ratio ~0.65, max_loss ~-2% → quality ~80
                # 神宇: pos_ratio ~0.50, max_loss ~-5% → quality ~30
                (
                    pl.col("positive_day_ratio_20d").fill_null(0.5) * 50
                    + (1 - pl.col("max_loss_20d").abs().clip(0.0, 10.0) / 10.0) * 50
                )
                .fill_null(50.0)
                .alias("trend_quality_20d"),
            ]
        )

        # --- Phase 3: Anti-climax & accumulation indicators ---
        eps = 1e-9

        # Candle structure: close strength & upper shadow
        df = df.with_columns(
            [
                # close_strength: close near high = strong (no selling pressure)
                ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + eps))
                .fill_null(0.5)
                .clip(0.0, 1.0)
                .alias("close_strength"),
                # upper_shadow_ratio: high selling pressure = bad
                ((pl.col("high") - pl.col("close")) / (pl.col("high") - pl.col("low") + eps))
                .fill_null(0.0)
                .clip(0.0, 1.0)
                .alias("upper_shadow_ratio"),
            ]
        )

        # Volume ramp vs spike indicators
        df = df.with_columns(
            [
                # 20d volume moving average (5d already exists as volume_ma5)
                pl.col("volume")
                .cast(pl.Float64)
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .alias("vol_ma20"),
                # Volume coefficient of variation over 10d (low = gradual ramp)
                (
                    pl.col("volume")
                    .cast(pl.Float64)
                    .rolling_std(window_size=10)
                    .over("code", order_by="date")
                    / (
                        pl.col("volume")
                        .cast(pl.Float64)
                        .rolling_mean(window_size=10)
                        .over("code", order_by="date")
                        + eps
                    )
                )
                .fill_null(0.5)
                .alias("vol_cv_10d"),
                # Volume ratio vs 20d average
                (
                    pl.col("volume").cast(pl.Float64)
                    / (pl.col("volume_ma5").shift(1).over("code", order_by="date") + eps)
                )
                .fill_null(1.0)
                .alias("vol_jump_1d"),
                # Volume ramp: 5d avg / 20d avg - 1 (gradual increase > 0)
                (
                    (
                        pl.col("volume")
                        .cast(pl.Float64)
                        .rolling_mean(window_size=5)
                        .over("code", order_by="date")
                        / (
                            pl.col("volume")
                            .cast(pl.Float64)
                            .rolling_mean(window_size=20)
                            .over("code", order_by="date")
                            + eps
                        )
                    )
                    - 1
                )
                .fill_null(0.0)
                .alias("vol_ramp_5v20"),
                # Turnover ramp: 5d avg / 20d avg - 1
                (
                    (
                        pl.col("turn")
                        .fill_null(0.0)
                        .rolling_mean(window_size=5)
                        .over("code", order_by="date")
                        / (
                            pl.col("turn")
                            .fill_null(0.0)
                            .rolling_mean(window_size=20)
                            .over("code", order_by="date")
                            + eps
                        )
                    )
                    - 1
                )
                .fill_null(0.0)
                .alias("turn_ramp_5v20"),
                # Volume peak today (20d)
                (
                    pl.col("volume").cast(pl.Float64)
                    >= pl.col("volume")
                    .cast(pl.Float64)
                    .rolling_max(window_size=20)
                    .over("code", order_by="date")
                )
                .cast(pl.Float64)
                .alias("vol_peak_today_20d"),
            ]
        )

        # main_strength_proxy (0-100): quiet multi-day accumulation signal
        # Iter 3: replaced volume_ratio_5d (rewarded same-day spikes) with
        # gradual ramp + consistency + up-volume concentration + turnover ramp
        df = df.with_columns(
            [
                (
                    pl.col("vol_ramp_5v20").clip(0.0, 0.6) / 0.6 * 30
                    + (1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 0.8)) / 0.7 * 25
                    + pl.col("up_volume_ratio").fill_null(0.5).clip(0.3, 0.8) / 0.8 * 25
                    + (pl.col("turnover_change_20d").clip(-0.5, 0.5) + 0.5) * 20
                )
                .clip(0.0, 100.0)
                .alias("main_strength_proxy"),
            ]
        )

        # Iter 5: volume_spike_penalty (0-100) — penalizes stocks where today's
        # volume exploded vs 20d average (like 神宇 10x spike on 12-02/03).
        # vol/vol_ma20 > 3.0 → penalty starts; > 5.0 → max penalty
        df = df.with_columns(
            [
                (
                    (
                        (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + eps)).clip(
                            1.0, 5.0
                        )
                        - 1.0
                    )
                    / 4.0
                    * 100
                )
                .fill_null(0.0)
                .alias("volume_spike_penalty"),
                # volume_consistency_score (0-100): inverse of vol_cv_10d
                # Low CV = gradual ramp (恒邦 pattern, good)
                # High CV = explosive spikes (神宇 pattern, bad)
                # vol_cv_10d typical range: 0.1 (very consistent) to 1.0+ (explosive)
                ((1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 1.0)) / 0.9 * 100)
                .fill_null(50.0)
                .alias("volume_consistency_score"),
            ]
        )

        # Iter 5: volume_buildup_quality (0-100) — sequential accumulation signal.
        # Compares 3d-avg volume vs 10d-avg volume, penalized by today's spike.
        # A stock where 3d > 5d > 10d avg volume AND today isn't a spike = quality buildup.
        # vol_ma5 already computed above.
        df = df.with_columns(
            [
                pl.col("volume")
                .cast(pl.Float64)
                .rolling_mean(window_size=3)
                .over("code", order_by="date")
                .alias("vol_ma3"),
                pl.col("volume")
                .cast(pl.Float64)
                .rolling_mean(window_size=10)
                .over("code", order_by="date")
                .alias("vol_ma10"),
            ]
        )

        # Continuous volume_buildup_quality (Iter 10 de-ceiling)
        # Log-ratio alignment strength + acceleration + smoothness for within-regime differentiation
        _vol3 = pl.col("vol_ma3").clip(lower_bound=1.0)
        _vol5 = pl.col("volume_ma5").clip(lower_bound=1.0)
        _vol10 = pl.col("vol_ma10").clip(lower_bound=1.0)

        _r35 = (_vol3 / _vol5).log()
        _r510 = (_vol5 / _vol10).log()
        _r310 = (_vol3 / _vol10).log()

        _ratio35 = _sig(_r35 / 0.06)
        _ratio510 = _sig(_r510 / 0.06)
        _ratio310 = _sig(_r310 / 0.08)

        _vol3_lag3 = pl.col("vol_ma3").shift(3).over("code", order_by="date").clip(lower_bound=1.0)
        _accel = _sig((_vol3 / _vol3_lag3).log().fill_null(0.0) / 0.18)

        _vmean10 = (
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=10)
            .over("code", order_by="date")
            .clip(lower_bound=1.0)
        )
        _vstd10 = (
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_std(window_size=10)
            .over("code", order_by="date")
            .fill_null(0.0)
        )
        _cv10 = _vstd10 / _vmean10
        _smooth = pl.lit(1.0) - _sig((_cv10 - 1.0) / 0.35)

        _vbq_strength80 = 0.55 * (0.5 * _ratio35 + 0.5 * _ratio510) + 0.25 * _accel + 0.20 * _smooth
        _vbq_strength60 = 0.70 * _ratio310 + 0.30 * _smooth
        _vbq_strength50 = 0.70 * _ratio510 + 0.30 * _smooth
        _vbq_strength25 = _smooth

        _vbq_base = (
            pl.when(
                (pl.col("vol_ma3") > pl.col("volume_ma5"))
                & (pl.col("volume_ma5") > pl.col("vol_ma10"))
            )
            .then(74.0 + 6.0 * _vbq_strength80)
            .when(pl.col("vol_ma3") > pl.col("vol_ma10"))
            .then(56.0 + 4.0 * _vbq_strength60)
            .when(pl.col("volume_ma5") > pl.col("vol_ma10"))
            .then(47.0 + 3.0 * _vbq_strength50)
            .otherwise(22.0 + 3.0 * _vbq_strength25)
        )

        _vbq_spike = (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma3") + eps) - 1.0).clip(
            0.0, 2.0
        ) * 20.0

        df = df.with_columns(
            [
                (_vbq_base - _vbq_spike)
                .clip(0.0, 100.0)
                .fill_null(40.0)
                .alias("volume_buildup_quality"),
            ]
        )

        df = df.drop(["vol_ma3", "vol_ma10"])

        # --- Iter 6: Time-since-volume-peak factors ---
        # 12-08 failure: massive vol explosions 2-4 days before → post-spike decay.
        # 01-05 success: vol peak 6-10 days earlier → consolidated at elevated levels.
        # Existing volume_spike_penalty only checks TODAY's vol vs 20d MA, missing
        # the temporal dimension entirely.

        # Factor 1: days_since_vol_peak_20d (0-20)
        # Trading days since 20d volume maximum. Peak day = 0. Penalty when < 5.
        vol_rolling_max_20 = (
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_max(window_size=20)
            .over("code", order_by="date")
        )
        df = df.with_columns(
            [
                (pl.col("volume").cast(pl.Float64) >= vol_rolling_max_20 - eps)
                .cast(pl.Int32)
                .alias("_is_vol_peak"),
            ]
        )
        # cum_sum of peak markers creates group IDs; counting rows within each group
        # gives days since last peak (peak day itself = row 1 → subtract 1)
        df = df.with_columns(
            [pl.col("_is_vol_peak").cum_sum().over("code", order_by="date").alias("_peak_group")]
        )
        df = df.with_columns(
            [pl.col("date").rank("ordinal").over(["code", "_peak_group"]).alias("_rows_in_group")]
        )
        df = df.with_columns(
            [
                (pl.col("_rows_in_group") - 1)
                .clip(0, 20)
                .cast(pl.Float64)
                .alias("days_since_vol_peak_20d"),
            ]
        )
        df = df.drop(["_is_vol_peak", "_peak_group", "_rows_in_group"])

        # Factor 2: recent_vol_spike_max (0-100)
        # Max(vol/vol_ma20) over last 5 days. Captures explosions in IMMEDIATE past
        # even if today's volume subsided. Normalized: ratio 1.0→0, 5.0+→100.
        # 12-08 picks ~8-13x (spike 2-4 days ago); 01-05 picks ~2-4x (spike >5 days ago)
        df = df.with_columns(
            [
                (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + eps))
                .fill_null(1.0)
                .alias("_vol_ratio_today"),
            ]
        )
        df = df.with_columns(
            [
                pl.col("_vol_ratio_today")
                .rolling_max(window_size=5)
                .over("code", order_by="date")
                .fill_null(1.0)
                .alias("_recent_max_vol_ratio"),
            ]
        )
        df = df.with_columns(
            [
                ((pl.col("_recent_max_vol_ratio").clip(1.0, 5.0) - 1.0) / 4.0 * 100)
                .fill_null(0.0)
                .alias("recent_vol_spike_max"),
            ]
        )
        df = df.drop(["_vol_ratio_today", "_recent_max_vol_ratio"])

        # Factor 3: post_spike_consolidation (0-100)
        # Rewards 01-05 pattern: vol subsided + enough time since peak + no recent mega-spike.
        # A: temporal distance (0-40pts), B: vol calm-down (0-30pts), C: no recent spike (0-30pts)
        df = df.with_columns(
            [
                (
                    (pl.col("days_since_vol_peak_20d").clip(0.0, 10.0) / 10.0 * 40)
                    + (
                        (
                            1.0
                            - (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + eps)).clip(
                                0.5, 2.0
                            )
                            / 2.0
                        )
                        * 30
                    ).fill_null(15.0)
                    + ((1.0 - pl.col("recent_vol_spike_max") / 100.0) * 30)
                )
                .clip(0.0, 100.0)
                .fill_null(50.0)
                .alias("post_spike_consolidation"),
            ]
        )

        # --- Iter 8: Resistance proximity, momentum quality, price range position ---
        # Qualitative analysis of failed vs successful recs revealed:
        # 1. Stocks at 20d ceiling after sharp bounce with expanding volume = trap
        # 2. Concentrated recent momentum (5d >> 10d) = sprint exhaustion
        # 3. Smart tab picks declining/flat stocks with nice volume = no catalyst
        #
        # See: .sisyphus/research/iteration-8-resistance-momentum-quality.md

        # 20d high and low for range position
        df = df.with_columns(
            [
                pl.col("high")
                .rolling_max(window_size=20)
                .over("code", order_by="date")
                .alias("high_20d"),
                pl.col("low")
                .rolling_min(window_size=20)
                .over("code", order_by="date")
                .alias("low_20d"),
            ]
        )

        # Factor 1: price_range_position_20d (0-100)
        # Where the close sits in the 20-day high-low range.
        # Failed stocks on 12-22: avg ~77-93% (at ceiling). Success: ~30-73% (room to expand).
        # Unlike price_position_60d which uses 60d range, this captures SHORT-TERM ceiling.
        df = df.with_columns(
            [
                (
                    (pl.col("close") - pl.col("low_20d"))
                    / (pl.col("high_20d") - pl.col("low_20d") + eps)
                )
                .fill_null(0.5)
                .clip(0.0, 1.0)
                .alias("price_range_position_20d"),
            ]
        )

        # Factor 2: momentum_quality_ratio (0-100)
        # Compares 5d return vs 10d return to detect sprint exhaustion.
        # 5d and 10d returns computed from close prices (no look-ahead).
        # When 5d_ret >> 10d_ret: all momentum concentrated in recent days → exhaustion → low score.
        # When 5d_ret ≤ 10d_ret: sustained trend → high score.
        # Formula: ratio = 10d_ret / (5d_ret + eps). Clamped and normalized.
        #
        # 恩捷 (01-19 failure): momentum_acceleration = 12.2 → almost all gains in last 5 days.
        # 恒邦 (01-20 success): gradual climb over 10+ days, 5d_ret ≈ 10d_ret.
        df = df.with_columns(
            [
                pl.col("close").shift(5).over("code", order_by="date").alias("_close_5d_ago"),
                pl.col("close").shift(10).over("code", order_by="date").alias("_close_10d_ago"),
            ]
        )
        df = df.with_columns(
            [
                ((pl.col("close") / (pl.col("_close_5d_ago") + eps) - 1) * 100)
                .fill_null(0.0)
                .alias("_ret_5d"),
                ((pl.col("close") / (pl.col("_close_10d_ago") + eps) - 1) * 100)
                .fill_null(0.0)
                .alias("_ret_10d"),
            ]
        )
        # momentum_quality: high when momentum is spread over 10 days (not concentrated in 5).
        # If 5d_ret is positive and > 10d_ret → concentration penalty.
        # If 5d_ret is positive and ≤ 10d_ret → sustained trend, reward.
        # If 5d_ret is negative → no momentum quality issue (different problem).
        df = df.with_columns(
            [
                (
                    pl.when(pl.col("_ret_5d") > 1.0)  # Only evaluate when there IS momentum
                    .then(
                        pl.when(pl.col("_ret_10d") > pl.col("_ret_5d"))
                        # 10d > 5d: sustained trend. Score scales with how spread out the gains are.
                        # 10d_ret/5d_ret > 2 → very spread → 100. Around 1 → 60.
                        .then(
                            (pl.col("_ret_10d") / (pl.col("_ret_5d") + eps))
                            .clip(1.0, 3.0)
                            .alias("_ratio")
                            * 100
                            / 3.0
                        )
                        .otherwise(
                            # 5d > 10d: concentrated momentum (sprint).
                            # Score: lower when 5d/10d ratio is higher.
                            # 5d_ret/10d_ret = 1 → 60. = 2 → 30. >= 3 → 0.
                            (
                                1.0
                                - (pl.col("_ret_5d") / (pl.col("_ret_10d").abs() + eps) - 1.0).clip(
                                    0.0, 2.0
                                )
                                / 2.0
                            )
                            * 60.0
                        )
                    )
                    .otherwise(pl.lit(50.0))  # No upward momentum → neutral
                )
                .fill_null(50.0)
                .clip(0.0, 100.0)
                .alias("momentum_quality_ratio"),
            ]
        )

        # Factor 3: resistance_proximity_penalty (0-100)
        # Two trap patterns detected:
        # A) Surge into resistance: at 20d ceiling + strong 5d return + expanding volume
        # B) Stall at resistance: at 20d ceiling + strong 5d return + today's gain near zero
        #    12-08 losers: pos=86-97%, ret_5d=2-4%, today_chg=0.0-1.0% → exhaustion
        #    This pattern was missed when volume is declining (post-peak)
        ceiling_proximity = (pl.col("price_range_position_20d") - 0.85).clip(0.0, 0.15) / 0.15
        ret_5d_factor = pl.col("_ret_5d").clip(0.0, 8.0) / 8.0

        vol_expansion = (
            pl.col("vol_ramp_5v20").clip(0.0, 1.0)
            + (pl.col("vol_jump_1d").clip(1.0, 3.0) - 1.0) / 2.0
        ).clip(0.0, 1.0)

        # B: Stall signal — today's gain is small relative to 5d return
        # When _ret_5d > 2% but today_chg < 1%, momentum has stalled at the ceiling
        stall_signal = (
            pl.when(pl.col("_ret_5d") > 2.0)
            .then((1.0 - pl.col("pct_chg").fill_null(0.0).clip(0.0, 2.0) / 2.0))
            .otherwise(pl.lit(0.0))
        )

        df = df.with_columns(
            [
                (
                    ceiling_proximity
                    * ret_5d_factor
                    * (vol_expansion + stall_signal).clip(0.0, 1.5)
                    * 100.0
                )
                .fill_null(0.0)
                .clip(0.0, 100.0)
                .alias("resistance_proximity_penalty"),
            ]
        )

        # Factor 4: exhaustion_at_ceiling (0-100, for panorama tab)
        # More targeted than resistance_proximity_penalty — requires THREE simultaneous signals:
        # 1. Price near 20d ceiling (pos > 0.80)
        # 2. Prior 5d uptrend (ret_5d > 1.5%)
        # 3. Today's momentum has stalled (pct_chg < 1.0%)
        # This catches the 12-08 pattern: stocks that ran up 2-4% over 5d, reached ceiling,
        # but today barely moved — exhaustion/distribution. Unlike resistance_proximity_penalty,
        # this does NOT penalize stocks with strong today's momentum (genuine breakouts).
        ceiling_signal = (pl.col("price_range_position_20d") - 0.80).clip(0.0, 0.20) / 0.20
        prior_uptrend = (pl.col("_ret_5d") - 1.5).clip(0.0, 6.5) / 6.5
        # Stall: today's gain is small. Max penalty when pct_chg <= 0, tapers to 0 at pct_chg >= 1.5
        momentum_stall = (1.5 - pl.col("pct_chg").fill_null(0.0).clip(0.0, 1.5)) / 1.5

        df = df.with_columns(
            [
                (ceiling_signal * prior_uptrend * momentum_stall * 100.0)
                .fill_null(0.0)
                .clip(0.0, 100.0)
                .alias("exhaustion_at_ceiling"),
            ]
        )

        # Factor 5: late_stage_stall — REVERTED (Iter 10)
        # Tested 3 versions: v1 (full multiplicative) too narrow (2/40 non-zero),
        # v2 (soft additive) regressed WR 70→60%, v3 (pairwise multiplicative) no effect.
        # Column kept at 0.0 for schema compatibility via _ensure_columns default.

        df = df.with_columns(
            [
                pl.col("_ret_5d").fill_null(0.0).alias("return_5d"),
            ]
        )

        df = df.drop(
            ["_close_5d_ago", "_close_10d_ago", "_ret_5d", "_ret_10d", "high_20d", "low_20d"]
        )

        # Limit-up / near-limit detection
        is_20pct = pl.col("code").str.contains(r"^(sz\.(300|301)|sh\.688)")
        limit_threshold = pl.when(is_20pct).then(pl.lit(19.0)).otherwise(pl.lit(9.5))

        df = df.with_columns(
            [
                (pl.col("pct_chg").fill_null(0.0) >= limit_threshold).alias("near_limit_up"),
            ]
        )

        # Composite scores: accumulation (good) and climax (bad)
        df = df.with_columns(
            [
                # Accumulation score: gradual volume/turnover ramp + strong close + up-volume
                (
                    pl.col("vol_ramp_5v20").clip(-0.2, 1.0) * 0.30
                    + pl.col("turn_ramp_5v20").clip(-0.2, 1.0) * 0.30
                    + pl.col("close_strength").clip(0.0, 1.0) * 0.20
                    + pl.col("up_volume_ratio").fill_null(0.5).clip(0.0, 1.0) * 0.20
                )
                .fill_null(0.0)
                .alias("accumulation_score"),
                # Climax score: big daily gain + volume spike + at 60d high + upper shadow + vol peak
                (
                    (pl.col("pct_chg").fill_null(0.0).clip(0.0, 20.0) / 10.0) * 0.30
                    + (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + eps))
                    .clip(0.0, 5.0)
                    .fill_null(1.0)
                    / 5.0
                    * 0.25
                    + pl.col("price_position_60d").fill_null(0.5).clip(0.0, 1.0) * 0.20
                    + pl.col("upper_shadow_ratio").fill_null(0.0).clip(0.0, 1.0) * 0.15
                    + pl.col("vol_peak_today_20d").fill_null(0.0) * 0.10
                )
                .fill_null(0.0)
                .alias("climax_score"),
            ]
        )

        # Drop intermediate columns to keep DataFrame clean
        df = df.drop(
            ["up_volume_20d", "total_volume_20d", "ma_10", "ma_20", "vol_ma20", "max_loss_20d"]
        )

        return df

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
