"""Sector Rotation service for Alpha Radar.

Provides industry rotation matrix for multi-day analysis.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.polars_engine import PolarsEngine
from app.api.v1.alpha_radar import (
    SectorRotationResponse,
    SectorRotationColumn,
    SectorDayCell,
    SectorRotationStats,
    RotationTopStock,
    RotationCellSignal,
    CellSignalType,
    RotationSortBy,
)


# 行业产业链顺序（上游 → 中游 → 下游）
# 申万一级行业31个（使用数据库实际名称）
INDUSTRY_CHAIN_ORDER: Dict[str, int] = {
    # 上游资源 (1-10)
    "煤炭": 1,
    "石油石化": 2,
    "钢铁": 3,
    "有色金属": 4,
    "基础化工": 5,
    "建筑材料": 6,
    # 中游制造 (11-20)
    "机械设备": 11,
    "电力设备": 12,
    "国防军工": 13,
    "电子": 14,
    "计算机": 15,
    "通信": 16,
    "汽车": 17,
    "家用电器": 18,
    "轻工制造": 19,
    "纺织服饰": 20,
    # 下游消费 (21-30)
    "食品饮料": 21,
    "医药生物": 22,
    "农林牧渔": 23,
    "商贸零售": 24,
    "社会服务": 25,
    "美容护理": 26,
    "传媒": 27,
    # 公用基建 (31-40)
    "公用事业": 31,
    "交通运输": 32,
    "建筑装饰": 33,
    "环保": 34,
    "房地产": 35,
    # 金融 (41-50)
    "银行": 41,
    "非银金融": 42,
    "综合": 50,
}


class SectorRotationService:
    """
    Service for generating sector rotation matrix data.

    Creates a date × industry matrix for analyzing sector rotation patterns.
    Supports multiple sorting modes and algorithm signals.
    """

    # Days for calculating industry volume baseline
    VOLUME_BASELINE_DAYS = 120

    # Shanghai Composite Index code
    MARKET_INDEX_CODE = "sh.000001"

    def __init__(self, db: AsyncSession):
        self.db = db
        self.polars_engine = PolarsEngine(db)

    async def _get_market_changes(
        self, trading_days: List[date]
    ) -> Dict[str, Decimal]:
        """
        Get market (上证指数) daily changes for the given trading days.

        Returns:
            Dict mapping date string (YYYY-MM-DD) to change percent
        """
        if not trading_days:
            return {}

        # Query market index data
        result = await self.db.execute(
            text("""
                SELECT date, pct_chg
                FROM market_daily
                WHERE code = :code
                AND date = ANY(:dates)
            """),
            {"code": self.MARKET_INDEX_CODE, "dates": trading_days}
        )
        rows = result.fetchall()

        market_changes = {}
        for row in rows:
            day, pct_chg = row
            if pct_chg is not None:
                market_changes[day.isoformat()] = self._to_decimal(pct_chg) or Decimal("0")
            else:
                market_changes[day.isoformat()] = Decimal("0")

        return market_changes

    async def _get_industry_volume_baselines(
        self, end_date: date
    ) -> Dict[str, Decimal]:
        """
        Get 120-day average volume for each industry.

        Returns:
            Dict mapping industry name to average daily volume (in 亿)
        """
        # Calculate date range for baseline
        result = await self.db.execute(
            text("""
                SELECT DISTINCT date FROM market_daily
                WHERE date <= :end_date
                ORDER BY date DESC
                LIMIT :days
            """),
            {"end_date": end_date, "days": self.VOLUME_BASELINE_DAYS}
        )
        dates = [row[0] for row in result.fetchall()]
        if not dates:
            return {}

        start_date = dates[-1]

        # Query average DAILY TOTAL volume per industry over the baseline period
        # First sum by date+industry (daily total), then average across days
        result = await self.db.execute(
            text("""
                WITH daily_totals AS (
                    SELECT
                        sp.sw_industry_l1,
                        md.date,
                        SUM(md.amount) as daily_amount
                    FROM market_daily md
                    JOIN stock_profile sp ON md.code = sp.code
                    WHERE md.date >= :start_date
                    AND md.date <= :end_date
                    AND sp.sw_industry_l1 IS NOT NULL
                    GROUP BY sp.sw_industry_l1, md.date
                )
                SELECT
                    sw_industry_l1,
                    AVG(daily_amount) as avg_daily_amount
                FROM daily_totals
                GROUP BY sw_industry_l1
            """),
            {"start_date": start_date, "end_date": end_date}
        )
        rows = result.fetchall()

        baselines = {}
        for row in rows:
            industry, avg_amount = row
            if avg_amount is not None:
                # Convert to 亿 (amount is in yuan)
                baselines[industry] = self._to_decimal(float(avg_amount) / 1e8) or Decimal("0")
            else:
                baselines[industry] = Decimal("0")

        return baselines

    async def get_rotation_matrix(
        self,
        days: int = 60,
        end_date: Optional[date] = None,
        sort_by: str = "today_change",
    ) -> SectorRotationResponse:
        """
        Get sector rotation matrix data.

        Args:
            days: Number of trading days to include (5-120)
            end_date: End date (default: latest trading day)
            sort_by: Sort method (today_change, period_change, money_flow, momentum)

        Returns:
            SectorRotationResponse with matrix data
        """
        # Resolve end date
        if end_date is None:
            end_date = await self.polars_engine.get_latest_trading_date()
        if end_date is None:
            return self._empty_response(sort_by, days)

        # Calculate start date (approximate, will be refined by actual trading days)
        start_date = end_date - timedelta(days=days * 2)  # Buffer for non-trading days

        # Load market data for the period
        df = await self.polars_engine.load_market_data(
            start_date=start_date,
            end_date=end_date,
            lookback_days=70,  # Extra lookback for 60-day technical indicator calculations
        )

        if df.is_empty():
            return self._empty_response(sort_by, days)

        # Calculate technical indicators FIRST (needs historical data for rolling windows)
        df = self.polars_engine.calculate_technical_indicators(df)

        # Load stock profiles for industry classification
        profile_df = await self.polars_engine.load_stock_profiles()
        if profile_df.is_empty():
            return self._empty_response(sort_by, days)

        # Join with profiles to get industry classification
        df = df.join(profile_df, on="code", how="left")
        df = df.filter(pl.col("sw_industry_l1").is_not_null())

        if df.is_empty():
            return self._empty_response(sort_by, days)

        # Mark limit-up stocks for 涨停榜 and 龙头榜 (needs name for ST detection)
        df = self._mark_limit_up(df)

        # Load valuation data for market cap (for 龙头战法 filtering)
        valuation_df = await self.polars_engine.load_valuation_data(end_date)
        if not valuation_df.is_empty():
            df = df.join(
                valuation_df.select(["code", "total_mv"]),
                on="code",
                how="left",
            )
        else:
            # Add empty total_mv column if no valuation data
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("total_mv"))

        # Get unique trading days (most recent N days)
        trading_days = (
            df.select("date")
            .unique()
            .sort("date", descending=True)
            .head(days)
            .to_series()
            .to_list()
        )

        if not trading_days:
            return self._empty_response(sort_by, days)

        # Filter data to only include the selected trading days (AFTER technical indicators)
        df = df.filter(pl.col("date").is_in(trading_days))

        # Aggregate by date and industry L1
        matrix_df = self._compute_industry_matrix(df)

        # Calculate algorithm signals
        matrix_df = self._compute_signals(matrix_df, trading_days)

        # Get top stock per cell
        top_stocks_df = self._compute_top_stocks(df)

        # Get limit-up stocks list per cell (for 涨停榜)
        limit_up_stocks = self._compute_limit_up_stocks(df)

        # Get dragon stocks per cell (for 龙头榜)
        dragon_stocks_df = self._compute_dragon_stocks(df, matrix_df)

        # Get market changes for weighted calculations
        market_changes = await self._get_market_changes(trading_days)

        # Get industry volume baselines
        volume_baselines = await self._get_industry_volume_baselines(end_date)

        # Build response structure
        industries = self._build_industry_columns(
            matrix_df, top_stocks_df, trading_days, sort_by, volume_baselines,
            limit_up_stocks, dragon_stocks_df
        )

        # Calculate stats
        stats = self._compute_stats(matrix_df, industries, trading_days)

        return SectorRotationResponse(
            trading_days=sorted(trading_days, reverse=True),  # T-day first
            industries=industries,
            stats=stats,
            sort_by=RotationSortBy(sort_by),
            days=len(trading_days),
            market_changes=market_changes,
        )

    def _mark_limit_up(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Mark limit-up stocks with board-specific thresholds.

        Rules:
        - STAR Market (sh.688xxx) / ChiNext (sz.300xxx, sz.301xxx): >= 19.8%
        - ST stocks (name contains ST): >= 4.9%
        - Main board: >= 9.9%

        Note: code format is "sh.688xxx" or "sz.300xxx", so we use contains()
        """
        return df.with_columns([
            pl.when(
                # STAR Market (sh.688xxx) / ChiNext (sz.300xxx, sz.301xxx): 20%
                pl.col("code").str.contains("688") |
                pl.col("code").str.contains(r"\.30[01]")  # matches .300 or .301
            ).then(pl.col("pct_chg") >= 19.8)
            .when(
                # ST stocks (name contains ST): 5%
                pl.col("name").str.contains("ST")
            ).then(pl.col("pct_chg") >= 4.9)
            .otherwise(
                # Main board: 10%
                pl.col("pct_chg") >= 9.9
            ).alias("is_limit_up"),

            # Is one-line board (can't buy in - amplitude < 0.5%)
            ((pl.col("high") - pl.col("low")) / pl.col("preclose") * 100 < 0.5)
            .alias("is_one_line"),
        ])

    def _compute_industry_matrix(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate data by date × industry."""
        return df.group_by(["date", "sw_industry_l1"]).agg([
            # Weighted average change (by market cap if available, else simple avg)
            pl.col("pct_chg").mean().alias("change_pct"),
            # Total amount as money flow proxy
            pl.col("amount").sum().alias("total_amount"),
            # Main strength proxy average
            pl.col("main_strength_proxy").mean().alias("main_strength"),
            # Count for debugging
            pl.len().alias("stock_count"),
            # 涨停榜: 涨停股票数量
            pl.col("is_limit_up").sum().cast(pl.Int32).alias("limit_up_count"),
            # 龙头筛选: 行业成交额中位数 (用于放量判断)
            pl.col("amount").median().alias("amount_median"),
        ])

    def _compute_signals(
        self, matrix_df: pl.DataFrame, trading_days: List[date]
    ) -> pl.DataFrame:
        """
        Compute algorithm signals for each cell.

        Signals:
        - momentum: 5-day up streak, cumulative >10%
        - reversal: 10-day cumulative <-15%, today >3%
        - divergence: price flat but large money inflow
        """
        # Sort by industry and date for window functions
        matrix_df = matrix_df.sort(["sw_industry_l1", "date"])

        # Calculate rolling metrics per industry
        matrix_df = matrix_df.with_columns([
            # 5-day cumulative change
            pl.col("change_pct")
            .rolling_sum(5, min_periods=1)
            .over("sw_industry_l1")
            .alias("change_5d"),

            # 10-day cumulative change
            pl.col("change_pct")
            .rolling_sum(10, min_periods=1)
            .over("sw_industry_l1")
            .alias("change_10d"),

            # 5-day up day count
            (pl.col("change_pct") > 0)
            .cast(pl.Int32)
            .rolling_sum(5, min_periods=1)
            .over("sw_industry_l1")
            .alias("up_days_5d"),

            # 5-day average amount
            pl.col("total_amount")
            .rolling_mean(5, min_periods=1)
            .over("sw_industry_l1")
            .alias("avg_amount_5d"),
        ])

        # Signal flags
        matrix_df = matrix_df.with_columns([
            # Momentum: 4+ up days in 5, cumulative >8%
            ((pl.col("up_days_5d") >= 4) & (pl.col("change_5d") > 8))
            .alias("signal_momentum"),

            # Reversal: 10d drop >12%, today up >2.5%
            ((pl.col("change_10d") < -12) & (pl.col("change_pct") > 2.5))
            .alias("signal_reversal"),

            # Divergence: price flat (-1% to 1% over 5d) but amount surge (>1.5x avg)
            (
                (pl.col("change_5d").abs() < 3) &
                (pl.col("total_amount") > pl.col("avg_amount_5d") * 1.5)
            )
            .alias("signal_divergence"),
        ])

        return matrix_df

    def _compute_top_stocks(self, df: pl.DataFrame) -> pl.DataFrame:
        """Get top performing stock per date × industry."""
        return (
            df.sort(["date", "sw_industry_l1", "pct_chg"], descending=[False, False, True])
            .group_by(["date", "sw_industry_l1"])
            .first()
            .select([
                "date",
                "sw_industry_l1",
                pl.col("code").alias("top_code"),
                pl.col("name").alias("top_name"),
                pl.col("pct_chg").alias("top_change"),
            ])
        )

    def _compute_limit_up_stocks(self, df: pl.DataFrame) -> Dict[str, List[RotationTopStock]]:
        """
        Get list of limit-up stocks per date × industry.

        Returns:
            Dict mapping "date|industry" key to list of RotationTopStock
        """
        limit_up_df = (
            df.filter(pl.col("is_limit_up"))
            .sort(["date", "sw_industry_l1", "pct_chg"], descending=[False, False, True])
            .select(["date", "sw_industry_l1", "code", "name", "pct_chg"])
        )

        result: Dict[str, List[RotationTopStock]] = {}

        for row in limit_up_df.iter_rows(named=True):
            key = f"{row['date']}|{row['sw_industry_l1']}"
            if key not in result:
                result[key] = []
            result[key].append(RotationTopStock(
                code=row["code"],
                name=row["name"],
                change_pct=self._to_decimal(row["pct_chg"]) or Decimal("0"),
            ))

        return result

    def _compute_dragon_stocks(
        self,
        df: pl.DataFrame,
        matrix_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Compute dragon stocks per date × industry using 龙头战法 criteria.

        Dragon stock must meet ALL conditions:
        1. is_limit_up: Must be limit-up
        2. Not one-line board (amplitude > 0.5%)
        3. High volume: amount > industry median for the day
        4. Turnover rate: 3% <= turn <= 25%
        5. Market cap: 30B <= total_mv <= 1000B

        Sorted by amount (highest first), take top 1 per cell.
        If no stock meets criteria, cell has no dragon (宁缺毋滥).
        """
        # Join with matrix to get industry amount median
        df_with_median = df.join(
            matrix_df.select(["date", "sw_industry_l1", "amount_median"]),
            on=["date", "sw_industry_l1"],
            how="left",
        )

        # Filter by dragon criteria
        # Note: total_mv is in 亿 (hundred million) units in the database
        dragon_candidates = df_with_median.filter(
            pl.col("is_limit_up") &                              # Must be limit-up
            ~pl.col("is_one_line") &                             # Not one-line board
            (pl.col("amount") > pl.col("amount_median")) &       # High volume
            (pl.col("turn") >= 3) & (pl.col("turn") <= 25) &     # Turnover 3%-25%
            (pl.col("total_mv") >= 30) &                         # Market cap >= 30亿
            (pl.col("total_mv") <= 1000)                         # Market cap <= 1000亿
        )

        if dragon_candidates.is_empty():
            return pl.DataFrame(schema={
                "date": pl.Date,
                "sw_industry_l1": pl.Utf8,
                "dragon_code": pl.Utf8,
                "dragon_name": pl.Utf8,
                "dragon_change": pl.Float64,
            })

        # Sort by amount (highest first) and take top 1 per cell
        return (
            dragon_candidates
            .sort(["date", "sw_industry_l1", "amount"], descending=[False, False, True])
            .group_by(["date", "sw_industry_l1"])
            .first()
            .select([
                "date",
                "sw_industry_l1",
                pl.col("code").alias("dragon_code"),
                pl.col("name").alias("dragon_name"),
                pl.col("pct_chg").alias("dragon_change"),
            ])
        )

    def _build_industry_columns(
        self,
        matrix_df: pl.DataFrame,
        top_stocks_df: pl.DataFrame,
        trading_days: List[date],
        sort_by: str,
        volume_baselines: Optional[Dict[str, Decimal]] = None,
        limit_up_stocks: Optional[Dict[str, List[RotationTopStock]]] = None,
        dragon_stocks_df: Optional[pl.DataFrame] = None,
    ) -> List[SectorRotationColumn]:
        """Build industry columns with cells and sorting metrics."""
        volume_baselines = volume_baselines or {}
        limit_up_stocks = limit_up_stocks or {}
        # Get unique industries
        industries = matrix_df.select("sw_industry_l1").unique().to_series().to_list()

        # Join matrix with top stocks
        matrix_df = matrix_df.join(
            top_stocks_df,
            on=["date", "sw_industry_l1"],
            how="left",
        )

        # Join matrix with dragon stocks
        if dragon_stocks_df is not None and not dragon_stocks_df.is_empty():
            matrix_df = matrix_df.join(
                dragon_stocks_df,
                on=["date", "sw_industry_l1"],
                how="left",
            )

        # Today is the most recent trading day
        today = max(trading_days)

        columns = []
        for industry in industries:
            industry_data = matrix_df.filter(pl.col("sw_industry_l1") == industry)

            # Build cells for each trading day
            cells = []
            for day in trading_days:
                day_data = industry_data.filter(pl.col("date") == day)

                if day_data.is_empty():
                    # No data for this day
                    cells.append(SectorDayCell(
                        date=day,
                        change_pct=Decimal("0"),
                        money_flow=None,
                        main_strength=None,
                        top_stock=None,
                        signals=[],
                        limit_up_count=0,
                        limit_up_stocks=[],
                        dragon_stock=None,
                    ))
                else:
                    row = day_data.row(0, named=True)

                    # Build signals list
                    signals = []
                    if row.get("signal_momentum"):
                        signals.append(RotationCellSignal(
                            type=CellSignalType.MOMENTUM,
                            label="主线🔥",
                        ))
                    if row.get("signal_reversal"):
                        signals.append(RotationCellSignal(
                            type=CellSignalType.REVERSAL,
                            label="反转⚡️",
                        ))
                    if row.get("signal_divergence"):
                        signals.append(RotationCellSignal(
                            type=CellSignalType.DIVERGENCE,
                            label="资金背离",
                        ))

                    # Build top stock
                    top_stock = None
                    if row.get("top_code"):
                        top_stock = RotationTopStock(
                            code=row["top_code"],
                            name=row.get("top_name") or row["top_code"],
                            change_pct=self._to_decimal(row.get("top_change")) or Decimal("0"),
                        )

                    # Build dragon stock (龙头战法筛选)
                    dragon_stock = None
                    if row.get("dragon_code"):
                        dragon_stock = RotationTopStock(
                            code=row["dragon_code"],
                            name=row.get("dragon_name") or row["dragon_code"],
                            change_pct=self._to_decimal(row.get("dragon_change")) or Decimal("0"),
                        )

                    # Get limit-up stocks for this cell
                    cell_key = f"{day}|{industry}"
                    cell_limit_up_stocks = limit_up_stocks.get(cell_key, [])

                    cells.append(SectorDayCell(
                        date=day,
                        change_pct=self._to_decimal(row.get("change_pct")) or Decimal("0"),
                        money_flow=self._to_decimal(row.get("total_amount")),
                        main_strength=self._to_decimal(row.get("main_strength")),
                        top_stock=top_stock,
                        signals=signals,
                        limit_up_count=int(row.get("limit_up_count") or 0),
                        limit_up_stocks=cell_limit_up_stocks,
                        dragon_stock=dragon_stock,
                    ))

            # Calculate sorting metrics
            today_data = industry_data.filter(pl.col("date") == today)
            today_change = Decimal("0")
            if not today_data.is_empty():
                today_change = self._to_decimal(
                    today_data.select("change_pct").item()
                ) or Decimal("0")

            # Period change (5-day cumulative)
            period_change = Decimal("0")
            if not today_data.is_empty():
                period_change = self._to_decimal(
                    today_data.select("change_5d").item()
                ) or Decimal("0")

            # Total flow (sum of recent 5 days)
            recent_5_days = sorted(trading_days, reverse=True)[:5]
            recent_data = industry_data.filter(pl.col("date").is_in(recent_5_days))
            total_flow = Decimal("0")
            if not recent_data.is_empty():
                total_flow = self._to_decimal(
                    recent_data.select("total_amount").sum().item()
                ) or Decimal("0")

            # Momentum score (weighted by recency)
            momentum_score = self._calculate_momentum_score(industry_data, trading_days)

            # Get volume baseline for this industry
            industry_volume_baseline = volume_baselines.get(industry)

            columns.append(SectorRotationColumn(
                name=industry,
                code=industry,  # Using name as code for SW industries
                cells=sorted(cells, key=lambda c: c.date, reverse=True),  # T-day first
                today_change=today_change,
                period_change=period_change,
                total_flow=total_flow,
                momentum_score=momentum_score,
                volume_baseline=industry_volume_baseline,
            ))

        # Always return in upstream order (industry chain order)
        # Frontend handles all sorting logic
        columns.sort(key=lambda c: INDUSTRY_CHAIN_ORDER.get(c.name, 99))

        return columns

    def _calculate_momentum_score(
        self, industry_data: pl.DataFrame, trading_days: List[date]
    ) -> Decimal:
        """
        Calculate momentum score with recency weighting.

        Score = sum(daily_change * recency_weight)
        More recent days have higher weight.
        """
        if industry_data.is_empty():
            return Decimal("0")

        total_score = 0.0
        total_weight = 0.0

        for i, day in enumerate(sorted(trading_days, reverse=True)):
            day_data = industry_data.filter(pl.col("date") == day)
            if not day_data.is_empty():
                change = day_data.select("change_pct").item() or 0
                weight = 1.0 / (i + 1)  # Recent days have higher weight
                total_score += change * weight
                total_weight += weight

        if total_weight > 0:
            return self._to_decimal(total_score / total_weight * 10) or Decimal("0")
        return Decimal("0")

    def _compute_stats(
        self,
        matrix_df: pl.DataFrame,
        industries: List[SectorRotationColumn],
        trading_days: List[date],
    ) -> SectorRotationStats:
        """Compute overall statistics for the matrix."""
        all_changes = matrix_df.select("change_pct").to_series().to_list()
        all_changes = [c for c in all_changes if c is not None]

        avg_change = sum(all_changes) / len(all_changes) if all_changes else 0
        max_change = max(all_changes) if all_changes else 0
        min_change = min(all_changes) if all_changes else 0

        # Hot/cold industries based on period change
        sorted_by_momentum = sorted(
            industries, key=lambda c: float(c.period_change), reverse=True
        )
        hot_industries = [c.name for c in sorted_by_momentum[:5]]
        cold_industries = [c.name for c in sorted_by_momentum[-5:]]

        return SectorRotationStats(
            total_industries=len(industries),
            trading_days=len(trading_days),
            avg_change=self._to_decimal(avg_change) or Decimal("0"),
            max_change=self._to_decimal(max_change) or Decimal("0"),
            min_change=self._to_decimal(min_change) or Decimal("0"),
            hot_industries=hot_industries,
            cold_industries=cold_industries,
        )

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        """Convert value to Decimal, handling None and NaN."""
        if value is None:
            return None
        try:
            if isinstance(value, float):
                import math
                if math.isnan(value) or math.isinf(value):
                    return None
            return Decimal(str(round(float(value), 4)))
        except (ValueError, TypeError):
            return None

    def _empty_response(self, sort_by: str, days: int) -> SectorRotationResponse:
        """Return empty response structure."""
        return SectorRotationResponse(
            trading_days=[],
            industries=[],
            stats=SectorRotationStats(
                total_industries=0,
                trading_days=0,
                avg_change=Decimal("0"),
                max_change=Decimal("0"),
                min_change=Decimal("0"),
                hot_industries=[],
                cold_industries=[],
            ),
            sort_by=RotationSortBy(sort_by),
            days=days,
            market_changes={},
        )
