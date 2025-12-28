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
        result = await self.db.execute(
            text("SELECT MAX(date) FROM market_daily")
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None

    async def get_date_range(self) -> tuple[Optional[date], Optional[date]]:
        """Get available date range from market_daily."""
        result = await self.db.execute(
            text("SELECT MIN(date), MAX(date) FROM market_daily")
        )
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
                {"target_date": target_date, "lookback": lookback_days}
            )
            dates = [row[0] for row in result.fetchall()]
            calc_start_date = dates[-1] if dates else target_date

            result = await self.db.execute(
                query,
                {"target_date": target_date, "start_date": calc_start_date}
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
                {"start_date": start_date, "lookback": lookback_days}
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
                query,
                {"start_date": calc_start_date, "end_date": end_date}
            )

        rows = result.fetchall()
        columns = result.keys()

        # Convert to Polars DataFrame
        if not rows:
            return pl.DataFrame()

        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        df = pl.DataFrame(data)

        # Convert types
        df = df.with_columns([
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
        ])

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
                volatility_20d,
                vol_category,
                avg_turnover_20d,
                turnover_category,
                value_category,
                momentum_20d,
                momentum_60d
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
        df = df.with_columns([
            # 5-day average volume
            pl.col("volume")
            .rolling_mean(window_size=5)
            .over("code")
            .alias("volume_ma5"),

            # 20-day average turnover
            pl.col("turn")
            .rolling_mean(window_size=20)
            .over("code")
            .alias("turnover_ma20"),

            # 5-day average turnover
            pl.col("turn")
            .rolling_mean(window_size=5)
            .over("code")
            .alias("turnover_ma5"),

            # 60-day high
            pl.col("high")
            .rolling_max(window_size=60)
            .over("code")
            .alias("high_60d"),

            # 60-day low
            pl.col("low")
            .rolling_min(window_size=60)
            .over("code")
            .alias("low_60d"),
        ])

        # Calculate derived indicators
        df = df.with_columns([
            # Volume ratio (current / 5d avg)
            (pl.col("volume") / pl.col("volume_ma5").shift(1).over("code"))
            .fill_null(1.0)
            .alias("volume_ratio_5d"),

            # Turnover change (5d avg / 20d avg - 1)
            ((pl.col("turnover_ma5") / pl.col("turnover_ma20")) - 1)
            .fill_null(0.0)
            .alias("turnover_change_20d"),

            # Price position (0-1)
            ((pl.col("close") - pl.col("low_60d")) /
             (pl.col("high_60d") - pl.col("low_60d")))
            .fill_null(0.5)
            .clip(0.0, 1.0)
            .alias("price_position_60d"),
        ])

        # Calculate main strength proxy (0-100)
        df = df.with_columns([
            (
                # Volume ratio contribution (max 60)
                pl.col("volume_ratio_5d").clip(0.0, 3.0) * 20 +
                # Turnover change contribution (0-30)
                (pl.col("turnover_change_20d").clip(-1.0, 1.0) + 1) * 15 +
                # Price position contribution (0-10)
                pl.col("price_position_60d") * 10
            )
            .clip(0.0, 100.0)
            .alias("main_strength_proxy"),
        ])

        return df

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
        period_df = df.filter(
            (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
        )

        # Calculate per-stock aggregations
        result = period_df.group_by("code").agg([
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
        ])

        # Calculate period return
        result = result.with_columns([
            ((pl.col("end_close") / pl.col("start_close")) - 1)
            .fill_null(0.0)
            .alias("period_return"),
        ])

        # Calculate max drawdown (simplified - peak-to-trough)
        # Note: Full drawdown calculation would require row-by-row processing
        # For now, use a simplified approximation
        result = result.with_columns([
            pl.lit(0.0).alias("max_drawdown"),  # Placeholder - implement full logic later
        ])

        return result
