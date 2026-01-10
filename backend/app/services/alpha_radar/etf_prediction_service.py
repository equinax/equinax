"""ETF Tomorrow Prediction service for Alpha Radar.

Calculates Ambush Score for ETF subcategories to predict tomorrow's potential winners.
Based on three factors:
- Divergence: Price flat/down but flow increasing
- Compression: Volume at 60-day low (pre-breakout signal)
- Activation: Small ETFs leading the subcategory
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.etf_classifier import ETFClassifier, get_industry_for_subcategory


# Category display order (exclude "other" which contains money market funds)
ETF_CATEGORY_ORDER = ["broad", "sector", "theme", "cross_border", "commodity"]
# Categories to exclude from prediction (low volatility instruments)
EXCLUDED_CATEGORIES = {"other", "bond"}

# Lookback periods
VOLUME_LOOKBACK_DAYS = 60
PRICE_LOOKBACK_DAYS = 3  # Reduced from 5 for faster signal response


class ETFPredictionService:
    """
    Service for calculating tomorrow's alpha prediction signals.

    Ambush Score Components (0-100):
    - Divergence Score (0-40): Price rank low + flow rank high
    - Compression Score (0-30): Volume near 60-day low
    - Activation Score (0-30): Small ETFs showing unusual moves
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_predictions(
        self,
        target_date: Optional[date] = None,
        min_score: float = 0,
    ) -> Dict[str, Any]:
        """
        Get tomorrow's alpha predictions for ETF subcategories.

        Args:
            target_date: Target date (default: latest trading day)
            min_score: Minimum ambush score to include (0-100)

        Returns:
            Dict with predictions list and metadata
        """
        # Resolve target date
        if target_date is None:
            target_date = await self._get_latest_trading_date()
        if target_date is None:
            return self._empty_response()

        # Get trading days for lookback
        trading_days = await self._get_trading_days(target_date, VOLUME_LOOKBACK_DAYS)
        if not trading_days or len(trading_days) < 10:
            return self._empty_response()

        # Load ETF data
        df = await self._load_etf_data(trading_days)
        if df.is_empty():
            return self._empty_response()

        # Load industry volume data for stock-level signals
        industry_df = await self._load_industry_volume(trading_days)

        # Classify ETFs
        df = self._classify_etfs(df)

        # Filter out excluded categories (money market funds, bonds, etc.)
        df = df.filter(~pl.col("category").is_in(list(EXCLUDED_CATEGORIES)))
        if df.is_empty():
            return self._empty_response()

        # Calculate subcategory aggregates
        subcategory_df = self._aggregate_by_subcategory(df)

        # Calculate scores (using industry volume where available)
        scores_df = self._calculate_scores(subcategory_df, df, target_date, industry_df)

        # Generate signals
        predictions = self._build_predictions(scores_df, df, target_date, min_score)

        return {
            "date": target_date,
            "predictions": predictions,
            "total_subcategories": len(predictions),
        }

    async def _get_latest_trading_date(self) -> Optional[date]:
        """Get the latest trading date with ETF data."""
        result = await self.db.execute(
            text("""
                SELECT MAX(md.date)
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                WHERE am.asset_type = 'ETF'
            """)
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None

    async def _get_trading_days(
        self, end_date: date, days: int
    ) -> List[date]:
        """Get the most recent N trading days ending at end_date."""
        result = await self.db.execute(
            text("""
                SELECT DISTINCT date
                FROM market_daily
                WHERE date <= :end_date
                ORDER BY date DESC
                LIMIT :days
            """),
            {"end_date": end_date, "days": days}
        )
        return [row[0] for row in result.fetchall()]

    async def _load_etf_data(self, trading_days: List[date]) -> pl.DataFrame:
        """Load ETF market data for the given trading days."""
        query = text("""
            SELECT
                am.code,
                am.name,
                md.date,
                md.pct_chg AS change_pct,
                md.amount,
                md.volume
            FROM asset_meta am
            JOIN market_daily md ON am.code = md.code
            WHERE am.asset_type = 'ETF'
              AND am.status = 1
              AND md.date = ANY(:dates)
              AND md.amount > 0
        """)

        result = await self.db.execute(query, {"dates": trading_days})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame([dict(zip(columns, row)) for row in rows])
        # Cast Decimal columns to Float64 for calculations
        return df.with_columns([
            pl.col("change_pct").cast(pl.Float64),
            pl.col("amount").cast(pl.Float64),
        ])

    async def _load_industry_volume(self, trading_days: List[date]) -> pl.DataFrame:
        """Load stock volume aggregated by Shenwan L1 industry."""
        query = text("""
            SELECT
                sp.sw_industry_l1 as industry,
                md.date,
                SUM(md.amount) as industry_amount,
                AVG(md.pct_chg) as industry_change
            FROM market_daily md
            JOIN stock_profile sp ON md.code = sp.code
            WHERE md.date = ANY(:dates)
              AND sp.sw_industry_l1 IS NOT NULL
              AND md.amount > 0
            GROUP BY sp.sw_industry_l1, md.date
        """)

        result = await self.db.execute(query, {"dates": trading_days})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame([dict(zip(columns, row)) for row in rows])
        return df.with_columns([
            pl.col("industry_amount").cast(pl.Float64),
            pl.col("industry_change").cast(pl.Float64),
        ])

    def _classify_etfs(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add classification columns and industry mapping to the dataframe."""
        classifications = []
        for row in df.iter_rows(named=True):
            category, sub_category = ETFClassifier.classify(row["name"])
            # Get mapped industry for this sub_category (may be None)
            mapped_industry = get_industry_for_subcategory(sub_category)
            classifications.append({
                "code": row["code"],
                "date": row["date"],
                "category": category,
                "sub_category": sub_category,
                "mapped_industry": mapped_industry,
            })

        class_df = pl.DataFrame(classifications)
        df = df.join(class_df, on=["code", "date"], how="left")
        return df

    def _aggregate_by_subcategory(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate ETF data by date × subcategory."""
        result = df.group_by(["date", "category", "sub_category"]).agg([
            pl.col("change_pct").mean().alias("avg_change"),
            pl.col("amount").sum().alias("total_amount"),
            # Flow proxy: sign(change) * amount
            (pl.col("change_pct").sign() * pl.col("amount")).sum().alias("flow_proxy"),
            pl.len().alias("etf_count"),
            # Representative ETF (highest amount)
            pl.col("code").sort_by("amount", descending=True).first().alias("rep_code"),
            pl.col("name").sort_by("amount", descending=True).first().alias("rep_name"),
            pl.col("change_pct").sort_by("amount", descending=True).first().alias("rep_change"),
            # Mapped industry (first one, they're all the same per sub_category)
            pl.col("mapped_industry").first().alias("mapped_industry"),
        ])
        # Cast Decimal columns to Float64 for rolling operations
        return result.with_columns([
            pl.col("avg_change").cast(pl.Float64),
            pl.col("total_amount").cast(pl.Float64),
            pl.col("flow_proxy").cast(pl.Float64),
            pl.col("rep_change").cast(pl.Float64),
        ])

    def _calculate_scores(
        self,
        subcategory_df: pl.DataFrame,
        etf_df: pl.DataFrame,
        target_date: date,
        industry_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Calculate Ambush Score for each subcategory.

        Components:
        1. Divergence (0-40): Low price rank + High flow rank
        2. Compression (0-30): Volume near 60-day low
        3. Activation (0-30): Small ETFs outperforming subcategory

        Uses industry-level stock volume where sub_category maps to a SW L1 industry.
        Falls back to ETF volume for cross-border, commodity, and unmapped categories.
        """
        # Sort for rolling calculations
        subcategory_df = subcategory_df.sort(["sub_category", "date"])

        # Join industry volume data to subcategory data
        if not industry_df.is_empty():
            # Prepare industry volume with rolling calculations
            industry_df = industry_df.sort(["industry", "date"])
            industry_df = industry_df.with_columns([
                pl.col("industry_amount")
                .rolling_sum(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
                .over("industry")
                .alias("industry_flow_nd"),

                pl.col("industry_amount")
                .rolling_min(window_size=60, min_periods=10)
                .over("industry")
                .alias("industry_vol_60d_min"),

                pl.col("industry_amount")
                .rolling_max(window_size=60, min_periods=10)
                .over("industry")
                .alias("industry_vol_60d_max"),

                pl.col("industry_amount")
                .rolling_mean(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
                .over("industry")
                .alias("industry_flow_avg"),
            ])

            # Join to subcategory_df based on mapped_industry and date
            subcategory_df = subcategory_df.join(
                industry_df.select([
                    "industry", "date", "industry_amount",
                    "industry_flow_nd", "industry_vol_60d_min",
                    "industry_vol_60d_max", "industry_flow_avg"
                ]),
                left_on=["mapped_industry", "date"],
                right_on=["industry", "date"],
                how="left",
            )

        # Calculate rolling metrics per subcategory
        subcategory_df = subcategory_df.with_columns([
            # 3-day cumulative change (reduced from 5 for faster response)
            pl.col("avg_change")
            .rolling_sum(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
            .over("sub_category")
            .alias("change_nd"),

            # 3-day average flow for ratio
            pl.col("flow_proxy")
            .rolling_mean(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
            .over("sub_category")
            .alias("flow_nd_avg"),

            # 60-day volume min/max for compression
            pl.col("total_amount")
            .rolling_min(window_size=60, min_periods=10)
            .over("sub_category")
            .alias("volume_60d_min"),

            pl.col("total_amount")
            .rolling_max(window_size=60, min_periods=10)
            .over("sub_category")
            .alias("volume_60d_max"),
        ])

        # Filter to target date only
        target_df = subcategory_df.filter(pl.col("date") == target_date)

        if target_df.is_empty():
            return target_df

        # Use industry volume when available, fall back to ETF volume
        # For flow ratio: use industry flow ratio when mapped_industry is set
        has_industry_data = "industry_amount" in target_df.columns

        if has_industry_data:
            # Calculate flow ratio using industry volume where available
            target_df = target_df.with_columns([
                pl.when(pl.col("industry_flow_avg").is_not_null())
                .then(pl.col("industry_amount") / pl.col("industry_flow_avg").abs().clip(lower_bound=1e6))
                .otherwise(pl.col("flow_proxy") / pl.col("flow_nd_avg").abs().clip(lower_bound=1e6))
                .alias("flow_ratio"),
            ])

            # Calculate volume percentile using industry volume where available
            target_df = target_df.with_columns([
                pl.when(pl.col("industry_vol_60d_min").is_not_null())
                .then(
                    (pl.col("industry_amount") - pl.col("industry_vol_60d_min")) /
                    (pl.col("industry_vol_60d_max") - pl.col("industry_vol_60d_min") + 1e6)
                )
                .otherwise(
                    (pl.col("total_amount") - pl.col("volume_60d_min")) /
                    (pl.col("volume_60d_max") - pl.col("volume_60d_min") + 1e6)
                )
                .clip(0, 1).alias("volume_percentile"),
            ])
        else:
            # No industry data, use ETF volume only
            target_df = target_df.with_columns([
                (pl.col("flow_proxy") / pl.col("flow_nd_avg").abs().clip(lower_bound=1e6))
                .alias("flow_ratio"),
            ])

            target_df = target_df.with_columns([
                (
                    (pl.col("total_amount") - pl.col("volume_60d_min")) /
                    (pl.col("volume_60d_max") - pl.col("volume_60d_min") + 1e6)
                ).clip(0, 1).alias("volume_percentile"),
            ])

        # Calculate ranks across all subcategories
        n_subcats = target_df.height
        if n_subcats == 0:
            return target_df

        target_df = target_df.with_columns([
            (pl.col("change_nd").rank() / n_subcats).alias("price_rank"),
            (pl.col("flow_ratio").rank() / n_subcats).alias("flow_rank"),
        ])

        # Calculate Divergence Score (0-40)
        # High score when: price rank low (beaten down) AND flow rank high (money coming in)
        target_df = target_df.with_columns([
            ((1 - pl.col("price_rank")) * pl.col("flow_rank") * 40)
            .clip(0, 40)
            .alias("divergence_score"),
        ])

        # Calculate Compression Score (0-30)
        # High score when volume is near 60-day low
        target_df = target_df.with_columns([
            ((1 - pl.col("volume_percentile")) * 30)
            .clip(0, 30)
            .alias("compression_score"),
        ])

        # Calculate Activation Score (0-30)
        activation_scores = self._calculate_activation_scores(etf_df, target_df, target_date)
        target_df = target_df.with_columns([
            pl.col("sub_category").replace(activation_scores, default=0.0).alias("activation_score"),
        ])

        # Calculate total Ambush Score
        target_df = target_df.with_columns([
            (
                pl.col("divergence_score") +
                pl.col("compression_score") +
                pl.col("activation_score")
            ).clip(0, 100).alias("ambush_score"),
        ])

        return target_df

    def _calculate_activation_scores(
        self,
        etf_df: pl.DataFrame,
        subcategory_df: pl.DataFrame,
        target_date: date,
    ) -> Dict[str, float]:
        """
        Calculate activation scores based on small ETF outperformance.

        Small ETFs (bottom 30% by amount) moving up while subcategory flat
        signals potential breakout (leaders testing the waters).
        """
        # Get target date ETF data
        target_etfs = etf_df.filter(pl.col("date") == target_date)

        if target_etfs.is_empty():
            return {}

        # Rank ETFs by amount within each subcategory
        target_etfs = target_etfs.with_columns([
            pl.col("amount")
            .rank()
            .over("sub_category")
            .alias("amount_rank"),
            pl.len().over("sub_category").alias("subcategory_count"),
        ])

        # Small ETFs = bottom 30% by amount
        target_etfs = target_etfs.with_columns([
            (pl.col("amount_rank") / pl.col("subcategory_count") <= 0.3)
            .alias("is_small_etf"),
        ])

        # Calculate average change for small ETFs per subcategory
        small_etf_perf = (
            target_etfs.filter(pl.col("is_small_etf"))
            .group_by("sub_category")
            .agg([
                pl.col("change_pct").mean().alias("small_etf_avg_change"),
            ])
        )

        # Get subcategory average changes
        subcat_changes = subcategory_df.select(["sub_category", "avg_change"])

        # Join with subcat_changes as the left table (FIX: reversed join direction)
        # This ensures all subcategories are included even if they have no small ETFs
        activation_df = subcat_changes.join(
            small_etf_perf,
            on="sub_category",
            how="left",
        )

        # Fill NULL small_etf_avg_change with avg_change (no outperformance = 0 activation)
        activation_df = activation_df.with_columns([
            pl.col("small_etf_avg_change").fill_null(pl.col("avg_change"))
        ])

        # Activation score: Small ETFs outperforming subcategory average
        # Scale factor: 6 points per 1% outperformance, max 30
        activation_df = activation_df.with_columns([
            (
                (pl.col("small_etf_avg_change") - pl.col("avg_change")) * 6
            ).clip(0, 30).alias("activation_score"),
        ])

        return {
            row["sub_category"]: row["activation_score"]
            for row in activation_df.iter_rows(named=True)
            if row["activation_score"] is not None
        }

    def _build_predictions(
        self,
        scores_df: pl.DataFrame,
        etf_df: pl.DataFrame,
        target_date: date,
        min_score: float,
    ) -> List[Dict[str, Any]]:
        """Build prediction items with signals."""
        if scores_df.is_empty():
            return []

        # Filter by minimum score
        if min_score > 0:
            scores_df = scores_df.filter(pl.col("ambush_score") >= min_score)

        # Sort by ambush score descending
        scores_df = scores_df.sort("ambush_score", descending=True)

        predictions = []
        for row in scores_df.iter_rows(named=True):
            # Generate signals
            signals = self._generate_signals(row)

            predictions.append({
                "sub_category": row["sub_category"],
                "category": row["category"],
                "category_label": ETFClassifier.get_category_label(row["category"]),
                "ambush_score": self._to_decimal(row["ambush_score"]),
                "divergence_score": self._to_decimal(row["divergence_score"]),
                "compression_score": self._to_decimal(row["compression_score"]),
                "activation_score": self._to_decimal(row["activation_score"]),
                "change_5d": self._to_decimal(row.get("change_nd")),  # Now 3-day change
                "flow_ratio": self._to_decimal(row.get("flow_ratio")),
                "volume_percentile": self._to_decimal(row.get("volume_percentile")),
                "signals": signals,
                "rep_code": row.get("rep_code"),
                "rep_name": row.get("rep_name"),
                "rep_change": self._to_decimal(row.get("rep_change")),
            })

        return predictions

    def _generate_signals(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate human-readable signal explanations."""
        signals = []

        # Divergence signal
        price_rank = row.get("price_rank", 0.5)
        flow_rank = row.get("flow_rank", 0.5)
        if price_rank < 0.3 and flow_rank > 0.7:
            signals.append({
                "type": "divergence",
                "score": self._to_decimal(row.get("divergence_score", 0)),
                "description": f"{PRICE_LOOKBACK_DAYS}日涨幅排名后30%，但资金流入排名前30%，价量背离明显",
            })

        # Compression signal
        volume_pct = row.get("volume_percentile", 0.5)
        if volume_pct < 0.2:
            signals.append({
                "type": "compression",
                "score": self._to_decimal(row.get("compression_score", 0)),
                "description": f"成交量处于60日低位{volume_pct*100:.0f}%，量能极致压缩",
            })

        # Activation signal
        activation_score = row.get("activation_score", 0)
        if activation_score > 10:
            signals.append({
                "type": "activation",
                "score": self._to_decimal(activation_score),
                "description": f"小市值ETF领先板块异动，龙头试盘信号",
            })

        return signals

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

    def _empty_response(self) -> Dict[str, Any]:
        """Return empty response structure."""
        return {
            "date": None,
            "predictions": [],
            "total_subcategories": 0,
        }
