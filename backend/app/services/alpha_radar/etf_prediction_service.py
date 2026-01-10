"""ETF Tomorrow Prediction service for Alpha Radar.

Calculates Ambush Score for ETF subcategories to predict tomorrow's potential winners.

V2 Algorithm (Percentile-normalized, multi-factor):
- Divergence (0-35): Volume surge + price stagnation
- RSI Reversal (0-20): Oversold + RSI uptick
- Relative Strength (0-15): Underperformance vs market (mean reversion)
- Momentum (0-15): Short-term momentum turning positive
- Activation (0-15): Small ETFs outperforming

All factors use percentile ranking for even score distribution.
"""

from dataclasses import dataclass, field
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
VOLUME_BASELINE_DAYS = 120  # For divergence baseline calculation
RSI_PERIOD = 14


@dataclass
class FactorConfig:
    """Configuration for a single factor."""
    enabled: bool = True
    weight: float = 1.0  # Relative weight (will be normalized)


@dataclass
class PredictionConfig:
    """Configuration for the prediction algorithm."""
    # V2 default weights optimized for A-share momentum characteristics
    divergence: FactorConfig = field(default_factory=lambda: FactorConfig(enabled=True, weight=0.20))
    rsi: FactorConfig = field(default_factory=lambda: FactorConfig(enabled=True, weight=0.15))  # Volume signal
    relative_strength: FactorConfig = field(default_factory=lambda: FactorConfig(enabled=True, weight=0.30))  # 5-day momentum
    momentum: FactorConfig = field(default_factory=lambda: FactorConfig(enabled=True, weight=0.25))  # Trend aligned
    activation: FactorConfig = field(default_factory=lambda: FactorConfig(enabled=True, weight=0.10))

    def get_normalized_weights(self) -> Dict[str, float]:
        """Get weights normalized to sum to 1.0 for enabled factors only."""
        factors = {
            "divergence": self.divergence,
            "rsi": self.rsi,
            "relative_strength": self.relative_strength,
            "momentum": self.momentum,
            "activation": self.activation,
        }
        enabled_weights = {k: v.weight for k, v in factors.items() if v.enabled}
        total = sum(enabled_weights.values())
        if total == 0:
            return {}
        return {k: w / total for k, w in enabled_weights.items()}

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PredictionConfig":
        """Create config from a dictionary."""
        config = cls()
        for factor_name, factor_cfg in config_dict.items():
            if hasattr(config, factor_name):
                factor = getattr(config, factor_name)
                if isinstance(factor_cfg, dict):
                    factor.enabled = factor_cfg.get("enabled", True)
                    factor.weight = factor_cfg.get("weight", factor.weight)
        return config


# Default config for backward compatibility
DEFAULT_CONFIG = PredictionConfig()


class ETFPredictionService:
    """
    Service for calculating tomorrow's alpha prediction signals.

    V2 Ambush Score Components (0-100, percentile-normalized):
    - Divergence Score: Volume surge + price stagnation (default 35%)
    - RSI Score: Oversold zone + RSI uptick (default 20%)
    - Relative Strength Score: Underperformance vs market (default 15%)
    - Momentum Score: Short-term momentum turning positive (default 15%)
    - Activation Score: Small ETFs outperforming (default 15%)
    """

    def __init__(self, db: AsyncSession, config: Optional[PredictionConfig] = None):
        self.db = db
        self.config = config or DEFAULT_CONFIG

    async def get_predictions(
        self,
        target_date: Optional[date] = None,
        min_score: float = 0,
        config: Optional[PredictionConfig] = None,
    ) -> Dict[str, Any]:
        """
        Get tomorrow's alpha predictions for ETF subcategories.

        Args:
            target_date: Target date (default: latest trading day)
            min_score: Minimum ambush score to include (0-100)
            config: Optional factor configuration override

        Returns:
            Dict with predictions list and metadata
        """
        # Use provided config or instance config
        active_config = config or self.config

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

        # Load industry volume data for stock-level signals (with 120-day baseline)
        industry_df = await self._load_industry_volume(trading_days, target_date)

        # Classify ETFs
        df = self._classify_etfs(df)

        # Filter out excluded categories (money market funds, bonds, etc.)
        df = df.filter(~pl.col("category").is_in(list(EXCLUDED_CATEGORIES)))
        if df.is_empty():
            return self._empty_response()

        # Calculate subcategory aggregates
        subcategory_df = self._aggregate_by_subcategory(df)

        # Calculate scores with percentile normalization (V2 algorithm)
        scores_df = self._calculate_scores_v2(subcategory_df, df, target_date, industry_df, active_config)

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

    async def _load_industry_volume(self, trading_days: List[date], target_date: date) -> pl.DataFrame:
        """Load stock volume aggregated by Shenwan L1 industry with 120-day baseline."""
        # First, get the baseline start date (120 trading days before target)
        baseline_days_result = await self.db.execute(
            text("""
                SELECT DISTINCT date FROM market_daily
                WHERE date <= :end_date
                ORDER BY date DESC
                LIMIT :days
            """),
            {"end_date": target_date, "days": VOLUME_BASELINE_DAYS}
        )
        baseline_dates = [row[0] for row in baseline_days_result.fetchall()]

        if not baseline_dates:
            return pl.DataFrame()

        # Load daily industry volumes with baseline calculation
        query = text("""
            WITH daily_totals AS (
                SELECT
                    sp.sw_industry_l1 as industry,
                    md.date,
                    SUM(md.amount) as industry_amount,
                    AVG(md.pct_chg) as industry_change
                FROM market_daily md
                JOIN stock_profile sp ON md.code = sp.code
                WHERE md.date = ANY(:all_dates)
                  AND sp.sw_industry_l1 IS NOT NULL
                  AND md.amount > 0
                GROUP BY sp.sw_industry_l1, md.date
            ),
            baselines AS (
                SELECT
                    industry,
                    AVG(industry_amount) as vol_120d_avg
                FROM daily_totals
                GROUP BY industry
            )
            SELECT
                dt.industry,
                dt.date,
                dt.industry_amount,
                dt.industry_change,
                b.vol_120d_avg
            FROM daily_totals dt
            JOIN baselines b ON dt.industry = b.industry
            WHERE dt.date = ANY(:trading_dates)
        """)

        result = await self.db.execute(query, {
            "all_dates": baseline_dates,
            "trading_dates": trading_days
        })
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame([dict(zip(columns, row)) for row in rows])
        return df.with_columns([
            pl.col("industry_amount").cast(pl.Float64),
            pl.col("industry_change").cast(pl.Float64),
            pl.col("vol_120d_avg").cast(pl.Float64),
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

        class_df = pl.DataFrame(classifications, infer_schema_length=None)
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

            # Calculate daily volume deviation from 120-day baseline
            industry_df = industry_df.with_columns([
                pl.when(pl.col("vol_120d_avg") > 0)
                .then((pl.col("industry_amount") - pl.col("vol_120d_avg")) / pl.col("vol_120d_avg") * 100)
                .otherwise(0.0)
                .alias("daily_deviation_pct"),
            ])

            # Calculate rolling metrics including 3-day average deviation
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

                # 3-day average deviation for divergence calculation
                pl.col("daily_deviation_pct")
                .rolling_mean(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
                .over("industry")
                .alias("volume_deviation_3d_avg"),
            ])

            # Join to subcategory_df based on mapped_industry and date
            subcategory_df = subcategory_df.join(
                industry_df.select([
                    "industry", "date", "industry_amount",
                    "industry_flow_nd", "industry_vol_60d_min",
                    "industry_vol_60d_max", "industry_flow_avg",
                    "volume_deviation_3d_avg"  # For new divergence formula
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

        # Calculate ranks across all subcategories (still needed for some analysis)
        n_subcats = target_df.height
        if n_subcats == 0:
            return target_df

        target_df = target_df.with_columns([
            (pl.col("change_nd").rank() / n_subcats).alias("price_rank"),
            (pl.col("flow_ratio").rank() / n_subcats).alias("flow_rank"),
        ])

        # Calculate Divergence Score (0-50) - 量升价滞 formula
        # High score when: volume surge (above 120d avg) + price stagnation (small absolute change)
        has_deviation_data = "volume_deviation_3d_avg" in target_df.columns

        if has_deviation_data:
            target_df = target_df.with_columns([
                # Volume surge score: 3-day avg deviation / 80, capped at 1.0
                # +80% deviation = 1.0 (max), +40% = 0.5, 0% = 0
                pl.when(pl.col("volume_deviation_3d_avg").is_not_null())
                .then((pl.col("volume_deviation_3d_avg") / 80).clip(0, 1))
                .otherwise(0.0)
                .alias("volume_surge_score"),

                # 3-day average price change
                (pl.col("change_nd") / PRICE_LOOKBACK_DAYS).alias("change_3d_avg"),

                # Price stagnation score: 1 - abs(3d_avg_change) / 2.5
                # 0% change = 1.0, 1.25% change = 0.5, 2.5%+ change = 0
                (1 - (pl.col("change_nd") / PRICE_LOOKBACK_DAYS).abs() / 2.5).clip(0, 1)
                .alias("price_stagnation_score"),
            ])

            # Divergence = volume_surge × price_stagnation × 50
            target_df = target_df.with_columns([
                (pl.col("volume_surge_score") * pl.col("price_stagnation_score") * 50)
                .clip(0, 50)
                .alias("divergence_score"),
            ])
        else:
            # Fallback to old formula if no deviation data
            target_df = target_df.with_columns([
                ((1 - pl.col("price_rank")) * pl.col("flow_rank") * 40)
                .clip(0, 40)
                .alias("divergence_score"),
            ])

        # Calculate Compression Score (0-10) - reduced weight
        # Only score when volume is in bottom 20% of 60-day range
        target_df = target_df.with_columns([
            pl.when(pl.col("volume_percentile") < 0.2)
            .then(((0.2 - pl.col("volume_percentile")) / 0.2 * 10).clip(0, 10))
            .otherwise(0.0)
            .alias("compression_score"),
        ])

        # Calculate Activation Score (0-40) - increased weight
        activation_data = self._calculate_activation_scores(etf_df, target_df, target_date)
        # Extract just scores for DataFrame column
        activation_scores = {k: v["score"] for k, v in activation_data.items()}
        target_df = target_df.with_columns([
            pl.col("sub_category").replace(activation_scores, default=0.0).alias("activation_score"),
        ])
        # Store full activation data for signal generation
        self._activation_data = activation_data

        # Calculate total Ambush Score
        target_df = target_df.with_columns([
            (
                pl.col("divergence_score") +
                pl.col("compression_score") +
                pl.col("activation_score")
            ).clip(0, 100).alias("ambush_score"),
        ])

        return target_df

    def _calculate_scores_v2(
        self,
        subcategory_df: pl.DataFrame,
        etf_df: pl.DataFrame,
        target_date: date,
        industry_df: pl.DataFrame,
        config: PredictionConfig,
    ) -> pl.DataFrame:
        """
        V2 Scoring with percentile normalization and configurable factors.

        Uses percentile ranks for even score distribution across 0-100.
        """
        # Get normalized weights for enabled factors
        weights = config.get_normalized_weights()
        if not weights:
            return subcategory_df.filter(pl.col("date") == target_date)

        # Sort for rolling calculations
        subcategory_df = subcategory_df.sort(["sub_category", "date"])

        # Join industry volume data if available
        if not industry_df.is_empty():
            industry_df = industry_df.sort(["industry", "date"])

            # Calculate volume deviation from baseline
            industry_df = industry_df.with_columns([
                pl.when(pl.col("vol_120d_avg") > 0)
                .then((pl.col("industry_amount") - pl.col("vol_120d_avg")) / pl.col("vol_120d_avg") * 100)
                .otherwise(0.0)
                .alias("daily_deviation_pct"),
            ])

            # Rolling metrics
            industry_df = industry_df.with_columns([
                pl.col("daily_deviation_pct")
                .rolling_mean(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
                .over("industry")
                .alias("volume_deviation_3d_avg"),
            ])

            subcategory_df = subcategory_df.join(
                industry_df.select(["industry", "date", "volume_deviation_3d_avg"]),
                left_on=["mapped_industry", "date"],
                right_on=["industry", "date"],
                how="left",
            )

        # Calculate rolling metrics per subcategory
        subcategory_df = subcategory_df.with_columns([
            # N-day cumulative change
            pl.col("avg_change")
            .rolling_sum(window_size=PRICE_LOOKBACK_DAYS, min_periods=1)
            .over("sub_category")
            .alias("change_nd"),

            # 5-day change for momentum
            pl.col("avg_change")
            .rolling_sum(window_size=5, min_periods=1)
            .over("sub_category")
            .alias("change_5d"),

            # 20-day change for long-term momentum
            pl.col("avg_change")
            .rolling_sum(window_size=20, min_periods=5)
            .over("sub_category")
            .alias("change_20d"),

            # Volume percentile
            pl.col("total_amount")
            .rolling_min(window_size=60, min_periods=10)
            .over("sub_category")
            .alias("volume_60d_min"),

            pl.col("total_amount")
            .rolling_max(window_size=60, min_periods=10)
            .over("sub_category")
            .alias("volume_60d_max"),
        ])

        # Filter to target date
        target_df = subcategory_df.filter(pl.col("date") == target_date)

        if target_df.is_empty():
            return target_df

        n_subcats = target_df.height
        if n_subcats == 0:
            return target_df

        # === Calculate Raw Factor Values ===

        # Volume deviation (use industry data or 0)
        if "volume_deviation_3d_avg" in target_df.columns:
            target_df = target_df.with_columns([
                pl.col("volume_deviation_3d_avg").fill_null(0.0).alias("volume_deviation"),
            ])
        else:
            target_df = target_df.with_columns([
                pl.lit(0.0).alias("volume_deviation"),
            ])

        # Price stagnation (absolute value of 3d change - lower is better)
        target_df = target_df.with_columns([
            (pl.col("change_nd") / PRICE_LOOKBACK_DAYS).abs().alias("price_volatility"),
        ])

        # Short-term momentum
        target_df = target_df.with_columns([
            pl.col("change_5d").fill_null(0.0).alias("momentum_5d"),
            pl.col("change_20d").fill_null(0.0).alias("momentum_20d"),
        ])

        # Volume percentile
        target_df = target_df.with_columns([
            pl.when(pl.col("volume_60d_max") > pl.col("volume_60d_min"))
            .then(
                (pl.col("total_amount") - pl.col("volume_60d_min")) /
                (pl.col("volume_60d_max") - pl.col("volume_60d_min"))
            )
            .otherwise(0.5)
            .clip(0, 1)
            .alias("volume_percentile"),
        ])

        # === Calculate Percentile Ranks (0-1) for Each Factor ===

        # Divergence: high volume deviation + low price volatility
        # Volume deviation rank (higher is better)
        target_df = target_df.with_columns([
            (pl.col("volume_deviation").rank() / n_subcats).alias("vol_dev_rank"),
            # Price volatility rank (lower is better, so 1 - rank)
            (1 - pl.col("price_volatility").rank() / n_subcats).alias("price_stag_rank"),
        ])

        # Volume signal: high volume = institutional attention (momentum signal)
        target_df = target_df.with_columns([
            # Higher volume percentile is better (institutions accumulating)
            pl.col("volume_percentile").alias("rsi_raw"),
        ])
        target_df = target_df.with_columns([
            (pl.col("rsi_raw").rank() / n_subcats).alias("rsi_rank"),
        ])

        # Relative strength: MOMENTUM approach (strong recent performance continues)
        target_df = target_df.with_columns([
            # The BETTER it performed, the higher the rank (momentum, not mean reversion)
            (pl.col("momentum_5d").rank() / n_subcats).alias("rs_rank"),
        ])

        # Momentum: consistent uptrend signals (TREND FOLLOWING)
        target_df = target_df.with_columns([
            # Short-term positive (core signal)
            pl.when(pl.col("momentum_5d") > 0).then(0.4).otherwise(0.0).alias("mom_short_pos"),
            # Long-term positive (sustained trend)
            pl.when(pl.col("momentum_20d") > 0).then(0.3).otherwise(0.0).alias("mom_long_pos"),
            # Both aligned (strong trend)
            pl.when(
                (pl.col("momentum_5d") > 0) & (pl.col("momentum_20d") > 0)
            ).then(0.3).otherwise(0.0).alias("mom_aligned"),
        ])
        target_df = target_df.with_columns([
            (pl.col("mom_short_pos") + pl.col("mom_long_pos") + pl.col("mom_aligned")).alias("momentum_raw"),
        ])
        target_df = target_df.with_columns([
            (pl.col("momentum_raw").rank() / n_subcats).alias("momentum_rank"),
        ])

        # === Calculate Activation Score ===
        activation_data = self._calculate_activation_scores(etf_df, target_df, target_date)
        activation_scores = {k: v["score"] for k, v in activation_data.items()}
        self._activation_data = activation_data

        # Add raw activation scores, then rank
        target_df = target_df.with_columns([
            pl.col("sub_category").replace(activation_scores, default=0.0).alias("activation_raw"),
        ])
        target_df = target_df.with_columns([
            (pl.col("activation_raw").rank() / n_subcats).alias("activation_rank"),
        ])

        # === Combine Factor Scores ===
        # Each factor contributes its percentile rank * weight * 100

        # Initialize scores
        target_df = target_df.with_columns([
            pl.lit(0.0).alias("divergence_score"),
            pl.lit(0.0).alias("rsi_score"),
            pl.lit(0.0).alias("relative_strength_score"),
            pl.lit(0.0).alias("momentum_score"),
            pl.lit(0.0).alias("activation_score"),
        ])

        # Divergence: combine volume deviation and price stagnation
        if "divergence" in weights:
            w = weights["divergence"]
            target_df = target_df.with_columns([
                ((pl.col("vol_dev_rank") * 0.6 + pl.col("price_stag_rank") * 0.4) * w * 100)
                .alias("divergence_score"),
            ])

        # RSI
        if "rsi" in weights:
            w = weights["rsi"]
            target_df = target_df.with_columns([
                (pl.col("rsi_rank") * w * 100).alias("rsi_score"),
            ])

        # Relative Strength
        if "relative_strength" in weights:
            w = weights["relative_strength"]
            target_df = target_df.with_columns([
                (pl.col("rs_rank") * w * 100).alias("relative_strength_score"),
            ])

        # Momentum
        if "momentum" in weights:
            w = weights["momentum"]
            target_df = target_df.with_columns([
                (pl.col("momentum_rank") * w * 100).alias("momentum_score"),
            ])

        # Activation
        if "activation" in weights:
            w = weights["activation"]
            target_df = target_df.with_columns([
                (pl.col("activation_rank") * w * 100).alias("activation_score"),
            ])

        # Total Ambush Score
        target_df = target_df.with_columns([
            (
                pl.col("divergence_score") +
                pl.col("rsi_score") +
                pl.col("relative_strength_score") +
                pl.col("momentum_score") +
                pl.col("activation_score")
            ).clip(0, 100).alias("ambush_score"),
        ])

        # For backward compatibility, alias compression_score
        target_df = target_df.with_columns([
            pl.col("rsi_score").alias("compression_score"),  # Reuse for API compatibility
        ])

        # Add metadata columns for signal generation
        target_df = target_df.with_columns([
            pl.col("volume_deviation").alias("volume_deviation_3d_avg"),
            (pl.col("change_nd") / PRICE_LOOKBACK_DAYS).alias("change_3d_avg"),
            pl.col("vol_dev_rank").alias("volume_surge_score"),
            pl.col("price_stag_rank").alias("price_stagnation_score"),
        ])

        return target_df

    def _calculate_activation_scores(
        self,
        etf_df: pl.DataFrame,
        subcategory_df: pl.DataFrame,
        target_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate activation scores based on small ETF outperformance.

        Small ETFs (bottom 30% by amount) moving up while subcategory flat
        signals potential breakout (leaders testing the waters).

        Returns dict with score and details for each subcategory.
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
                pl.len().alias("small_etf_count"),
            ])
        )

        # Get subcategory average changes
        subcat_changes = subcategory_df.select(["sub_category", "avg_change"])

        # Join with subcat_changes as the left table
        activation_df = subcat_changes.join(
            small_etf_perf,
            on="sub_category",
            how="left",
        )

        # Fill NULL small_etf_avg_change with avg_change (no outperformance = 0 activation)
        activation_df = activation_df.with_columns([
            pl.col("small_etf_avg_change").fill_null(pl.col("avg_change")),
            pl.col("small_etf_count").fill_null(0),
        ])

        # Calculate outperformance
        activation_df = activation_df.with_columns([
            (pl.col("small_etf_avg_change") - pl.col("avg_change")).alias("activation_outperformance"),
        ])

        # Activation score: Small ETFs outperforming subcategory average
        # Scale factor: 15 points per 1% outperformance, max 40
        activation_df = activation_df.with_columns([
            (pl.col("activation_outperformance") * 15).clip(0, 40).alias("activation_score"),
        ])

        return {
            row["sub_category"]: {
                "score": row["activation_score"] or 0.0,
                "small_etf_avg_change": row["small_etf_avg_change"] or 0.0,
                "subcategory_avg_change": row["avg_change"] or 0.0,
                "outperformance": row["activation_outperformance"] or 0.0,
                "small_etf_count": row["small_etf_count"] or 0,
            }
            for row in activation_df.iter_rows(named=True)
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
                "volume_deviation_pct": self._to_decimal(row.get("volume_deviation_3d_avg")),  # 3-day avg deviation from 120d baseline
                "signals": signals,
                "rep_code": row.get("rep_code"),
                "rep_name": row.get("rep_name"),
                "rep_change": self._to_decimal(row.get("rep_change")),
            })

        return predictions

    def _generate_signals(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate human-readable signal explanations."""
        signals = []

        # Divergence signal - 量升价滞
        volume_surge = row.get("volume_surge_score", 0) or 0
        price_stag = row.get("price_stagnation_score", 0) or 0
        volume_dev = row.get("volume_deviation_3d_avg", 0) or 0
        change_3d_avg = row.get("change_3d_avg", 0) or 0

        if volume_surge > 0.15 and price_stag > 0.4:
            signals.append({
                "type": "divergence",
                "score": self._to_decimal(row.get("divergence_score", 0)),
                "description": f"量升价滞：近3日成交量较均值+{volume_dev:.0f}%，日均涨幅仅{change_3d_avg:.1f}%",
            })

        # Compression signal - only show when volume < 20% percentile
        volume_pct = row.get("volume_percentile", 0.5)
        if volume_pct < 0.2:
            signals.append({
                "type": "compression",
                "score": self._to_decimal(row.get("compression_score", 0)),
                "description": f"成交量处于60日低位{volume_pct*100:.0f}%，量能极致压缩",
            })

        # Activation signal - with detailed explanation
        activation_score = row.get("activation_score", 0) or 0
        sub_category = row.get("sub_category", "")
        activation_info = getattr(self, "_activation_data", {}).get(sub_category, {})

        if activation_score > 5:  # Lowered threshold from 10 to 5
            outperformance = activation_info.get("outperformance", 0)
            small_etf_change = activation_info.get("small_etf_avg_change", 0)
            subcat_change = activation_info.get("subcategory_avg_change", 0)
            small_count = activation_info.get("small_etf_count", 0)

            if outperformance > 0:
                signals.append({
                    "type": "activation",
                    "score": self._to_decimal(activation_score),
                    "description": f"小市值ETF({small_count}只)涨{small_etf_change:.2f}%，领先板块均值{outperformance:.2f}%",
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
