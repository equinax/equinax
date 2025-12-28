"""Scoring engine for Alpha Radar.

This module contains configurable scoring formulas for different screener tabs.
Scores are designed to be easily adjustable during the exploration phase.
"""

from dataclasses import dataclass
from typing import List, Optional

import polars as pl


@dataclass
class ScoreWeights:
    """Weight configuration for composite scores."""
    # Panorama tab weights
    panorama_momentum: float = 0.25
    panorama_value: float = 0.20
    panorama_quality: float = 0.20
    panorama_smart_money: float = 0.20
    panorama_technical: float = 0.15

    # Smart accumulation tab weights
    smart_main_strength: float = 0.35
    smart_volume_pattern: float = 0.30
    smart_price_position: float = 0.20
    smart_institutional: float = 0.15

    # Deep value tab weights
    value_valuation_rank: float = 0.40
    value_quality: float = 0.25
    value_stability: float = 0.20
    value_dividend: float = 0.15

    # Super trend tab weights
    trend_momentum: float = 0.30
    trend_breakout: float = 0.25
    trend_volume_confirm: float = 0.25
    trend_strength: float = 0.20


# Default weights - can be modified without database migration
DEFAULT_WEIGHTS = ScoreWeights()


class ScoringEngine:
    """
    Engine for calculating composite scores based on different strategies.

    All scores are normalized to 0-100 range for easy comparison.
    """

    def __init__(self, weights: Optional[ScoreWeights] = None):
        self.weights = weights or DEFAULT_WEIGHTS

    def calculate_panorama_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate panorama (全景) composite score.

        Components:
        - Momentum: Based on momentum_20d and momentum_60d
        - Value: Inverse of PE/PB percentiles
        - Quality: ROE proxy (not available, use placeholder)
        - Smart Money: main_strength_proxy
        - Technical: Price position and MA alignment
        """
        if df.is_empty():
            return df

        # Ensure we have required columns, use defaults if missing
        df = self._ensure_columns(df)

        df = df.with_columns([
            # Momentum score (0-100)
            # Higher momentum_20d = higher score
            (
                pl.col("momentum_20d")
                .fill_null(0.0)
                .rank()
                .over(pl.lit(1))  # Global rank
                / pl.len()
                * 100
            ).alias("momentum_score"),

            # Value score (0-100)
            # Lower PE percentile = higher value score
            (
                (1 - pl.col("pe_percentile").fill_null(0.5))
                * 100
            ).alias("value_score"),

            # Quality score - placeholder using stability proxy
            (pl.lit(50.0)).alias("quality_score"),

            # Smart money score - use main_strength_proxy
            (
                pl.col("main_strength_proxy")
                .fill_null(50.0)
            ).alias("smart_money_score"),

            # Technical score - price position based
            (
                pl.col("price_position_60d")
                .fill_null(0.5)
                * 100
            ).alias("technical_score"),
        ])

        # Calculate weighted composite
        w = self.weights
        df = df.with_columns([
            (
                pl.col("momentum_score") * w.panorama_momentum +
                pl.col("value_score") * w.panorama_value +
                pl.col("quality_score") * w.panorama_quality +
                pl.col("smart_money_score") * w.panorama_smart_money +
                pl.col("technical_score") * w.panorama_technical
            ).clip(0.0, 100.0).alias("panorama_score"),
        ])

        return df

    def calculate_smart_accumulation_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate smart accumulation (聪明钱吸筹) score.

        Components:
        - Main strength: Volume and turnover patterns
        - Volume pattern: Volume ratio and expansion detection
        - Price position: Lower position = better accumulation opportunity
        - Institutional: Placeholder for institutional activity
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns([
            # Main strength component
            (
                pl.col("main_strength_proxy")
                .fill_null(50.0)
            ).alias("main_strength_component"),

            # Volume pattern score
            # Higher volume ratio indicates potential accumulation
            (
                pl.col("volume_ratio_5d")
                .fill_null(1.0)
                .clip(0.5, 3.0)
                .map_elements(lambda x: min(100, (x - 0.5) * 40), return_dtype=pl.Float64)
            ).alias("volume_pattern_component"),

            # Price position score (inverse - lower is better for accumulation)
            (
                (1 - pl.col("price_position_60d").fill_null(0.5))
                * 100
            ).alias("price_position_component"),

            # Institutional activity placeholder
            (pl.lit(50.0)).alias("institutional_component"),
        ])

        # Calculate weighted composite
        w = self.weights
        df = df.with_columns([
            (
                pl.col("main_strength_component") * w.smart_main_strength +
                pl.col("volume_pattern_component") * w.smart_volume_pattern +
                pl.col("price_position_component") * w.smart_price_position +
                pl.col("institutional_component") * w.smart_institutional
            ).clip(0.0, 100.0).alias("smart_score"),
        ])

        return df

    def calculate_deep_value_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate deep value (深度价值) score.

        Components:
        - Valuation rank: Combined PE/PB ranking (lower = better)
        - Quality: ROE and profit stability proxy
        - Stability: Low volatility preference
        - Dividend: Dividend yield proxy
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns([
            # Valuation component (lower PE/PB = higher score)
            (
                (1 - pl.col("pe_percentile").fill_null(0.5))
                * 100
            ).alias("valuation_component"),

            # Quality component placeholder
            (pl.lit(50.0)).alias("quality_component"),

            # Stability component (lower volatility = higher stability)
            # Assuming vol_percentile is available
            (
                (1 - pl.col("vol_percentile").fill_null(0.5))
                * 100
            ).alias("stability_component"),

            # Dividend component placeholder
            (pl.lit(50.0)).alias("dividend_component"),
        ])

        # Calculate weighted composite
        w = self.weights
        df = df.with_columns([
            (
                pl.col("valuation_component") * w.value_valuation_rank +
                pl.col("quality_component") * w.value_quality +
                pl.col("stability_component") * w.value_stability +
                pl.col("dividend_component") * w.value_dividend
            ).clip(0.0, 100.0).alias("value_score"),
        ])

        return df

    def calculate_super_trend_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate super trend (趋势共振) score.

        Components:
        - Momentum: Short-term price momentum
        - Breakout: Price breakout signals
        - Volume confirm: Volume confirmation of price moves
        - Trend strength: Overall trend strength
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns([
            # Momentum component (20d momentum percentile)
            (
                pl.col("momentum_20d")
                .fill_null(0.0)
                .rank()
                .over(pl.lit(1))
                / pl.len()
                * 100
            ).alias("momentum_component"),

            # Breakout component (high price position = potential breakout)
            (
                pl.col("price_position_60d")
                .fill_null(0.5)
                * 100
            ).alias("breakout_component"),

            # Volume confirmation (high volume ratio with positive price = confirmed)
            (
                pl.when(
                    (pl.col("pct_chg").fill_null(0.0) > 0) &
                    (pl.col("volume_ratio_5d").fill_null(1.0) > 1.2)
                )
                .then(pl.lit(80.0))
                .when(pl.col("volume_ratio_5d").fill_null(1.0) > 1.0)
                .then(pl.lit(60.0))
                .otherwise(pl.lit(40.0))
            ).alias("volume_confirm_component"),

            # Trend strength placeholder
            (pl.lit(50.0)).alias("trend_strength_component"),
        ])

        # Calculate weighted composite
        w = self.weights
        df = df.with_columns([
            (
                pl.col("momentum_component") * w.trend_momentum +
                pl.col("breakout_component") * w.trend_breakout +
                pl.col("volume_confirm_component") * w.trend_volume_confirm +
                pl.col("trend_strength_component") * w.trend_strength
            ).clip(0.0, 100.0).alias("trend_score"),
        ])

        return df

    def generate_quant_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Generate quantitative labels for stocks.

        Labels:
        - main_accumulation: High main_strength_proxy + low price position
        - undervalued: Low PE/PB percentile
        - oversold: Low price position + negative momentum
        - high_volatility: High volatility percentile
        - breakout: High price position + high volume
        - volume_surge: Volume ratio > 2x
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns([
            # Main accumulation signal
            pl.when(
                (pl.col("main_strength_proxy").fill_null(0.0) > 70) &
                (pl.col("price_position_60d").fill_null(1.0) < 0.3)
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_main_accumulation"),

            # Undervalued signal
            pl.when(
                pl.col("pe_percentile").fill_null(1.0) < 0.25
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_undervalued"),

            # Oversold signal
            pl.when(
                (pl.col("price_position_60d").fill_null(1.0) < 0.2) &
                (pl.col("momentum_20d").fill_null(0.0) < -0.05)
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_oversold"),

            # High volatility signal
            pl.when(
                pl.col("vol_percentile").fill_null(0.0) > 0.8
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_high_volatility"),

            # Breakout signal
            pl.when(
                (pl.col("price_position_60d").fill_null(0.0) > 0.9) &
                (pl.col("volume_ratio_5d").fill_null(0.0) > 1.5)
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_breakout"),

            # Volume surge signal
            pl.when(
                pl.col("volume_ratio_5d").fill_null(0.0) > 2.0
            ).then(pl.lit(True)).otherwise(pl.lit(False)).alias("label_volume_surge"),
        ])

        return df

    def _ensure_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ensure required columns exist with default values."""
        required_columns = {
            "momentum_20d": 0.0,
            "momentum_60d": 0.0,
            "pe_percentile": 0.5,
            "vol_percentile": 0.5,
            "main_strength_proxy": 50.0,
            "price_position_60d": 0.5,
            "volume_ratio_5d": 1.0,
            "pct_chg": 0.0,
        }

        for col, default in required_columns.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(default).alias(col))

        return df

    def aggregate_labels_to_list(self, row: dict) -> List[str]:
        """Convert label boolean columns to a list of active labels."""
        labels = []
        label_mapping = {
            "label_main_accumulation": "main_accumulation",
            "label_undervalued": "undervalued",
            "label_oversold": "oversold",
            "label_high_volatility": "high_volatility",
            "label_breakout": "breakout",
            "label_volume_surge": "volume_surge",
        }
        for col, label_name in label_mapping.items():
            if row.get(col, False):
                labels.append(label_name)
        return labels
