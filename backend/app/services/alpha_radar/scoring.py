"""Scoring engine for Alpha Radar.

This module contains configurable scoring formulas for different screener tabs.
Scores are designed to be easily adjustable during the exploration phase.
"""

from typing import List

import polars as pl


class ScoringEngine:
    """
    Engine for calculating composite scores based on different strategies.

    All scores are normalized to 0-100 range for easy comparison.
    """

    def __init__(
        self,
        market_regime_score: float = 50.0,
    ):
        self.market_regime_score = market_regime_score

    def _apply_regime_discount(self, raw_score_expr, regime_weight: float = 0.20):
        """Apply market regime discount to a raw score expression.

        Iter 10: Widened effective range and steeper response curve.
        - regime >= 60: boost up to +25% (was +20%)
        - regime 40-60: mild linear range (±15%)
        - regime 30-40: moderate penalty (-15% to -25%)
        - regime < 30: steep penalty (-25% to -40%)

        The Iter 7 curve was too flat in the 35-55 range — both 12-08 (regime ~43
        after Iter 10 adjustments) and 01-05 (regime ~55+) got near-zero discounts.
        This widened curve ensures meaningful discrimination.
        """
        rs = self.market_regime_score
        if rs >= 60:
            # Strong regime: boost scores up to +25%
            discount = 1.0 + (rs - 60.0) / 40.0 * 0.25
            discount = min(1.25, discount)
        elif rs >= 40:
            # Neutral zone: mild linear adjustment (-15% to +0%)
            discount = 1.0 + (rs - 50.0) / 50.0 * 0.15
            discount = max(0.85, min(1.0, discount))
        elif rs >= 30:
            # Weak regime: -15% to -25%
            discount = 0.85 - (40.0 - rs) / 10.0 * 0.10
        else:
            # Very weak regime: -25% to -40%
            discount = 0.75 - (30.0 - rs) / 30.0 * 0.15
            discount = max(0.60, discount)
        return (raw_score_expr * discount).clip(0.0, 100.0)

    def generate_quant_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """Generate quantitative labels for stocks."""
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                pl.when(
                    (pl.col("main_strength_proxy").fill_null(0.0) > 70)
                    & (pl.col("price_position_60d").fill_null(1.0) < 0.3)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_main_accumulation"),
                pl.when(pl.col("pe_percentile").fill_null(1.0) < 0.25)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_undervalued"),
                pl.when(
                    (pl.col("price_position_60d").fill_null(1.0) < 0.2)
                    & (pl.col("momentum_20d").fill_null(0.0) < -0.05)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_oversold"),
                pl.when(pl.col("vol_percentile").fill_null(0.0) > 0.8)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_high_volatility"),
                pl.when(
                    (pl.col("price_position_60d").fill_null(0.0) > 0.9)
                    & (pl.col("volume_ratio_5d").fill_null(0.0) > 1.5)
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_breakout"),
                pl.when(pl.col("volume_ratio_5d").fill_null(0.0) > 2.0)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("label_volume_surge"),
            ]
        )

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
            "value_percentile": 0.5,
            "momentum_percentile": 0.5,
            "turnover_percentile": 0.5,
            "size_percentile": 0.5,
            "ep_ratio": 0.0,
            "bp_ratio": 0.0,
            "turnover_change_20d": 0.0,
            "positive_day_ratio_20d": 0.5,
            "up_volume_ratio": 0.5,
            "ma_alignment_score": 50.0,
            "return_std_20d": 2.0,
            "return_mean_20d": 0.0,
            "accumulation_score": 0.0,
            "climax_score": 0.0,
            "close_strength": 0.5,
            "upper_shadow_ratio": 0.0,
            "vol_ramp_5v20": 0.0,
            "turn_ramp_5v20": 0.0,
            "vol_peak_today_20d": 0.0,
            "stability_score": 50.0,
            "trend_quality_20d": 50.0,
            "volume_spike_penalty": 0.0,
            "volume_consistency_score": 50.0,
            "volume_buildup_quality": 40.0,
            "days_since_vol_peak_20d": 10.0,
            "recent_vol_spike_max": 0.0,
            "post_spike_consolidation": 50.0,
            "price_range_position_20d": 0.5,
            "momentum_quality_ratio": 50.0,
            "resistance_proximity_penalty": 0.0,
            "exhaustion_at_ceiling": 0.0,
            "return_5d": 0.0,
            "sector_momentum_5d": 0.0,
            "late_stage_stall": 0.0,
            "mf_net_percentile": 50.0,
            "elg_net_percentile": 50.0,
            "near_limit_up": 0,
            "vol_jump_1d": 1.0,
            "vol_cv_10d": 0.5,
            "atr_20d": 0.0,
            "max_loss_20d": 0.0,
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
