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

    panorama_momentum: float = 0.20
    panorama_value: float = 0.20
    panorama_quality: float = 0.20
    panorama_smart_money: float = 0.20
    panorama_technical: float = 0.20

    smart_main_strength: float = 0.30
    smart_volume_pattern: float = 0.25
    smart_price_position: float = 0.20
    smart_institutional: float = 0.25

    value_valuation_rank: float = 0.35
    value_quality: float = 0.25
    value_stability: float = 0.25
    value_dividend: float = 0.15

    trend_momentum: float = 0.25
    trend_breakout: float = 0.20
    trend_volume_confirm: float = 0.25
    trend_strength: float = 0.30


# Default weights - can be modified without database migration
DEFAULT_WEIGHTS = ScoreWeights()


class ScoringEngine:
    """
    Engine for calculating composite scores based on different strategies.

    All scores are normalized to 0-100 range for easy comparison.
    """

    def __init__(self, weights: Optional[ScoreWeights] = None, market_regime_score: float = 50.0):
        self.weights = weights or DEFAULT_WEIGHTS
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

    def calculate_panorama_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate panorama (全景) composite score.

        Iter 11: Low-VBQ penalty from cross-date failure analysis (12-01, 12-22).

        Observation: On mixed-result dates, losers have significantly lower VBQ
        than winners.  12-01: loser group avg VBQ=67.3 vs winner group avg VBQ=78.3
        (delta +11.6).  11-10: delta +14.8.  Low VBQ indicates sloppy/inconsistent
        volume buildup — distribution rather than accumulation.

        Implementation: penalty fires only when VBQ < 65.
        Formula: ((65 - VBQ).clip(0, 25) / 25) * 100.  Weight: 0.05.

        Result: Panorama WR 80.0% → 82.9% (+2.9pp).  Other tabs unchanged.

        Note: Quadratic spike penalty was also tested (Attempt A) but REJECTED —
        it reduced penalty on low-spike losers (荣昌生物 spike=5.5, T+5=-8.64%),
        promoting them into top 5 and causing panorama regression to 74.3%.
        Linear spike * 0.15 retained.
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
                (pl.col("ma_alignment_score").fill_null(50.0)).alias("alignment_component"),
                (pl.col("momentum_percentile").fill_null(0.5) * 100).alias("momentum_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                # Iter 11: low-VBQ penalty — fires only when VBQ < 65
                (
                    (65.0 - pl.col("volume_buildup_quality").fill_null(40.0)).clip(0.0, 25.0)
                    / 25.0
                    * 100
                ).alias("low_vbq_penalty"),
            ]
        )

        raw_score = (
            pl.col("accumulation_component") * 0.15
            + pl.col("alignment_component") * 0.10
            + pl.col("momentum_component") * 0.05
            + pl.col("trend_quality_component") * 0.10
            + pl.col("buildup_component") * 0.10
            + pl.col("anti_climax_component") * 0.20
            + pl.col("consolidation_component") * 0.15
            - pl.col("recent_spike_penalty") * 0.15
            - pl.col("low_vbq_penalty") * 0.05
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("panorama_score")]
        )

        return df

    def calculate_smart_accumulation_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate smart accumulation (聪明钱吸筹) score.

        Iter 9: added sector_momentum_penalty (-0.05) for stocks in weak sectors.
        Reduced consolidation 0.15→0.10 to make room.

        Iter 12: Cross-date qualitative analysis of Smart tab failures (12-01 WR=40%,
        12-22 WR=60%) revealed two key factor mispricing:

        1. close_strength (CS) is REVERSED — top-5 losers avg CS=0.90 vs #6-10
           winners avg CS=0.71 (delta -0.19).  High CS means "closed near daily
           high" = already surged today = chasing late momentum.  Weight: 0.10→0.00.

        2. volume_consistency_score (VCS) is the STRONGEST cross-date discriminator
           — top-5 losers avg VCS=84.5 vs #6-10 winners avg VCS=91.6 (delta +7.12).
           High VCS = steady, reliable volume accumulation without erratic spikes.
           Weight: 0.05→0.12.

        3. main_strength_proxy (MSP) is the second-strongest positive signal —
           losers avg MSP=51.6 vs winners avg MSP=56.4 (delta +4.78).
           Weight: 0.10→0.13.

        Budget: -0.10 (CS) + 0.07 (VCS) + 0.03 (MSP) = 0.00 net change.
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("main_strength_proxy").fill_null(50.0)).alias("main_strength_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                (pl.col("close_strength").fill_null(0.5) * 100).alias("close_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_consistency_score").fill_null(50.0)).alias("consistency_component"),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (((pl.col("pct_chg").fill_null(0.0).abs() - 3.0).clip(0.0, 4.0) / 4.0) * 100).alias(
                    "surge_penalty"
                ),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (pl.col("momentum_quality_ratio").fill_null(50.0)).alias(
                    "momentum_quality_component"
                ),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
                (
                    (-pl.col("return_5d").fill_null(0.0).clip(-8.0, -1.0) - 1.0).clip(0.0, 7.0)
                    / 7.0
                    * 100
                )
                .fill_null(0.0)
                .alias("negative_momentum_penalty"),
                ((-pl.col("sector_momentum_5d").fill_null(0.0).clip(-5.0, 0.0)) / 5.0 * 100)
                .fill_null(0.0)
                .alias("sector_weakness_penalty"),
            ]
        )

        raw_score = (
            pl.col("main_strength_component") * 0.13
            + pl.col("buildup_component") * 0.10
            + pl.col("trend_quality_component") * 0.10
            + pl.col("consistency_component") * 0.12
            + pl.col("anti_climax_component") * 0.10
            + pl.col("consolidation_component") * 0.10
            + pl.col("momentum_quality_component") * 0.10
            - pl.col("surge_penalty") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("resistance_penalty") * 0.10
            - pl.col("negative_momentum_penalty") * 0.05
            - pl.col("sector_weakness_penalty") * 0.05
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("smart_score")]
        )

        return df

    def calculate_deep_value_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate deep value (深度价值) score.

        Iter 9: reduced resistance_penalty 0.05→0.02 (value stocks naturally trade
        near highs after re-rating). No sector penalty for value tab — fundamental
        cheapness is sector-independent.
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("value_percentile").fill_null(0.5) * 100).alias("valuation_component"),
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("trigger_component"),
                (pl.col("ma_alignment_score").fill_null(50.0)).alias("alignment_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (pl.col("momentum_quality_ratio").fill_null(50.0)).alias(
                    "momentum_quality_component"
                ),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
            ]
        )

        raw_score = (
            pl.col("valuation_component") * 0.35
            + pl.col("trigger_component") * 0.10
            + pl.col("alignment_component") * 0.05
            + pl.col("trend_quality_component") * 0.10
            + pl.col("buildup_component") * 0.05
            + pl.col("anti_climax_component") * 0.10
            + pl.col("consolidation_component") * 0.13
            + pl.col("momentum_quality_component") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("resistance_penalty") * 0.02
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("value_score")]
        )

        return df

    def calculate_super_trend_score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate super trend (趋势共振) score.

        Iter 9: added sector_weakness_penalty (-0.03), restored buildup at 0.02.
        Net: sector_weakness lighter than smart to preserve trend capture.
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accum_component"),
                (pl.col("ma_alignment_score").fill_null(50.0)).alias("alignment_component"),
                (pl.col("close_strength").fill_null(0.5) * 100).alias("close_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_consistency_score").fill_null(50.0)).alias("consistency_component"),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (pl.col("momentum_quality_ratio").fill_null(50.0)).alias(
                    "momentum_quality_component"
                ),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                ((-pl.col("sector_momentum_5d").fill_null(0.0).clip(-5.0, 0.0)) / 5.0 * 100)
                .fill_null(0.0)
                .alias("sector_weakness_penalty"),
            ]
        )

        raw_score = (
            pl.col("accum_component") * 0.05
            + pl.col("alignment_component") * 0.10
            + pl.col("close_component") * 0.10
            + pl.col("trend_quality_component") * 0.10
            + pl.col("consistency_component") * 0.05
            + pl.col("anti_climax_component") * 0.15
            + pl.col("consolidation_component") * 0.10
            + pl.col("momentum_quality_component") * 0.10
            + pl.col("buildup_component") * 0.02
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("resistance_penalty") * 0.10
            - pl.col("sector_weakness_penalty") * 0.03
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("trend_score")]
        )

        return df

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
