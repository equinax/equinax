"""Rally (主升浪) scoring engine — large-cap main rally, sector rotation.

VERBATIM copy of ScoringEngine.calculate_main_rally_score() from scoring.py.
Do NOT modify weights or logic without updating theory.md and running backtest.
"""

import polars as pl

from app.services.alpha_radar.scoring import ScoringEngine


class RallyScoringEngine(ScoringEngine):
    """Scoring engine for the Rally (主升浪) strategy tab.

    Inherits _ensure_columns() and _apply_regime_discount() from ScoringEngine.
    """

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate rally_score for all rows in df.

        Stocks entering main rally phase — strong MA alignment, steady volume
        buildup, NOT at climax. Emphasizes trend quality and volume consistency
        over raw momentum to avoid chasing late-stage moves.
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("ma_alignment_score").fill_null(50.0)).alias("alignment_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (pl.col("momentum_quality_ratio").fill_null(50.0)).alias(
                    "momentum_quality_component"
                ),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
            ]
        )

        raw_score = (
            pl.col("alignment_component") * 0.20
            + pl.col("trend_quality_component") * 0.15
            + pl.col("buildup_component") * 0.15
            + pl.col("accumulation_component") * 0.10
            + pl.col("consolidation_component") * 0.10
            + pl.col("momentum_quality_component") * 0.10
            + pl.col("anti_climax_component") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("resistance_penalty") * 0.05
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("rally_score")]
        )

        return df
