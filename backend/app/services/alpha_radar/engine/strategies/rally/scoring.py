"""Rally (主升浪) scoring engine — main uptrend, sector rotation.

Iter 6: Rally-specific abstain when breadth_5d_avg < 40% (narrow market).
Applied in backtest and screener_service.py, NOT in scoring formula.
Iter 3b scoring formula (Iter 2 + RALLY_MIN_SCORE=55) retained.
Iter 4/5 tested elg_net, value, stability as scoring components — all regressed.

Do NOT modify weights or logic without updating theory.md and running backtest.
"""

import polars as pl

from app.services.alpha_radar.scoring import ScoringEngine

RALLY_MIN_SCORE = 55.0


class RallyScoringEngine(ScoringEngine):
    """Scoring engine for the Rally (主升浪) strategy tab."""

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        sector_mom_ok = pl.col("sector_momentum_5d").fill_null(0.0) >= -3.0

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
                (pl.col("sector_momentum_5d").fill_null(0.0).clip(-10.0, 10.0) * 5.0 + 50.0).alias(
                    "sector_momentum_component"
                ),
                ((1.0 - pl.col("size_percentile").fill_null(0.5)) * 100.0).alias(
                    "size_edge_component"
                ),
            ]
        )

        raw_score = (
            pl.col("alignment_component") * 0.10
            + pl.col("trend_quality_component") * 0.15
            + pl.col("buildup_component") * 0.15
            + pl.col("accumulation_component") * 0.10
            + pl.col("consolidation_component") * 0.15
            + pl.col("momentum_quality_component") * 0.10
            + pl.col("anti_climax_component") * 0.05
            + pl.col("sector_momentum_component") * 0.10
            + pl.col("size_edge_component") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
        )

        filtered_score = pl.when(sector_mom_ok).then(raw_score.clip(0.0, 100.0)).otherwise(0.0)

        df = df.with_columns([self._apply_regime_discount(filtered_score).alias("rally_score")])

        return df
