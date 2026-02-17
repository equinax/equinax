"""Weekly (周内短线) scoring engine — stable T+6 profit plays.

Conservative formula targeting stable 5-day profit with emphasis on
low volatility, high consistency, and avoiding chasing.
"""

import polars as pl

from app.services.alpha_radar.engine.config_loader import load_strategy_config, score_from_config
from app.services.alpha_radar.scoring import ScoringEngine


class WeeklyScoringEngine(ScoringEngine):
    """Scoring engine for the Weekly (周内短线) strategy tab.

    Inherits _ensure_columns() and _apply_regime_discount() from ScoringEngine.
    """

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate weekly_score for all rows in df."""
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        if self.config_mode:
            cfg = load_strategy_config("weekly")
            return score_from_config(cfg, df, self._apply_regime_discount)

        df = df.with_columns(
            [
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("volume_consistency_score").fill_null(50.0)).alias("consistency_component"),
                (pl.col("post_spike_consolidation").fill_null(50.0)).alias(
                    "consolidation_component"
                ),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
                (pl.col("momentum_quality_ratio").fill_null(50.0)).alias(
                    "momentum_quality_component"
                ),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
                (((pl.col("pct_chg").fill_null(0.0).abs() - 3.0).clip(0.0, 4.0) / 4.0) * 100).alias(
                    "surge_penalty"
                ),
            ]
        )

        raw_score = (
            pl.col("anti_climax_component") * 0.20
            + pl.col("consistency_component") * 0.15
            + pl.col("consolidation_component") * 0.15
            + pl.col("trend_quality_component") * 0.10
            + pl.col("buildup_component") * 0.10
            + pl.col("accumulation_component") * 0.10
            + pl.col("momentum_quality_component") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("resistance_penalty") * 0.10
            - pl.col("surge_penalty") * 0.05
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("weekly_score")]
        )

        return df
