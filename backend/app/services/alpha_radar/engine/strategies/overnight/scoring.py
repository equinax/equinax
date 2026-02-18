import polars as pl

from app.services.alpha_radar.engine.config_loader import load_strategy_config, score_from_config
from app.services.alpha_radar.scoring import ScoringEngine


class OvernightScoringEngine(ScoringEngine):
    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        if self.config_mode:
            cfg = load_strategy_config("overnight")
            return score_from_config(cfg, df, self._apply_regime_discount)

        raw_score = (
            pl.col("close_strength").fill_null(0.5) * 100 * 0.20
            + pl.col("trend_quality_20d").fill_null(50.0) * 0.15
            + pl.col("positive_day_ratio_20d").fill_null(0.5) * 100 * 0.10
            + pl.col("accumulation_score").fill_null(0.0) * 100 * 0.10
            + pl.col("volume_consistency_score").fill_null(50.0) * 0.10
            + (1 - pl.col("climax_score").fill_null(0.0)) * 100 * 0.10
            + pl.col("stability_score").fill_null(50.0) * 0.10
            + ((pl.col("return_5d").fill_null(0.0) + 5.0) / 10.0).clip(0.0, 1.0) * 100 * 0.05
            - (pl.col("upper_shadow_ratio").fill_null(0.0) * 5.0).clip(0.0, 1.0) * 100 * 0.08
            - pl.col("recent_vol_spike_max").fill_null(0.0) * 0.06
            - pl.col("resistance_proximity_penalty").fill_null(0.0) * 0.06
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("overnight_score")]
        )

        return df
