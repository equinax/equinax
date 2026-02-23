import polars as pl

from app.services.alpha_radar.engine.config_loader import load_strategy_config, score_from_config
from app.services.alpha_radar.scoring import ScoringEngine

OVERNIGHT_MIN_SCORE = 30.0


class OvernightScoringEngine(ScoringEngine):
    def score(self, df: pl.DataFrame, version: str | None = None) -> pl.DataFrame:
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        cfg = load_strategy_config("overnight", version=version)
        return score_from_config(cfg, df, self._apply_regime_discount)
