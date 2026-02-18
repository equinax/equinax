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

        cfg = load_strategy_config("weekly")
        return score_from_config(cfg, df, self._apply_regime_discount)
