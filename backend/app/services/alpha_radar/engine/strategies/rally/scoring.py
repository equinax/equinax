"""Rally (主升浪) scoring engine — main uptrend, sector rotation.

Iter 6: Rally-specific abstain when breadth_5d_avg < 40% (narrow market).
Applied in backtest and screener_service.py, NOT in scoring formula.
Iter 3b scoring formula (Iter 2 + RALLY_MIN_SCORE=55) retained.
Iter 4/5 tested elg_net, value, stability as scoring components — all regressed.

Do NOT modify weights or logic without updating theory.yaml and running backtest.
"""

import polars as pl

from app.services.alpha_radar.engine.config_loader import load_strategy_config, score_from_config
from app.services.alpha_radar.scoring import ScoringEngine

RALLY_MIN_SCORE = 55.0


class RallyScoringEngine(ScoringEngine):
    """Scoring engine for the Rally (主升浪) strategy tab."""

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        cfg = load_strategy_config("rally")
        return score_from_config(cfg, df, self._apply_regime_discount)
