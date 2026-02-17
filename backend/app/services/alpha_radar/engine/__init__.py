"""Alpha Radar Recommendation Engine.

Layered architecture:
  Layer 1 (factors/): Market-wide factors that apply across all strategies
  Layer 2 (strategies/): Strategy-specific scoring engines

Entry point: score_tab(tab, df, market_regime_score) -> (df, score_col)
"""

from typing import Literal, Tuple

import polars as pl

from app.services.alpha_radar.engine.config import STRATEGIES, ScreenerTabKey

# Lazy imports to avoid circular dependencies
_strategy_engines = {}


def _get_strategy_engine(tab: ScreenerTabKey):
    """Lazy-load strategy scoring engines."""
    if tab not in _strategy_engines:
        if tab == "weekly":
            from app.services.alpha_radar.engine.strategies.weekly.scoring import (
                WeeklyScoringEngine,
            )

            _strategy_engines[tab] = WeeklyScoringEngine
        elif tab == "rally":
            from app.services.alpha_radar.engine.strategies.rally.scoring import (
                RallyScoringEngine,
            )

            _strategy_engines[tab] = RallyScoringEngine
        elif tab == "dragon":
            from app.services.alpha_radar.engine.strategies.dragon.scoring import (
                DragonScoringEngine,
            )

            _strategy_engines[tab] = DragonScoringEngine
        else:
            raise ValueError(f"Unknown tab: {tab}")
    return _strategy_engines[tab]


def score_tab(
    tab: ScreenerTabKey,
    df: pl.DataFrame,
    market_regime_score: float = 50.0,
    config_mode: bool = False,
) -> Tuple[pl.DataFrame, str]:
    """Score a DataFrame for a given strategy tab.

    Args:
        tab: Strategy tab key ("weekly", "rally", "dragon")
        df: DataFrame with technical indicators already computed
        market_regime_score: Current market regime score (0-100)
        config_mode: If True, use YAML config-driven scoring instead of hardcoded

    Returns:
        Tuple of (scored DataFrame, score column name)
    """
    config = STRATEGIES[tab]
    engine_cls = _get_strategy_engine(tab)
    engine = engine_cls(market_regime_score=market_regime_score, config_mode=config_mode)

    scored_df = engine.score(df)

    return scored_df, config.score_column
