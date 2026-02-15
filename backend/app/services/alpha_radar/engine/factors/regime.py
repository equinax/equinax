"""Market Regime Factor.

Thin wrapper around ScoringEngine._apply_regime_discount().
The regime factor adjusts all strategy scores based on macro market conditions.

Regime Score Ranges (from polars_engine market regime calculation):
  - >= 60: Strong bull → boost scores up to +25%
  - 40-60: Neutral → mild adjustment (±15%)
  - 30-40: Weak → moderate penalty (-15% to -25%)
  - < 30:  Very weak → steep penalty (-25% to -40%)

The actual discount logic lives in ScoringEngine._apply_regime_discount() and is
inherited by all strategy scoring engines. This module documents the factor
and provides the shared abstain threshold logic.

Abstain Logic (from polars_engine.load_market_regime):
  When market signals are hostile across multiple dimensions (breadth, momentum,
  volume), the system abstains from recommending any stocks. This is a binary
  gate, not a scoring adjustment.
"""


def get_regime_description(regime_score: float) -> str:
    """Get human-readable regime description."""
    if regime_score >= 70:
        return "强势上涨"
    elif regime_score >= 60:
        return "偏强震荡"
    elif regime_score >= 50:
        return "中性偏强"
    elif regime_score >= 40:
        return "中性偏弱"
    elif regime_score >= 30:
        return "偏弱震荡"
    else:
        return "弱势下跌"
