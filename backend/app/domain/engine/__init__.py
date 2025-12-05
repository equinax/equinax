"""Backtrader engine integration module."""

from .backtrader_engine import BacktraderEngine, BacktestConfig, BacktestResult
from .data_feed import PostgreSQLDataFeed, AdjustedDataFeed
from .strategy_loader import StrategyLoader, StrategyValidationError, STRATEGY_TEMPLATES
from .analyzers import CustomAnalyzers

__all__ = [
    "BacktraderEngine",
    "BacktestConfig",
    "BacktestResult",
    "PostgreSQLDataFeed",
    "AdjustedDataFeed",
    "StrategyLoader",
    "StrategyValidationError",
    "STRATEGY_TEMPLATES",
    "CustomAnalyzers",
]
