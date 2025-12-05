"""
Example Trading Strategies for Backtrader

This module contains classic trading strategy implementations that can be used
as examples or templates for developing custom strategies.

Available Strategies:
- SMACrossover: Simple Moving Average Crossover
- RSIStrategy: RSI Overbought/Oversold
- MACDStrategy: MACD Signal Crossover
- BollingerBandsStrategy: Bollinger Bands Mean Reversion

Usage:
    from backend.examples.strategies import sma_crossover

    # Read the strategy code
    with open('backend/examples/strategies/sma_crossover.py') as f:
        code = f.read()

    # Or import as module for reference
    from backend.examples.strategies.sma_crossover import STRATEGY_CODE
"""

__all__ = [
    'sma_crossover',
    'rsi_strategy',
    'macd_strategy',
    'bollinger_bands',
]
