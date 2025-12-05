"""
MACD Strategy - MACD 金叉死叉策略

This is a momentum-based strategy that uses the MACD (Moving Average Convergence
Divergence) indicator to generate trading signals.

Strategy Logic:
- Buy when MACD line crosses above signal line (bullish crossover)
- Sell when MACD line crosses below signal line (bearish crossover)

Parameters:
- fast_period (default: 12): Period for fast EMA
- slow_period (default: 26): Period for slow EMA
- signal_period (default: 9): Period for signal line

Best suited for:
- Trending markets with momentum
- Medium-term trading
- Stocks with clear momentum patterns
"""

import backtrader as bt


class MACDStrategy(bt.Strategy):
    """MACD Crossover Strategy"""

    params = (
        ('fast_period', 12),
        ('slow_period', 26),
        ('signal_period', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.fast_period,
            period_me2=self.p.slow_period,
            period_signal=self.p.signal_period,
        )

    def next(self):
        if not self.position:
            if self.macd.macd > self.macd.signal:
                self.buy()
        elif self.macd.macd < self.macd.signal:
            self.close()


# Strategy code as string for database storage
STRATEGY_CODE = '''
class MACDStrategy(bt.Strategy):
    """MACD Crossover Strategy"""

    params = (
        ('fast_period', 12),
        ('slow_period', 26),
        ('signal_period', 9),
    )

    def __init__(self):
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.p.fast_period,
            period_me2=self.p.slow_period,
            period_signal=self.p.signal_period,
        )

    def next(self):
        if not self.position:
            if self.macd.macd > self.macd.signal:
                self.buy()
        elif self.macd.macd < self.macd.signal:
            self.close()
'''

# Default parameters
DEFAULT_PARAMS = {
    'fast_period': 12,
    'slow_period': 26,
    'signal_period': 9,
}

# Strategy metadata
METADATA = {
    'name': 'MACD Strategy',
    'name_zh': 'MACD 金叉死叉策略',
    'type': 'momentum',
    'description': 'A momentum strategy based on MACD signal line crossovers',
    'description_zh': '基于MACD指标的金叉死叉信号进行趋势跟踪交易',
    'indicators': ['MACD', 'EMA'],
}
