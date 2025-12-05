"""
SMA Crossover Strategy - 双均线交叉策略

This is a classic trend-following strategy that uses two Simple Moving Averages
(fast and slow) to generate trading signals.

Strategy Logic:
- Buy when fast MA crosses above slow MA (golden cross)
- Sell when fast MA crosses below slow MA (death cross)

Parameters:
- fast_period (default: 10): Period for fast moving average
- slow_period (default: 30): Period for slow moving average

Best suited for:
- Trending markets
- Medium to long-term trading
- Stocks with clear trend patterns
"""

import backtrader as bt


class SMACrossover(bt.Strategy):
    """Simple Moving Average Crossover Strategy"""

    params = (
        ('fast_period', 10),
        ('slow_period', 30),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()


# Strategy code as string for database storage
STRATEGY_CODE = '''
class SMACrossover(bt.Strategy):
    """Simple Moving Average Crossover Strategy"""

    params = (
        ('fast_period', 10),
        ('slow_period', 30),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_period)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()
'''

# Default parameters
DEFAULT_PARAMS = {
    'fast_period': 10,
    'slow_period': 30,
}

# Strategy metadata
METADATA = {
    'name': 'SMA Crossover',
    'name_zh': '双均线交叉策略',
    'type': 'trend_following',
    'description': 'A classic trend-following strategy using two moving averages',
    'description_zh': '使用快慢两条均线的金叉死叉信号进行交易',
    'indicators': ['SMA', 'CrossOver'],
}
