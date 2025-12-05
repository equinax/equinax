"""
RSI Strategy - RSI 超买超卖策略

This is a classic mean-reversion strategy that uses the Relative Strength Index
to identify overbought and oversold conditions.

Strategy Logic:
- Buy when RSI drops below oversold level (default: 30)
- Sell when RSI rises above overbought level (default: 70)

Parameters:
- rsi_period (default: 14): Period for RSI calculation
- overbought (default: 70): RSI level indicating overbought condition
- oversold (default: 30): RSI level indicating oversold condition

Best suited for:
- Range-bound markets
- Mean-reverting assets
- Shorter-term trading
"""

import backtrader as bt


class RSIStrategy(bt.Strategy):
    """RSI Overbought/Oversold Strategy"""

    params = (
        ('rsi_period', 14),
        ('overbought', 70),
        ('oversold', 30),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.oversold:
                self.buy()
        else:
            if self.rsi > self.p.overbought:
                self.close()


# Strategy code as string for database storage
STRATEGY_CODE = '''
class RSIStrategy(bt.Strategy):
    """RSI Overbought/Oversold Strategy"""

    params = (
        ('rsi_period', 14),
        ('overbought', 70),
        ('oversold', 30),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)

    def next(self):
        if not self.position:
            if self.rsi < self.p.oversold:
                self.buy()
        else:
            if self.rsi > self.p.overbought:
                self.close()
'''

# Default parameters
DEFAULT_PARAMS = {
    'rsi_period': 14,
    'overbought': 70,
    'oversold': 30,
}

# Strategy metadata
METADATA = {
    'name': 'RSI Strategy',
    'name_zh': 'RSI 超买超卖策略',
    'type': 'mean_reversion',
    'description': 'A mean-reversion strategy based on RSI overbought/oversold levels',
    'description_zh': '基于RSI指标的超买超卖信号进行逆势交易',
    'indicators': ['RSI'],
}
