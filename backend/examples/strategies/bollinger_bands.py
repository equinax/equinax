"""
Bollinger Bands Strategy - 布林带均值回归策略

This is a mean-reversion strategy that uses Bollinger Bands to identify
overbought and oversold conditions based on statistical standard deviations.

Strategy Logic:
- Buy when price drops below lower band (oversold)
- Sell when price returns to middle band (mean reversion)

Parameters:
- period (default: 20): Period for moving average and standard deviation
- devfactor (default: 2.0): Number of standard deviations for bands

Best suited for:
- Range-bound markets
- High volatility environments
- Stocks with regular mean-reverting behavior
"""

import backtrader as bt


class BollingerBandsStrategy(bt.Strategy):
    """Bollinger Bands Mean Reversion Strategy"""

    params = (
        ('period', 20),
        ('devfactor', 2.0),
    )

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.period,
            devfactor=self.p.devfactor,
        )

    def next(self):
        if not self.position:
            if self.data.close < self.boll.lines.bot:
                self.buy()
        else:
            if self.data.close > self.boll.lines.mid:
                self.close()


# Strategy code as string for database storage
STRATEGY_CODE = '''
class BollingerBandsStrategy(bt.Strategy):
    """Bollinger Bands Mean Reversion Strategy"""

    params = (
        ('period', 20),
        ('devfactor', 2.0),
    )

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.period,
            devfactor=self.p.devfactor,
        )

    def next(self):
        if not self.position:
            if self.data.close < self.boll.lines.bot:
                self.buy()
        else:
            if self.data.close > self.boll.lines.mid:
                self.close()
'''

# Default parameters
DEFAULT_PARAMS = {
    'period': 20,
    'devfactor': 2.0,
}

# Strategy metadata
METADATA = {
    'name': 'Bollinger Bands Strategy',
    'name_zh': '布林带均值回归策略',
    'type': 'mean_reversion',
    'description': 'A mean-reversion strategy using Bollinger Bands for entry and exit',
    'description_zh': '基于布林带的均值回归策略，当价格跌破下轨时买入，回归中轨时卖出',
    'indicators': ['BollingerBands', 'SMA', 'StandardDeviation'],
}
