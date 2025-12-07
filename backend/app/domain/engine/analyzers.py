"""Custom analyzers for backtest metrics calculation."""

from typing import Dict, Any, List
import backtrader as bt


class PerformanceAnalyzer(bt.Analyzer):
    """
    Comprehensive performance analyzer that calculates all key metrics.
    """

    def __init__(self):
        self.trades = []
        self.equity_curve = []
        self.starting_value = 0.0
        self.peak_value = 0.0

    def start(self):
        self.starting_value = self.strategy.broker.getvalue()
        self.peak_value = self.starting_value

    def notify_trade(self, trade):
        if trade.isclosed:
            self.trades.append({
                'ref': trade.ref,
                'size': trade.size,
                'price': trade.price,
                'pnl': trade.pnl,
                'pnlcomm': trade.pnlcomm,
                'commission': trade.commission,
                'baropen': len(self.strategy),
                'barclose': len(self.strategy),
                'dtopen': str(trade.open_datetime()),
                'dtclose': str(trade.close_datetime()),
            })

    def next(self):
        current_value = self.strategy.broker.getvalue()
        current_date = self.strategy.datetime.date(0)

        self.equity_curve.append({
            'date': str(current_date),
            'value': current_value,
        })

        if current_value > self.peak_value:
            self.peak_value = current_value

    def get_analysis(self) -> Dict[str, Any]:
        final_value = self.strategy.broker.getvalue()
        total_return = (final_value - self.starting_value) / self.starting_value

        # Calculate drawdown
        max_drawdown = 0.0
        if self.equity_curve:
            peak = self.starting_value
            for point in self.equity_curve:
                value = point['value']
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak if peak > 0 else 0
                max_drawdown = max(max_drawdown, drawdown)

        # Trade statistics
        total_trades = len(self.trades)
        winning_trades = sum(1 for t in self.trades if t['pnlcomm'] > 0)
        losing_trades = sum(1 for t in self.trades if t['pnlcomm'] < 0)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0.0

        # Profit metrics
        gross_profit = sum(t['pnlcomm'] for t in self.trades if t['pnlcomm'] > 0)
        gross_loss = abs(sum(t['pnlcomm'] for t in self.trades if t['pnlcomm'] < 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        return {
            'starting_value': self.starting_value,
            'final_value': final_value,
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
            'profit_factor': profit_factor,
            'equity_curve': self.equity_curve,
            'trades': self.trades,
        }


class SharpeRatioAnalyzer(bt.Analyzer):
    """
    Calculate Sharpe Ratio using daily returns.
    """

    params = (
        ('risk_free_rate', 0.02),  # Annual risk-free rate (2%)
        ('trading_days', 252),
    )

    def __init__(self):
        self.returns = []
        self.prev_value = None

    def start(self):
        self.prev_value = self.strategy.broker.getvalue()

    def next(self):
        current_value = self.strategy.broker.getvalue()
        if self.prev_value and self.prev_value > 0:
            daily_return = (current_value - self.prev_value) / self.prev_value
            self.returns.append(daily_return)
        self.prev_value = current_value

    def get_analysis(self) -> Dict[str, float]:
        if len(self.returns) < 2:
            return {'sharpe_ratio': 0.0, 'annual_return': 0.0, 'annual_volatility': 0.0}

        import numpy as np

        returns = np.array(self.returns)
        mean_return = np.mean(returns)
        std_return = np.std(returns, ddof=1)

        # Annualize
        annual_return = mean_return * self.p.trading_days
        annual_volatility = std_return * np.sqrt(self.p.trading_days)

        # Sharpe Ratio
        if annual_volatility > 0:
            sharpe = (annual_return - self.p.risk_free_rate) / annual_volatility
        else:
            sharpe = 0.0

        return {
            'sharpe_ratio': sharpe,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
        }


class DrawdownAnalyzer(bt.Analyzer):
    """
    Detailed drawdown analysis.
    """

    def __init__(self):
        self.peak = 0.0
        self.drawdowns = []
        self.current_dd_start = None
        self.current_dd_peak = 0.0

    def start(self):
        self.peak = self.strategy.broker.getvalue()
        self.current_dd_peak = self.peak

    def next(self):
        value = self.strategy.broker.getvalue()
        date = self.strategy.datetime.date(0)

        if value > self.peak:
            # New peak - close any existing drawdown
            if self.current_dd_start is not None:
                self.drawdowns.append({
                    'start_date': str(self.current_dd_start),
                    'end_date': str(date),
                    'peak_value': self.current_dd_peak,
                    'trough_value': self.peak - (self.peak * self._current_max_dd),
                    'max_drawdown': self._current_max_dd,
                })
                self.current_dd_start = None

            self.peak = value
            self.current_dd_peak = value
            self._current_max_dd = 0.0
        else:
            # In drawdown
            if self.current_dd_start is None:
                self.current_dd_start = date
                self._current_max_dd = 0.0

            dd = (self.peak - value) / self.peak if self.peak > 0 else 0
            self._current_max_dd = max(getattr(self, '_current_max_dd', 0), dd)

    def get_analysis(self) -> Dict[str, Any]:
        max_dd = max((d['max_drawdown'] for d in self.drawdowns), default=0.0)
        avg_dd = sum(d['max_drawdown'] for d in self.drawdowns) / len(self.drawdowns) if self.drawdowns else 0.0

        return {
            'max_drawdown': max_dd,
            'average_drawdown': avg_dd,
            'drawdown_count': len(self.drawdowns),
            'drawdown_details': self.drawdowns[:10],  # Keep top 10
        }


class TradeAnalyzer(bt.Analyzer):
    """
    Detailed trade-by-trade analysis.
    """

    def __init__(self):
        self.trades = []
        self.open_trades = {}

    def notify_trade(self, trade):
        if trade.isopen:
            self.open_trades[trade.ref] = {
                'ref': trade.ref,
                'data': trade.data._name,
                'size': trade.size,
                'price': trade.price,
                'value': trade.value,
                'open_datetime': str(trade.open_datetime()) if trade.open_datetime() else None,
            }
        elif trade.isclosed:
            open_info = self.open_trades.pop(trade.ref, {})
            # Determine direction safely - check if history exists
            if trade.history and len(trade.history) > 0:
                direction = 'long' if trade.history[0].event.size > 0 else 'short'
            else:
                # Fallback: infer from trade size (positive = was long, negative = was short)
                direction = 'long' if trade.size >= 0 else 'short'

            open_datetime = open_info.get('open_datetime')
            close_datetime = str(trade.close_datetime()) if trade.close_datetime() else None
            open_price = open_info.get('price', trade.price)
            close_price = trade.price

            self.trades.append({
                'ref': trade.ref,
                'data': trade.data._name,
                'direction': direction,
                'size': abs(trade.size),
                # Original fields (backward compatibility)
                'open_price': open_price,
                'close_price': close_price,
                'open_datetime': open_datetime,
                'close_datetime': close_datetime,
                # Frontend-compatible fields
                'type': direction.upper(),  # 'LONG' or 'SHORT'
                'entry_price': open_price,
                'exit_price': close_price,
                'entry_date': open_datetime.split(' ')[0] if open_datetime else None,
                'exit_date': close_datetime.split(' ')[0] if close_datetime else None,
                # PnL fields
                'pnl': trade.pnl,
                'pnl_percent': (trade.pnl / open_info.get('value', 1)) if open_info.get('value') else 0,
                'commission': trade.commission,
                'net_pnl': trade.pnlcomm,
                'bars_held': trade.barclose - trade.baropen,
            })

    def get_analysis(self) -> Dict[str, Any]:
        if not self.trades:
            return {
                'total_trades': 0,
                'long_trades': 0,
                'short_trades': 0,
                'avg_bars_held': 0,
                'avg_pnl': 0,
                'avg_pnl_percent': 0,
                'best_trade': None,
                'worst_trade': None,
                'trade_details': [],
            }

        long_trades = [t for t in self.trades if t['direction'] == 'long']
        short_trades = [t for t in self.trades if t['direction'] == 'short']

        return {
            'total_trades': len(self.trades),
            'long_trades': len(long_trades),
            'short_trades': len(short_trades),
            'avg_bars_held': sum(t['bars_held'] for t in self.trades) / len(self.trades),
            'avg_pnl': sum(t['net_pnl'] for t in self.trades) / len(self.trades),
            'avg_pnl_percent': sum(t['pnl_percent'] for t in self.trades) / len(self.trades),
            'best_trade': max(self.trades, key=lambda t: t['net_pnl']),
            'worst_trade': min(self.trades, key=lambda t: t['net_pnl']),
            'trade_details': self.trades,
        }


class CustomAnalyzers:
    """Factory class for adding all custom analyzers to a cerebro instance."""

    @staticmethod
    def add_all(cerebro: bt.Cerebro) -> bt.Cerebro:
        """Add all custom analyzers to the cerebro instance."""
        cerebro.addanalyzer(PerformanceAnalyzer, _name='performance')
        cerebro.addanalyzer(SharpeRatioAnalyzer, _name='sharpe')
        cerebro.addanalyzer(DrawdownAnalyzer, _name='drawdown')
        cerebro.addanalyzer(TradeAnalyzer, _name='trades')
        return cerebro

    @staticmethod
    def extract_results(strategy) -> Dict[str, Any]:
        """Extract all analyzer results from a completed strategy."""
        results = {}

        for analyzer_name in ['performance', 'sharpe', 'drawdown', 'trades']:
            try:
                analyzer = strategy.analyzers.getbyname(analyzer_name)
                results[analyzer_name] = analyzer.get_analysis()
            except Exception:
                results[analyzer_name] = {}

        return results
