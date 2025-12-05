"""Main Backtrader engine for executing backtests."""

import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import pandas as pd
import backtrader as bt

from .data_feed import PostgreSQLDataFeed, AdjustedDataFeed
from .strategy_loader import StrategyLoader, StrategyValidationError
from .analyzers import CustomAnalyzers


@dataclass
class BacktestConfig:
    """Configuration for a single backtest run."""
    initial_capital: float = 100000.0
    commission: float = 0.001  # 0.1% commission
    slippage_perc: float = 0.0005  # 0.05% slippage
    stake_type: str = 'percent'  # 'percent' or 'fixed'
    stake_value: float = 95.0  # 95% of portfolio or fixed amount
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    adjust_type: str = 'forward'  # 'forward', 'backward', 'none'


@dataclass
class BacktestResult:
    """Complete results from a backtest execution."""
    success: bool
    error_message: Optional[str] = None

    # Core metrics
    initial_capital: float = 0.0
    final_value: float = 0.0
    total_return: float = 0.0
    annual_return: float = 0.0

    # Risk metrics
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    annual_volatility: float = 0.0

    # Trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0

    # Detailed data
    equity_curve: List[Dict[str, Any]] = None
    trades: List[Dict[str, Any]] = None
    drawdown_details: List[Dict[str, Any]] = None

    # Execution info
    execution_time_ms: int = 0
    data_points: int = 0


class PercentSizer(bt.Sizer):
    """Position sizer using percentage of portfolio."""

    params = (('percents', 95),)

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            # Use percentage of current cash
            size = (cash * self.p.percents / 100) / data.close[0]
            return int(size)
        else:
            # Close full position
            return self.broker.getposition(data).size


class BacktraderEngine:
    """
    Main engine for executing backtests using Backtrader.

    Usage:
        engine = BacktraderEngine()
        result = engine.run(
            strategy_code="class MyStrategy(bt.Strategy): ...",
            data_df=df,
            stock_code="sh.600519",
            config=BacktestConfig(initial_capital=100000)
        )
    """

    @staticmethod
    def run(
        strategy_code: str,
        data_df: pd.DataFrame,
        stock_code: str,
        config: Optional[BacktestConfig] = None,
        parameters: Optional[Dict[str, Any]] = None,
        adjust_factors_df: Optional[pd.DataFrame] = None,
    ) -> BacktestResult:
        """
        Execute a backtest.

        Args:
            strategy_code: Python code defining a bt.Strategy subclass
            data_df: DataFrame with OHLCV data (date, open, high, low, close, volume)
            stock_code: Stock code for identification
            config: Backtest configuration
            parameters: Strategy parameter overrides
            adjust_factors_df: Optional adjustment factors for price adjustment

        Returns:
            BacktestResult with all metrics and trade details
        """
        import time
        start_time = time.time()

        config = config or BacktestConfig()

        try:
            # Load and validate strategy
            strategy_class = StrategyLoader.load_strategy(
                strategy_code,
                f"Strategy_{stock_code}",
                parameters
            )
        except StrategyValidationError as e:
            return BacktestResult(
                success=False,
                error_message=str(e),
            )

        try:
            # Create Cerebro instance
            cerebro = bt.Cerebro()

            # Add strategy
            cerebro.addstrategy(strategy_class)

            # Create data feed
            if adjust_factors_df is not None and not adjust_factors_df.empty:
                data_feed = AdjustedDataFeed.from_dataframe_with_adjust(
                    data_df,
                    adjust_factors_df,
                    stock_code,
                    adjust_type=config.adjust_type,
                    start_date=config.start_date,
                    end_date=config.end_date,
                )
            else:
                data_feed = PostgreSQLDataFeed.from_dataframe(
                    data_df,
                    stock_code,
                    start_date=config.start_date,
                    end_date=config.end_date,
                )

            cerebro.adddata(data_feed)

            # Set broker settings
            cerebro.broker.setcash(config.initial_capital)
            cerebro.broker.setcommission(commission=config.commission)

            # Add slippage
            if config.slippage_perc > 0:
                cerebro.broker.set_slippage_perc(config.slippage_perc)

            # Set position sizer
            if config.stake_type == 'percent':
                cerebro.addsizer(PercentSizer, percents=config.stake_value)
            else:
                cerebro.addsizer(bt.sizers.FixedSize, stake=int(config.stake_value))

            # Add analyzers
            CustomAnalyzers.add_all(cerebro)

            # Run backtest
            results = cerebro.run()

            execution_time = int((time.time() - start_time) * 1000)

            if not results:
                return BacktestResult(
                    success=False,
                    error_message="Backtest returned no results",
                    execution_time_ms=execution_time,
                )

            # Extract results from first strategy
            strategy = results[0]
            analyzer_results = CustomAnalyzers.extract_results(strategy)

            # Build result object
            perf = analyzer_results.get('performance', {})
            sharpe = analyzer_results.get('sharpe', {})
            drawdown = analyzer_results.get('drawdown', {})
            trades_analysis = analyzer_results.get('trades', {})

            return BacktestResult(
                success=True,
                initial_capital=config.initial_capital,
                final_value=perf.get('final_value', config.initial_capital),
                total_return=perf.get('total_return', 0.0),
                annual_return=sharpe.get('annual_return', 0.0),
                sharpe_ratio=sharpe.get('sharpe_ratio', 0.0),
                max_drawdown=perf.get('max_drawdown', 0.0),
                annual_volatility=sharpe.get('annual_volatility', 0.0),
                total_trades=perf.get('total_trades', 0),
                winning_trades=perf.get('winning_trades', 0),
                losing_trades=perf.get('losing_trades', 0),
                win_rate=perf.get('win_rate', 0.0),
                profit_factor=perf.get('profit_factor', 0.0),
                equity_curve=perf.get('equity_curve', []),
                trades=trades_analysis.get('trade_details', []),
                drawdown_details=drawdown.get('drawdown_details', []),
                execution_time_ms=execution_time,
                data_points=len(data_df),
            )

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            error_detail = f"Backtest execution failed: {str(e)}\n{traceback.format_exc()}"
            return BacktestResult(
                success=False,
                error_message=error_detail,
                execution_time_ms=execution_time,
            )

    @staticmethod
    def run_multi_stock(
        strategy_code: str,
        data_dict: Dict[str, pd.DataFrame],
        config: Optional[BacktestConfig] = None,
        parameters: Optional[Dict[str, Any]] = None,
        adjust_factors_dict: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> BacktestResult:
        """
        Execute a backtest with multiple stocks (portfolio backtest).

        Args:
            strategy_code: Python code defining a bt.Strategy subclass
            data_dict: Dictionary mapping stock_code -> DataFrame
            config: Backtest configuration
            parameters: Strategy parameter overrides
            adjust_factors_dict: Optional dict of adjustment factors per stock

        Returns:
            BacktestResult with combined metrics
        """
        import time
        start_time = time.time()

        config = config or BacktestConfig()

        try:
            strategy_class = StrategyLoader.load_strategy(
                strategy_code,
                "MultiStockStrategy",
                parameters
            )
        except StrategyValidationError as e:
            return BacktestResult(
                success=False,
                error_message=str(e),
            )

        try:
            cerebro = bt.Cerebro()
            cerebro.addstrategy(strategy_class)

            # Add all data feeds
            total_data_points = 0
            for stock_code, df in data_dict.items():
                adjust_df = None
                if adjust_factors_dict:
                    adjust_df = adjust_factors_dict.get(stock_code)

                if adjust_df is not None and not adjust_df.empty:
                    data_feed = AdjustedDataFeed.from_dataframe_with_adjust(
                        df,
                        adjust_df,
                        stock_code,
                        adjust_type=config.adjust_type,
                        start_date=config.start_date,
                        end_date=config.end_date,
                    )
                else:
                    data_feed = PostgreSQLDataFeed.from_dataframe(
                        df,
                        stock_code,
                        start_date=config.start_date,
                        end_date=config.end_date,
                    )

                cerebro.adddata(data_feed, name=stock_code)
                total_data_points += len(df)

            # Set broker settings
            cerebro.broker.setcash(config.initial_capital)
            cerebro.broker.setcommission(commission=config.commission)

            if config.slippage_perc > 0:
                cerebro.broker.set_slippage_perc(config.slippage_perc)

            if config.stake_type == 'percent':
                # Divide position across stocks
                per_stock_percent = config.stake_value / len(data_dict)
                cerebro.addsizer(PercentSizer, percents=per_stock_percent)
            else:
                cerebro.addsizer(bt.sizers.FixedSize, stake=int(config.stake_value))

            CustomAnalyzers.add_all(cerebro)

            results = cerebro.run()
            execution_time = int((time.time() - start_time) * 1000)

            if not results:
                return BacktestResult(
                    success=False,
                    error_message="Backtest returned no results",
                    execution_time_ms=execution_time,
                )

            strategy = results[0]
            analyzer_results = CustomAnalyzers.extract_results(strategy)

            perf = analyzer_results.get('performance', {})
            sharpe = analyzer_results.get('sharpe', {})
            drawdown = analyzer_results.get('drawdown', {})
            trades_analysis = analyzer_results.get('trades', {})

            return BacktestResult(
                success=True,
                initial_capital=config.initial_capital,
                final_value=perf.get('final_value', config.initial_capital),
                total_return=perf.get('total_return', 0.0),
                annual_return=sharpe.get('annual_return', 0.0),
                sharpe_ratio=sharpe.get('sharpe_ratio', 0.0),
                max_drawdown=perf.get('max_drawdown', 0.0),
                annual_volatility=sharpe.get('annual_volatility', 0.0),
                total_trades=perf.get('total_trades', 0),
                winning_trades=perf.get('winning_trades', 0),
                losing_trades=perf.get('losing_trades', 0),
                win_rate=perf.get('win_rate', 0.0),
                profit_factor=perf.get('profit_factor', 0.0),
                equity_curve=perf.get('equity_curve', []),
                trades=trades_analysis.get('trade_details', []),
                drawdown_details=drawdown.get('drawdown_details', []),
                execution_time_ms=execution_time,
                data_points=total_data_points,
            )

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            error_detail = f"Backtest execution failed: {str(e)}\n{traceback.format_exc()}"
            return BacktestResult(
                success=False,
                error_message=error_detail,
                execution_time_ms=execution_time,
            )

    @staticmethod
    def validate_strategy(code: str) -> Dict[str, Any]:
        """
        Validate strategy code without executing.

        Args:
            code: Python code string

        Returns:
            Validation result dict
        """
        return StrategyLoader.validate_code(code)
