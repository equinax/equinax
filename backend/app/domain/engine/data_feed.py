"""PostgreSQL data feed for Backtrader."""

import logging
from datetime import datetime, date
from typing import List, Optional, Union
import pandas as pd
import backtrader as bt

logger = logging.getLogger(__name__)


class PostgreSQLDataFeed(bt.feeds.PandasData):
    """
    Backtrader data feed that accepts pre-loaded pandas DataFrame from PostgreSQL.

    Expected DataFrame columns:
    - date: datetime
    - open: float
    - high: float
    - low: float
    - close: float
    - volume: float
    - amount: float (optional)
    - turn: float (optional, turnover rate)
    - pctChg: float (optional, percent change)
    """

    # Add custom lines for additional data
    lines = ('amount', 'turn', 'pctChg',)

    params = (
        ('datetime', 'date'),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'volume'),
        ('openinterest', -1),  # Not used for stocks
        ('amount', 'amount'),
        ('turn', 'turn'),
        ('pctChg', 'pctChg'),
    )

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        stock_code: str,
        start_date: Optional[Union[datetime, date]] = None,
        end_date: Optional[Union[datetime, date]] = None,
    ) -> 'PostgreSQLDataFeed':
        """
        Create a data feed from a pandas DataFrame.

        Args:
            df: DataFrame with OHLCV data
            stock_code: Stock code for naming
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            PostgreSQLDataFeed instance
        """
        # Ensure date column is datetime
        if 'date' not in df.columns:
            raise ValueError("DataFrame must have 'date' column")

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_index()

        # Filter by date range (convert date to datetime for comparison)
        if start_date:
            start_dt = pd.Timestamp(start_date)
            df = df[df.index >= start_dt]
        if end_date:
            end_dt = pd.Timestamp(end_date)
            df = df[df.index <= end_dt]

        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"DataFrame missing required column: {col}")

        # Add optional columns with NaN if not present
        optional_cols = ['amount', 'turn', 'pctChg']
        for col in optional_cols:
            if col not in df.columns:
                df[col] = float('nan')

        return cls(
            dataname=df,
            name=stock_code,
            datetime=None,  # Use index
            open='open',
            high='high',
            low='low',
            close='close',
            volume='volume',
            openinterest=-1,
        )


class AdjustedDataFeed(PostgreSQLDataFeed):
    """
    Data feed with price adjustment support (forward/backward adjust).

    Uses dynamic base point: normalizes adjustment factors relative to
    the backtest start date, so prices at start date ≈ original prices.
    """

    @classmethod
    def from_dataframe_with_adjust(
        cls,
        df: pd.DataFrame,
        adjust_factors: pd.DataFrame,
        stock_code: str,
        adjust_type: str = 'backward',  # 'backward' (recommended), 'forward', 'none'
        start_date: Optional[Union[datetime, date]] = None,
        end_date: Optional[Union[datetime, date]] = None,
    ) -> 'AdjustedDataFeed':
        """
        Create a data feed with price adjustment using dynamic base point.

        The adjustment factors are normalized relative to the start_date,
        so that prices at start_date equal original prices (factor ≈ 1.0).
        This ensures correct position sizing when using real capital amounts.

        Args:
            df: DataFrame with OHLCV data
            adjust_factors: DataFrame with date, foreAdjustFactor, backAdjustFactor
                           (cumulative factors from IPO to each dividend date)
            stock_code: Stock code for naming
            adjust_type: Type of adjustment ('forward', 'backward', 'none')
            start_date: Optional start date filter (also used as base point)
            end_date: Optional end date filter

        Returns:
            AdjustedDataFeed instance
        """
        if adjust_type == 'none':
            return cls.from_dataframe(df, stock_code, start_date, end_date)

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])

        if not adjust_factors.empty:
            adjust_factors = adjust_factors.copy()
            adjust_factors['date'] = pd.to_datetime(adjust_factors['date'])

            # Select factor column based on adjustment type
            factor_col = 'foreAdjustFactor' if adjust_type == 'forward' else 'backAdjustFactor'

            # Sort by date for proper lookup
            adjust_factors = adjust_factors.sort_values('date')

            # === Dynamic Base Point ===
            # Determine the base date (backtest start date)
            if start_date:
                base_date = pd.to_datetime(start_date)
            else:
                base_date = df['date'].min()

            logger.info(f"[AdjustFactor] stock_code={stock_code}, adjust_type={adjust_type}")
            logger.info(f"[AdjustFactor] base_date={base_date}")
            logger.info(f"[AdjustFactor] adjust_factors:\n{adjust_factors[[factor_col, 'date']].to_string()}")

            # Find the most recent factor before or on the base date
            factors_before_start = adjust_factors[adjust_factors['date'] <= base_date]

            logger.info(f"[AdjustFactor] factors_before_start count={len(factors_before_start)}")

            if not factors_before_start.empty:
                # Use the most recent factor before start date as base
                base_factor = factors_before_start.iloc[-1][factor_col]
                logger.info(f"[AdjustFactor] Using factor before start: {base_factor} from {factors_before_start.iloc[-1]['date']}")
            else:
                # No dividend records before start date
                # Use the first available factor, or 1.0 if none
                if not adjust_factors.empty:
                    base_factor = adjust_factors.iloc[0][factor_col]
                    logger.info(f"[AdjustFactor] No factors before start, using first factor: {base_factor}")
                else:
                    base_factor = 1.0
                    logger.info(f"[AdjustFactor] No factors at all, using 1.0")

            # Normalize: all factors divided by base factor
            # Result: factor at start_date ≈ 1.0, later dividends show as factor > 1.0
            adjust_factors['normalized_factor'] = adjust_factors[factor_col] / base_factor

            logger.info(f"[AdjustFactor] base_factor={base_factor}")
            logger.info(f"[AdjustFactor] normalized_factors:\n{adjust_factors[['date', 'normalized_factor']].to_string()}")

            # Merge factors to price data using merge_asof (backward fill)
            # Each date gets the most recent factor from dividend dates
            df = df.sort_values('date')
            df = pd.merge_asof(
                df,
                adjust_factors[['date', 'normalized_factor']],
                on='date',
                direction='backward'
            )

            # Fill NaN (dates before first dividend) with 1.0
            df['normalized_factor'] = df['normalized_factor'].fillna(1.0)

            # Log first few rows after merge
            logger.info(f"[AdjustFactor] First 5 rows after merge:\n{df[['date', 'open', 'close', 'normalized_factor']].head().to_string()}")
            logger.info(f"[AdjustFactor] Last 5 rows after merge:\n{df[['date', 'open', 'close', 'normalized_factor']].tail().to_string()}")

            # Apply normalized factors to prices
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in df.columns:
                    df[col] = df[col] * df['normalized_factor']

            # Log prices after adjustment
            logger.info(f"[AdjustFactor] First 5 rows AFTER adjustment:\n{df[['date', 'open', 'close']].head().to_string()}")

            # Remove temporary column
            df = df.drop(columns=['normalized_factor'])

        return cls.from_dataframe(df, stock_code, start_date, end_date)
