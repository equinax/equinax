"""PostgreSQL data feed for Backtrader."""

from datetime import datetime, date
from typing import List, Optional, Union
import pandas as pd
import backtrader as bt


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
        Create a data feed with price adjustment.

        Args:
            df: DataFrame with OHLCV data
            adjust_factors: DataFrame with date, foreAdjustFactor, backAdjustFactor
            stock_code: Stock code for naming
            adjust_type: Type of adjustment ('forward', 'backward', 'none')
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            AdjustedDataFeed instance
        """
        if adjust_type == 'none':
            return cls.from_dataframe(df, stock_code, start_date, end_date)

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])

        # Merge with adjustment factors
        if not adjust_factors.empty:
            adjust_factors = adjust_factors.copy()
            adjust_factors['date'] = pd.to_datetime(adjust_factors['date'])

            df = df.merge(adjust_factors[['date', 'foreAdjustFactor', 'backAdjustFactor']],
                         on='date', how='left')

            # Fill missing factors with 1.0
            df['foreAdjustFactor'] = df['foreAdjustFactor'].fillna(1.0)
            df['backAdjustFactor'] = df['backAdjustFactor'].fillna(1.0)

            # Apply adjustment
            if adjust_type == 'forward':
                factor_col = 'foreAdjustFactor'
            else:  # backward
                factor_col = 'backAdjustFactor'

            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in df.columns:
                    df[col] = df[col] * df[factor_col]

        return cls.from_dataframe(df, stock_code, start_date, end_date)
