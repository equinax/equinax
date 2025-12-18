"""Technical indicator calculation tasks for ARQ."""

from typing import Dict, Any, List, Optional
from datetime import date
from decimal import Decimal

import pandas as pd
import numpy as np
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import settings
from app.db.models.asset import MarketDaily
from app.db.models.indicator import TechnicalIndicator


# Create engine for worker
worker_engine = create_async_engine(
    settings.database_url,
    pool_size=5,
    max_overflow=5,
)
worker_session_maker = async_sessionmaker(
    worker_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


def calculate_ma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return series.rolling(window=period).mean()


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def calculate_macd(close: pd.Series) -> tuple:
    """Calculate MACD indicator."""
    ema12 = calculate_ema(close, 12)
    ema26 = calculate_ema(close, 26)
    dif = ema12 - ema26
    dea = calculate_ema(dif, 9)
    hist = (dif - dea) * 2
    return dif, dea, hist


def calculate_rsi(close: pd.Series, period: int) -> pd.Series:
    """Calculate RSI indicator."""
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_kdj(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9) -> tuple:
    """Calculate KDJ indicator."""
    low_n = low.rolling(window=n).min()
    high_n = high.rolling(window=n).max()
    rsv = (close - low_n) / (high_n - low_n) * 100

    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    j = 3 * k - 2 * d

    return k, d, j


def calculate_bollinger(close: pd.Series, period: int = 20, std: int = 2) -> tuple:
    """Calculate Bollinger Bands."""
    middle = calculate_ma(close, period)
    std_dev = close.rolling(window=period).std()
    upper = middle + (std_dev * std)
    lower = middle - (std_dev * std)
    return upper, middle, lower


async def calculate_indicators(
    ctx: dict,
    stock_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate and store technical indicators for a stock.

    Args:
        ctx: ARQ context
        stock_code: Stock code to calculate indicators for
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        Dict with calculation results
    """
    async with worker_session_maker() as db:
        # Load OHLCV data
        query = select(MarketDaily).where(MarketDaily.code == stock_code)

        if start_date:
            query = query.where(MarketDaily.date >= date.fromisoformat(start_date))
        if end_date:
            query = query.where(MarketDaily.date <= date.fromisoformat(end_date))

        query = query.order_by(MarketDaily.date)

        result = await db.execute(query)
        records = result.scalars().all()

        if not records:
            return {"error": "No data found", "stock_code": stock_code}

        # Convert to DataFrame
        df = pd.DataFrame([
            {
                "date": r.date,
                "open": float(r.open) if r.open else None,
                "high": float(r.high) if r.high else None,
                "low": float(r.low) if r.low else None,
                "close": float(r.close) if r.close else None,
                "volume": r.volume,
            }
            for r in records
        ])

        df = df.dropna(subset=["close"])
        if df.empty:
            return {"error": "No valid close prices", "stock_code": stock_code}

        # Calculate indicators
        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        # Moving Averages
        df["ma_5"] = calculate_ma(close, 5)
        df["ma_10"] = calculate_ma(close, 10)
        df["ma_20"] = calculate_ma(close, 20)
        df["ma_60"] = calculate_ma(close, 60)
        df["ma_120"] = calculate_ma(close, 120)
        df["ma_250"] = calculate_ma(close, 250)

        # EMA
        df["ema_12"] = calculate_ema(close, 12)
        df["ema_26"] = calculate_ema(close, 26)

        # MACD
        df["macd_dif"], df["macd_dea"], df["macd_hist"] = calculate_macd(close)

        # RSI
        df["rsi_6"] = calculate_rsi(close, 6)
        df["rsi_12"] = calculate_rsi(close, 12)
        df["rsi_24"] = calculate_rsi(close, 24)

        # KDJ
        df["kdj_k"], df["kdj_d"], df["kdj_j"] = calculate_kdj(high, low, close)

        # Bollinger Bands
        df["boll_upper"], df["boll_middle"], df["boll_lower"] = calculate_bollinger(close)

        # Volume MA
        df["vol_ma_5"] = volume.rolling(window=5).mean()
        df["vol_ma_10"] = volume.rolling(window=10).mean()

        # Delete existing indicators for this stock (if recalculating)
        if start_date or end_date:
            delete_query = delete(TechnicalIndicator).where(
                TechnicalIndicator.code == stock_code
            )
            if start_date:
                delete_query = delete_query.where(
                    TechnicalIndicator.date >= date.fromisoformat(start_date)
                )
            if end_date:
                delete_query = delete_query.where(
                    TechnicalIndicator.date <= date.fromisoformat(end_date)
                )
            await db.execute(delete_query)

        # Insert new indicators
        indicators_to_insert = []
        for _, row in df.iterrows():
            indicator = TechnicalIndicator(
                code=stock_code,
                date=row["date"],
                ma_5=Decimal(str(row["ma_5"])) if pd.notna(row["ma_5"]) else None,
                ma_10=Decimal(str(row["ma_10"])) if pd.notna(row["ma_10"]) else None,
                ma_20=Decimal(str(row["ma_20"])) if pd.notna(row["ma_20"]) else None,
                ma_60=Decimal(str(row["ma_60"])) if pd.notna(row["ma_60"]) else None,
                ma_120=Decimal(str(row["ma_120"])) if pd.notna(row["ma_120"]) else None,
                ma_250=Decimal(str(row["ma_250"])) if pd.notna(row["ma_250"]) else None,
                ema_12=Decimal(str(row["ema_12"])) if pd.notna(row["ema_12"]) else None,
                ema_26=Decimal(str(row["ema_26"])) if pd.notna(row["ema_26"]) else None,
                macd_dif=Decimal(str(row["macd_dif"])) if pd.notna(row["macd_dif"]) else None,
                macd_dea=Decimal(str(row["macd_dea"])) if pd.notna(row["macd_dea"]) else None,
                macd_hist=Decimal(str(row["macd_hist"])) if pd.notna(row["macd_hist"]) else None,
                rsi_6=Decimal(str(row["rsi_6"])) if pd.notna(row["rsi_6"]) else None,
                rsi_12=Decimal(str(row["rsi_12"])) if pd.notna(row["rsi_12"]) else None,
                rsi_24=Decimal(str(row["rsi_24"])) if pd.notna(row["rsi_24"]) else None,
                kdj_k=Decimal(str(row["kdj_k"])) if pd.notna(row["kdj_k"]) else None,
                kdj_d=Decimal(str(row["kdj_d"])) if pd.notna(row["kdj_d"]) else None,
                kdj_j=Decimal(str(row["kdj_j"])) if pd.notna(row["kdj_j"]) else None,
                boll_upper=Decimal(str(row["boll_upper"])) if pd.notna(row["boll_upper"]) else None,
                boll_middle=Decimal(str(row["boll_middle"])) if pd.notna(row["boll_middle"]) else None,
                boll_lower=Decimal(str(row["boll_lower"])) if pd.notna(row["boll_lower"]) else None,
                vol_ma_5=int(row["vol_ma_5"]) if pd.notna(row["vol_ma_5"]) else None,
                vol_ma_10=int(row["vol_ma_10"]) if pd.notna(row["vol_ma_10"]) else None,
            )
            indicators_to_insert.append(indicator)

        db.add_all(indicators_to_insert)
        await db.commit()

        return {
            "stock_code": stock_code,
            "records_processed": len(df),
            "indicators_created": len(indicators_to_insert),
        }
