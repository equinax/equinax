import re
from datetime import date
from typing import List, Dict, Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.stock_tracker import BaostockMinuteCache


def ts_code_to_bs(ts_code: str) -> str:
    code, exchange = ts_code.split(".")
    return f"{exchange.lower()}.{code}"


def bs_code_to_ts(bs_code: str) -> str:
    exchange, code = bs_code.split(".")
    return f"{code}.{exchange.upper()}"


async def get_cached_minute_data(
    db: AsyncSession,
    ts_code: str,
    trade_date: date,
    frequency: str = "5",
) -> Optional[List[Dict[str, Any]]]:
    result = await db.execute(
        select(BaostockMinuteCache).where(
            BaostockMinuteCache.ts_code == ts_code,
            BaostockMinuteCache.trade_date == trade_date,
            BaostockMinuteCache.frequency == frequency,
        )
    )
    cache = result.scalar_one_or_none()
    if cache:
        return cache.candles
    return None


async def store_minute_data(
    db: AsyncSession,
    ts_code: str,
    trade_date: date,
    frequency: str,
    candles: List[Dict[str, Any]],
) -> BaostockMinuteCache:
    existing = await db.execute(
        select(BaostockMinuteCache).where(
            BaostockMinuteCache.ts_code == ts_code,
            BaostockMinuteCache.trade_date == trade_date,
            BaostockMinuteCache.frequency == frequency,
        )
    )
    cache = existing.scalar_one_or_none()
    if cache:
        cache.candles = candles
    else:
        cache = BaostockMinuteCache(
            ts_code=ts_code,
            trade_date=trade_date,
            frequency=frequency,
            candles=candles,
        )
        db.add(cache)
    await db.flush()
    return cache


def fetch_minute_data_from_baostock(
    ts_code: str,
    trade_date: date,
    frequency: str = "5",
) -> List[Dict[str, Any]]:
    try:
        import baostock as bs
    except ImportError:
        raise RuntimeError("baostock not installed. Run: pip install baostock")

    bs.login()
    try:
        bs_code = ts_code_to_bs(ts_code)
        date_str = trade_date.strftime("%Y-%m-%d")
        rs = bs.query_history_k_data_plus(
            bs_code,
            "time,open,high,low,close,volume",
            start_date=date_str,
            end_date=date_str,
            frequency=frequency,
            adjustflag="2",
        )

        candles: List[Dict[str, Any]] = []
        while (rs.error_code == "0") and rs.next():
            row = rs.get_row_data()
            raw_time = row[0]
            time_match = re.match(r"\d{8}(\d{2})(\d{2})\d+", raw_time)
            time_str = f"{time_match.group(1)}:{time_match.group(2)}" if time_match else raw_time

            candles.append(
                {
                    "time": time_str,
                    "open": float(row[1]) if row[1] else 0,
                    "high": float(row[2]) if row[2] else 0,
                    "low": float(row[3]) if row[3] else 0,
                    "close": float(row[4]) if row[4] else 0,
                    "volume": int(float(row[5])) if row[5] else 0,
                }
            )
        return candles
    finally:
        bs.logout()


async def get_or_fetch_minute_data(
    db: AsyncSession,
    ts_code: str,
    trade_date: date,
    frequency: str = "5",
) -> List[Dict[str, Any]]:
    cached = await get_cached_minute_data(db, ts_code, trade_date, frequency)
    if cached:
        return cached

    candles = fetch_minute_data_from_baostock(ts_code, trade_date, frequency)
    if candles:
        await store_minute_data(db, ts_code, trade_date, frequency, candles)
        await db.commit()
    return candles
