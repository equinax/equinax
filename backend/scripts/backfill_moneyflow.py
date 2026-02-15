#!/usr/bin/env python3
"""
Backfill Moneyflow and Limit List Data

Usage:
    python -m scripts.backfill_moneyflow --start-date 2025-02-01 --end-date 2026-02-08
"""

import argparse
import asyncio
import logging
import os
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional

import asyncpg
import pandas as pd
import tushare as ts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default DB URL
DEFAULT_POSTGRES_URL = (
    os.environ.get("DATABASE_URL", "postgresql://quant:quant_dev_password@localhost:5432/quantdb")
    .replace("+asyncpg", "")
    .replace("postgresql+asyncpg", "postgresql")
)


def get_tushare_pro():
    """Initialize TuShare Pro API."""
    api_key = os.environ.get("TUSHARE_API_KEY")
    if not api_key:
        raise ValueError("TUSHARE_API_KEY environment variable not set")
    ts.set_token(api_key)
    return ts.pro_api()


def convert_ts_code(ts_code: str) -> str:
    """Convert 000001.SZ to sz.000001"""
    if not ts_code or "." not in ts_code:
        return ts_code
    parts = ts_code.split(".")
    return f"{parts[1].lower()}.{parts[0]}"


def safe_decimal(value) -> Optional[Decimal]:
    """Convert value to Decimal safely."""
    if pd.isna(value) or value == "" or value == "-":
        return None
    try:
        return Decimal(str(value))
    except:
        return None


def safe_int(value) -> Optional[int]:
    """Convert value to int safely."""
    if pd.isna(value) or value == "" or value == "-":
        return None
    try:
        f = float(value)
        if pd.isna(f):
            return None
        return int(f)
    except:
        return None


async def backfill_date(pool: asyncpg.Pool, pro, trade_date: date):
    """Fetch and insert data for a single date."""
    date_str = trade_date.strftime("%Y%m%d")
    logger.info(f"Processing {date_str}...")

    # 1. Fetch Moneyflow
    try:
        df_mf = pro.moneyflow(trade_date=date_str)
        if df_mf is not None and not df_mf.empty:
            mf_records = []
            for _, row in df_mf.iterrows():
                mf_records.append(
                    (
                        convert_ts_code(row["ts_code"]),
                        trade_date,
                        safe_decimal(row.get("buy_sm_amount")),
                        safe_decimal(row.get("buy_md_amount")),
                        safe_decimal(row.get("buy_lg_amount")),
                        safe_decimal(row.get("buy_elg_amount")),
                        safe_decimal(row.get("sell_sm_amount")),
                        safe_decimal(row.get("sell_md_amount")),
                        safe_decimal(row.get("sell_lg_amount")),
                        safe_decimal(row.get("sell_elg_amount")),
                        safe_decimal(row.get("net_mf_amount")),
                    )
                )

            if mf_records:
                await pool.executemany(
                    """
                    INSERT INTO moneyflow_daily (
                        code, date, 
                        buy_sm_amount, buy_md_amount, buy_lg_amount, buy_elg_amount,
                        sell_sm_amount, sell_md_amount, sell_lg_amount, sell_elg_amount,
                        net_mf_amount
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (code, date) DO UPDATE SET
                        buy_sm_amount = EXCLUDED.buy_sm_amount,
                        buy_md_amount = EXCLUDED.buy_md_amount,
                        buy_lg_amount = EXCLUDED.buy_lg_amount,
                        buy_elg_amount = EXCLUDED.buy_elg_amount,
                        sell_sm_amount = EXCLUDED.sell_sm_amount,
                        sell_md_amount = EXCLUDED.sell_md_amount,
                        sell_lg_amount = EXCLUDED.sell_lg_amount,
                        sell_elg_amount = EXCLUDED.sell_elg_amount,
                        net_mf_amount = EXCLUDED.net_mf_amount,
                        created_at = CURRENT_TIMESTAMP
                """,
                    mf_records,
                )
                logger.info(f"  Upserted {len(mf_records)} moneyflow records")
        else:
            logger.info(f"  No moneyflow data for {date_str}")

    except Exception as e:
        logger.error(f"  Error fetching moneyflow for {date_str}: {e}")

    # Rate limit
    time.sleep(0.5)

    # 2. Fetch Limit List
    try:
        df_limit = pro.limit_list_d(trade_date=date_str)
        if df_limit is not None and not df_limit.empty:
            limit_records = []
            for _, row in df_limit.iterrows():
                limit_records.append(
                    (
                        convert_ts_code(row["ts_code"]),
                        trade_date,
                        row.get("name") if pd.notna(row.get("name")) else None,
                        safe_decimal(row.get("close")),
                        safe_decimal(row.get("pct_chg")),
                        safe_decimal(row.get("fd_amount")),
                        row.get("first_time") if pd.notna(row.get("first_time")) else None,
                        row.get("last_time") if pd.notna(row.get("last_time")) else None,
                        safe_int(row.get("open_times")),
                        row.get("up_stat") if pd.notna(row.get("up_stat")) else None,
                        safe_int(row.get("limit_times")),
                        row.get("limit") if pd.notna(row.get("limit")) else None,
                    )
                )

            if limit_records:
                await pool.executemany(
                    """
                    INSERT INTO limit_list_daily (
                        code, date, name, close, pct_chg, fd_amount,
                        first_time, last_time, open_times, up_stat, limit_times, limit_type
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (code, date) DO UPDATE SET
                        name = EXCLUDED.name,
                        close = EXCLUDED.close,
                        pct_chg = EXCLUDED.pct_chg,
                        fd_amount = EXCLUDED.fd_amount,
                        first_time = EXCLUDED.first_time,
                        last_time = EXCLUDED.last_time,
                        open_times = EXCLUDED.open_times,
                        up_stat = EXCLUDED.up_stat,
                        limit_times = EXCLUDED.limit_times,
                        limit_type = EXCLUDED.limit_type,
                        created_at = CURRENT_TIMESTAMP
                """,
                    limit_records,
                )
                logger.info(f"  Upserted {len(limit_records)} limit_list records")
        else:
            logger.info(f"  No limit_list data for {date_str}")

    except Exception as e:
        logger.error(f"  Error fetching limit_list for {date_str}: {e}")

    # Rate limit
    time.sleep(0.5)


async def main():
    parser = argparse.ArgumentParser(description="Backfill Moneyflow and Limit List Data")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--database-url", default=DEFAULT_POSTGRES_URL, help="Database URL")

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    pro = get_tushare_pro()

    # Get trading days
    df_cal = pro.trade_cal(
        exchange="SSE",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        is_open="1",
    )

    if df_cal is None or df_cal.empty:
        logger.warning("No trading days found")
        return

    trading_days = [datetime.strptime(d, "%Y%m%d").date() for d in df_cal["cal_date"].tolist()]
    logger.info(f"Found {len(trading_days)} trading days from {start_date} to {end_date}")

    # Connect to DB
    pool = await asyncpg.create_pool(args.database_url)

    try:
        for trade_date in trading_days:
            await backfill_date(pool, pro, trade_date)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
