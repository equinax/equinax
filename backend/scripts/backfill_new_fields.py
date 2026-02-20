#!/usr/bin/env python3
"""
Backfill new fields added in the schema restructuring migration.

- market_daily.change — from TuShare pro.daily()
- indicator_valuation new columns — from TuShare pro.daily_basic()

Strategy: decompress chunks → fetch from TuShare in batches → bulk UPDATE via temp table → recompress.

Usage:
    docker compose exec api python -m scripts.backfill_new_fields
    docker compose exec api python -m scripts.backfill_new_fields --start 2024-01-02 --end 2024-06-30
    docker compose exec api python -m scripts.backfill_new_fields --table market_daily
    docker compose exec api python -m scripts.backfill_new_fields --table indicator_valuation
    docker compose exec api python -m scripts.backfill_new_fields --dry-run
"""

import argparse
import asyncio
import logging
import os
import time
from datetime import date
from decimal import Decimal
from typing import List, Optional

import asyncpg
import pandas as pd
import tushare as ts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_POSTGRES_URL = (
    os.environ.get("DATABASE_URL", "postgresql://quant:quant_dev_password@localhost:5432/quantdb")
    .replace("+asyncpg", "")
    .replace("postgresql+asyncpg", "postgresql")
)

TUSHARE_RATE_LIMIT_DELAY = 0.12
BATCH_SIZE = 50


def get_tushare_pro():
    api_key = os.environ.get("TUSHARE_API_KEY")
    if not api_key:
        raise ValueError("TUSHARE_API_KEY environment variable not set")
    ts.set_token(api_key)
    return ts.pro_api()


def convert_ts_code(ts_code: str) -> str:
    if not ts_code or "." not in ts_code:
        return ts_code
    parts = ts_code.split(".")
    return f"{parts[1].lower()}.{parts[0]}"


def safe_decimal(value) -> Optional[Decimal]:
    if pd.isna(value) or value == "" or value == "-":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


async def get_dates_needing_backfill(
    pool: asyncpg.Pool, table: str, start: Optional[date] = None, end: Optional[date] = None
) -> List[date]:
    if table == "market_daily":
        query = "SELECT DISTINCT date FROM market_daily WHERE change IS NULL"
    else:
        query = (
            "SELECT DISTINCT date FROM indicator_valuation "
            "WHERE turnover_rate_f IS NULL AND pe IS NULL AND total_share IS NULL"
        )

    conditions = []
    args = []
    if start:
        conditions.append(f"date >= ${len(args) + 1}")
        args.append(start)
    if end:
        conditions.append(f"date <= ${len(args) + 1}")
        args.append(end)

    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY date"

    rows = await pool.fetch(query, *args)
    return [row["date"] for row in rows]


async def decompress_table(conn, table: str):
    logger.info(f"Decompressing {table} chunks...")
    rows = await conn.fetch(
        "SELECT chunk_name, chunk_schema FROM timescaledb_information.chunks "
        "WHERE hypertable_name = $1 AND is_compressed = true",
        table,
    )
    for row in rows:
        chunk_fqn = f"{row['chunk_schema']}.{row['chunk_name']}"
        await conn.execute(f"SELECT decompress_chunk('{chunk_fqn}')")
    logger.info(f"Decompressed {len(rows)} chunks for {table}")


async def recompress_table(pool, table: str):
    logger.info(f"Recompressing {table}...")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT chunk_name, chunk_schema FROM timescaledb_information.chunks "
            "WHERE hypertable_name = $1 AND is_compressed = false",
            table,
        )
        for row in rows:
            chunk_fqn = f"{row['chunk_schema']}.{row['chunk_name']}"
            try:
                await conn.execute(f"SELECT compress_chunk('{chunk_fqn}')")
            except Exception as e:
                logger.warning(f"Failed to compress {chunk_fqn}: {e}")
    logger.info(f"Recompression done for {table}")


def fetch_daily_change(pro, trade_date: date) -> list:
    date_str = trade_date.strftime("%Y%m%d")
    try:
        df = pro.daily(trade_date=date_str, fields="ts_code,trade_date,change")
    except Exception as e:
        logger.error(f"TuShare daily() failed for {date_str}: {e}")
        return []

    if df is None or df.empty:
        return []

    records = []
    for _, row in df.iterrows():
        code = convert_ts_code(row["ts_code"])
        change = safe_decimal(row.get("change"))
        if change is not None:
            records.append((code, trade_date, change))
    return records


def fetch_daily_basic(pro, trade_date: date) -> list:
    date_str = trade_date.strftime("%Y%m%d")
    try:
        df = pro.daily_basic(trade_date=date_str)
    except Exception as e:
        logger.error(f"TuShare daily_basic() failed for {date_str}: {e}")
        return []

    if df is None or df.empty:
        return []

    records = []
    for _, row in df.iterrows():
        code = convert_ts_code(row["ts_code"])
        total_mv_raw = safe_decimal(row.get("total_mv"))
        circ_mv_raw = safe_decimal(row.get("circ_mv"))
        records.append(
            (
                code,
                trade_date,
                safe_decimal(row.get("close")),
                safe_decimal(row.get("turnover_rate")),
                safe_decimal(row.get("turnover_rate_f")),
                safe_decimal(row.get("volume_ratio")),
                safe_decimal(row.get("pe")),
                safe_decimal(row.get("pe_ttm")),
                safe_decimal(row.get("pb")),
                safe_decimal(row.get("ps")),
                safe_decimal(row.get("ps_ttm")),
                safe_decimal(row.get("dv_ratio")),
                safe_decimal(row.get("dv_ttm")),
                Decimal(str(float(total_mv_raw) / 10000)) if total_mv_raw else None,
                Decimal(str(float(circ_mv_raw) / 10000)) if circ_mv_raw else None,
                safe_decimal(row.get("total_share")),
                safe_decimal(row.get("float_share")),
                safe_decimal(row.get("free_share")),
            )
        )
    return records


async def bulk_update_market_daily(conn, all_records: list) -> int:
    if not all_records:
        return 0

    await conn.execute(
        "CREATE TEMP TABLE _tmp_change (code TEXT, date DATE, change NUMERIC) ON COMMIT DROP"
    )
    await conn.copy_records_to_table(
        "_tmp_change", records=all_records, columns=["code", "date", "change"]
    )
    result = await conn.execute(
        "UPDATE market_daily md SET change = t.change "
        "FROM _tmp_change t WHERE md.code = t.code AND md.date = t.date AND md.change IS NULL"
    )
    return int(result.split()[-1])


async def bulk_upsert_valuation(conn, all_records: list) -> int:
    if not all_records:
        return 0

    cols = [
        "code",
        "date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb_mrq",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_mv",
        "circ_mv",
        "total_share",
        "float_share",
        "free_share",
    ]
    col_defs = ", ".join(
        f"{c} {'TEXT' if c == 'code' else 'DATE' if c == 'date' else 'NUMERIC'}" for c in cols
    )

    await conn.execute(f"CREATE TEMP TABLE _tmp_valuation ({col_defs}) ON COMMIT DROP")
    await conn.copy_records_to_table("_tmp_valuation", records=all_records, columns=cols)

    update_sets = ", ".join(f"{c} = COALESCE(t.{c}, iv.{c})" for c in cols[2:])
    result = await conn.execute(
        f"UPDATE indicator_valuation iv SET {update_sets} "
        f"FROM _tmp_valuation t WHERE iv.code = t.code AND iv.date = t.date"
    )
    updated = int(result.split()[-1])

    inserted = await conn.execute(
        f"INSERT INTO indicator_valuation ({', '.join(cols)}) "
        f"SELECT {', '.join(f't.{c}' for c in cols)} FROM _tmp_valuation t "
        f"LEFT JOIN indicator_valuation iv ON iv.code = t.code AND iv.date = t.date "
        f"WHERE iv.code IS NULL"
    )
    ins_count = int(inserted.split()[-1])

    return updated + ins_count


async def main():
    parser = argparse.ArgumentParser(description="Backfill new schema fields from TuShare")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--table",
        choices=["market_daily", "indicator_valuation"],
        help="Only backfill this table (default: both)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show dates without executing")
    parser.add_argument("--db-url", type=str, default=DEFAULT_POSTGRES_URL)
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    tables = [args.table] if args.table else ["market_daily", "indicator_valuation"]

    pool = await asyncpg.create_pool(args.db_url, min_size=2, max_size=5)
    pro = get_tushare_pro()

    try:
        for table in tables:
            dates = await get_dates_needing_backfill(pool, table, start_date, end_date)
            logger.info(f"[{table}] {len(dates)} dates need backfill")

            if not dates:
                continue

            if args.dry_run:
                logger.info(f"  First: {dates[0]}, Last: {dates[-1]}")
                continue

            async with pool.acquire() as conn:
                await decompress_table(conn, table)

            total_records = 0
            t0 = time.time()

            for batch_start in range(0, len(dates), BATCH_SIZE):
                batch_dates = dates[batch_start : batch_start + BATCH_SIZE]
                batch_records = []

                for d in batch_dates:
                    if table == "market_daily":
                        records = fetch_daily_change(pro, d)
                    else:
                        records = fetch_daily_basic(pro, d)
                    batch_records.extend(records)
                    time.sleep(TUSHARE_RATE_LIMIT_DELAY)

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        if table == "market_daily":
                            count = await bulk_update_market_daily(conn, batch_records)
                        else:
                            count = await bulk_upsert_valuation(conn, batch_records)
                        total_records += count

                done = min(batch_start + BATCH_SIZE, len(dates))
                elapsed = time.time() - t0
                rate = done / elapsed * 60
                logger.info(
                    f"  [{table}] {done}/{len(dates)} dates done, "
                    f"{total_records} records, {rate:.0f} dates/min"
                )

            try:
                await recompress_table(pool, table)
            except Exception as e:
                logger.warning(f"Recompression failed (non-fatal): {e}")

            elapsed = time.time() - t0
            logger.info(
                f"[{table}] Done: {len(dates)} dates, {total_records} records in {elapsed:.1f}s"
            )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
