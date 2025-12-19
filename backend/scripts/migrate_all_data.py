#!/usr/bin/env python3
"""
Unified Data Migration Script

Migrates data from SQLite databases to the new PostgreSQL schema.
Supports:
- Stock data (stock_basic -> asset_meta + stock_profile, daily_k_data -> market_daily + indicator_valuation)
- ETF data (etf_basic -> asset_meta + etf_profile, etf_daily -> market_daily)
- Adjust factors

Usage:
    # Migrate stock data
    python scripts/migrate_all_data.py --source /path/to/a_stock_2024.db --type stock

    # Migrate ETF data
    python scripts/migrate_all_data.py --source /path/to/etf_2024.db --type etf

    # Migrate all from directory
    python scripts/migrate_all_data.py --source-dir /path/to/trading_data --all
"""

import argparse
import asyncio
import os
import sqlite3
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Optional, Dict, Any, List

import asyncpg

# Default paths
SCRIPT_DIR = Path(__file__).parent
DEFAULT_POSTGRES_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://quant:quant_dev_password@localhost:5432/quantdb"
).replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")

BATCH_SIZE = 10000


# =============================================================================
# Utility Functions
# =============================================================================

def parse_date(val) -> Optional[date]:
    """Parse date string to date object."""
    if val is None or val == "":
        return None
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except:
        return None


def safe_decimal(val) -> Optional[Decimal]:
    """Convert value to Decimal safely."""
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except:
        return None


def safe_int(val) -> Optional[int]:
    """Convert value to int safely."""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except:
        return None


def determine_exchange(code: str) -> str:
    """Determine exchange from code."""
    if code.startswith("sh."):
        return "sh"
    elif code.startswith("sz."):
        return "sz"
    elif code.startswith("bj."):
        return "bj"
    return "sh"


def determine_category(code: str) -> str:
    """Determine category (board) from code."""
    code_num = code.split('.')[-1] if '.' in code else code

    if code_num.startswith('688'):
        return "科创板"
    elif code_num.startswith('30'):
        return "创业板"
    elif code_num.startswith('8') or code_num.startswith('4'):
        return "北交所"
    elif code_num.startswith('51') or code_num.startswith('15') or code_num.startswith('56'):
        return "ETF"
    elif code_num.startswith('60') or code_num.startswith('00'):
        return "主板"
    return "其他"


# =============================================================================
# Stock Migration
# =============================================================================

async def migrate_stock_basic(
    sqlite_conn: sqlite3.Connection,
    pg_conn: asyncpg.Connection
) -> int:
    """Migrate stock_basic -> asset_meta + stock_profile."""
    print("\nMigrating stock_basic -> asset_meta + stock_profile...")

    cursor = sqlite_conn.execute("SELECT * FROM stock_basic")
    rows = cursor.fetchall()

    if not rows:
        print("  No data in stock_basic")
        return 0

    columns = [desc[0] for desc in cursor.description]
    print(f"  Found {len(rows)} records")

    # Prepare asset_meta records
    asset_records = []
    profile_records = []

    for row in rows:
        record = dict(zip(columns, row))
        code = record.get("code", "")
        exchange = determine_exchange(code)
        category = determine_category(code)

        # Convert status to int (SQLite may store as string)
        status_val = record.get("status")
        if status_val is not None:
            status_val = int(status_val)
        else:
            status_val = 1

        asset_records.append((
            code,
            record.get("code_name") or code,
            "STOCK",
            exchange,
            parse_date(record.get("ipo_date") or record.get("ipoDate")),
            parse_date(record.get("out_date") or record.get("outDate")),
            status_val,
            category,
        ))

        # Stock profile with industry info
        # Model columns: sw_industry_l1/l2/l3, concepts, province, total_shares, float_shares
        profile_records.append((
            code,
            record.get("sw_industry") or record.get("industry"),  # sw_industry_l1
            None,  # sw_industry_l2
            None,  # sw_industry_l3
            record.get("area"),  # province
        ))

    # Insert asset_meta
    await pg_conn.executemany(
        """
        INSERT INTO asset_meta (code, name, asset_type, exchange, list_date, delist_date, status, category)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            exchange = EXCLUDED.exchange,
            list_date = EXCLUDED.list_date,
            delist_date = EXCLUDED.delist_date,
            status = EXCLUDED.status,
            category = EXCLUDED.category,
            updated_at = CURRENT_TIMESTAMP
        """,
        asset_records,
    )

    # Insert stock_profile
    await pg_conn.executemany(
        """
        INSERT INTO stock_profile (code, sw_industry_l1, sw_industry_l2, sw_industry_l3, province)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (code) DO UPDATE SET
            sw_industry_l1 = COALESCE(EXCLUDED.sw_industry_l1, stock_profile.sw_industry_l1),
            sw_industry_l2 = COALESCE(EXCLUDED.sw_industry_l2, stock_profile.sw_industry_l2),
            sw_industry_l3 = COALESCE(EXCLUDED.sw_industry_l3, stock_profile.sw_industry_l3),
            province = COALESCE(EXCLUDED.province, stock_profile.province),
            updated_at = CURRENT_TIMESTAMP
        """,
        profile_records,
    )

    print(f"  Migrated {len(asset_records)} stock records to asset_meta + stock_profile")
    return len(asset_records)


async def migrate_daily_k_data(
    sqlite_conn: sqlite3.Connection,
    pg_conn: asyncpg.Connection
) -> int:
    """Migrate daily_k_data -> market_daily + indicator_valuation."""
    print("\nMigrating daily_k_data -> market_daily + indicator_valuation...")

    # Get total count
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM daily_k_data")
    total = cursor.fetchone()[0]
    print(f"  Total records: {total:,}")

    cursor = sqlite_conn.execute("SELECT * FROM daily_k_data")
    columns = [desc[0] for desc in cursor.description]

    migrated = 0
    market_batch = []
    valuation_batch = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            # Market daily data (OHLCV only)
            market_batch.append((
                record.get("code"),
                parse_date(record.get("date")),
                safe_decimal(record.get("open")),
                safe_decimal(record.get("high")),
                safe_decimal(record.get("low")),
                safe_decimal(record.get("close")),
                safe_decimal(record.get("preclose")),
                safe_int(record.get("volume")),
                safe_decimal(record.get("amount")),
                safe_decimal(record.get("turn")),
                safe_decimal(record.get("pctChg")),
                safe_int(record.get("tradestatus")),
            ))

            # Valuation data (if any valuation field is present)
            pe_ttm = safe_decimal(record.get("peTTM"))
            pb_mrq = safe_decimal(record.get("pbMRQ"))
            ps_ttm = safe_decimal(record.get("psTTM"))
            pcf = safe_decimal(record.get("pcfNcfTTM"))
            is_st = safe_int(record.get("isST"))

            if any([pe_ttm, pb_mrq, ps_ttm, pcf, is_st]):
                valuation_batch.append((
                    record.get("code"),
                    parse_date(record.get("date")),
                    pe_ttm,
                    pb_mrq,
                    ps_ttm,
                    pcf,
                    None,  # total_mv
                    None,  # circ_mv
                    is_st,
                ))

        # Insert market_daily batch
        await pg_conn.executemany(
            """
            INSERT INTO market_daily (code, date, open, high, low, close, preclose, volume, amount, turn, pct_chg, trade_status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (code, date) DO NOTHING
            """,
            market_batch,
        )

        # Insert indicator_valuation batch
        if valuation_batch:
            await pg_conn.executemany(
                """
                INSERT INTO indicator_valuation (code, date, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, total_mv, circ_mv, is_st)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (code, date) DO NOTHING
                """,
                valuation_batch,
            )

        migrated += len(market_batch)
        progress = (migrated / total) * 100
        print(f"  Progress: {migrated:,}/{total:,} ({progress:.1f}%)")
        market_batch = []
        valuation_batch = []

    print(f"  Migrated {migrated:,} daily records to market_daily + indicator_valuation")
    return migrated


# =============================================================================
# ETF Migration
# =============================================================================

async def migrate_etf_basic(
    sqlite_conn: sqlite3.Connection,
    pg_conn: asyncpg.Connection
) -> int:
    """Migrate etf_basic -> asset_meta + etf_profile."""
    print("\nMigrating etf_basic -> asset_meta + etf_profile...")

    try:
        cursor = sqlite_conn.execute("SELECT * FROM etf_basic")
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        print("  No etf_basic table found")
        return 0

    if not rows:
        print("  No data in etf_basic")
        return 0

    columns = [desc[0] for desc in cursor.description]
    print(f"  Found {len(rows)} records")

    asset_records = []
    profile_records = []

    for row in rows:
        record = dict(zip(columns, row))
        code = record.get("code", "")
        exchange = determine_exchange(code)

        # Convert status to int (SQLite may store as string)
        status_val = record.get("status")
        if status_val is not None:
            status_val = int(status_val)
        else:
            status_val = 1

        asset_records.append((
            code,
            record.get("code_name") or record.get("name") or code,
            "ETF",
            exchange,
            parse_date(record.get("ipo_date") or record.get("list_date")),
            parse_date(record.get("out_date") or record.get("delist_date")),
            status_val,
            "ETF",
        ))

        # ETF profile - fund_type, tracking_index_code, tracking_index_name, management_fee, custodian_fee
        profile_records.append((
            code,
            record.get("etf_type") or record.get("fund_type"),
            record.get("underlying_index_code") or record.get("index_code") or record.get("tracking_index_code"),
            record.get("underlying_index_name") or record.get("index_name") or record.get("tracking_index_name"),
            safe_decimal(record.get("management_fee")),
            safe_decimal(record.get("custody_fee") or record.get("custodian_fee")),
        ))

    # Insert asset_meta
    await pg_conn.executemany(
        """
        INSERT INTO asset_meta (code, name, asset_type, exchange, list_date, delist_date, status, category)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            asset_type = EXCLUDED.asset_type,
            exchange = EXCLUDED.exchange,
            list_date = EXCLUDED.list_date,
            delist_date = EXCLUDED.delist_date,
            status = EXCLUDED.status,
            category = EXCLUDED.category,
            updated_at = CURRENT_TIMESTAMP
        """,
        asset_records,
    )

    # Insert etf_profile
    await pg_conn.executemany(
        """
        INSERT INTO etf_profile (code, etf_type, underlying_index_code, underlying_index_name, management_fee, custody_fee)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (code) DO UPDATE SET
            etf_type = COALESCE(EXCLUDED.etf_type, etf_profile.etf_type),
            underlying_index_code = COALESCE(EXCLUDED.underlying_index_code, etf_profile.underlying_index_code),
            underlying_index_name = COALESCE(EXCLUDED.underlying_index_name, etf_profile.underlying_index_name),
            management_fee = COALESCE(EXCLUDED.management_fee, etf_profile.management_fee),
            custody_fee = COALESCE(EXCLUDED.custody_fee, etf_profile.custody_fee),
            updated_at = CURRENT_TIMESTAMP
        """,
        profile_records,
    )

    print(f"  Migrated {len(asset_records)} ETF records to asset_meta + etf_profile")
    return len(asset_records)


async def migrate_etf_daily(
    sqlite_conn: sqlite3.Connection,
    pg_conn: asyncpg.Connection
) -> int:
    """Migrate etf_daily -> market_daily + indicator_etf."""
    print("\nMigrating etf_daily -> market_daily + indicator_etf...")

    try:
        cursor = sqlite_conn.execute("SELECT COUNT(*) FROM etf_daily")
        total = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        # Try alternative table name
        try:
            cursor = sqlite_conn.execute("SELECT COUNT(*) FROM daily_k_data")
            total = cursor.fetchone()[0]
            table_name = "daily_k_data"
        except:
            print("  No ETF daily table found")
            return 0
    else:
        table_name = "etf_daily"

    print(f"  Total records: {total:,}")

    cursor = sqlite_conn.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]

    migrated = 0
    market_batch = []
    etf_indicator_batch = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            # Market daily data
            market_batch.append((
                record.get("code"),
                parse_date(record.get("date")),
                safe_decimal(record.get("open")),
                safe_decimal(record.get("high")),
                safe_decimal(record.get("low")),
                safe_decimal(record.get("close")),
                safe_decimal(record.get("preclose")),
                safe_int(record.get("volume")),
                safe_decimal(record.get("amount")),
                safe_decimal(record.get("turn")),
                safe_decimal(record.get("pctChg") or record.get("pct_chg")),
                safe_int(record.get("tradestatus") or record.get("trade_status")),
            ))

            # ETF specific indicators
            iopv = safe_decimal(record.get("iopv"))
            discount_rate = safe_decimal(record.get("discount_rate"))
            unit_total = safe_decimal(record.get("unit_total"))

            if any([iopv, discount_rate, unit_total]):
                etf_indicator_batch.append((
                    record.get("code"),
                    parse_date(record.get("date")),
                    iopv,
                    discount_rate,
                    unit_total,
                    None,  # tracking_error
                ))

        # Insert market_daily batch
        await pg_conn.executemany(
            """
            INSERT INTO market_daily (code, date, open, high, low, close, preclose, volume, amount, turn, pct_chg, trade_status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (code, date) DO NOTHING
            """,
            market_batch,
        )

        # Insert indicator_etf batch
        if etf_indicator_batch:
            await pg_conn.executemany(
                """
                INSERT INTO indicator_etf (code, date, iopv, discount_rate, unit_total, tracking_error)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (code, date) DO NOTHING
                """,
                etf_indicator_batch,
            )

        migrated += len(market_batch)
        progress = (migrated / total) * 100
        print(f"  Progress: {migrated:,}/{total:,} ({progress:.1f}%)")
        market_batch = []
        etf_indicator_batch = []

    print(f"  Migrated {migrated:,} ETF daily records to market_daily")
    return migrated


# =============================================================================
# Adjust Factor Migration
# =============================================================================

async def migrate_adjust_factor(
    sqlite_conn: sqlite3.Connection,
    pg_conn: asyncpg.Connection
) -> int:
    """Migrate adjust_factor table."""
    print("\nMigrating adjust_factor...")

    try:
        cursor = sqlite_conn.execute("SELECT * FROM adjust_factor")
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        print("  No adjust_factor table found")
        return 0

    if not rows:
        print("  No data in adjust_factor")
        return 0

    columns = [desc[0] for desc in cursor.description]
    print(f"  Found {len(rows)} records")

    records = []
    for row in rows:
        record = dict(zip(columns, row))

        records.append((
            record.get("code"),
            parse_date(record.get("dividOperateDate") or record.get("divid_operate_date")),
            safe_decimal(record.get("foreAdjustFactor") or record.get("fore_adjust_factor")),
            safe_decimal(record.get("backAdjustFactor") or record.get("back_adjust_factor")),
            safe_decimal(record.get("adjustFactor") or record.get("adjust_factor")),
        ))

    await pg_conn.executemany(
        """
        INSERT INTO adjust_factor (code, divid_operate_date, fore_adjust_factor, back_adjust_factor, adjust_factor)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (code, divid_operate_date) DO NOTHING
        """,
        records,
    )

    print(f"  Migrated {len(records)} adjust_factor records")
    return len(records)


# =============================================================================
# Main Functions
# =============================================================================

async def migrate_stock_database(source_path: Path, postgres_url: str) -> Dict[str, int]:
    """Migrate a stock database."""
    results = {}

    sqlite_conn = sqlite3.connect(str(source_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        pg_conn = await asyncpg.connect(postgres_url)

        try:
            results['stock_basic'] = await migrate_stock_basic(sqlite_conn, pg_conn)
            results['daily_k_data'] = await migrate_daily_k_data(sqlite_conn, pg_conn)
            results['adjust_factor'] = await migrate_adjust_factor(sqlite_conn, pg_conn)
        finally:
            await pg_conn.close()
    finally:
        sqlite_conn.close()

    return results


async def migrate_etf_database(source_path: Path, postgres_url: str) -> Dict[str, int]:
    """Migrate an ETF database."""
    results = {}

    sqlite_conn = sqlite3.connect(str(source_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        pg_conn = await asyncpg.connect(postgres_url)

        try:
            results['etf_basic'] = await migrate_etf_basic(sqlite_conn, pg_conn)
            results['etf_daily'] = await migrate_etf_daily(sqlite_conn, pg_conn)
            results['adjust_factor'] = await migrate_adjust_factor(sqlite_conn, pg_conn)
        finally:
            await pg_conn.close()
    finally:
        sqlite_conn.close()

    return results


async def main(
    source: Optional[Path] = None,
    source_dir: Optional[Path] = None,
    data_type: str = "stock",
    postgres_url: str = DEFAULT_POSTGRES_URL,
    migrate_all: bool = False,
):
    """Main migration function."""
    print("=" * 60)
    print("Unified Data Migration")
    print("=" * 60)

    start_time = datetime.now()
    all_results = {}

    if migrate_all and source_dir:
        # Migrate all databases in directory
        print(f"\nScanning directory: {source_dir}")

        # Look for stock databases
        for pattern in ["a_stock*.db", "stock*.db"]:
            for db_file in source_dir.glob(pattern):
                print(f"\n[STOCK] Processing: {db_file.name}")
                results = await migrate_stock_database(db_file, postgres_url)
                all_results[f"stock:{db_file.name}"] = results

        # Look for ETF databases
        for pattern in ["etf*.db", "ETF*.db"]:
            for db_file in source_dir.glob(pattern):
                print(f"\n[ETF] Processing: {db_file.name}")
                results = await migrate_etf_database(db_file, postgres_url)
                all_results[f"etf:{db_file.name}"] = results

    elif source:
        # Single database migration
        print(f"\nSource: {source}")
        print(f"Type: {data_type}")

        if not source.exists():
            print(f"\nError: Database not found at {source}")
            return 1

        if data_type == "etf":
            results = await migrate_etf_database(source, postgres_url)
        else:
            results = await migrate_stock_database(source, postgres_url)

        all_results[source.name] = results

    else:
        print("\nError: Please specify --source or --source-dir with --all")
        return 1

    elapsed = datetime.now() - start_time

    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print(f"\nResults:")
    for db_name, results in all_results.items():
        print(f"\n  {db_name}:")
        for table, count in results.items():
            print(f"    - {table}: {count:,} records")
    print(f"\n  Total time: {elapsed}")

    return 0


def cli():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Migrate stock/ETF data to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--source", "-s",
        type=Path,
        help="Path to SQLite database"
    )

    parser.add_argument(
        "--source-dir",
        type=Path,
        help="Directory containing multiple SQLite databases"
    )

    parser.add_argument(
        "--type", "-t",
        choices=["stock", "etf"],
        default="stock",
        help="Type of data to migrate (default: stock)"
    )

    parser.add_argument(
        "--database-url", "-d",
        type=str,
        default=DEFAULT_POSTGRES_URL,
        help="PostgreSQL connection URL"
    )

    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Migrate all databases in source-dir"
    )

    args = parser.parse_args()

    exit_code = asyncio.run(main(
        source=args.source,
        source_dir=args.source_dir,
        data_type=args.type,
        postgres_url=args.database_url,
        migrate_all=args.all,
    ))
    exit(exit_code)


if __name__ == "__main__":
    cli()
