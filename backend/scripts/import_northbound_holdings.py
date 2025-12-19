#!/usr/bin/env python3
"""
Northbound Holdings Data Import Script

Imports northbound holdings data from SQLite to PostgreSQL.
Updates the stock_microstructure table's northbound-related fields.

Usage:
    python scripts/import_northbound_holdings.py --source /path/to/northbound_holdings.db
    python scripts/import_northbound_holdings.py --source-dir /path/to/trading_data
"""

import argparse
import asyncio
import os
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import asyncpg

# Default paths
SCRIPT_DIR = Path(__file__).parent
DEFAULT_TRADING_DATA_DIR = Path("/Users/dan/Code/q/trading_data")
DEFAULT_POSTGRES_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://quant:quant_dev_password@localhost:54321/quantdb"
).replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")

BATCH_SIZE = 1000

# Threshold for "heavy" northbound holdings
NORTHBOUND_HEAVY_THRESHOLD = 5.0  # 5%


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


async def get_existing_dates(pg_conn: asyncpg.Connection) -> set:
    """Get dates that already have northbound data in stock_microstructure."""
    rows = await pg_conn.fetch("""
        SELECT DISTINCT date FROM stock_microstructure
        WHERE northbound_holding_ratio IS NOT NULL
        ORDER BY date DESC
        LIMIT 100
    """)
    return {row['date'] for row in rows}


async def import_northbound_holdings(
    sqlite_path: Path,
    pg_conn: asyncpg.Connection,
    force: bool = False
) -> int:
    """
    Import northbound holdings data from SQLite to PostgreSQL.

    Updates the stock_microstructure table's northbound fields.
    """
    print(f"\nImporting northbound holdings from: {sqlite_path}")

    if not sqlite_path.exists():
        print(f"  SQLite file not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        # Get count
        cursor = sqlite_conn.execute("SELECT COUNT(*) FROM northbound_holdings")
        total_count = cursor.fetchone()[0]
        print(f"  Found {total_count:,} northbound holding records in SQLite")

        if total_count == 0:
            return 0

        # Get date range
        cursor = sqlite_conn.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date,
                   COUNT(DISTINCT code) as stock_count
            FROM northbound_holdings
        """)
        date_range = cursor.fetchone()
        print(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")
        print(f"  Unique stocks: {date_range['stock_count']:,}")

        # Check existing data in PostgreSQL
        if not force:
            existing_dates = await get_existing_dates(pg_conn)
            if existing_dates:
                print(f"  PostgreSQL already has {len(existing_dates)} dates with northbound data")

        # Process in batches
        cursor = sqlite_conn.execute("""
            SELECT code, date, holding_ratio, holding_change
            FROM northbound_holdings
            ORDER BY date, code
        """)

        batch = []
        total_imported = 0

        for row in cursor:
            holding_ratio = safe_decimal(row['holding_ratio'])
            holding_change = safe_decimal(row['holding_change'])

            # Calculate is_northbound_heavy flag
            is_heavy = holding_ratio is not None and holding_ratio > NORTHBOUND_HEAVY_THRESHOLD

            batch.append((
                row['code'],
                parse_date(row['date']),
                holding_ratio,
                holding_change,
                is_heavy,
            ))

            if len(batch) >= BATCH_SIZE:
                imported = await upsert_batch(pg_conn, batch)
                total_imported += imported
                print(f"    Imported batch: {total_imported:,} / {total_count:,}")
                batch = []

        # Final batch
        if batch:
            imported = await upsert_batch(pg_conn, batch)
            total_imported += imported

        print(f"  Total imported: {total_imported:,} records")
        return total_imported

    finally:
        sqlite_conn.close()


async def upsert_batch(
    pg_conn: asyncpg.Connection,
    records: list
) -> int:
    """
    Upsert northbound holdings data into stock_microstructure.

    If the row doesn't exist, creates it with just northbound data.
    If the row exists, updates only northbound fields.
    """
    if not records:
        return 0

    # Use INSERT ... ON CONFLICT to upsert
    # Note: stock_microstructure has (code, date) as primary key
    # We need to include all NOT NULL boolean columns with defaults
    await pg_conn.executemany(
        """
        INSERT INTO stock_microstructure (
            code, date, northbound_holding_ratio, northbound_holding_change, is_northbound_heavy,
            is_institutional, is_retail_hot, is_main_controlled
        )
        VALUES ($1, $2, $3, $4, $5, FALSE, FALSE, FALSE)
        ON CONFLICT (code, date) DO UPDATE SET
            northbound_holding_ratio = COALESCE(EXCLUDED.northbound_holding_ratio, stock_microstructure.northbound_holding_ratio),
            northbound_holding_change = COALESCE(EXCLUDED.northbound_holding_change, stock_microstructure.northbound_holding_change),
            is_northbound_heavy = EXCLUDED.is_northbound_heavy
        """,
        records,
    )

    return len(records)


async def calculate_northbound_heavy(pg_conn: asyncpg.Connection, calc_date: date = None):
    """
    Recalculate is_northbound_heavy flag for all records.

    Args:
        pg_conn: PostgreSQL connection
        calc_date: Specific date to calculate, or None for all dates
    """
    if calc_date:
        print(f"\nRecalculating is_northbound_heavy for {calc_date}...")
        result = await pg_conn.execute("""
            UPDATE stock_microstructure
            SET is_northbound_heavy = (northbound_holding_ratio > $1)
            WHERE date = $2
            AND northbound_holding_ratio IS NOT NULL
        """, NORTHBOUND_HEAVY_THRESHOLD, calc_date)
    else:
        print("\nRecalculating is_northbound_heavy for all records...")
        result = await pg_conn.execute("""
            UPDATE stock_microstructure
            SET is_northbound_heavy = (northbound_holding_ratio > $1)
            WHERE northbound_holding_ratio IS NOT NULL
        """, NORTHBOUND_HEAVY_THRESHOLD)

    print(f"  Updated: {result}")


async def print_statistics(pg_conn: asyncpg.Connection):
    """Print statistics about northbound data in PostgreSQL."""
    print("\n" + "=" * 50)
    print("PostgreSQL Northbound Holdings Statistics")
    print("=" * 50)

    # Total records with northbound data
    row = await pg_conn.fetchrow("""
        SELECT COUNT(*) as total,
               COUNT(northbound_holding_ratio) as with_northbound,
               COUNT(*) FILTER (WHERE is_northbound_heavy) as heavy_count
        FROM stock_microstructure
    """)
    print(f"Total microstructure records: {row['total']:,}")
    print(f"With northbound data: {row['with_northbound']:,}")
    print(f"Northbound heavy (>5%): {row['heavy_count']:,}")

    # Date range with northbound data
    row = await pg_conn.fetchrow("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM stock_microstructure
        WHERE northbound_holding_ratio IS NOT NULL
    """)
    if row['min_date']:
        print(f"Date range (northbound): {row['min_date']} to {row['max_date']}")

    # Latest date statistics
    row = await pg_conn.fetchrow("""
        SELECT date,
               COUNT(*) as stock_count,
               AVG(northbound_holding_ratio) as avg_ratio,
               COUNT(*) FILTER (WHERE is_northbound_heavy) as heavy_count
        FROM stock_microstructure
        WHERE northbound_holding_ratio IS NOT NULL
        GROUP BY date
        ORDER BY date DESC
        LIMIT 1
    """)
    if row:
        print(f"\nLatest data ({row['date']}):")
        print(f"  Stocks with data: {row['stock_count']:,}")
        print(f"  Average holding ratio: {float(row['avg_ratio'] or 0):.2f}%")
        print(f"  Northbound heavy stocks: {row['heavy_count']}")

    # Top 10 northbound heavy stocks
    rows = await pg_conn.fetch("""
        SELECT code, northbound_holding_ratio
        FROM stock_microstructure
        WHERE date = (SELECT MAX(date) FROM stock_microstructure WHERE northbound_holding_ratio IS NOT NULL)
        AND northbound_holding_ratio IS NOT NULL
        ORDER BY northbound_holding_ratio DESC
        LIMIT 10
    """)
    if rows:
        print("\nTop 10 northbound holdings:")
        for row in rows:
            print(f"  {row['code']}: {float(row['northbound_holding_ratio']):.2f}%")

    print("=" * 50)


async def main():
    parser = argparse.ArgumentParser(description="Import northbound holdings data to PostgreSQL")
    parser.add_argument("--source", type=str, help="Path to northbound_holdings.db")
    parser.add_argument("--source-dir", type=str, help="Directory containing northbound_holdings.db")
    parser.add_argument("-d", "--database", type=str, default=DEFAULT_POSTGRES_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--force", action="store_true", help="Force re-import all data")
    parser.add_argument("--recalc", action="store_true", help="Recalculate is_northbound_heavy flag")
    parser.add_argument("--status", action="store_true", help="Show statistics only")
    args = parser.parse_args()

    # Determine source file
    if args.source:
        sqlite_path = Path(args.source)
    elif args.source_dir:
        sqlite_path = Path(args.source_dir) / "northbound_holdings.db"
    else:
        sqlite_path = DEFAULT_TRADING_DATA_DIR / "northbound_holdings.db"

    print(f"SQLite source: {sqlite_path}")
    print(f"PostgreSQL: {args.database}")

    # Connect to PostgreSQL
    pg_conn = await asyncpg.connect(args.database)

    try:
        if args.status:
            await print_statistics(pg_conn)
        elif args.recalc:
            await calculate_northbound_heavy(pg_conn)
            await print_statistics(pg_conn)
        else:
            imported = await import_northbound_holdings(sqlite_path, pg_conn, force=args.force)
            await print_statistics(pg_conn)
            print(f"\nTotal imported: {imported:,} records")
    finally:
        await pg_conn.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
