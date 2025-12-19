#!/usr/bin/env python3
"""
Market Cap Data Import Script

Imports market cap data from SQLite to PostgreSQL.
Updates the indicator_valuation table's total_mv and circ_mv fields.

Usage:
    python scripts/import_market_cap.py --source /path/to/market_cap.db
    python scripts/import_market_cap.py --source-dir /path/to/trading_data
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

BATCH_SIZE = 5000


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
    """Get dates that already have market cap data in indicator_valuation."""
    rows = await pg_conn.fetch("""
        SELECT DISTINCT date FROM indicator_valuation
        WHERE total_mv IS NOT NULL
        ORDER BY date DESC
        LIMIT 100
    """)
    return {row['date'] for row in rows}


async def import_market_cap(
    sqlite_path: Path,
    pg_conn: asyncpg.Connection,
    force: bool = False
) -> int:
    """
    Import market cap data from SQLite to PostgreSQL.

    Updates the indicator_valuation table's total_mv and circ_mv fields.
    """
    print(f"\nImporting market cap data from: {sqlite_path}")

    if not sqlite_path.exists():
        print(f"  SQLite file not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        # Get count
        cursor = sqlite_conn.execute("SELECT COUNT(*) FROM stock_market_cap")
        total_count = cursor.fetchone()[0]
        print(f"  Found {total_count:,} market cap records in SQLite")

        if total_count == 0:
            return 0

        # Get date range
        cursor = sqlite_conn.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM stock_market_cap
        """)
        date_range = cursor.fetchone()
        print(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")

        # Check existing data in PostgreSQL
        if not force:
            existing_dates = await get_existing_dates(pg_conn)
            if existing_dates:
                print(f"  PostgreSQL already has {len(existing_dates)} dates with market cap data")

        # Process in batches
        cursor = sqlite_conn.execute("""
            SELECT code, date, total_mv, circ_mv
            FROM stock_market_cap
            ORDER BY date, code
        """)

        batch = []
        total_imported = 0

        for row in cursor:
            batch.append((
                row['code'],
                parse_date(row['date']),
                safe_decimal(row['total_mv']),  # 亿元
                safe_decimal(row['circ_mv']),   # 亿元
            ))

            if len(batch) >= BATCH_SIZE:
                imported = await upsert_market_cap_batch(pg_conn, batch)
                total_imported += imported
                print(f"    Imported batch: {total_imported:,} / {total_count:,}")
                batch = []

        # Final batch
        if batch:
            imported = await upsert_market_cap_batch(pg_conn, batch)
            total_imported += imported

        print(f"  Total imported: {total_imported:,} records")
        return total_imported

    finally:
        sqlite_conn.close()


async def upsert_market_cap_batch(
    pg_conn: asyncpg.Connection,
    records: list
) -> int:
    """
    Upsert market cap data into indicator_valuation.

    If the row doesn't exist, creates it with just market cap data.
    If the row exists, updates only total_mv and circ_mv fields.
    """
    if not records:
        return 0

    # Use INSERT ... ON CONFLICT to upsert
    # Note: indicator_valuation has (code, date) as primary key
    await pg_conn.executemany(
        """
        INSERT INTO indicator_valuation (code, date, total_mv, circ_mv)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (code, date) DO UPDATE SET
            total_mv = COALESCE(EXCLUDED.total_mv, indicator_valuation.total_mv),
            circ_mv = COALESCE(EXCLUDED.circ_mv, indicator_valuation.circ_mv),
            updated_at = CURRENT_TIMESTAMP
        """,
        records,
    )

    return len(records)


async def print_statistics(pg_conn: asyncpg.Connection):
    """Print statistics about market cap data in PostgreSQL."""
    print("\n" + "=" * 50)
    print("PostgreSQL Market Cap Statistics")
    print("=" * 50)

    # Total records with market cap
    row = await pg_conn.fetchrow("""
        SELECT COUNT(*) as total,
               COUNT(total_mv) as with_mv,
               COUNT(*) - COUNT(total_mv) as without_mv
        FROM indicator_valuation
    """)
    print(f"Total valuation records: {row['total']:,}")
    print(f"With market cap: {row['with_mv']:,}")
    print(f"Without market cap: {row['without_mv']:,}")

    # Date range with market cap
    row = await pg_conn.fetchrow("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM indicator_valuation
        WHERE total_mv IS NOT NULL
    """)
    if row['min_date']:
        print(f"Date range (with mv): {row['min_date']} to {row['max_date']}")

    # Unique codes with market cap
    row = await pg_conn.fetchrow("""
        SELECT COUNT(DISTINCT code) as code_count
        FROM indicator_valuation
        WHERE total_mv IS NOT NULL
    """)
    print(f"Unique codes with market cap: {row['code_count']:,}")

    print("=" * 50)


async def main():
    parser = argparse.ArgumentParser(description="Import market cap data to PostgreSQL")
    parser.add_argument("--source", type=str, help="Path to market_cap.db")
    parser.add_argument("--source-dir", type=str, help="Directory containing market_cap.db")
    parser.add_argument("-d", "--database", type=str, default=DEFAULT_POSTGRES_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--force", action="store_true", help="Force re-import all data")
    parser.add_argument("--status", action="store_true", help="Show statistics only")
    args = parser.parse_args()

    # Determine source file
    if args.source:
        sqlite_path = Path(args.source)
    elif args.source_dir:
        sqlite_path = Path(args.source_dir) / "market_cap.db"
    else:
        sqlite_path = DEFAULT_TRADING_DATA_DIR / "market_cap.db"

    print(f"SQLite source: {sqlite_path}")
    print(f"PostgreSQL: {args.database}")

    # Connect to PostgreSQL
    pg_conn = await asyncpg.connect(args.database)

    try:
        if args.status:
            await print_statistics(pg_conn)
        else:
            imported = await import_market_cap(sqlite_path, pg_conn, force=args.force)
            await print_statistics(pg_conn)
            print(f"\nTotal imported: {imported:,} records")
    finally:
        await pg_conn.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
