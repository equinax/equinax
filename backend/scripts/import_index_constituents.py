#!/usr/bin/env python3
"""
Index Constituents Data Import Script

Imports index constituent data from SQLite to PostgreSQL.
Updates the index_constituents table.

Usage:
    python scripts/import_index_constituents.py --source /path/to/index_constituents.db
    python scripts/import_index_constituents.py --source-dir /path/to/trading_data
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

# Index code mapping (SQLite uses 000300, PostgreSQL uses hs300)
INDEX_CODE_MAP = {
    "000300": "hs300",
    "000905": "zz500",
    "000852": "zz1000",
    "000016": "sz50",
    "399006": "cyb",
    "000688": "kc50",
}


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


def map_index_code(code: str) -> str:
    """Map numeric index code to short name."""
    return INDEX_CODE_MAP.get(code, code)


async def get_existing_indices(pg_conn: asyncpg.Connection) -> dict:
    """Get existing index data in PostgreSQL."""
    rows = await pg_conn.fetch("""
        SELECT index_code, COUNT(*) as count, MAX(effective_date) as latest
        FROM index_constituents
        GROUP BY index_code
    """)
    return {row['index_code']: {'count': row['count'], 'latest': row['latest']} for row in rows}


async def import_index_constituents(
    sqlite_path: Path,
    pg_conn: asyncpg.Connection,
    force: bool = False
) -> int:
    """
    Import index constituent data from SQLite to PostgreSQL.
    """
    print(f"\nImporting index constituents from: {sqlite_path}")

    if not sqlite_path.exists():
        print(f"  SQLite file not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        # Get count
        cursor = sqlite_conn.execute("SELECT COUNT(*) FROM index_constituents")
        total_count = cursor.fetchone()[0]
        print(f"  Found {total_count:,} constituent records in SQLite")

        if total_count == 0:
            return 0

        # Get summary by index
        cursor = sqlite_conn.execute("""
            SELECT index_code, index_name, COUNT(*) as count, MAX(effective_date) as latest
            FROM index_constituents
            GROUP BY index_code
        """)
        print("  SQLite index summary:")
        for row in cursor:
            print(f"    {row['index_name'] or row['index_code']}: {row['count']} stocks (as of {row['latest']})")

        # Check existing data in PostgreSQL
        if not force:
            existing = await get_existing_indices(pg_conn)
            if existing:
                print("  PostgreSQL existing data:")
                for idx, info in existing.items():
                    print(f"    {idx}: {info['count']} stocks (as of {info['latest']})")

        # Process in batches
        cursor = sqlite_conn.execute("""
            SELECT index_code, stock_code, weight, effective_date
            FROM index_constituents
            ORDER BY index_code, stock_code
        """)

        batch = []
        total_imported = 0

        for row in cursor:
            batch.append((
                map_index_code(row['index_code']),
                row['stock_code'],
                safe_decimal(row['weight']),
                parse_date(row['effective_date']),
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
    Upsert index constituent data into PostgreSQL.
    """
    if not records:
        return 0

    # Use INSERT ... ON CONFLICT to upsert
    await pg_conn.executemany(
        """
        INSERT INTO index_constituents (index_code, stock_code, weight, effective_date)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (index_code, stock_code, effective_date) DO UPDATE SET
            weight = COALESCE(EXCLUDED.weight, index_constituents.weight)
        """,
        records,
    )

    return len(records)


async def clear_index_data(pg_conn: asyncpg.Connection, index_code: str = None):
    """Clear existing index data."""
    if index_code:
        mapped_code = map_index_code(index_code)
        await pg_conn.execute(
            "DELETE FROM index_constituents WHERE index_code = $1",
            mapped_code
        )
        print(f"  Cleared data for index: {mapped_code}")
    else:
        await pg_conn.execute("DELETE FROM index_constituents")
        print("  Cleared all index constituent data")


async def print_statistics(pg_conn: asyncpg.Connection):
    """Print statistics about index constituent data in PostgreSQL."""
    print("\n" + "=" * 50)
    print("PostgreSQL Index Constituents Statistics")
    print("=" * 50)

    # Total records
    row = await pg_conn.fetchrow("""
        SELECT COUNT(*) as total,
               COUNT(DISTINCT index_code) as indices,
               COUNT(DISTINCT stock_code) as stocks
        FROM index_constituents
    """)
    print(f"Total records: {row['total']:,}")
    print(f"Distinct indices: {row['indices']}")
    print(f"Distinct stocks: {row['stocks']:,}")

    # By index
    rows = await pg_conn.fetch("""
        SELECT index_code,
               COUNT(*) as stock_count,
               MAX(effective_date) as latest_date
        FROM index_constituents
        WHERE expire_date IS NULL
        GROUP BY index_code
        ORDER BY stock_count DESC
    """)

    if rows:
        print("\nBy Index (current constituents):")
        for row in rows:
            print(f"  {row['index_code']}: {row['stock_count']:,} stocks (as of {row['latest_date']})")

    print("=" * 50)


async def main():
    parser = argparse.ArgumentParser(description="Import index constituent data to PostgreSQL")
    parser.add_argument("--source", type=str, help="Path to index_constituents.db")
    parser.add_argument("--source-dir", type=str, help="Directory containing index_constituents.db")
    parser.add_argument("-d", "--database", type=str, default=DEFAULT_POSTGRES_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--force", action="store_true", help="Force re-import all data")
    parser.add_argument("--clear", action="store_true", help="Clear existing data before import")
    parser.add_argument("--clear-index", type=str, help="Clear specific index before import")
    parser.add_argument("--status", action="store_true", help="Show statistics only")
    args = parser.parse_args()

    # Determine source file
    if args.source:
        sqlite_path = Path(args.source)
    elif args.source_dir:
        sqlite_path = Path(args.source_dir) / "index_constituents.db"
    else:
        sqlite_path = DEFAULT_TRADING_DATA_DIR / "index_constituents.db"

    print(f"SQLite source: {sqlite_path}")
    print(f"PostgreSQL: {args.database}")

    # Connect to PostgreSQL
    pg_conn = await asyncpg.connect(args.database)

    try:
        if args.status:
            await print_statistics(pg_conn)
        else:
            if args.clear:
                await clear_index_data(pg_conn)
            elif args.clear_index:
                await clear_index_data(pg_conn, args.clear_index)

            imported = await import_index_constituents(sqlite_path, pg_conn, force=args.force)
            await print_statistics(pg_conn)
            print(f"\nTotal imported: {imported:,} records")
    finally:
        await pg_conn.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
