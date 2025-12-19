#!/usr/bin/env python3
"""
Institutional Holdings Data Import Script

Imports institutional holdings data from SQLite to PostgreSQL.
Updates the stock_microstructure table's institutional-related fields.

Usage:
    python scripts/import_institutional_holdings.py --source /path/to/institutional_holdings.db
    python scripts/import_institutional_holdings.py --source-dir /path/to/trading_data
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

# Threshold for "institutional" holdings
INSTITUTIONAL_THRESHOLD = 10.0  # 10% of float


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


async def get_existing_quarters(pg_conn: asyncpg.Connection) -> set:
    """Get quarters that already have institutional data in stock_microstructure."""
    rows = await pg_conn.fetch("""
        SELECT DISTINCT date FROM stock_microstructure
        WHERE fund_holding_ratio IS NOT NULL
        ORDER BY date DESC
        LIMIT 20
    """)
    return {row['date'] for row in rows}


async def import_institutional_holdings(
    sqlite_path: Path,
    pg_conn: asyncpg.Connection,
    force: bool = False
) -> int:
    """
    Import institutional holdings data from SQLite to PostgreSQL.

    Updates the stock_microstructure table's fund holding fields.
    """
    print(f"\nImporting institutional holdings from: {sqlite_path}")

    if not sqlite_path.exists():
        print(f"  SQLite file not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        # Get count
        cursor = sqlite_conn.execute("SELECT COUNT(*) FROM institutional_holdings")
        total_count = cursor.fetchone()[0]
        print(f"  Found {total_count:,} institutional holding records in SQLite")

        if total_count == 0:
            return 0

        # Get summary
        cursor = sqlite_conn.execute("""
            SELECT report_date, COUNT(*) as cnt, AVG(holding_ratio) as avg_ratio
            FROM institutional_holdings
            GROUP BY report_date
            ORDER BY report_date DESC
        """)
        print("  SQLite quarters:")
        for row in cursor:
            print(f"    {row['report_date']}: {row['cnt']} stocks, avg ratio {row['avg_ratio']:.2f}%" if row['avg_ratio'] else f"    {row['report_date']}: {row['cnt']} stocks")

        # Check existing data in PostgreSQL
        if not force:
            existing_quarters = await get_existing_quarters(pg_conn)
            if existing_quarters:
                print(f"  PostgreSQL already has {len(existing_quarters)} quarters with institutional data")

        # Get the latest quarter's data
        cursor = sqlite_conn.execute("""
            SELECT code, report_date, holding_ratio, holding_change
            FROM institutional_holdings
            WHERE report_date = (SELECT MAX(report_date) FROM institutional_holdings)
            ORDER BY code
        """)

        batch = []
        total_imported = 0

        for row in cursor:
            holding_ratio = safe_decimal(row['holding_ratio'])
            holding_change = safe_decimal(row['holding_change'])
            report_date = parse_date(row['report_date'])

            # Calculate is_institutional flag
            is_inst = holding_ratio is not None and holding_ratio > INSTITUTIONAL_THRESHOLD

            batch.append((
                row['code'],
                report_date,
                holding_ratio,
                holding_change,
                is_inst,
            ))

            if len(batch) >= BATCH_SIZE:
                imported = await upsert_batch(pg_conn, batch)
                total_imported += imported
                print(f"    Imported batch: {total_imported:,}")
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
    Upsert institutional holdings data into stock_microstructure.

    If the row doesn't exist, creates it with just institutional data.
    If the row exists, updates only fund holding fields.
    """
    if not records:
        return 0

    # Use INSERT ... ON CONFLICT to upsert
    # Note: stock_microstructure has (code, date) as primary key
    # We need to include all NOT NULL boolean columns with defaults
    await pg_conn.executemany(
        """
        INSERT INTO stock_microstructure (
            code, date, fund_holding_ratio, fund_holding_change, is_institutional,
            is_northbound_heavy, is_retail_hot, is_main_controlled
        )
        VALUES ($1, $2, $3, $4, $5, FALSE, FALSE, FALSE)
        ON CONFLICT (code, date) DO UPDATE SET
            fund_holding_ratio = COALESCE(EXCLUDED.fund_holding_ratio, stock_microstructure.fund_holding_ratio),
            fund_holding_change = COALESCE(EXCLUDED.fund_holding_change, stock_microstructure.fund_holding_change),
            is_institutional = EXCLUDED.is_institutional
        """,
        records,
    )

    return len(records)


async def calculate_institutional_flag(pg_conn: asyncpg.Connection, calc_date: date = None):
    """
    Recalculate is_institutional flag for all records.

    Args:
        pg_conn: PostgreSQL connection
        calc_date: Specific date to calculate, or None for all dates
    """
    if calc_date:
        print(f"\nRecalculating is_institutional for {calc_date}...")
        result = await pg_conn.execute("""
            UPDATE stock_microstructure
            SET is_institutional = (fund_holding_ratio > $1)
            WHERE date = $2
            AND fund_holding_ratio IS NOT NULL
        """, INSTITUTIONAL_THRESHOLD, calc_date)
    else:
        print("\nRecalculating is_institutional for all records...")
        result = await pg_conn.execute("""
            UPDATE stock_microstructure
            SET is_institutional = (fund_holding_ratio > $1)
            WHERE fund_holding_ratio IS NOT NULL
        """, INSTITUTIONAL_THRESHOLD)

    print(f"  Updated: {result}")


async def print_statistics(pg_conn: asyncpg.Connection):
    """Print statistics about institutional data in PostgreSQL."""
    print("\n" + "=" * 50)
    print("PostgreSQL Institutional Holdings Statistics")
    print("=" * 50)

    # Total records with institutional data
    row = await pg_conn.fetchrow("""
        SELECT COUNT(*) as total,
               COUNT(fund_holding_ratio) as with_fund,
               COUNT(*) FILTER (WHERE is_institutional) as inst_count
        FROM stock_microstructure
    """)
    print(f"Total microstructure records: {row['total']:,}")
    print(f"With fund data: {row['with_fund']:,}")
    print(f"Institutional (>10%): {row['inst_count']:,}")

    # Date range with fund data
    row = await pg_conn.fetchrow("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM stock_microstructure
        WHERE fund_holding_ratio IS NOT NULL
    """)
    if row['min_date']:
        print(f"Date range (fund data): {row['min_date']} to {row['max_date']}")

    # Latest date statistics
    row = await pg_conn.fetchrow("""
        SELECT date,
               COUNT(*) as stock_count,
               AVG(fund_holding_ratio) as avg_ratio,
               COUNT(*) FILTER (WHERE is_institutional) as inst_count
        FROM stock_microstructure
        WHERE fund_holding_ratio IS NOT NULL
        GROUP BY date
        ORDER BY date DESC
        LIMIT 1
    """)
    if row:
        print(f"\nLatest data ({row['date']}):")
        print(f"  Stocks with data: {row['stock_count']:,}")
        print(f"  Average holding ratio: {float(row['avg_ratio'] or 0):.2f}%")
        print(f"  Institutional stocks: {row['inst_count']}")

    # Top 10 institutional holdings
    rows = await pg_conn.fetch("""
        SELECT code, fund_holding_ratio
        FROM stock_microstructure
        WHERE date = (SELECT MAX(date) FROM stock_microstructure WHERE fund_holding_ratio IS NOT NULL)
        AND fund_holding_ratio IS NOT NULL
        ORDER BY fund_holding_ratio DESC
        LIMIT 10
    """)
    if rows:
        print("\nTop 10 institutional holdings:")
        for row in rows:
            print(f"  {row['code']}: {float(row['fund_holding_ratio']):.2f}%")

    print("=" * 50)


async def main():
    parser = argparse.ArgumentParser(description="Import institutional holdings data to PostgreSQL")
    parser.add_argument("--source", type=str, help="Path to institutional_holdings.db")
    parser.add_argument("--source-dir", type=str, help="Directory containing institutional_holdings.db")
    parser.add_argument("-d", "--database", type=str, default=DEFAULT_POSTGRES_URL,
                        help="PostgreSQL connection URL")
    parser.add_argument("--force", action="store_true", help="Force re-import all data")
    parser.add_argument("--recalc", action="store_true", help="Recalculate is_institutional flag")
    parser.add_argument("--status", action="store_true", help="Show statistics only")
    args = parser.parse_args()

    # Determine source file
    if args.source:
        sqlite_path = Path(args.source)
    elif args.source_dir:
        sqlite_path = Path(args.source_dir) / "institutional_holdings.db"
    else:
        sqlite_path = DEFAULT_TRADING_DATA_DIR / "institutional_holdings.db"

    print(f"SQLite source: {sqlite_path}")
    print(f"PostgreSQL: {args.database}")

    # Connect to PostgreSQL
    pg_conn = await asyncpg.connect(args.database)

    try:
        if args.status:
            await print_statistics(pg_conn)
        elif args.recalc:
            await calculate_institutional_flag(pg_conn)
            await print_statistics(pg_conn)
        else:
            imported = await import_institutional_holdings(sqlite_path, pg_conn, force=args.force)
            await print_statistics(pg_conn)
            print(f"\nTotal imported: {imported:,} records")
    finally:
        await pg_conn.close()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
