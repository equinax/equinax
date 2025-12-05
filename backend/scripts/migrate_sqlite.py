#!/usr/bin/env python3
"""
SQLite to PostgreSQL Data Migration Script

Migrates data from the existing SQLite database to PostgreSQL.
Source: /Users/dan/Code/q/trading_data/a_stock_2024.db
"""

import asyncio
import sqlite3
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import asyncpg

# Configuration
SQLITE_PATH = Path("/Users/dan/Code/q/trading_data/a_stock_2024.db")
POSTGRES_URL = "postgresql://quant:quant_dev_password@localhost:5432/quantdb"
BATCH_SIZE = 10000


async def create_tables(conn: asyncpg.Connection) -> None:
    """Create tables if they don't exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_basic (
            code VARCHAR(20) PRIMARY KEY,
            code_name VARCHAR(100),
            ipo_date DATE,
            out_date DATE,
            stock_type INTEGER,
            status INTEGER,
            exchange VARCHAR(10),
            sector VARCHAR(50),
            industry VARCHAR(100),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_k_data (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL,
            code VARCHAR(20) NOT NULL,
            open NUMERIC(12, 4),
            high NUMERIC(12, 4),
            low NUMERIC(12, 4),
            close NUMERIC(12, 4),
            preclose NUMERIC(12, 4),
            volume BIGINT,
            amount NUMERIC(18, 2),
            turn NUMERIC(8, 4),
            trade_status INTEGER,
            pct_chg NUMERIC(8, 4),
            pe_ttm NUMERIC(12, 4),
            pb_mrq NUMERIC(12, 4),
            ps_ttm NUMERIC(12, 4),
            pcf_ncf_ttm NUMERIC(12, 4),
            is_st INTEGER,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, date)
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS adjust_factor (
            id BIGSERIAL PRIMARY KEY,
            code VARCHAR(20) NOT NULL,
            divid_operate_date DATE,
            fore_adjust_factor NUMERIC(12, 6),
            back_adjust_factor NUMERIC(12, 6),
            adjust_factor NUMERIC(12, 6),
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code, divid_operate_date)
        )
    """)

    # Create indexes
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_date ON daily_k_data(date)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_code ON daily_k_data(code)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_adjust_factor_code ON adjust_factor(code)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_basic_exchange ON stock_basic(exchange)")

    print("Tables and indexes created successfully")


async def migrate_stock_basic(sqlite_conn: sqlite3.Connection, pg_conn: asyncpg.Connection) -> int:
    """Migrate stock_basic table."""
    print("\nMigrating stock_basic...")

    cursor = sqlite_conn.execute("SELECT * FROM stock_basic")
    rows = cursor.fetchall()

    if not rows:
        print("  No data in stock_basic")
        return 0

    # Get column names
    columns = [desc[0] for desc in cursor.description]
    print(f"  Found {len(rows)} records")

    # Prepare data
    records = []
    for row in rows:
        record = dict(zip(columns, row))
        code = record.get("code", "")
        exchange = "sh" if code.startswith("sh.") else "sz" if code.startswith("sz.") else None

        records.append((
            code,
            record.get("code_name"),
            record.get("ipo_date"),
            record.get("out_date"),
            record.get("type"),
            record.get("status"),
            exchange,
            None,  # sector
            None,  # industry
        ))

    # Batch insert
    await pg_conn.executemany(
        """
        INSERT INTO stock_basic (code, code_name, ipo_date, out_date, stock_type, status, exchange, sector, industry)
        VALUES ($1, $2, $3::date, $4::date, $5, $6, $7, $8, $9)
        ON CONFLICT (code) DO UPDATE SET
            code_name = EXCLUDED.code_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        records,
    )

    print(f"  Migrated {len(records)} stock_basic records")
    return len(records)


async def migrate_daily_k_data(sqlite_conn: sqlite3.Connection, pg_conn: asyncpg.Connection) -> int:
    """Migrate daily_k_data table in batches."""
    print("\nMigrating daily_k_data...")

    # Get total count
    cursor = sqlite_conn.execute("SELECT COUNT(*) FROM daily_k_data")
    total = cursor.fetchone()[0]
    print(f"  Total records: {total:,}")

    cursor = sqlite_conn.execute("SELECT * FROM daily_k_data")
    columns = [desc[0] for desc in cursor.description]

    migrated = 0
    batch = []

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            # Convert values
            def safe_decimal(val):
                if val is None or val == "":
                    return None
                try:
                    return Decimal(str(val))
                except:
                    return None

            def safe_int(val):
                if val is None or val == "":
                    return None
                try:
                    return int(float(val))
                except:
                    return None

            batch.append((
                record.get("date"),
                record.get("code"),
                safe_decimal(record.get("open")),
                safe_decimal(record.get("high")),
                safe_decimal(record.get("low")),
                safe_decimal(record.get("close")),
                safe_decimal(record.get("preclose")),
                safe_int(record.get("volume")),
                safe_decimal(record.get("amount")),
                safe_decimal(record.get("turn")),
                safe_int(record.get("tradestatus")),
                safe_decimal(record.get("pctChg")),
                safe_decimal(record.get("peTTM")),
                safe_decimal(record.get("pbMRQ")),
                safe_decimal(record.get("psTTM")),
                safe_decimal(record.get("pcfNcfTTM")),
                safe_int(record.get("isST")),
            ))

        # Insert batch
        await pg_conn.executemany(
            """
            INSERT INTO daily_k_data (
                date, code, open, high, low, close, preclose, volume, amount,
                turn, trade_status, pct_chg, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, is_st
            )
            VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (code, date) DO NOTHING
            """,
            batch,
        )

        migrated += len(batch)
        progress = (migrated / total) * 100
        print(f"  Progress: {migrated:,}/{total:,} ({progress:.1f}%)")
        batch = []

    print(f"  Migrated {migrated:,} daily_k_data records")
    return migrated


async def migrate_adjust_factor(sqlite_conn: sqlite3.Connection, pg_conn: asyncpg.Connection) -> int:
    """Migrate adjust_factor table."""
    print("\nMigrating adjust_factor...")

    cursor = sqlite_conn.execute("SELECT * FROM adjust_factor")
    rows = cursor.fetchall()

    if not rows:
        print("  No data in adjust_factor")
        return 0

    columns = [desc[0] for desc in cursor.description]
    print(f"  Found {len(rows)} records")

    records = []
    for row in rows:
        record = dict(zip(columns, row))

        def safe_decimal(val):
            if val is None or val == "":
                return None
            try:
                return Decimal(str(val))
            except:
                return None

        records.append((
            record.get("code"),
            record.get("dividOperateDate"),
            safe_decimal(record.get("foreAdjustFactor")),
            safe_decimal(record.get("backAdjustFactor")),
            safe_decimal(record.get("adjustFactor")),
        ))

    await pg_conn.executemany(
        """
        INSERT INTO adjust_factor (code, divid_operate_date, fore_adjust_factor, back_adjust_factor, adjust_factor)
        VALUES ($1, $2::date, $3, $4, $5)
        ON CONFLICT (code, divid_operate_date) DO NOTHING
        """,
        records,
    )

    print(f"  Migrated {len(records)} adjust_factor records")
    return len(records)


async def main():
    """Main migration function."""
    print("=" * 60)
    print("SQLite to PostgreSQL Migration")
    print("=" * 60)
    print(f"\nSource: {SQLITE_PATH}")
    print(f"Target: {POSTGRES_URL.split('@')[1]}")  # Hide credentials

    # Check SQLite database exists
    if not SQLITE_PATH.exists():
        print(f"\nError: SQLite database not found at {SQLITE_PATH}")
        return

    # Connect to SQLite
    print("\nConnecting to SQLite...")
    sqlite_conn = sqlite3.connect(str(SQLITE_PATH))
    sqlite_conn.row_factory = sqlite3.Row

    # Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    pg_conn = await asyncpg.connect(POSTGRES_URL)

    try:
        # Create tables
        await create_tables(pg_conn)

        # Migrate tables
        start_time = datetime.now()

        stock_count = await migrate_stock_basic(sqlite_conn, pg_conn)
        kdata_count = await migrate_daily_k_data(sqlite_conn, pg_conn)
        adjust_count = await migrate_adjust_factor(sqlite_conn, pg_conn)

        elapsed = datetime.now() - start_time

        print("\n" + "=" * 60)
        print("Migration Complete!")
        print("=" * 60)
        print(f"\nSummary:")
        print(f"  - stock_basic: {stock_count:,} records")
        print(f"  - daily_k_data: {kdata_count:,} records")
        print(f"  - adjust_factor: {adjust_count:,} records")
        print(f"  - Total time: {elapsed}")

    finally:
        sqlite_conn.close()
        await pg_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
