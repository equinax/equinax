"""Import industry classification from local SQLite database.

This script imports industry classification data from the local SQLite database
(created by download_industry_data.py in trading_data directory) into PostgreSQL.

This approach:
- Avoids network dependency during data import
- Uses local cached data that can be updated periodically
- Is faster and more reliable than fetching from network each time

Usage:
    python -m scripts.import_sw_industry

    # Specify source database
    python -m scripts.import_sw_industry --source /path/to/industry_classification.db

    # Specify PostgreSQL database
    DATABASE_URL=postgresql://... python -m scripts.import_sw_industry
"""

import argparse
import asyncio
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import text, select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent to path for imports
sys.path.insert(0, "/Users/dan/Code/q/v1/backend")

from app.config import settings
from app.db.models.classification import IndustryClassification, StockIndustryMapping


# Default source database path - use cache or fixtures
CACHE_DB = Path(__file__).parent.parent / "data" / "cache" / "industry_classification.db"
FIXTURES_DB = Path(__file__).parent.parent / "data" / "fixtures" / "sample_industries.db"
DEFAULT_SOURCE_DB = CACHE_DB if CACHE_DB.exists() else FIXTURES_DB


async def get_async_session():
    """Create async database session."""
    engine = create_async_engine(
        settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
        echo=False,
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session(), engine


async def import_industries_from_sqlite(
    session: AsyncSession,
    source_db: Path,
    system: Optional[str] = None
) -> tuple[int, int]:
    """Import industry classification from local SQLite database.

    Args:
        session: Async database session
        source_db: Path to source SQLite database
        system: Classification system to import (None = all, 'em', 'sw')

    Returns:
        Tuple of (industries_count, mappings_count)
    """
    print(f"\n=== Importing Industry Classification from SQLite ===")
    print(f"Source database: {source_db}")

    if not source_db.exists():
        print(f"Error: Source database not found: {source_db}")
        print("Please run download_industry_data.py first to create the database.")
        return 0, 0

    # Connect to source SQLite database
    sqlite_conn = sqlite3.connect(str(source_db))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        cursor = sqlite_conn.cursor()

        # Build WHERE clause for system filter
        system_filter = ""
        if system:
            system_filter = f"WHERE classification_system = '{system}'"

        # Get industries from SQLite
        cursor.execute(f"""
            SELECT industry_code, industry_name, industry_level, parent_code, classification_system
            FROM industry_classification
            {system_filter}
        """)
        industries = cursor.fetchall()

        if not industries:
            print("No industries found in source database")
            return 0, 0

        print(f"Found {len(industries)} industries to import")

        # Clear existing data
        if system:
            print(f"Clearing existing {system} industry data...")
            await session.execute(delete(StockIndustryMapping).where(
                StockIndustryMapping.classification_system == system
            ))
            await session.execute(delete(IndustryClassification).where(
                IndustryClassification.industry_code.in_(
                    [row['industry_code'] for row in industries]
                )
            ))
        else:
            print("Clearing all existing industry data...")
            await session.execute(delete(StockIndustryMapping))
            await session.execute(delete(IndustryClassification))
        await session.commit()

        # Import industries
        industries_count = 0
        for row in industries:
            industry = IndustryClassification(
                industry_code=row['industry_code'],
                industry_name=row['industry_name'],
                industry_level=row['industry_level'],
                parent_code=row['parent_code'],
            )
            session.add(industry)
            industries_count += 1

        await session.commit()
        print(f"Imported {industries_count} industries")

        # Get mappings from SQLite
        cursor.execute(f"""
            SELECT stock_code, industry_code, classification_system, effective_date, expire_date
            FROM stock_industry_mapping
            {system_filter}
        """)
        mappings = cursor.fetchall()

        print(f"Found {len(mappings)} stock-industry mappings to import")

        # Import mappings in batches
        mappings_count = 0
        batch_size = 1000

        for i in range(0, len(mappings), batch_size):
            batch = mappings[i:i + batch_size]

            for row in batch:
                mapping = StockIndustryMapping(
                    stock_code=row['stock_code'],
                    industry_code=row['industry_code'],
                    classification_system=row['classification_system'],
                    effective_date=date.fromisoformat(row['effective_date']),
                    expire_date=date.fromisoformat(row['expire_date']) if row['expire_date'] else None,
                )
                session.add(mapping)
                mappings_count += 1

            await session.commit()
            print(f"  Imported {mappings_count}/{len(mappings)} mappings...")

        print(f"Imported {mappings_count} stock-industry mappings")

        return industries_count, mappings_count

    finally:
        sqlite_conn.close()


async def update_stock_profile_industries(session: AsyncSession):
    """Update stock_profile table with primary industry classification.

    Uses Shenwan (SW) L1 classification as the default industry.
    """
    print("\n=== Updating Stock Profile Industries ===\n")

    # Get all SW L1 mappings with industry names
    result = await session.execute(
        select(StockIndustryMapping, IndustryClassification)
        .join(IndustryClassification,
              StockIndustryMapping.industry_code == IndustryClassification.industry_code)
        .where(StockIndustryMapping.classification_system == "sw")
        .where(IndustryClassification.industry_level == 1)
        .where(StockIndustryMapping.expire_date.is_(None))
    )
    mappings = result.all()

    print(f"Found {len(mappings)} active SW L1 industry mappings")

    # Update stock_profile
    updated = 0
    for mapping, industry in mappings:
        result = await session.execute(
            text("""
                UPDATE stock_profile
                SET sw_industry_l1 = :industry_name,
                    updated_at = NOW()
                WHERE code = :stock_code
            """),
            {"industry_name": industry.industry_name, "stock_code": mapping.stock_code}
        )
        if result.rowcount > 0:
            updated += 1

    await session.commit()
    print(f"Updated {updated} stock profiles with industry classification")


async def main(source_db: Path, system: Optional[str] = None):
    """Main entry point."""
    print("=" * 60)
    print("Industry Classification Import (from SQLite)")
    print("=" * 60)
    print(f"\nSource database: {source_db}")
    print(f"Target database: {settings.database_url[:50]}...")
    print(f"Classification system: {system or 'all'}")
    print(f"Start time: {datetime.now()}")

    session, engine = await get_async_session()

    try:
        # Import from SQLite
        industries, mappings = await import_industries_from_sqlite(session, source_db, system)

        # Update stock profiles with primary classification
        if industries > 0:
            await update_stock_profile_industries(session)

        print("\n" + "=" * 60)
        print("Import Summary")
        print("=" * 60)
        print(f"Industries imported: {industries}")
        print(f"Mappings imported: {mappings}")
        print(f"End time: {datetime.now()}")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await session.close()
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import industry classification from SQLite to PostgreSQL"
    )
    parser.add_argument(
        "--source", "-s",
        type=Path,
        default=DEFAULT_SOURCE_DB,
        help=f"Source SQLite database path (default: {DEFAULT_SOURCE_DB})"
    )
    parser.add_argument(
        "--system",
        choices=["em", "sw"],
        default=None,
        help="Import only this classification system (default: all)"
    )

    args = parser.parse_args()

    asyncio.run(main(args.source, args.system))
