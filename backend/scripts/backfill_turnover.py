#!/usr/bin/env python
"""
Backfill missing turnover data in market_daily from TuShare valuation data.

TuShare's daily() API doesn't return turnover rate, but daily_basic() does.
This script fetches valuation data for dates where market_daily.turn is NULL
and updates the table.

Usage:
    # Backfill last 10 days
    python -m scripts.backfill_turnover --days 10

    # Backfill specific date range
    python -m scripts.backfill_turnover --start 2026-01-16 --end 2026-01-26
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def get_dates_missing_turnover(
    session: AsyncSession, start_date: date, end_date: date
) -> list[date]:
    """Find dates where most stocks are missing turnover data."""
    result = await session.execute(
        text("""
            SELECT date, COUNT(*) as total, COUNT(turn) as with_turn
            FROM market_daily
            WHERE date >= :start_date AND date <= :end_date
              AND (code LIKE 'sh.6%%' OR code LIKE 'sz.0%%' OR code LIKE 'sz.3%%')
            GROUP BY date
            HAVING COUNT(turn) < COUNT(*) * 0.1
            ORDER BY date
        """),
        {"start_date": start_date, "end_date": end_date},
    )

    dates = []
    for row in result.fetchall():
        dates.append(row[0])
        logger.info(f"  {row[0]}: {row[2]}/{row[1]} stocks have turnover")

    return dates


async def backfill_turnover_for_date(session: AsyncSession, trade_date: date) -> int:
    """Fetch valuation data from TuShare and update market_daily.turn."""
    from workers.data_sources import get_data_source

    source = get_data_source()

    try:
        df = source.fetch_valuation_by_date(trade_date)

        if df.empty:
            logger.warning(f"No valuation data for {trade_date}")
            return 0

        # Filter records with valid turnover
        df_with_turn = df[df["turn"].notna()]

        if df_with_turn.empty:
            logger.warning(f"No turnover data in valuation for {trade_date}")
            return 0

        # Update market_daily.turn
        update_sql = text("""
            UPDATE market_daily 
            SET turn = :turn
            WHERE code = :code AND date = :date
        """)

        count = 0
        batch = []
        for _, row in df_with_turn.iterrows():
            batch.append(
                {
                    "code": row["code"],
                    "date": row["trade_date"],
                    "turn": float(row["turn"]) if row["turn"] else None,
                }
            )

            if len(batch) >= 1000:
                await session.execute(update_sql, batch)
                count += len(batch)
                batch = []

        if batch:
            await session.execute(update_sql, batch)
            count += len(batch)

        await session.commit()
        return count

    except Exception as e:
        logger.error(f"Error fetching valuation for {trade_date}: {e}")
        raise


async def main():
    parser = argparse.ArgumentParser(description="Backfill missing turnover data")
    parser.add_argument("--days", type=int, default=10, help="Backfill last N days")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be done")
    args = parser.parse_args()

    # Parse date range
    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        start_date = date.today() - timedelta(days=args.days)

    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = date.today()

    logger.info(f"Backfilling turnover data from {start_date} to {end_date}")

    # Create database connection
    engine = create_async_engine(settings.database_url)
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_maker() as session:
        # Find dates with missing turnover
        logger.info("Finding dates with missing turnover data...")
        dates = await get_dates_missing_turnover(session, start_date, end_date)

        if not dates:
            logger.info("No dates with missing turnover data found!")
            await engine.dispose()
            return

        logger.info(f"Found {len(dates)} dates with missing turnover")

        if args.dry_run:
            logger.info("Dry run - no changes made")
            await engine.dispose()
            return

        # Backfill each date
        total_updated = 0
        for trade_date in dates:
            logger.info(f"Backfilling {trade_date}...")
            count = await backfill_turnover_for_date(session, trade_date)
            logger.info(f"  Updated {count} records")
            total_updated += count

            # Rate limit
            await asyncio.sleep(0.5)

        logger.info(f"Done! Total updated: {total_updated} records")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
