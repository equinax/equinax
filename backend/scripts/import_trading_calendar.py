"""Import trading calendar from TuShare trade_cal API.

Fetches SSE trading calendar and populates the trading_calendar table.
Should be run once to seed historical data (2015-2027), then periodically
to extend into future years.

Usage:
    docker compose exec api python -m scripts.import_trading_calendar
    docker compose exec api python -m scripts.import_trading_calendar --start 20260101 --end 20271231
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def get_async_session():
    engine = create_async_engine(
        settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
        echo=False,
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session(), engine


def fetch_trade_cal(start_date: str, end_date: str) -> list[dict]:
    """Fetch trading calendar from TuShare.

    Args:
        start_date: YYYYMMDD format
        end_date: YYYYMMDD format

    Returns:
        List of dicts with keys: cal_date, exchange, is_open, pretrade_date
    """
    import tushare as ts

    api_key = os.environ.get("TUSHARE_API_KEY")
    if not api_key:
        raise ValueError("TUSHARE_API_KEY environment variable is required")

    ts.set_token(api_key)
    pro = ts.pro_api()

    logger.info(f"Fetching trade_cal from TuShare: {start_date} ~ {end_date}")
    df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date)

    if df is None or df.empty:
        logger.warning("TuShare returned empty trade_cal data")
        return []

    logger.info(f"Fetched {len(df)} calendar rows from TuShare")

    rows = []
    for _, r in df.iterrows():
        cal_date_str = str(r["cal_date"])
        # TuShare returns YYYYMMDD strings
        cal_date = datetime.strptime(cal_date_str, "%Y%m%d").date()

        pretrade_str = (
            str(r["pretrade_date"])
            if r["pretrade_date"] and str(r["pretrade_date"]) != ""
            else None
        )
        pretrade_date = None
        if pretrade_str and pretrade_str != "nan" and pretrade_str != "None":
            try:
                pretrade_date = datetime.strptime(pretrade_str, "%Y%m%d").date()
            except (ValueError, TypeError):
                pass

        rows.append(
            {
                "cal_date": cal_date,
                "exchange": str(r["exchange"]),
                "is_open": int(r["is_open"]),
                "pretrade_date": pretrade_date,
            }
        )

    return rows


async def upsert_trading_calendar(session: AsyncSession, rows: list[dict]) -> int:
    """UPSERT rows into trading_calendar using ON CONFLICT."""
    if not rows:
        return 0

    # Batch insert with ON CONFLICT DO UPDATE
    upsert_sql = text("""
        INSERT INTO trading_calendar (cal_date, exchange, is_open, pretrade_date)
        VALUES (:cal_date, :exchange, :is_open, :pretrade_date)
        ON CONFLICT (cal_date, exchange)
        DO UPDATE SET
            is_open = EXCLUDED.is_open,
            pretrade_date = EXCLUDED.pretrade_date
    """)

    # Execute in batches of 1000
    batch_size = 1000
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        await session.execute(upsert_sql, batch)
        total += len(batch)
        if total % 2000 == 0:
            logger.info(f"  Upserted {total}/{len(rows)} rows...")

    await session.commit()
    logger.info(f"Upserted {total} rows into trading_calendar")
    return total


async def main(start_date: str, end_date: str):
    session, engine = await get_async_session()

    try:
        rows = fetch_trade_cal(start_date, end_date)
        if not rows:
            logger.error("No data fetched, aborting")
            return

        trading_days = sum(1 for r in rows if r["is_open"] == 1)
        non_trading = sum(1 for r in rows if r["is_open"] == 0)
        logger.info(
            f"Calendar summary: {trading_days} trading days, {non_trading} non-trading days"
        )

        count = await upsert_trading_calendar(session, rows)
        logger.info(f"Done! {count} total rows written to trading_calendar")

        # Verify
        result = await session.execute(
            text("SELECT COUNT(*) FROM trading_calendar WHERE is_open = 1")
        )
        open_count = result.scalar()
        logger.info(f"Verification: {open_count} trading days in trading_calendar")

    finally:
        await session.close()
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import trading calendar from TuShare")
    parser.add_argument(
        "--start", default="20150101", help="Start date YYYYMMDD (default: 20150101)"
    )
    parser.add_argument("--end", default="20271231", help="End date YYYYMMDD (default: 20271231)")
    args = parser.parse_args()

    asyncio.run(main(args.start, args.end))
