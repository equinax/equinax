#!/usr/bin/env python
"""
ETF 数据重新同步脚本

使用 TuShare 重新同步所有 ETF 历史数据，修复成交量单位不一致问题。
数据将使用 ON CONFLICT DO UPDATE 覆盖旧数据。

Usage:
    # 重新同步所有 ETF 数据（从 2024-01-01 至今）
    python -m scripts.resync_etf_data

    # 指定日期范围
    python -m scripts.resync_etf_data --start 2024-01-01 --end 2026-01-26
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="重新同步 ETF 历史数据")
    parser.add_argument("--start", type=str, default="2024-01-01", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="结束日期 (YYYY-MM-DD)，默认今天")
    args = parser.parse_args()

    os.environ["DATA_SOURCE"] = "tushare"

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end) if args.end else date.today()

    logger.info("=" * 60)
    logger.info("ETF 数据重新同步")
    logger.info("=" * 60)
    logger.info(f"  数据源: TuShare")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    logger.info("=" * 60)

    engine = create_async_engine(settings.database_url)
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    from workers.source_sync import backfill_missing_dates

    async def progress_callback(message, progress, detail):
        logger.info(f"[{progress}%] {message}")

    async with session_maker() as session:
        result = await backfill_missing_dates(
            session,
            start_date,
            end_date,
            asset_types=["etf"],
            progress_callback=progress_callback,
        )

    await engine.dispose()

    logger.info("=" * 60)
    logger.info("ETF 数据重新同步完成!")
    logger.info(f"  状态: {result['status']}")
    logger.info(f"  同步天数: {result['dates_synced']}")
    logger.info(f"  ETF记录: {result['etf_count']}")

    if result.get("errors"):
        logger.warning(f"  错误数: {len(result['errors'])}")
        for error in result["errors"][:5]:
            logger.warning(f"    - {error}")


if __name__ == "__main__":
    asyncio.run(main())
