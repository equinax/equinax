#!/usr/bin/env python
"""
数据补全脚本

使用数据源抽象层补全缺失的市场数据。

Usage:
    # 补全最近 7 天的数据
    python -m scripts.backfill_data --days 7
    
    # 补全指定日期范围
    python -m scripts.backfill_data --start 2026-01-16 --end 2026-01-22
    
    # 只补全 ETF 数据
    python -m scripts.backfill_data --days 7 --types etf
    
    # 使用 akshare 数据源
    python -m scripts.backfill_data --days 7 --source akshare
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import date, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description='补全缺失的市场数据')
    parser.add_argument('--days', type=int, default=7, help='补全最近 N 天的数据')
    parser.add_argument('--start', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--types', type=str, default='stock,etf,index',
                        help='资产类型，逗号分隔 (stock,etf,index)')
    parser.add_argument('--source', type=str, default='tushare',
                        choices=['tushare', 'akshare'],
                        help='数据源 (tushare/akshare)')
    args = parser.parse_args()
    
    # 设置数据源
    os.environ['DATA_SOURCE'] = args.source
    
    # 解析日期范围
    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        start_date = date.today() - timedelta(days=args.days)
    
    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = date.today()
    
    # 解析资产类型
    asset_types = [t.strip() for t in args.types.split(',')]
    
    logger.info(f"数据补全配置:")
    logger.info(f"  数据源: {args.source}")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    logger.info(f"  资产类型: {asset_types}")
    
    # 创建数据库连接
    engine = create_async_engine(settings.database_url)
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    # 导入同步模块
    from workers.source_sync import backfill_missing_dates
    
    async def progress_callback(message, progress, detail):
        logger.info(f"[{progress}%] {message}")
    
    async with session_maker() as session:
        result = await backfill_missing_dates(
            session,
            start_date,
            end_date,
            asset_types=asset_types,
            progress_callback=progress_callback,
        )
    
    await engine.dispose()
    
    # 输出结果
    logger.info("=" * 60)
    logger.info("补全完成!")
    logger.info(f"  状态: {result['status']}")
    logger.info(f"  数据源: {result['data_source']}")
    logger.info(f"  同步天数: {result['dates_synced']}")
    logger.info(f"  股票记录: {result['stock_count']}")
    logger.info(f"  ETF记录: {result['etf_count']}")
    logger.info(f"  指数记录: {result['index_count']}")
    logger.info(f"  估值记录: {result['valuation_count']}")
    
    if result['errors']:
        logger.warning(f"  错误数: {len(result['errors'])}")
        for error in result['errors'][:5]:
            logger.warning(f"    - {error}")


if __name__ == '__main__':
    asyncio.run(main())
