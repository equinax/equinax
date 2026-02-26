"""Data download and update tasks for ARQ.

Implements daily data update workflow:
1. Download today's stock/ETF market data
2. Import data to PostgreSQL (with circ_mv calculation)
3. Trigger classification recalculation

Note: northbound and market_cap download tasks have been removed
(APIs only support current-day snapshots, not historical data).
circ_mv is now calculated from amount/turnover during import.

These tasks can be scheduled via cron or triggered manually.
"""

import asyncio
import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import settings
from app.core.redis_pubsub import publish_data_sync_event

logger = logging.getLogger(__name__)

# Paths
BACKEND_DIR = Path(__file__).parent.parent
DATA_DIR = BACKEND_DIR / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
CACHE_DIR = DATA_DIR / "cache"
SCRIPTS_DIR = BACKEND_DIR / "scripts"

# Create engine for worker
worker_engine = create_async_engine(
    settings.database_url,
    pool_size=5,
    max_overflow=5,
)
worker_session_maker = async_sessionmaker(
    worker_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# =============================================================================
# Download Tasks
# =============================================================================


async def download_stock_data(ctx: Dict[str, Any], recent_days: int = 1) -> Dict[str, Any]:
    """Download stock market data using incremental update mode.

    The download script will check for outdated stocks and only download
    data for those that need updating.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to check (default: 1)

    Returns:
        Result dict with status and counts
    """
    logger.info(f"Starting stock data download (recent={recent_days})")

    script_path = DOWNLOADS_DIR / "download_a_stock_data.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
        # Run download script with --recent for incremental update
        result = subprocess.run(
            [sys.executable, str(script_path), "--recent", str(recent_days)],
            cwd=str(BACKEND_DIR),
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )

        if result.returncode != 0:
            logger.error(f"Stock download failed: {result.stderr}")
            return {
                "status": "error",
                "message": result.stderr[-500:] if result.stderr else "Unknown error",
            }

        logger.info("Stock data download completed")
        return {
            "status": "success",
            "message": "Stock data downloaded",
            "output": result.stdout[-1000:] if result.stdout else "",
        }

    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Download timed out after 1 hour"}
    except Exception as e:
        logger.exception("Stock download error")
        return {"status": "error", "message": str(e)}


async def download_etf_data(ctx: Dict[str, Any], recent_days: int = 1) -> Dict[str, Any]:
    """Download ETF market data using incremental update mode.

    The download script will check for outdated ETFs and only download
    data for those that need updating.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to check (default: 1)

    Returns:
        Result dict with status
    """
    logger.info(f"Starting ETF data download (recent={recent_days})")

    script_path = DOWNLOADS_DIR / "download_etf_data.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
        # Run download script with --recent for incremental update
        result = subprocess.run(
            [sys.executable, str(script_path), "--recent", str(recent_days)],
            cwd=str(BACKEND_DIR),
            capture_output=True,
            text=True,
            timeout=1800,  # 30 min timeout
        )

        if result.returncode != 0:
            logger.error(f"ETF download failed: {result.stderr}")
            return {
                "status": "error",
                "message": result.stderr[-500:] if result.stderr else "Unknown error",
            }

        logger.info("ETF data download completed")
        return {"status": "success", "message": "ETF data downloaded"}

    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Download timed out"}
    except Exception as e:
        logger.exception("ETF download error")
        return {"status": "error", "message": str(e)}


# =============================================================================
# Import Tasks
# =============================================================================


async def import_stock_data(ctx: Dict[str, Any], recent_days: int = 1) -> Dict[str, Any]:
    """Import stock data from SQLite cache to PostgreSQL.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to import

    Returns:
        Result dict with counts
    """
    logger.info(f"Starting stock data import for recent {recent_days} days")

    try:
        from scripts.migrate_all_data import migrate_stock_database

        # Find the latest stock database
        stock_dbs = sorted(CACHE_DIR.glob("stocks_*.db"), reverse=True)
        if not stock_dbs:
            # Try trading_data directory as fallback
            trading_data_dir = Path("/Users/dan/Code/q/trading_data")
            stock_dbs = sorted(trading_data_dir.glob("a_stock_data_*.db"), reverse=True)

        if not stock_dbs:
            return {"status": "error", "message": "No stock database found"}

        stock_db = stock_dbs[0]
        logger.info(f"Importing from {stock_db}")

        pg_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")

        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=recent_days)

        results = await migrate_stock_database(
            stock_db,
            pg_url,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )

        logger.info(f"Stock import completed: {results}")
        return {"status": "success", "results": results}

    except Exception as e:
        logger.exception("Stock import error")
        return {"status": "error", "message": str(e)}


async def import_etf_data(ctx: Dict[str, Any], recent_days: int = 1) -> Dict[str, Any]:
    """Import ETF data from SQLite cache to PostgreSQL.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to import

    Returns:
        Result dict with counts
    """
    logger.info(f"Starting ETF data import for recent {recent_days} days")

    try:
        from scripts.migrate_all_data import migrate_etf_database

        # Find the latest ETF database
        etf_dbs = sorted(CACHE_DIR.glob("etfs_*.db"), reverse=True)
        if not etf_dbs:
            trading_data_dir = Path("/Users/dan/Code/q/trading_data")
            etf_dbs = sorted(trading_data_dir.glob("etf_data_*.db"), reverse=True)

        if not etf_dbs:
            return {"status": "error", "message": "No ETF database found"}

        etf_db = etf_dbs[0]
        logger.info(f"Importing from {etf_db}")

        pg_url = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")

        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=recent_days)

        results = await migrate_etf_database(
            etf_db,
            pg_url,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )

        logger.info(f"ETF import completed: {results}")
        return {"status": "success", "results": results}

    except Exception as e:
        logger.exception("ETF import error")
        return {"status": "error", "message": str(e)}


# =============================================================================
# Combined Daily Update Task
# =============================================================================


async def daily_data_update(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Run the complete daily data update workflow.

    This task should be scheduled to run after market close (e.g., 4:00 PM CST).

    Workflow:
    1. Download today's stock data
    2. Download today's ETF data
    3. Import to PostgreSQL (with circ_mv calculation)
    4. Trigger classification update

    Returns:
        Summary of all steps
    """
    logger.info("Starting daily data update workflow")
    start_time = datetime.now()

    results = {
        "started_at": start_time.isoformat(),
        "steps": {},
    }

    # Step 1: Download stock data
    logger.info("[1/4] Downloading stock data...")
    stock_dl = await download_stock_data(ctx)
    results["steps"]["download_stocks"] = stock_dl
    if stock_dl.get("status") == "error":
        logger.warning(f"Stock download had issues: {stock_dl.get('message')}")

    # Step 2: Download ETF data
    logger.info("[2/4] Downloading ETF data...")
    etf_dl = await download_etf_data(ctx)
    results["steps"]["download_etfs"] = etf_dl

    # Step 3: Import to PostgreSQL
    logger.info("[3/4] Importing data to PostgreSQL...")
    stock_imp = await import_stock_data(ctx, recent_days=1)
    results["steps"]["import_stocks"] = stock_imp

    etf_imp = await import_etf_data(ctx, recent_days=1)
    results["steps"]["import_etfs"] = etf_imp

    # Step 4: Trigger classification update
    logger.info("[4/4] Triggering classification update...")
    try:
        from workers.classification_tasks import daily_classification_update
        from workers.trading_days import get_latest_trading_day

        # 使用实际交易日而非 date.today()
        trading_day = get_latest_trading_day()
        classification_result = await daily_classification_update(ctx, str(trading_day))
        results["steps"]["classification"] = classification_result
    except Exception as e:
        logger.exception("Classification update failed")
        results["steps"]["classification"] = {"status": "error", "message": str(e)}

    # Summary
    end_time = datetime.now()
    results["completed_at"] = end_time.isoformat()
    results["duration_seconds"] = (end_time - start_time).total_seconds()

    # Count successes and failures
    successes = sum(1 for step in results["steps"].values() if step.get("status") == "success")
    failures = sum(1 for step in results["steps"].values() if step.get("status") == "error")

    results["summary"] = {
        "total_steps": len(results["steps"]),
        "successes": successes,
        "failures": failures,
    }

    logger.info(
        f"Daily update completed in {results['duration_seconds']:.1f}s: "
        f"{successes} successes, {failures} failures"
    )

    return results


# =============================================================================
# Utility Tasks
# =============================================================================


async def check_data_status(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Check the current status of all data in the database.

    Returns:
        Status information for each data type
    """
    logger.info("Checking data status")

    async with worker_session_maker() as session:
        status = {}

        # Stock count and date range
        result = await session.execute(
            text("""
            SELECT
                COUNT(DISTINCT code) as stock_count,
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as record_count
            FROM market_daily
            WHERE code LIKE 'sh.%' OR code LIKE 'sz.%'
        """)
        )
        row = result.fetchone()
        status["stocks"] = {
            "count": row[0],
            "date_range": f"{row[1]} ~ {row[2]}" if row[1] else None,
            "records": row[3],
        }

        # ETF count
        result = await session.execute(
            text("""
            SELECT
                COUNT(DISTINCT code) as etf_count,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM market_daily
            WHERE code LIKE 'etf.%'
        """)
        )
        row = result.fetchone()
        status["etfs"] = {
            "count": row[0],
            "date_range": f"{row[1]} ~ {row[2]}" if row[1] else None,
        }

        # Index constituents
        result = await session.execute(
            text("""
            SELECT COUNT(*) FROM index_constituents
        """)
        )
        status["index_constituents"] = {"count": result.scalar()}

        # Industry classification
        try:
            result = await session.execute(
                text("""
                SELECT classification_system, COUNT(*)
                FROM industry_classification
                GROUP BY classification_system
            """)
            )
            status["industries"] = {row[0]: row[1] for row in result.fetchall()}
        except Exception:
            status["industries"] = {"error": "Table may not exist"}

        return status


async def get_download_status(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Get status of downloaded SQLite cache files.

    Returns:
        Information about cache files
    """
    logger.info("Checking download cache status")

    cache_files = {}

    # Check cache directory
    if CACHE_DIR.exists():
        for db_file in CACHE_DIR.glob("*.db"):
            stat = db_file.stat()
            cache_files[db_file.name] = {
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }

    # Also check trading_data directory
    trading_data_dir = Path("/Users/dan/Code/q/trading_data")
    if trading_data_dir.exists():
        for db_file in trading_data_dir.glob("*.db"):
            stat = db_file.stat()
            cache_files[f"trading_data/{db_file.name}"] = {
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }

    return {
        "cache_dir": str(CACHE_DIR),
        "files": cache_files,
        "total_files": len(cache_files),
    }


# =============================================================================
# API-triggered Sync Task
# =============================================================================


async def _publish_only(event_type: str, job_id: str, data: dict) -> None:
    """Publish SSE event to Redis only (no database persist)."""
    await publish_data_sync_event(event_type, job_id, data)


async def _publish_and_persist(
    event_type: str,
    job_id: str,
    data: dict,
    session: AsyncSession,
    sync_record,
) -> None:
    """Publish SSE event and persist to database for recovery."""
    from sqlalchemy.orm.attributes import flag_modified

    # Publish to Redis for live streaming
    await publish_data_sync_event(event_type, job_id, data)

    # Persist to database for recovery
    if sync_record.details is None:
        sync_record.details = {"steps": {}, "event_log": []}

    if "event_log" not in sync_record.details:
        sync_record.details["event_log"] = []

    sync_record.details["event_log"].append(
        {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data,
        }
    )

    flag_modified(sync_record, "details")
    await session.commit()


# =============================================================================
# 数据源抽象层同步任务
# =============================================================================
# 数据源抽象层同步任务
# =============================================================================


async def sync_with_data_source(
    ctx: Dict[str, Any],
    trade_date: str = None,
    asset_types: list = None,
) -> Dict[str, Any]:
    """
    使用数据源抽象层同步指定日期的市场数据

    这个任务使用新的数据源抽象层（默认 TuShare），一次请求获取全市场数据。

    Args:
        ctx: ARQ context
        trade_date: 交易日期 (YYYY-MM-DD)，默认使用最新交易日
        asset_types: 资产类型列表 ['stock', 'etf', 'index']，默认全部

    Returns:
        同步结果
    """
    from workers.source_sync import sync_daily_data_with_source
    from workers.trading_days import get_latest_trading_day

    # 确定交易日期
    if trade_date:
        sync_date = date.fromisoformat(trade_date)
    else:
        sync_date = get_latest_trading_day()

    logger.info(f"Starting data source sync for {sync_date}")

    async with worker_session_maker() as session:
        result = await sync_daily_data_with_source(
            session,
            sync_date,
            asset_types=asset_types,
        )
        return result


async def backfill_with_data_source(
    ctx: Dict[str, Any],
    start_date: str,
    end_date: str = None,
    asset_types: list = None,
) -> Dict[str, Any]:
    """
    使用数据源抽象层补全日期范围内的数据

    Args:
        ctx: ARQ context
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)，默认使用最新交易日
        asset_types: 资产类型列表

    Returns:
        补全结果
    """
    from workers.source_sync import backfill_missing_dates
    from workers.trading_days import get_latest_trading_day

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else get_latest_trading_day()

    logger.info(f"Starting backfill from {start} to {end}")

    async with worker_session_maker() as session:
        result = await backfill_missing_dates(
            session,
            start,
            end,
            asset_types=asset_types,
        )
        return result


async def api_triggered_sync_v2(ctx: Dict[str, Any], sync_record_id: str) -> Dict[str, Any]:
    """
    API-triggered sync using new data source abstraction (TuShare by default).

    This version uses the new source_sync module which fetches all market data
    in one API call per asset type. Much faster than the old batch_sync approach.

    Flow:
    1. check_update → Check if PG data is up to date
    2. sync_stocks → Batch fetch stocks from TuShare
    3. sync_etfs → Batch fetch ETFs from TuShare
    4. sync_indices → Batch fetch indices from TuShare
    5. sync_valuation → Sync valuation data

    Args:
        ctx: ARQ context
        sync_record_id: UUID of the SyncHistory record to update

    Returns:
        Result dict with status and summary
    """
    from app.db.models.sync import SyncHistory
    from sqlalchemy.orm.attributes import flag_modified
    from workers.source_sync import (
        sync_daily_data_with_source,
        get_pg_max_date,
        get_pg_index_max_date,
    )
    from workers.trading_days import get_latest_trading_day, get_trading_days_between
    from workers.data_sources import get_data_source
    from workers.classification_tasks import daily_classification_update

    logger.info(f"Starting API-triggered sync v2 (source mode): {sync_record_id}")
    start_time = datetime.now()

    async with worker_session_maker() as session:
        # Get the sync record
        result = await session.execute(select(SyncHistory).where(SyncHistory.id == sync_record_id))
        sync_record = result.scalar_one_or_none()

        if not sync_record:
            logger.error(f"SyncHistory record not found: {sync_record_id}")
            return {"status": "error", "message": "Record not found"}

        # Get data source info
        source = get_data_source()
        latest_trading_day = get_latest_trading_day()

        # Update status to running
        sync_record.status = "running"
        sync_record.started_at = start_time
        sync_record.details = {"steps": {}, "event_log": [], "data_source": source.name}
        await session.commit()

        try:
            await _publish_and_persist(
                "plan",
                sync_record_id,
                {
                    "steps": [
                        {"id": "check_update", "name": "检查数据状态"},
                        {"id": "sync_stocks", "name": "同步股票数据"},
                        {"id": "sync_etfs", "name": "同步ETF数据"},
                        {"id": "sync_indices", "name": "同步指数数据"},
                        {"id": "sync_valuation", "name": "同步估值数据"},
                        {"id": "adjust_factors", "name": "同步复权因子"},
                        {"id": "classification", "name": "更新分类数据"},
                    ],
                    "message": f"准备开始数据同步 (数据源: {source.name}, 目标日期: {latest_trading_day})...",
                },
                session,
                sync_record,
            )

            records_imported = 0
            step_errors = []

            # Step 1: Check if update is needed
            step_start = datetime.now()
            await _publish_and_persist(
                "progress",
                sync_record_id,
                {
                    "step": "check_update",
                    "progress": 5,
                    "message": "正在检查数据状态...",
                },
                session,
                sync_record,
            )

            pg_stock_date = await get_pg_max_date(session, "stock")
            pg_etf_date = await get_pg_max_date(session, "etf")
            pg_index_date = await get_pg_index_max_date(session)

            check_result = {
                "pg_stock_date": str(pg_stock_date) if pg_stock_date else None,
                "pg_etf_date": str(pg_etf_date) if pg_etf_date else None,
                "pg_index_date": str(pg_index_date) if pg_index_date else None,
                "latest_trading_day": str(latest_trading_day),
                "stock_needs_update": pg_stock_date is None or pg_stock_date < latest_trading_day,
                "etf_needs_update": pg_etf_date is None or pg_etf_date < latest_trading_day,
                "index_needs_update": pg_index_date is None or pg_index_date < latest_trading_day,
            }
            sync_record.details["steps"]["check_update"] = check_result
            step_duration = (datetime.now() - step_start).total_seconds()

            needs_update = (
                check_result["stock_needs_update"]
                or check_result["etf_needs_update"]
                or check_result["index_needs_update"]
            )

            # Calculate missing trading days (for backfilling)
            oldest_pg_date = None
            for d in [pg_stock_date, pg_etf_date, pg_index_date]:
                if d is not None:
                    if oldest_pg_date is None or d < oldest_pg_date:
                        oldest_pg_date = d

            if oldest_pg_date and oldest_pg_date < latest_trading_day:
                missing_trading_days = get_trading_days_between(oldest_pg_date, latest_trading_day)
            else:
                missing_trading_days = [latest_trading_day] if needs_update else []

            days_to_sync = len(missing_trading_days)

            check_message = f"PG股票: {pg_stock_date}, ETF: {pg_etf_date}, 指数: {pg_index_date}, 最近交易日: {latest_trading_day}, 需同步 {days_to_sync} 个交易日"

            await _publish_and_persist(
                "step_complete",
                sync_record_id,
                {
                    "step": "check_update",
                    "status": "success",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": check_message,
                    "message": f"数据状态检查完成 ({step_duration:.1f}s)",
                },
                session,
                sync_record,
            )

            # If no update needed, skip to completion
            if not needs_update:
                duration = (datetime.now() - start_time).total_seconds()

                # Skip all sync steps
                for step_id in [
                    "sync_stocks",
                    "sync_etfs",
                    "sync_indices",
                    "sync_valuation",
                    "adjust_factors",
                    "classification",
                ]:
                    sync_record.details["steps"][step_id] = {
                        "status": "skip",
                        "message": "数据已是最新",
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": step_id,
                            "status": "skip",
                            "records_count": 0,
                            "duration_seconds": 0,
                            "detail": "数据已是最新",
                            "message": f"跳过 (已是最新)",
                        },
                        session,
                        sync_record,
                    )

                await _publish_and_persist(
                    "job_complete",
                    sync_record_id,
                    {
                        "status": "success",
                        "records_count": 0,
                        "valuation_count": 0,
                        "duration_seconds": round(duration, 1),
                        "message": f"数据已是最新，无需同步 ({duration:.1f}s)",
                        "data_source": source.name,
                        "trade_date": str(latest_trading_day),
                    },
                    session,
                    sync_record,
                )

                sync_record.status = "success"
                sync_record.completed_at = datetime.now()
                sync_record.duration_seconds = duration
                sync_record.records_count = 0
                flag_modified(sync_record, "details")
                await session.commit()

                logger.info(f"Sync v2: Data is up to date, skipped in {duration:.1f}s")
                return {
                    "status": "success",
                    "sync_record_id": sync_record_id,
                    "records_count": 0,
                    "message": "Data is up to date",
                }

            # Step 2-5: Run sync using source_sync
            stock_count = 0
            etf_count = 0
            index_count = 0
            valuation_count = 0

            # Create progress callback
            async def progress_callback(message: str, progress: int, detail: dict):
                action = detail.get("action", "sync")
                if "stock" in action.lower():
                    step = "sync_stocks"
                elif "etf" in action.lower():
                    step = "sync_etfs"
                elif "index" in action.lower():
                    step = "sync_indices"
                elif "valuation" in action.lower():
                    step = "sync_valuation"
                else:
                    step = "sync_stocks"
                await _publish_only(
                    "progress",
                    sync_record_id,
                    {
                        "step": step,
                        "progress": progress,
                        "message": message,
                        "detail": detail,
                    },
                )

            # Step 2: Sync stocks
            if check_result["stock_needs_update"]:
                step_start = datetime.now()
                await _publish_and_persist(
                    "progress",
                    sync_record_id,
                    {
                        "step": "sync_stocks",
                        "progress": 10,
                        "message": f"[{source.name}] 正在获取股票数据 ({days_to_sync} 个交易日)...",
                    },
                    session,
                    sync_record,
                )

                try:
                    for day_idx, trade_date in enumerate(missing_trading_days):
                        day_progress = f"[{day_idx + 1}/{days_to_sync}]"
                        await _publish_only(
                            "progress",
                            sync_record_id,
                            {
                                "step": "sync_stocks",
                                "progress": 10 + int((day_idx / max(days_to_sync, 1)) * 20),
                                "message": f"[{source.name}] {day_progress} 获取股票数据 {trade_date}...",
                            },
                        )
                        sync_result = await sync_daily_data_with_source(
                            session,
                            trade_date,
                            asset_types=["stock"],
                            progress_callback=progress_callback,
                        )
                        stock_count += sync_result.get("stock_count", 0)
                    records_imported += stock_count
                    step_duration = (datetime.now() - step_start).total_seconds()

                    sync_record.details["steps"]["sync_stocks"] = {
                        "status": "success",
                        "count": stock_count,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_stocks",
                            "status": "success",
                            "records_count": stock_count,
                            "duration_seconds": round(step_duration, 1),
                            "detail": f"[{source.name}] 获取完成",
                            "message": f"同步股票数据: {stock_count} 条 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
                except Exception as e:
                    step_duration = (datetime.now() - step_start).total_seconds()
                    error_msg = f"股票同步失败: {str(e)}"
                    step_errors.append(error_msg)
                    logger.warning(f"Stock sync failed: {e}")
                    await session.rollback()
                    sync_record = await session.get(SyncHistory, sync_record_id)
                    sync_record.details["steps"]["sync_stocks"] = {
                        "status": "error",
                        "message": error_msg,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_stocks",
                            "status": "error",
                            "records_count": 0,
                            "duration_seconds": round(step_duration, 1),
                            "detail": error_msg,
                            "message": f"同步股票数据: 失败 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
            else:
                sync_record.details["steps"]["sync_stocks"] = {
                    "status": "skip",
                    "message": "数据已是最新",
                }
                await _publish_and_persist(
                    "step_complete",
                    sync_record_id,
                    {
                        "step": "sync_stocks",
                        "status": "skip",
                        "records_count": 0,
                        "duration_seconds": 0,
                        "detail": "数据已是最新",
                        "message": "同步股票数据: 跳过 (已是最新)",
                    },
                    session,
                    sync_record,
                )

            # Step 3: Sync ETFs
            if check_result["etf_needs_update"]:
                step_start = datetime.now()
                await _publish_and_persist(
                    "progress",
                    sync_record_id,
                    {
                        "step": "sync_etfs",
                        "progress": 40,
                        "message": f"[{source.name}] 正在获取ETF数据 ({days_to_sync} 个交易日)...",
                    },
                    session,
                    sync_record,
                )

                try:
                    for day_idx, trade_date in enumerate(missing_trading_days):
                        day_progress = f"[{day_idx + 1}/{days_to_sync}]"
                        await _publish_only(
                            "progress",
                            sync_record_id,
                            {
                                "step": "sync_etfs",
                                "progress": 40 + int((day_idx / max(days_to_sync, 1)) * 15),
                                "message": f"[{source.name}] {day_progress} 获取ETF数据 {trade_date}...",
                            },
                        )
                        sync_result = await sync_daily_data_with_source(
                            session,
                            trade_date,
                            asset_types=["etf"],
                            progress_callback=progress_callback,
                        )
                        etf_count += sync_result.get("etf_count", 0)
                        valuation_count += sync_result.get("valuation_count", 0)
                    records_imported += etf_count
                    step_duration = (datetime.now() - step_start).total_seconds()

                    sync_record.details["steps"]["sync_etfs"] = {
                        "status": "success",
                        "count": etf_count,
                        "valuation_count": valuation_count,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_etfs",
                            "status": "success",
                            "records_count": etf_count,
                            "valuation_count": valuation_count,
                            "duration_seconds": round(step_duration, 1),
                            "detail": f"[{source.name}] 获取完成",
                            "message": f"同步ETF数据: {etf_count} 条, 估值: {valuation_count} 条 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
                except Exception as e:
                    step_duration = (datetime.now() - step_start).total_seconds()
                    error_msg = f"ETF同步失败: {str(e)}"
                    step_errors.append(error_msg)
                    logger.warning(f"ETF sync failed: {e}")
                    await session.rollback()
                    sync_record = await session.get(SyncHistory, sync_record_id)
                    sync_record.details["steps"]["sync_etfs"] = {
                        "status": "error",
                        "message": error_msg,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_etfs",
                            "status": "error",
                            "records_count": 0,
                            "duration_seconds": round(step_duration, 1),
                            "detail": error_msg,
                            "message": f"同步ETF数据: 失败 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
            else:
                sync_record.details["steps"]["sync_etfs"] = {
                    "status": "skip",
                    "message": "数据已是最新",
                }
                await _publish_and_persist(
                    "step_complete",
                    sync_record_id,
                    {
                        "step": "sync_etfs",
                        "status": "skip",
                        "records_count": 0,
                        "duration_seconds": 0,
                        "detail": "数据已是最新",
                        "message": "同步ETF数据: 跳过 (已是最新)",
                    },
                    session,
                    sync_record,
                )

            # Step 4: Sync Indices
            if check_result["index_needs_update"]:
                step_start = datetime.now()
                await _publish_and_persist(
                    "progress",
                    sync_record_id,
                    {
                        "step": "sync_indices",
                        "progress": 70,
                        "message": f"[{source.name}] 正在获取指数数据 ({days_to_sync} 个交易日)...",
                    },
                    session,
                    sync_record,
                )

                try:
                    for day_idx, trade_date in enumerate(missing_trading_days):
                        day_progress = f"[{day_idx + 1}/{days_to_sync}]"
                        await _publish_only(
                            "progress",
                            sync_record_id,
                            {
                                "step": "sync_indices",
                                "progress": 70 + int((day_idx / max(days_to_sync, 1)) * 10),
                                "message": f"[{source.name}] {day_progress} 获取指数数据 {trade_date}...",
                            },
                        )
                        sync_result = await sync_daily_data_with_source(
                            session,
                            trade_date,
                            asset_types=["index"],
                            progress_callback=progress_callback,
                        )
                        index_count += sync_result.get("index_count", 0)
                    records_imported += index_count
                    step_duration = (datetime.now() - step_start).total_seconds()

                    sync_record.details["steps"]["sync_indices"] = {
                        "status": "success",
                        "count": index_count,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_indices",
                            "status": "success",
                            "records_count": index_count,
                            "duration_seconds": round(step_duration, 1),
                            "detail": f"[{source.name}] 获取完成",
                            "message": f"同步指数数据: {index_count} 条 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
                except Exception as e:
                    step_duration = (datetime.now() - step_start).total_seconds()
                    error_msg = f"指数同步失败: {str(e)}"
                    step_errors.append(error_msg)
                    logger.warning(f"Index sync failed: {e}")
                    await session.rollback()
                    sync_record = await session.get(SyncHistory, sync_record_id)
                    sync_record.details["steps"]["sync_indices"] = {
                        "status": "error",
                        "message": error_msg,
                    }
                    await _publish_and_persist(
                        "step_complete",
                        sync_record_id,
                        {
                            "step": "sync_indices",
                            "status": "error",
                            "records_count": 0,
                            "duration_seconds": round(step_duration, 1),
                            "detail": error_msg,
                            "message": f"同步指数数据: 失败 ({step_duration:.1f}s)",
                        },
                        session,
                        sync_record,
                    )
            else:
                sync_record.details["steps"]["sync_indices"] = {
                    "status": "skip",
                    "message": "数据已是最新",
                }
                await _publish_and_persist(
                    "step_complete",
                    sync_record_id,
                    {
                        "step": "sync_indices",
                        "status": "skip",
                        "records_count": 0,
                        "duration_seconds": 0,
                        "detail": "数据已是最新",
                        "message": "同步指数数据: 跳过 (已是最新)",
                    },
                    session,
                    sync_record,
                )

            # Step 5: Valuation (already included in ETF sync)
            sync_record.details["steps"]["sync_valuation"] = {
                "status": "success" if valuation_count > 0 else "skip",
                "count": valuation_count,
            }
            await _publish_and_persist(
                "step_complete",
                sync_record_id,
                {
                    "step": "sync_valuation",
                    "status": "success" if valuation_count > 0 else "skip",
                    "records_count": valuation_count,
                    "duration_seconds": 0,
                    "detail": "估值数据在ETF同步时已处理",
                    "message": f"同步估值数据: {valuation_count} 条",
                },
                session,
                sync_record,
            )

            # Step 6: Adjust factors sync - SKIPPED for daily sync (too slow, ~15 min)
            # Adjust factors are sparse events, can be synced separately via scheduled task
            adjust_factor_count = 0
            step_start = datetime.now()

            adjust_result = {
                "status": "skip",
                "message": "复权因子同步已跳过（日常同步模式）",
                "records": 0,
            }
            sync_record.details["steps"]["adjust_factors"] = adjust_result
            step_duration = (datetime.now() - step_start).total_seconds()

            await _publish_and_persist(
                "step_complete",
                sync_record_id,
                {
                    "step": "adjust_factors",
                    "status": "skip",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": "复权因子同步已跳过，可通过单独任务更新",
                    "message": f"同步复权因子: 已跳过 ({step_duration:.1f}s)",
                },
                session,
                sync_record,
            )

            # Step 7: Classification update (only if new data was imported)
            step_start = datetime.now()
            records_classified = 0

            if records_imported > 0:
                await _publish_and_persist(
                    "progress",
                    sync_record_id,
                    {
                        "step": "classification",
                        "progress": 92,
                        "message": "正在更新股票分类...",
                    },
                    session,
                    sync_record,
                )

                classification_result = {"status": "success", "message": "No changes"}
                try:
                    classification_result = await daily_classification_update(
                        ctx, str(latest_trading_day)
                    )
                except Exception as e:
                    classification_result = {"status": "error", "message": str(e)}

                sync_record.details["steps"]["classification"] = classification_result
                step_duration = (datetime.now() - step_start).total_seconds()
                class_count = classification_result.get("updated_count", 0)
                records_classified = class_count
                class_detail = classification_result.get("message", "")

                await _publish_and_persist(
                    "step_complete",
                    sync_record_id,
                    {
                        "step": "classification",
                        "status": classification_result.get("status", "success"),
                        "records_count": class_count,
                        "duration_seconds": round(step_duration, 1),
                        "detail": class_detail,
                        "message": f"更新分类: {class_count} 条 ({step_duration:.1f}s)",
                    },
                    session,
                    sync_record,
                )
            else:
                classification_result = {"status": "skipped", "message": "无新数据，跳过分类更新"}
                sync_record.details["steps"]["classification"] = classification_result
                step_duration = (datetime.now() - step_start).total_seconds()
                await _publish_and_persist(
                    "step_complete",
                    sync_record_id,
                    {
                        "step": "classification",
                        "status": "skip",
                        "records_count": 0,
                        "duration_seconds": round(step_duration, 1),
                        "detail": "无新数据，跳过分类更新",
                        "message": "更新分类: 跳过 (无新数据)",
                    },
                    session,
                    sync_record,
                )

            # Complete
            duration = (datetime.now() - start_time).total_seconds()
            has_errors = len(step_errors) > 0
            final_status = "partial" if has_errors else "success"

            sync_record.details["summary"] = {
                "stock_count": stock_count,
                "etf_count": etf_count,
                "index_count": index_count,
                "valuation_count": valuation_count,
                "adjust_factor_count": adjust_factor_count,
                "days_synced": days_to_sync,
                "errors": step_errors,
            }

            await _publish_and_persist(
                "job_complete",
                sync_record_id,
                {
                    "status": final_status,
                    "records_count": records_imported,
                    "valuation_count": valuation_count,
                    "duration_seconds": round(duration, 1),
                    "message": f"同步完成: {records_imported} 条行情, {valuation_count} 条估值, {adjust_factor_count} 条复权因子 ({days_to_sync} 个交易日, {duration:.1f}s)",
                    "data_source": source.name,
                    "trade_date": str(latest_trading_day),
                },
                session,
                sync_record,
            )

            sync_record.status = final_status
            sync_record.completed_at = datetime.now()
            sync_record.duration_seconds = duration
            sync_record.records_count = records_imported
            flag_modified(sync_record, "details")
            await session.commit()

            logger.info(
                f"Sync v2 completed: {records_imported} records, {adjust_factor_count} adjust factors, {days_to_sync} days in {duration:.1f}s"
            )

            return {
                "status": final_status,
                "sync_record_id": sync_record_id,
                "records_count": records_imported,
                "valuation_count": valuation_count,
                "adjust_factor_count": adjust_factor_count,
                "days_synced": days_to_sync,
                "duration_seconds": round(duration, 1),
                "data_source": source.name,
            }

        except Exception as e:
            logger.exception(f"API-triggered sync v2 failed: {sync_record_id}")

            try:
                await session.rollback()
                sync_record = await session.get(SyncHistory, sync_record_id)
                if sync_record:
                    sync_record.status = "failed"
                    sync_record.completed_at = datetime.now()
                    sync_record.duration_seconds = (datetime.now() - start_time).total_seconds()
                    sync_record.error_message = str(e)
                    await session.commit()

                    await _publish_and_persist(
                        "error",
                        sync_record_id,
                        {
                            "status": "failed",
                            "records_count": 0,
                            "duration_seconds": round(
                                (datetime.now() - start_time).total_seconds(), 1
                            ),
                            "message": f"同步失败: {str(e)}",
                        },
                        session,
                        sync_record,
                    )
            except Exception:
                pass

            return {
                "status": "error",
                "sync_record_id": sync_record_id,
                "message": str(e),
            }


# =============================================================================
# Targeted Backfill Task (Data Map)
# =============================================================================


async def targeted_backfill(
    ctx: Dict[str, Any],
    job_id: str,
    table_name: str,
    start_date_str: str,
    end_date_str: str,
) -> Dict[str, Any]:
    """ARQ worker: backfill a specific table for a date range. Updates SyncHistory(job_id)."""
    from app.db.models.sync import SyncHistory
    from sqlalchemy.orm.attributes import flag_modified

    start_date_val = date.fromisoformat(start_date_str)
    end_date_val = date.fromisoformat(end_date_str)

    logger.info(
        f"targeted_backfill: table={table_name}, "
        f"range={start_date_val} to {end_date_val}, job_id={job_id}"
    )

    async def _publish(event_type: str, **data):
        try:
            from app.core.redis_pubsub import publish_data_sync_event

            await publish_data_sync_event(event_type, job_id, data)
        except Exception:
            pass

    async with worker_session_maker() as session:
        result = await session.execute(select(SyncHistory).where(SyncHistory.id == job_id))
        sync_record = result.scalar_one_or_none()

        if not sync_record:
            logger.error(f"SyncHistory record not found: {job_id}")
            return {"status": "error", "message": "Record not found"}

        sync_record.status = "running"
        sync_record.started_at = datetime.now()
        sync_record.details = {
            "table": table_name,
            "start_date": start_date_str,
            "end_date": end_date_str,
        }
        await session.commit()
        await _publish(
            "backfill_start", table=table_name, start_date=start_date_str, end_date=end_date_str
        )

        try:
            total_records = 0

            if table_name == "market_daily":
                total_records = await _backfill_market_daily(session, start_date_val, end_date_val)

            elif table_name == "indicator_valuation":
                total_records = await _backfill_valuation(session, start_date_val, end_date_val)

            elif table_name == "moneyflow_daily":
                total_records = await _backfill_moneyflow(session, start_date_val, end_date_val)

            elif table_name == "limit_list_daily":
                total_records = await _backfill_limit_list(session, start_date_val, end_date_val)

            elif table_name == "adjust_factor":
                total_records = await _backfill_adjust_factor(session)

            elif table_name in (
                "stock_style_exposure",
                "market_regime",
            ):
                total_records = await _backfill_computed(
                    ctx, table_name, start_date_val, end_date_val
                )

            else:
                raise ValueError(f"Unsupported table for backfill: {table_name}")

            # Mark success
            duration = (datetime.now() - sync_record.started_at).total_seconds()
            sync_record.status = "success"
            sync_record.completed_at = datetime.now()
            sync_record.duration_seconds = duration
            sync_record.records_imported = total_records
            sync_record.details["records"] = total_records
            sync_record.details["duration_seconds"] = round(duration, 1)
            flag_modified(sync_record, "details")
            await session.commit()

            logger.info(
                f"targeted_backfill complete: {table_name}, "
                f"{total_records} records in {duration:.1f}s"
            )
            await _publish(
                "backfill_complete",
                table=table_name,
                status="success",
                records=total_records,
                duration_seconds=round(duration, 1),
            )
            return {
                "status": "success",
                "table": table_name,
                "records": total_records,
                "duration_seconds": round(duration, 1),
            }

        except Exception as e:
            logger.exception(f"targeted_backfill failed: {table_name}")
            await _publish(
                "backfill_complete", table=table_name, status="failed", error=str(e)[:200]
            )
            try:
                await session.rollback()
                sync_record = await session.get(SyncHistory, job_id)
                if sync_record:
                    duration = (
                        datetime.now() - (sync_record.started_at or datetime.now())
                    ).total_seconds()
                    sync_record.status = "failed"
                    sync_record.completed_at = datetime.now()
                    sync_record.duration_seconds = duration
                    sync_record.error_message = str(e)[:500]
                    await session.commit()
            except Exception:
                pass
            return {"status": "error", "table": table_name, "message": str(e)}


async def _backfill_market_daily(
    session: AsyncSession, start_date_val: date, end_date_val: date
) -> int:
    from workers.source_sync import backfill_missing_dates

    result = await backfill_missing_dates(
        session,
        start_date_val,
        end_date_val,
        asset_types=["stock", "etf", "index"],
    )
    return result.get("stock_count", 0) + result.get("etf_count", 0) + result.get("index_count", 0)


async def _backfill_valuation(
    session: AsyncSession, start_date_val: date, end_date_val: date
) -> int:
    from workers.source_sync import sync_daily_data_with_source
    from workers.data_sources import get_data_source

    source = get_data_source()
    trading_days = source.get_trading_days(start_date_val, end_date_val)

    total = 0
    for trade_date in trading_days:
        result = await sync_daily_data_with_source(session, trade_date, asset_types=["stock"])
        total += result.get("valuation_count", 0)
        await asyncio.sleep(0.3)  # TuShare rate limit

    return total


async def _backfill_moneyflow(
    session: AsyncSession, start_date_val: date, end_date_val: date
) -> int:
    from workers.data_sources import get_data_source

    source = get_data_source()
    trading_days = source.get_trading_days(start_date_val, end_date_val)

    total = 0
    for trade_date in trading_days:
        try:
            df = source.fetch_moneyflow_by_date(trade_date)
            if df is not None and not df.empty:
                records = df.to_dict("records")
                count = await _upsert_moneyflow(session, records)
                total += count
                logger.info(f"Backfilled {count} moneyflow records for {trade_date}")
        except Exception as e:
            logger.warning(f"Failed to backfill moneyflow for {trade_date}: {e}")

        await asyncio.sleep(0.5)  # TuShare rate limit

    await session.commit()
    return total


async def _upsert_moneyflow(session: AsyncSession, records: list) -> int:
    if not records:
        return 0

    sql = text("""
        INSERT INTO moneyflow_daily (
            code, date,
            buy_sm_amount, buy_md_amount, buy_lg_amount, buy_elg_amount,
            sell_sm_amount, sell_md_amount, sell_lg_amount, sell_elg_amount,
            net_mf_amount
        ) VALUES (
            :code, :date,
            :buy_sm_amount, :buy_md_amount, :buy_lg_amount, :buy_elg_amount,
            :sell_sm_amount, :sell_md_amount, :sell_lg_amount, :sell_elg_amount,
            :net_mf_amount
        )
        ON CONFLICT (code, date) DO UPDATE SET
            buy_sm_amount = EXCLUDED.buy_sm_amount,
            buy_md_amount = EXCLUDED.buy_md_amount,
            buy_lg_amount = EXCLUDED.buy_lg_amount,
            buy_elg_amount = EXCLUDED.buy_elg_amount,
            sell_sm_amount = EXCLUDED.sell_sm_amount,
            sell_md_amount = EXCLUDED.sell_md_amount,
            sell_lg_amount = EXCLUDED.sell_lg_amount,
            sell_elg_amount = EXCLUDED.sell_elg_amount,
            net_mf_amount = EXCLUDED.net_mf_amount
    """)

    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await session.execute(sql, batch)

    return len(records)


async def _backfill_limit_list(
    session: AsyncSession, start_date_val: date, end_date_val: date
) -> int:
    from workers.data_sources import get_data_source

    source = get_data_source()
    trading_days = source.get_trading_days(start_date_val, end_date_val)

    total = 0
    for trade_date in trading_days:
        try:
            df = source.fetch_limit_list_by_date(trade_date)
            if df is not None and not df.empty:
                import numpy as np

                df = df.replace({np.nan: None})
                records = df.to_dict("records")
                count = await _upsert_limit_list(session, records)
                total += count
                logger.info(f"Backfilled {count} limit_list records for {trade_date}")
        except Exception as e:
            logger.warning(f"Failed to backfill limit_list for {trade_date}: {e}")

        await asyncio.sleep(0.5)  # TuShare rate limit

    await session.commit()
    return total


async def _upsert_limit_list(session: AsyncSession, records: list) -> int:
    """Upsert limit_list records into limit_list_daily table."""
    if not records:
        return 0

    import math

    def _clean(val):
        if val is None:
            return None
        if isinstance(val, float) and math.isnan(val):
            return None
        return val

    cleaned = [{k: _clean(v) for k, v in r.items()} for r in records]

    sql = text("""
        INSERT INTO limit_list_daily (
            code, date, name, close, pct_chg, fd_amount,
            first_time, last_time, open_times, up_stat, limit_times, limit_type
        ) VALUES (
            :code, :date, :name, :close, :pct_chg, :fd_amount,
            :first_time, :last_time, :open_times, :up_stat, :limit_times, :limit_type
        )
        ON CONFLICT (code, date) DO UPDATE SET
            name = EXCLUDED.name,
            close = EXCLUDED.close,
            pct_chg = EXCLUDED.pct_chg,
            fd_amount = EXCLUDED.fd_amount,
            first_time = EXCLUDED.first_time,
            last_time = EXCLUDED.last_time,
            open_times = EXCLUDED.open_times,
            up_stat = EXCLUDED.up_stat,
            limit_times = EXCLUDED.limit_times,
            limit_type = EXCLUDED.limit_type
    """)

    batch_size = 1000
    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i : i + batch_size]
        await session.execute(sql, batch)

    return len(cleaned)

    return len(records)


async def _backfill_adjust_factor(session: AsyncSession) -> int:
    """Backfill adjust_factor using TuShare."""
    from workers.source_sync import sync_adjust_factors

    result = await sync_adjust_factors(session)
    return result.get("records", 0)


async def _backfill_computed(
    ctx: Dict[str, Any],
    table_name: str,
    start_date_val: date,
    end_date_val: date,
) -> int:
    from workers.classification_tasks import (
        calculate_style_factors,
        calculate_market_regime,
    )
    from workers.data_sources import get_data_source

    source = get_data_source()
    trading_days = source.get_trading_days(start_date_val, end_date_val)

    if table_name == "stock_style_exposure":
        total = 0
        for td in trading_days:
            result = await calculate_style_factors(ctx, calc_date=td.isoformat())
            total += result.get("records_count", result.get("count", 0))
        return total

    elif table_name == "market_regime":
        total = 0
        for td in trading_days:
            result = await calculate_market_regime(ctx, calc_date=td.isoformat())
            total += result.get("records_count", result.get("count", 0))
        return total

    elif table_name in ("technical_indicators", "stock_microstructure"):
        # These tables have been dropped — no-op
        logger.info(f"Table {table_name} has been dropped, skipping backfill")
        return 0

    return 0


# Export all tasks for registration
__all__ = [
    "download_stock_data",
    "download_etf_data",
    "import_stock_data",
    "import_etf_data",
    "daily_data_update",
    "check_data_status",
    "get_download_status",
    "api_triggered_sync",
    "api_triggered_sync_v2",
    "sync_with_data_source",
    "backfill_with_data_source",
    "targeted_backfill",
]
