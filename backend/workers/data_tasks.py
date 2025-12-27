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
            return {"status": "error", "message": result.stderr[-500:] if result.stderr else "Unknown error"}

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
        classification_result = await daily_classification_update(ctx)
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

    logger.info(f"Daily update completed in {results['duration_seconds']:.1f}s: "
                f"{successes} successes, {failures} failures")

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
        result = await session.execute(text("""
            SELECT
                COUNT(DISTINCT code) as stock_count,
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as record_count
            FROM market_daily
            WHERE code LIKE 'sh.%' OR code LIKE 'sz.%'
        """))
        row = result.fetchone()
        status["stocks"] = {
            "count": row[0],
            "date_range": f"{row[1]} ~ {row[2]}" if row[1] else None,
            "records": row[3],
        }

        # ETF count
        result = await session.execute(text("""
            SELECT
                COUNT(DISTINCT code) as etf_count,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM market_daily
            WHERE code LIKE 'etf.%'
        """))
        row = result.fetchone()
        status["etfs"] = {
            "count": row[0],
            "date_range": f"{row[1]} ~ {row[2]}" if row[1] else None,
        }

        # Index constituents
        result = await session.execute(text("""
            SELECT COUNT(*) FROM index_constituents
        """))
        status["index_constituents"] = {"count": result.scalar()}

        # Industry classification
        try:
            result = await session.execute(text("""
                SELECT classification_system, COUNT(*)
                FROM industry_classification
                GROUP BY classification_system
            """))
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

    sync_record.details["event_log"].append({
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data,
    })

    flag_modified(sync_record, "details")
    await session.commit()


async def api_triggered_sync(ctx: Dict[str, Any], sync_record_id: str) -> Dict[str, Any]:
    """API-triggered sync task that updates SyncHistory record.

    This task is enqueued when user clicks "Sync Now" in the UI.
    Uses batch API to fetch all market data in one call, then directly
    writes to PostgreSQL, skipping SQLite cache for maximum speed.

    Optimized flow (3 steps instead of 5):
    1. check_update → Check if PG data is up to date
    2. sync_data → Batch fetch from akshare → Direct PG insert
    3. classification → Update classifications

    Args:
        ctx: ARQ context
        sync_record_id: UUID of the SyncHistory record to update

    Returns:
        Result dict with status and summary
    """
    from app.db.models.sync import SyncHistory
    from sqlalchemy.orm.attributes import flag_modified
    from workers.batch_sync import (
        sync_stocks_batch,
        sync_etfs_batch,
        sync_indices_batch,
        get_pg_max_date,
        get_pg_index_max_date,
        get_latest_trading_day,
    )

    logger.info(f"Starting API-triggered sync (batch mode): {sync_record_id}")
    start_time = datetime.now()

    async with worker_session_maker() as session:
        # Get the sync record
        result = await session.execute(
            select(SyncHistory).where(SyncHistory.id == sync_record_id)
        )
        sync_record = result.scalar_one_or_none()

        if not sync_record:
            logger.error(f"SyncHistory record not found: {sync_record_id}")
            return {"status": "error", "message": "Record not found"}

        # Update status to running and init details
        sync_record.status = "running"
        sync_record.started_at = start_time
        sync_record.details = {"steps": {}, "event_log": []}
        await session.commit()

        try:
            # Publish plan event with optimized steps
            await _publish_and_persist("plan", sync_record_id, {
                "steps": [
                    {"id": "check_update", "name": "检查数据状态"},
                    {"id": "sync_stocks", "name": "同步股票数据"},
                    {"id": "sync_etfs", "name": "同步ETF数据"},
                    {"id": "sync_indices", "name": "同步指数数据"},
                    {"id": "classification", "name": "更新分类"},
                ],
                "message": "准备开始数据同步 (批量模式)...",
            }, session, sync_record)

            records_imported = 0
            records_classified = 0

            # Step 1: Check if update is needed
            step_start = datetime.now()
            await _publish_and_persist("progress", sync_record_id, {
                "step": "check_update",
                "progress": 5,
                "message": "正在检查数据状态...",
            }, session, sync_record)

            pg_stock_date = await get_pg_max_date(session, 'stock')
            pg_etf_date = await get_pg_max_date(session, 'etf')
            pg_index_date = await get_pg_index_max_date(session)
            latest_trading_day = get_latest_trading_day()

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

            needs_update = check_result["stock_needs_update"] or check_result["etf_needs_update"] or check_result["index_needs_update"]
            check_message = f"PG股票: {pg_stock_date}, ETF: {pg_etf_date}, 指数: {pg_index_date}, 最近交易日: {latest_trading_day}"

            await _publish_and_persist("step_complete", sync_record_id, {
                "step": "check_update",
                "status": "success",
                "records_count": 0,
                "duration_seconds": round(step_duration, 1),
                "detail": check_message,
                "message": f"数据状态检查完成 ({step_duration:.1f}s)",
            }, session, sync_record)

            # Step 2: Sync stock data (batch mode with progress callback)
            step_start = datetime.now()
            stock_count = 0

            if check_result["stock_needs_update"]:
                await _publish_and_persist("progress", sync_record_id, {
                    "step": "sync_stocks",
                    "progress": 20,
                    "message": "正在同步股票数据...",
                }, session, sync_record)

                # 定义进度回调函数 (仅推送到 SSE，不持久化到数据库)
                async def stock_progress_callback(message: str, progress: int, detail: dict):
                    """将同步进度实时推送到 SSE (不写入数据库，避免频繁IO)"""
                    await _publish_only("progress", sync_record_id, {
                        "step": "sync_stocks",
                        "progress": progress,
                        "message": message,
                        "detail": detail,
                    })

                stock_result = await sync_stocks_batch(session, stock_progress_callback)
                sync_record.details["steps"]["sync_stocks"] = stock_result
                step_duration = (datetime.now() - step_start).total_seconds()

                if stock_result.get("status") == "success":
                    stock_count = stock_result.get("market_daily_count", 0)
                    records_imported += stock_count

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "sync_stocks",
                    "status": stock_result.get("status"),
                    "records_count": stock_count,
                    "duration_seconds": round(step_duration, 1),
                    "detail": stock_result.get("message", ""),
                    "message": f"同步股票数据: {stock_count} 条 ({step_duration:.1f}s)",
                }, session, sync_record)
            else:
                sync_record.details["steps"]["sync_stocks"] = {"status": "skip", "message": "数据已是最新"}
                step_duration = (datetime.now() - step_start).total_seconds()

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "sync_stocks",
                    "status": "skip",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": "数据已是最新",
                    "message": f"同步股票数据: 跳过 (已是最新)",
                }, session, sync_record)

            # Step 3: Sync ETF data (batch mode) - with error isolation
            step_start = datetime.now()
            etf_count = 0
            step_errors = []

            if check_result["etf_needs_update"]:
                await _publish_and_persist("progress", sync_record_id, {
                    "step": "sync_etfs",
                    "progress": 60,
                    "message": "正在同步ETF数据...",
                }, session, sync_record)

                # ETF 进度回调
                async def etf_progress_callback(message: str, progress: int, detail: dict):
                    """将 ETF 同步进度实时推送到 SSE"""
                    await _publish_only("progress", sync_record_id, {
                        "step": "sync_etfs",
                        "progress": progress,
                        "message": message,
                        "detail": detail,
                    })

                try:
                    etf_result = await sync_etfs_batch(session, etf_progress_callback)
                    sync_record.details["steps"]["sync_etfs"] = etf_result
                    step_duration = (datetime.now() - step_start).total_seconds()

                    if etf_result.get("status") == "success":
                        etf_count = etf_result.get("market_daily_count", 0)
                        records_imported += etf_count

                    await _publish_and_persist("step_complete", sync_record_id, {
                        "step": "sync_etfs",
                        "status": etf_result.get("status"),
                        "records_count": etf_count,
                        "duration_seconds": round(step_duration, 1),
                        "detail": etf_result.get("message", ""),
                        "message": f"同步ETF数据: {etf_count} 条 ({step_duration:.1f}s)",
                    }, session, sync_record)
                except Exception as e:
                    step_duration = (datetime.now() - step_start).total_seconds()
                    error_msg = f"ETF同步失败: {str(e)}"
                    step_errors.append(error_msg)
                    logger.warning(f"ETF sync failed (continuing with other steps): {e}")
                    sync_record.details["steps"]["sync_etfs"] = {"status": "error", "message": error_msg}

                    await _publish_and_persist("step_complete", sync_record_id, {
                        "step": "sync_etfs",
                        "status": "error",
                        "records_count": 0,
                        "duration_seconds": round(step_duration, 1),
                        "detail": error_msg,
                        "message": f"同步ETF数据: 失败 ({step_duration:.1f}s)",
                    }, session, sync_record)
            else:
                sync_record.details["steps"]["sync_etfs"] = {"status": "skip", "message": "数据已是最新"}
                step_duration = (datetime.now() - step_start).total_seconds()

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "sync_etfs",
                    "status": "skip",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": "数据已是最新",
                    "message": f"同步ETF数据: 跳过 (已是最新)",
                }, session, sync_record)

            # Step 4: Sync index data (batch mode) - with error isolation
            step_start = datetime.now()
            index_count = 0

            if check_result["index_needs_update"]:
                await _publish_and_persist("progress", sync_record_id, {
                    "step": "sync_indices",
                    "progress": 75,
                    "message": "正在同步指数数据...",
                }, session, sync_record)

                # 指数进度回调
                async def index_progress_callback(message: str, progress: int, detail: dict):
                    """将指数同步进度实时推送到 SSE"""
                    await _publish_only("progress", sync_record_id, {
                        "step": "sync_indices",
                        "progress": progress,
                        "message": message,
                        "detail": detail,
                    })

                try:
                    index_result = await sync_indices_batch(session, index_progress_callback)
                    sync_record.details["steps"]["sync_indices"] = index_result
                    step_duration = (datetime.now() - step_start).total_seconds()

                    if index_result.get("status") == "success":
                        index_count = index_result.get("market_daily_count", 0)
                        records_imported += index_count

                    await _publish_and_persist("step_complete", sync_record_id, {
                        "step": "sync_indices",
                        "status": index_result.get("status"),
                        "records_count": index_count,
                        "duration_seconds": round(step_duration, 1),
                        "detail": index_result.get("message", ""),
                        "message": f"同步指数数据: {index_count} 条 ({step_duration:.1f}s)",
                    }, session, sync_record)
                except Exception as e:
                    step_duration = (datetime.now() - step_start).total_seconds()
                    error_msg = f"指数同步失败: {str(e)}"
                    step_errors.append(error_msg)
                    logger.warning(f"Index sync failed (continuing with other steps): {e}")
                    sync_record.details["steps"]["sync_indices"] = {"status": "error", "message": error_msg}

                    await _publish_and_persist("step_complete", sync_record_id, {
                        "step": "sync_indices",
                        "status": "error",
                        "records_count": 0,
                        "duration_seconds": round(step_duration, 1),
                        "detail": error_msg,
                        "message": f"同步指数数据: 失败 ({step_duration:.1f}s)",
                    }, session, sync_record)
            else:
                sync_record.details["steps"]["sync_indices"] = {"status": "skip", "message": "数据已是最新"}
                step_duration = (datetime.now() - step_start).total_seconds()

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "sync_indices",
                    "status": "skip",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": "数据已是最新",
                    "message": f"同步指数数据: 跳过 (已是最新)",
                }, session, sync_record)

            # Step 5: Classification update (conditional - only if new data was imported)
            step_start = datetime.now()

            if records_imported > 0:
                await _publish_and_persist("progress", sync_record_id, {
                    "step": "classification",
                    "progress": 90,
                    "message": "正在更新股票分类...",
                }, session, sync_record)

                classification_result = {"status": "success", "message": "No changes"}
                try:
                    from workers.classification_tasks import daily_classification_update
                    classification_result = await daily_classification_update(ctx)
                except Exception as e:
                    classification_result = {"status": "error", "message": str(e)}

                sync_record.details["steps"]["classification"] = classification_result
                step_duration = (datetime.now() - step_start).total_seconds()

                class_count = classification_result.get("updated_count", 0)
                records_classified = class_count
                class_detail = classification_result.get("message", "")

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "classification",
                    "status": classification_result.get("status", "success"),
                    "records_count": class_count,
                    "duration_seconds": round(step_duration, 1),
                    "detail": class_detail,
                    "message": f"更新分类: {class_count} 条 ({step_duration:.1f}s)",
                }, session, sync_record)
            else:
                # Skip classification when no new data was imported
                classification_result = {"status": "skipped", "message": "无新数据，跳过分类更新"}
                sync_record.details["steps"]["classification"] = classification_result
                step_duration = (datetime.now() - step_start).total_seconds()

                await _publish_and_persist("step_complete", sync_record_id, {
                    "step": "classification",
                    "status": "skipped",
                    "records_count": 0,
                    "duration_seconds": round(step_duration, 1),
                    "detail": "无新数据导入",
                    "message": "更新分类: 跳过 (无新数据)",
                }, session, sync_record)

            # Update sync record with success (or partial success if some steps failed)
            total_duration = (datetime.now() - start_time).total_seconds()

            # Determine final status based on step errors
            if step_errors:
                final_status = "partial"  # Some steps failed
                final_message = f"数据同步部分完成! 导入 {records_imported} 条记录. 失败步骤: {'; '.join(step_errors)}"
            else:
                final_status = "success"
                final_message = f"数据同步完成! 共导入 {records_imported} 条记录"

            sync_record.status = final_status
            sync_record.completed_at = datetime.now()
            sync_record.duration_seconds = total_duration
            sync_record.records_downloaded = records_imported  # In batch mode, download and import are combined
            sync_record.records_imported = records_imported
            sync_record.records_classified = records_classified
            if step_errors:
                sync_record.error_message = "; ".join(step_errors)
            flag_modified(sync_record, "details")
            await session.commit()

            # Publish completion event
            await _publish_and_persist("job_complete", sync_record_id, {
                "status": final_status,
                "progress": 100,
                "records_imported": records_imported,
                "records_classified": records_classified,
                "duration_seconds": round(total_duration, 1),
                "message": final_message,
            }, session, sync_record)

            logger.info(f"API-triggered sync completed ({final_status}): {sync_record_id}")
            return {
                "status": final_status,
                "sync_record_id": sync_record_id,
                "duration_seconds": total_duration,
                "records_imported": records_imported,
                "step_errors": step_errors if step_errors else None,
            }

        except Exception as e:
            logger.exception(f"API-triggered sync failed: {sync_record_id}")

            # Publish error event
            await _publish_and_persist("error", sync_record_id, {
                "status": "failed",
                "message": str(e),
            }, session, sync_record)

            # Update sync record with failure
            sync_record.status = "failed"
            sync_record.completed_at = datetime.now()
            sync_record.duration_seconds = (datetime.now() - start_time).total_seconds()
            sync_record.error_message = str(e)
            flag_modified(sync_record, "details")
            await session.commit()

            return {
                "status": "error",
                "sync_record_id": sync_record_id,
                "message": str(e),
            }


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
]
