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
    """Download recent stock market data using AKShare.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to download (default: 1 for today only)

    Returns:
        Result dict with status and counts
    """
    logger.info(f"Starting stock data download for recent {recent_days} days")

    script_path = DOWNLOADS_DIR / "download_a_stock_data.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
        # Run download script
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
    """Download recent ETF market data using AKShare.

    Args:
        ctx: ARQ context
        recent_days: Number of recent days to download

    Returns:
        Result dict with status
    """
    logger.info(f"Starting ETF data download for recent {recent_days} days")

    script_path = DOWNLOADS_DIR / "download_etf_data.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
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
    stock_dl = await download_stock_data(ctx, recent_days=1)
    results["steps"]["download_stocks"] = stock_dl
    if stock_dl.get("status") == "error":
        logger.warning(f"Stock download had issues: {stock_dl.get('message')}")

    # Step 2: Download ETF data
    logger.info("[2/4] Downloading ETF data...")
    etf_dl = await download_etf_data(ctx, recent_days=1)
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
    It runs the daily_data_update workflow and updates the corresponding
    SyncHistory record with progress and results.

    Args:
        ctx: ARQ context
        sync_record_id: UUID of the SyncHistory record to update

    Returns:
        Result dict with status and summary
    """
    from app.db.models.sync import SyncHistory
    from sqlalchemy.orm.attributes import flag_modified

    logger.info(f"Starting API-triggered sync: {sync_record_id}")
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
            # Publish plan event with steps
            await _publish_and_persist("plan", sync_record_id, {
                "steps": [
                    {"id": "download_stocks", "name": "下载股票数据"},
                    {"id": "download_etfs", "name": "下载ETF数据"},
                    {"id": "import_stocks", "name": "导入股票数据"},
                    {"id": "import_etfs", "name": "导入ETF数据"},
                    {"id": "classification", "name": "更新分类"},
                ],
                "message": "准备开始数据同步...",
            }, session, sync_record)

            records_downloaded = 0
            records_imported = 0
            records_classified = 0

            # Step 1: Download stock data
            step_start = datetime.now()
            await _publish_and_persist("progress", sync_record_id, {
                "step": "download_stocks",
                "progress": 10,
                "message": "正在下载股票日线数据...",
            }, session, sync_record)

            stock_dl = await download_stock_data(ctx, recent_days=1)
            sync_record.details["steps"]["download_stocks"] = stock_dl
            step_duration = (datetime.now() - step_start).total_seconds()

            await _publish_and_persist("step_complete", sync_record_id, {
                "step": "download_stocks",
                "status": stock_dl.get("status"),
                "records_count": 0,
                "duration_seconds": round(step_duration, 1),
                "detail": stock_dl.get("message", ""),
                "message": f"下载股票数据: 完成 ({step_duration:.1f}s)",
            }, session, sync_record)

            # Step 2: Download ETF data
            step_start = datetime.now()
            await _publish_and_persist("progress", sync_record_id, {
                "step": "download_etfs",
                "progress": 25,
                "message": "正在下载ETF数据...",
            }, session, sync_record)

            etf_dl = await download_etf_data(ctx, recent_days=1)
            sync_record.details["steps"]["download_etfs"] = etf_dl
            step_duration = (datetime.now() - step_start).total_seconds()

            await _publish_and_persist("step_complete", sync_record_id, {
                "step": "download_etfs",
                "status": etf_dl.get("status"),
                "records_count": 0,
                "duration_seconds": round(step_duration, 1),
                "detail": etf_dl.get("message", ""),
                "message": f"下载ETF数据: 完成 ({step_duration:.1f}s)",
            }, session, sync_record)

            # Step 3: Import stock data
            step_start = datetime.now()
            await _publish_and_persist("progress", sync_record_id, {
                "step": "import_stocks",
                "progress": 50,
                "message": "正在导入股票数据到数据库...",
            }, session, sync_record)

            stock_imp = await import_stock_data(ctx, recent_days=1)
            sync_record.details["steps"]["import_stocks"] = stock_imp
            step_duration = (datetime.now() - step_start).total_seconds()

            stock_count = 0
            stock_detail = ""
            if stock_imp.get("results") and isinstance(stock_imp["results"], dict):
                stock_count = stock_imp["results"].get("total_market_daily", 0)
                records_imported += stock_count
                new_count = stock_imp["results"].get("new_records", 0)
                updated_count = stock_imp["results"].get("updated_records", 0)
                if new_count or updated_count:
                    stock_detail = f"新增 {new_count}, 更新 {updated_count}"

            await _publish_and_persist("step_complete", sync_record_id, {
                "step": "import_stocks",
                "status": stock_imp.get("status"),
                "records_count": stock_count,
                "duration_seconds": round(step_duration, 1),
                "detail": stock_detail,
                "message": f"导入股票数据: {stock_count} 条 ({step_duration:.1f}s)",
            }, session, sync_record)

            # Step 4: Import ETF data
            step_start = datetime.now()
            await _publish_and_persist("progress", sync_record_id, {
                "step": "import_etfs",
                "progress": 75,
                "message": "正在导入ETF数据到数据库...",
            }, session, sync_record)

            etf_imp = await import_etf_data(ctx, recent_days=1)
            sync_record.details["steps"]["import_etfs"] = etf_imp
            step_duration = (datetime.now() - step_start).total_seconds()

            etf_count = 0
            etf_detail = ""
            if etf_imp.get("results") and isinstance(etf_imp["results"], dict):
                etf_count = etf_imp["results"].get("total_market_daily", 0)
                records_imported += etf_count
                new_count = etf_imp["results"].get("new_records", 0)
                updated_count = etf_imp["results"].get("updated_records", 0)
                if new_count or updated_count:
                    etf_detail = f"新增 {new_count}, 更新 {updated_count}"

            await _publish_and_persist("step_complete", sync_record_id, {
                "step": "import_etfs",
                "status": etf_imp.get("status"),
                "records_count": etf_count,
                "duration_seconds": round(step_duration, 1),
                "detail": etf_detail,
                "message": f"导入ETF数据: {etf_count} 条 ({step_duration:.1f}s)",
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

            # Update sync record with success
            total_duration = (datetime.now() - start_time).total_seconds()
            sync_record.status = "success"
            sync_record.completed_at = datetime.now()
            sync_record.duration_seconds = total_duration
            sync_record.records_downloaded = records_downloaded
            sync_record.records_imported = records_imported
            sync_record.records_classified = records_classified
            flag_modified(sync_record, "details")
            await session.commit()

            # Publish completion event
            await _publish_and_persist("job_complete", sync_record_id, {
                "status": "success",
                "progress": 100,
                "records_imported": records_imported,
                "records_classified": records_classified,
                "duration_seconds": round(total_duration, 1),
                "message": f"数据同步完成! 共导入 {records_imported} 条记录",
            }, session, sync_record)

            logger.info(f"API-triggered sync completed: {sync_record_id}")
            return {
                "status": "success",
                "sync_record_id": sync_record_id,
                "duration_seconds": total_duration,
                "records_imported": records_imported,
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
