"""Data download and update tasks for ARQ.

Implements daily data update workflow:
1. Download today's stock/ETF market data
2. Download northbound holdings data
3. Import data to PostgreSQL
4. Trigger classification recalculation

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


async def download_northbound_data(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Download today's northbound holdings data.

    Returns:
        Result dict with status
    """
    logger.info("Starting northbound holdings download")

    script_path = DOWNLOADS_DIR / "download_northbound_holdings.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
        result = subprocess.run(
            [sys.executable, str(script_path), "--today"],
            cwd=str(BACKEND_DIR),
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout
        )

        if result.returncode != 0:
            logger.error(f"Northbound download failed: {result.stderr}")
            return {"status": "error", "message": result.stderr[-500:] if result.stderr else "Unknown error"}

        logger.info("Northbound data download completed")
        return {"status": "success", "message": "Northbound data downloaded"}

    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Download timed out"}
    except Exception as e:
        logger.exception("Northbound download error")
        return {"status": "error", "message": str(e)}


async def download_market_cap_data(ctx: Dict[str, Any], target_date: Optional[str] = None) -> Dict[str, Any]:
    """Download market cap data for a specific date or today.

    Args:
        ctx: ARQ context
        target_date: Date in YYYY-MM-DD format, defaults to today

    Returns:
        Result dict with status
    """
    if target_date is None:
        target_date = date.today().strftime("%Y-%m-%d")

    logger.info(f"Starting market cap download for {target_date}")

    script_path = DOWNLOADS_DIR / "download_market_cap.py"
    if not script_path.exists():
        return {"status": "error", "message": f"Script not found: {script_path}"}

    try:
        result = subprocess.run(
            [sys.executable, str(script_path), "--date", target_date],
            cwd=str(BACKEND_DIR),
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout (market cap downloads can be slow)
        )

        if result.returncode != 0:
            logger.error(f"Market cap download failed: {result.stderr}")
            return {"status": "error", "message": result.stderr[-500:] if result.stderr else "Unknown error"}

        logger.info("Market cap data download completed")
        return {"status": "success", "message": f"Market cap data downloaded for {target_date}"}

    except subprocess.TimeoutExpired:
        return {"status": "error", "message": "Download timed out"}
    except Exception as e:
        logger.exception("Market cap download error")
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
    3. Download northbound holdings
    4. Import to PostgreSQL
    5. Trigger classification update

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
    logger.info("[1/5] Downloading stock data...")
    stock_dl = await download_stock_data(ctx, recent_days=1)
    results["steps"]["download_stocks"] = stock_dl
    if stock_dl.get("status") == "error":
        logger.warning(f"Stock download had issues: {stock_dl.get('message')}")

    # Step 2: Download ETF data
    logger.info("[2/5] Downloading ETF data...")
    etf_dl = await download_etf_data(ctx, recent_days=1)
    results["steps"]["download_etfs"] = etf_dl

    # Step 3: Download northbound data
    logger.info("[3/5] Downloading northbound data...")
    nb_dl = await download_northbound_data(ctx)
    results["steps"]["download_northbound"] = nb_dl

    # Step 4: Import to PostgreSQL
    logger.info("[4/5] Importing data to PostgreSQL...")
    stock_imp = await import_stock_data(ctx, recent_days=1)
    results["steps"]["import_stocks"] = stock_imp

    etf_imp = await import_etf_data(ctx, recent_days=1)
    results["steps"]["import_etfs"] = etf_imp

    # Step 5: Trigger classification update
    logger.info("[5/5] Triggering classification update...")
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
                MIN(trade_date) as min_date,
                MAX(trade_date) as max_date,
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
                MIN(trade_date) as min_date,
                MAX(trade_date) as max_date
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

        # Northbound holdings (if imported)
        try:
            result = await session.execute(text("""
                SELECT COUNT(*), MAX(snapshot_date)
                FROM stock_microstructure
                WHERE is_northbound_heavy = true
            """))
            row = result.fetchone()
            status["northbound_heavy"] = {
                "count": row[0],
                "latest_date": str(row[1]) if row[1] else None,
            }
        except Exception:
            status["northbound_heavy"] = {"error": "Table may not exist"}

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


# Export all tasks for registration
__all__ = [
    "download_stock_data",
    "download_etf_data",
    "download_northbound_data",
    "download_market_cap_data",
    "import_stock_data",
    "import_etf_data",
    "daily_data_update",
    "check_data_status",
    "get_download_status",
]
