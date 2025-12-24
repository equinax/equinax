"""Data sync monitoring API endpoints.

Provides endpoints for monitoring and triggering data synchronization.
"""

from datetime import datetime
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType, MarketDaily
from app.db.models.sync import SyncHistory

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class DataTableStatus(BaseModel):
    """Status of a data table."""
    name: str
    record_count: int
    date_range: Optional[str] = None
    status: str = "OK"


class SyncHistoryItem(BaseModel):
    """Sync history entry."""
    id: str
    sync_type: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_downloaded: int = 0
    records_imported: int = 0
    records_classified: int = 0
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class DataSyncStatusResponse(BaseModel):
    """Full data sync status response."""
    health_score: int = Field(ge=0, le=100, description="Overall health percentage")
    last_sync: Optional[SyncHistoryItem] = None
    next_scheduled: str = "Daily 16:30 CST"
    tables: List[DataTableStatus]
    missing_dates_count: int = 0
    worker_status: str = "unknown"


class TriggerSyncRequest(BaseModel):
    """Request to trigger a sync."""
    sync_type: str = Field(default="daily", description="Type: daily, full")
    force: bool = Field(default=False, description="Force sync even if not needed")


class TriggerSyncResponse(BaseModel):
    """Response after triggering sync."""
    job_id: str
    status: str
    message: str


class DataCompletenessResponse(BaseModel):
    """Data completeness metrics."""
    total_assets: int
    total_trading_days: int
    data_coverage_pct: float
    earliest_date: Optional[str]
    latest_date: Optional[str]
    missing_dates: List[str] = []


# ============================================
# API Endpoints
# ============================================

@router.get("/status", response_model=DataSyncStatusResponse)
async def get_sync_status(
    db: AsyncSession = Depends(get_db),
):
    """
    Get comprehensive data sync status.

    Returns:
    - Health score (0-100)
    - Last sync information
    - Table statistics
    - Missing dates count
    """
    # Get table statistics
    tables = []

    # Assets
    try:
        result = await db.execute(
            select(
                func.count(AssetMeta.code).label('count'),
                func.count().filter(AssetMeta.asset_type == AssetType.STOCK).label('stocks'),
                func.count().filter(AssetMeta.asset_type == AssetType.ETF).label('etfs'),
                func.count().filter(AssetMeta.asset_type == AssetType.INDEX).label('indices'),
            )
        )
        row = result.first()
        tables.append(DataTableStatus(
            name="Assets",
            record_count=row.count if row else 0,
            date_range=f"Stocks: {row.stocks}, ETFs: {row.etfs}, Indices: {row.indices}" if row else "-",
            status="OK" if row and row.count > 0 else "Empty",
        ))
    except Exception:
        tables.append(DataTableStatus(name="Assets", record_count=0, status="Error"))

    # Market Daily
    try:
        result = await db.execute(
            select(
                func.count(MarketDaily.id).label('count'),
                func.min(MarketDaily.trade_date).label('min_date'),
                func.max(MarketDaily.trade_date).label('max_date'),
            )
        )
        row = result.first()
        date_range = f"{row.min_date} ~ {row.max_date}" if row and row.min_date else "-"
        tables.append(DataTableStatus(
            name="Market Daily",
            record_count=row.count if row else 0,
            date_range=date_range,
            status="OK" if row and row.count > 0 else "Empty",
        ))
    except Exception:
        tables.append(DataTableStatus(name="Market Daily", record_count=0, status="Error"))

    # Calculate health score
    health_score = 100
    for t in tables:
        if t.status == "Empty":
            health_score -= 20
        elif t.status == "Error":
            health_score -= 10
    health_score = max(0, health_score)

    # Get last sync
    last_sync = None
    try:
        result = await db.execute(
            select(SyncHistory)
            .order_by(desc(SyncHistory.started_at))
            .limit(1)
        )
        sync_row = result.scalar_one_or_none()
        if sync_row:
            last_sync = SyncHistoryItem(
                id=sync_row.id,
                sync_type=sync_row.sync_type,
                status=sync_row.status,
                started_at=sync_row.started_at,
                completed_at=sync_row.completed_at,
                duration_seconds=float(sync_row.duration_seconds) if sync_row.duration_seconds else None,
                records_downloaded=sync_row.records_downloaded,
                records_imported=sync_row.records_imported,
                records_classified=sync_row.records_classified,
                error_message=sync_row.error_message,
            )
    except Exception:
        pass

    return DataSyncStatusResponse(
        health_score=health_score,
        last_sync=last_sync,
        tables=tables,
        missing_dates_count=0,
        worker_status="unknown",
    )


@router.post("/trigger", response_model=TriggerSyncResponse)
async def trigger_sync(
    request: TriggerSyncRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger a data sync.

    This enqueues a sync job to the ARQ worker queue.
    """
    from uuid import uuid4

    job_id = str(uuid4())

    # Create sync history record
    sync_record = SyncHistory(
        id=job_id,
        sync_type=request.sync_type,
        status="queued",
        triggered_by="api",
    )
    db.add(sync_record)
    await db.commit()

    # TODO: Enqueue to ARQ worker
    # For now, return immediately with queued status
    # In production: await arq_redis.enqueue_job('daily_data_update')

    return TriggerSyncResponse(
        job_id=job_id,
        status="queued",
        message=f"Sync job {request.sync_type} has been queued",
    )


@router.get("/history", response_model=List[SyncHistoryItem])
async def get_sync_history(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    Get sync history.

    Returns the most recent sync operations.
    """
    result = await db.execute(
        select(SyncHistory)
        .order_by(desc(SyncHistory.started_at))
        .limit(limit)
    )
    rows = result.scalars().all()

    return [
        SyncHistoryItem(
            id=row.id,
            sync_type=row.sync_type,
            status=row.status,
            started_at=row.started_at,
            completed_at=row.completed_at,
            duration_seconds=float(row.duration_seconds) if row.duration_seconds else None,
            records_downloaded=row.records_downloaded,
            records_imported=row.records_imported,
            records_classified=row.records_classified,
            error_message=row.error_message,
        )
        for row in rows
    ]


@router.get("/completeness", response_model=DataCompletenessResponse)
async def get_data_completeness(
    db: AsyncSession = Depends(get_db),
):
    """
    Get data completeness metrics.

    Returns coverage statistics for market data.
    """
    # Get asset count
    asset_result = await db.execute(select(func.count(AssetMeta.code)))
    total_assets = asset_result.scalar() or 0

    # Get date range and trading days
    date_result = await db.execute(
        select(
            func.count(func.distinct(MarketDaily.trade_date)).label('days'),
            func.min(MarketDaily.trade_date).label('min_date'),
            func.max(MarketDaily.trade_date).label('max_date'),
        )
    )
    date_row = date_result.first()

    total_trading_days = date_row.days if date_row else 0
    earliest = str(date_row.min_date) if date_row and date_row.min_date else None
    latest = str(date_row.max_date) if date_row and date_row.max_date else None

    # Calculate coverage (simplified)
    coverage = 100.0 if total_assets > 0 and total_trading_days > 0 else 0.0

    return DataCompletenessResponse(
        total_assets=total_assets,
        total_trading_days=total_trading_days,
        data_coverage_pct=coverage,
        earliest_date=earliest,
        latest_date=latest,
        missing_dates=[],
    )
