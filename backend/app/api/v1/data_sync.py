"""Data sync monitoring API endpoints.

Provides endpoints for monitoring and triggering data synchronization.
"""

from datetime import datetime
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType, MarketDaily
from app.db.models.sync import SyncHistory
from app.core.redis_pubsub import subscribe_data_sync_events

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


class HealthDeduction(BaseModel):
    """Health score deduction item."""
    table: str
    reason: str
    points: int


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
    health_deductions: List[HealthDeduction] = Field(default_factory=list, description="Health score deduction breakdown")
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


class SyncAnalysis(BaseModel):
    """Pre-sync data analysis."""
    latest_data_date: Optional[str] = None
    latest_trading_day: Optional[str] = None
    today: str
    days_to_update: int
    needs_sync: bool
    message: str


class SyncEventLog(BaseModel):
    """Single event log entry."""
    type: str
    timestamp: str
    data: dict


class SyncJobDetail(BaseModel):
    """Detailed sync job info including event log."""
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
    event_log: List[SyncEventLog] = Field(default_factory=list)
    steps_summary: Optional[dict] = None

    class Config:
        from_attributes = True


class PaginatedSyncHistory(BaseModel):
    """Paginated sync history response."""
    items: List[SyncHistoryItem]
    total: int
    limit: int
    offset: int


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
                func.count(MarketDaily.code).label('count'),
                func.min(MarketDaily.date).label('min_date'),
                func.max(MarketDaily.date).label('max_date'),
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

    # Calculate health score with deduction breakdown
    health_score = 100
    health_deductions: List[HealthDeduction] = []
    for t in tables:
        if t.status == "Empty":
            health_deductions.append(HealthDeduction(
                table=t.name,
                reason="表数据为空",
                points=-20,
            ))
            health_score -= 20
        elif t.status == "Error":
            health_deductions.append(HealthDeduction(
                table=t.name,
                reason="查询出错",
                points=-10,
            ))
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
        health_deductions=health_deductions,
        last_sync=last_sync,
        tables=tables,
        missing_dates_count=0,
        worker_status="unknown",
    )


@router.post("/trigger", response_model=TriggerSyncResponse)
async def trigger_sync(
    request: TriggerSyncRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Manually trigger a data sync.

    This enqueues a sync job to the ARQ worker queue.
    """
    from uuid import uuid4
    from app.core.arq import get_arq_pool

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

    # Enqueue to ARQ worker
    try:
        arq_pool = await get_arq_pool()
        await arq_pool.enqueue_job("api_triggered_sync", job_id)
    except Exception as e:
        # If ARQ is not available, update record to failed
        sync_record.status = "failed"
        sync_record.error_message = f"Failed to enqueue: {str(e)}"
        await db.commit()
        return TriggerSyncResponse(
            job_id=job_id,
            status="failed",
            message=f"Failed to enqueue sync job: {str(e)}",
        )

    return TriggerSyncResponse(
        job_id=job_id,
        status="queued",
        message=f"Sync job {request.sync_type} has been queued",
    )


@router.get("/history", response_model=PaginatedSyncHistory)
async def get_sync_history(
    limit: int = 10,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated sync history.

    Returns the most recent sync operations with pagination.
    """
    # Get total count
    count_result = await db.execute(select(func.count(SyncHistory.id)))
    total = count_result.scalar() or 0

    # Get paginated items
    result = await db.execute(
        select(SyncHistory)
        .order_by(desc(SyncHistory.started_at))
        .limit(limit)
        .offset(offset)
    )
    rows = result.scalars().all()

    items = [
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

    return PaginatedSyncHistory(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/job/{job_id}", response_model=SyncHistoryItem)
async def get_sync_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get the status of a specific sync job.

    Use this endpoint to poll for job progress after triggering a sync.
    """
    result = await db.execute(
        select(SyncHistory).where(SyncHistory.id == job_id)
    )
    row = result.scalar_one_or_none()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync job {job_id} not found",
        )

    return SyncHistoryItem(
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


@router.post("/cancel/{job_id}")
async def cancel_sync_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel a running or queued sync job.

    This marks the job as cancelled in the database.
    Note: This does NOT actually stop the worker process - it just updates the status.
    """
    result = await db.execute(
        select(SyncHistory).where(SyncHistory.id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync job {job_id} not found",
        )

    if job.status not in ("running", "queued"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status: {job.status}",
        )

    job.status = "cancelled"
    job.completed_at = datetime.now()
    job.error_message = "Manually cancelled by user"
    await db.commit()

    return {"status": "cancelled", "job_id": job_id, "message": "Sync job cancelled"}


# Stale task detection threshold (minutes)
STALE_THRESHOLD_MINUTES = 60


@router.get("/active", response_model=Optional[SyncHistoryItem])
async def get_active_sync_job(
    db: AsyncSession = Depends(get_db),
):
    """
    Get the current active sync job if one exists.

    Returns the most recent queued or running job, or null if no active job.
    Also detects and marks stale tasks (running > 60 minutes) automatically.
    """
    from datetime import timedelta

    result = await db.execute(
        select(SyncHistory)
        .where(SyncHistory.status.in_(["queued", "running"]))
        .order_by(desc(SyncHistory.started_at))
        .limit(1)
    )
    row = result.scalar_one_or_none()

    if not row:
        return None

    # Check for stale task
    if row.status == "running" and row.started_at:
        # Handle timezone-aware datetime
        now = datetime.now()
        started = row.started_at.replace(tzinfo=None) if row.started_at.tzinfo else row.started_at
        running_time = now - started

        if running_time > timedelta(minutes=STALE_THRESHOLD_MINUTES):
            # Mark as stale
            row.status = "stale"
            row.completed_at = datetime.now()
            row.error_message = f"Task exceeded {STALE_THRESHOLD_MINUTES} minutes timeout - marked as stale"
            await db.commit()

    return SyncHistoryItem(
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


@router.get("/job/{job_id}/events")
async def stream_sync_events(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Stream real-time sync events via Server-Sent Events (SSE).

    Subscribe to progress updates for a specific sync job.
    Events include: plan, progress, step_complete, job_complete, error.
    """
    # Verify job exists
    result = await db.execute(
        select(SyncHistory).where(SyncHistory.id == job_id)
    )
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync job {job_id} not found",
        )

    return StreamingResponse(
        subscribe_data_sync_events(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


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
            func.count(func.distinct(MarketDaily.date)).label('days'),
            func.min(MarketDaily.date).label('min_date'),
            func.max(MarketDaily.date).label('max_date'),
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


@router.get("/analyze", response_model=SyncAnalysis)
async def analyze_sync_requirements(
    db: AsyncSession = Depends(get_db),
):
    """
    Analyze what data needs to be synced.

    Returns details about current data state and what would be updated.
    Compares with latest trading day (not today) to account for weekends/holidays.
    Shows message like "当前最新数据日期是 2025-12-23，需要更新 1 天".
    """
    from datetime import date
    from workers.batch_sync import get_latest_trading_day, get_trading_days_between

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    # Get latest trading day (accounting for weekends/holidays)
    latest_trading_day = get_latest_trading_day()
    latest_trading_day_str = latest_trading_day.strftime("%Y-%m-%d")

    # Get latest data date from market_daily
    result = await db.execute(
        select(func.max(MarketDaily.date))
    )
    latest_date = result.scalar()
    latest_date_str = str(latest_date) if latest_date else None

    # Calculate trading days to update (not calendar days)
    if latest_date:
        if latest_date >= latest_trading_day:
            # Data is up to date
            days_diff = 0
            needs_sync = False
            message = f"数据已是最新 ({latest_date_str})，无需同步"
        else:
            # Get the actual trading days that need updating
            missing_days = get_trading_days_between(latest_date, latest_trading_day)
            days_diff = len(missing_days)
            needs_sync = days_diff > 0

            if days_diff == 0:
                message = f"数据已是最新 ({latest_date_str})，无需同步"
            elif days_diff == 1:
                message = f"当前最新数据日期是 {latest_date_str}，需要更新 1 个交易日数据"
            else:
                message = f"当前最新数据日期是 {latest_date_str}，需要更新 {days_diff} 个交易日数据"
    else:
        days_diff = 0
        needs_sync = True
        message = "数据库为空，需要进行完整数据同步"

    return SyncAnalysis(
        latest_data_date=latest_date_str,
        latest_trading_day=latest_trading_day_str,
        today=today_str,
        days_to_update=days_diff,
        needs_sync=needs_sync,
        message=message,
    )


@router.get("/job/{job_id}/detail", response_model=SyncJobDetail)
async def get_sync_job_detail(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed sync job info including event log.

    Used for:
    1. Progress recovery when SSE reconnects
    2. Viewing history logs
    """
    result = await db.execute(
        select(SyncHistory).where(SyncHistory.id == job_id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sync job {job_id} not found",
        )

    # Extract event_log from details
    event_log = []
    steps_summary = None
    if job.details:
        event_log_raw = job.details.get("event_log", [])
        event_log = [
            SyncEventLog(
                type=e.get("type", "unknown"),
                timestamp=e.get("timestamp", ""),
                data=e.get("data", {}),
            )
            for e in event_log_raw
        ]
        steps_summary = job.details.get("steps")

    return SyncJobDetail(
        id=job.id,
        sync_type=job.sync_type,
        status=job.status,
        started_at=job.started_at,
        completed_at=job.completed_at,
        duration_seconds=float(job.duration_seconds) if job.duration_seconds else None,
        records_downloaded=job.records_downloaded,
        records_imported=job.records_imported,
        records_classified=job.records_classified,
        error_message=job.error_message,
        event_log=event_log,
        steps_summary=steps_summary,
    )
