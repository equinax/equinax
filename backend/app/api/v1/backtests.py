"""Backtest execution API endpoints."""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.backtest import (
    BacktestJob,
    BacktestResult,
    BacktestEquity,
    BacktestTrade,
    BacktestStatus,
)
from app.core.arq import get_arq_pool
from app.core.redis_pubsub import subscribe_events

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class PositionSizing(BaseModel):
    """Position sizing configuration."""
    type: str = Field(default="percent", description="percent or fixed")
    value: float = Field(default=10, description="Percentage or fixed amount")


class BacktestCreate(BaseModel):
    """Schema for creating a backtest job."""
    name: Optional[str] = None
    description: Optional[str] = None
    strategy_ids: List[UUID] = Field(min_length=1)
    stock_codes: List[str] = Field(min_length=1)
    start_date: date
    end_date: date
    initial_capital: Decimal = Field(default=Decimal("1000000.00"))
    commission_rate: Decimal = Field(default=Decimal("0.0003"))
    slippage: Decimal = Field(default=Decimal("0.001"))
    position_sizing: PositionSizing = Field(default_factory=PositionSizing)


class BacktestJobSummaryMetrics(BaseModel):
    """Summary metrics for a completed backtest job."""
    avg_return: Optional[Decimal] = Field(default=None, description="Average return across all results")
    best_return: Optional[Decimal] = Field(default=None, description="Best return")
    worst_return: Optional[Decimal] = Field(default=None, description="Worst return")
    avg_sharpe: Optional[Decimal] = Field(default=None, description="Average Sharpe ratio")
    avg_max_drawdown: Optional[Decimal] = Field(default=None, description="Average max drawdown")
    avg_win_rate: Optional[Decimal] = Field(default=None, description="Average win rate")
    profitable_count: int = Field(default=0, description="Number of profitable backtests")


class BacktestJobResponse(BaseModel):
    """Schema for backtest job response."""
    id: UUID
    name: Optional[str]
    description: Optional[str]
    strategy_ids: List[str]
    stock_codes: List[str]
    start_date: date
    end_date: date
    initial_capital: Decimal
    commission_rate: Decimal
    slippage: Decimal
    position_sizing: dict
    status: str
    progress: Decimal
    error_message: Optional[str]
    total_backtests: int
    successful_backtests: int
    failed_backtests: int
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    # Summary metrics for completed jobs
    summary: Optional[BacktestJobSummaryMetrics] = None

    class Config:
        from_attributes = True


class BacktestResultResponse(BaseModel):
    """Schema for backtest result response."""
    id: UUID
    job_id: UUID
    strategy_id: UUID
    stock_code: str
    parameters: dict
    total_return: Optional[Decimal]
    annual_return: Optional[Decimal]
    sharpe_ratio: Optional[Decimal]
    sortino_ratio: Optional[Decimal]
    calmar_ratio: Optional[Decimal]
    max_drawdown: Optional[Decimal]
    max_drawdown_duration: Optional[int]
    volatility: Optional[Decimal]
    total_trades: Optional[int]
    winning_trades: Optional[int]
    losing_trades: Optional[int]
    win_rate: Optional[Decimal]
    profit_factor: Optional[Decimal]
    final_value: Optional[Decimal]
    execution_time_ms: Optional[int]
    status: str
    error_message: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class BacktestResultDetailResponse(BacktestResultResponse):
    """Detailed backtest result with monthly returns (equity_curve and trades moved to separate endpoints)."""
    monthly_returns: Optional[dict]


# ============================================
# New Schemas for Equity Curve and Trades
# ============================================

class EquityCurvePointResponse(BaseModel):
    """Schema for a single equity curve point."""
    date: date
    value: Decimal
    cash: Optional[Decimal] = None
    position_value: Optional[Decimal] = None
    drawdown: Optional[Decimal] = None
    daily_return: Optional[Decimal] = None

    class Config:
        from_attributes = True


class TradeRecordResponse(BaseModel):
    """Schema for a single trade record."""
    id: UUID
    stock_code: str
    direction: str
    entry_date: date
    entry_price: Decimal
    exit_date: Optional[date] = None
    exit_price: Optional[Decimal] = None
    size: int
    pnl: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    net_pnl: Optional[Decimal] = None
    pnl_percent: Optional[Decimal] = None
    bars_held: Optional[int] = None

    class Config:
        from_attributes = True


class PaginatedTradesResponse(BaseModel):
    """Schema for paginated trades list."""
    items: List[TradeRecordResponse]
    total: int
    page: int
    page_size: int
    pages: int


class BacktestListResponse(BaseModel):
    """Schema for paginated backtest list."""
    items: List[BacktestJobResponse]
    total: int
    page: int
    page_size: int
    pages: int


class ComparisonResponse(BaseModel):
    """Schema for strategy comparison."""
    strategies: List[dict]
    rankings: dict


# Temporary: Mock user ID until auth is implemented
MOCK_USER_ID = "00000000-0000-0000-0000-000000000001"


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=BacktestListResponse)
async def list_backtests(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    db: AsyncSession = Depends(get_db),
):
    """List all backtest jobs for the current user."""
    query = select(BacktestJob).where(BacktestJob.user_id == MOCK_USER_ID)

    if status_filter:
        query = query.where(BacktestJob.status == status_filter)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    query = query.order_by(BacktestJob.created_at.desc())

    result = await db.execute(query)
    jobs = result.scalars().all()

    # Get summary metrics for completed jobs (batch query for efficiency)
    completed_job_ids = [j.id for j in jobs if j.status == BacktestStatus.COMPLETED]
    job_summaries = {}

    if completed_job_ids:
        # Single query to get aggregated metrics for all completed jobs
        summary_query = (
            select(
                BacktestResult.job_id,
                func.avg(BacktestResult.total_return).label('avg_return'),
                func.max(BacktestResult.total_return).label('best_return'),
                func.min(BacktestResult.total_return).label('worst_return'),
                func.avg(BacktestResult.sharpe_ratio).label('avg_sharpe'),
                func.avg(BacktestResult.max_drawdown).label('avg_max_drawdown'),
                func.avg(BacktestResult.win_rate).label('avg_win_rate'),
                func.count().filter(BacktestResult.total_return > 0).label('profitable_count'),
            )
            .where(
                BacktestResult.job_id.in_(completed_job_ids),
                BacktestResult.status == 'completed',
            )
            .group_by(BacktestResult.job_id)
        )
        summary_result = await db.execute(summary_query)
        for row in summary_result.all():
            job_summaries[row.job_id] = BacktestJobSummaryMetrics(
                avg_return=Decimal(str(round(float(row.avg_return), 6))) if row.avg_return else None,
                best_return=Decimal(str(round(float(row.best_return), 6))) if row.best_return else None,
                worst_return=Decimal(str(round(float(row.worst_return), 6))) if row.worst_return else None,
                avg_sharpe=Decimal(str(round(float(row.avg_sharpe), 4))) if row.avg_sharpe else None,
                avg_max_drawdown=Decimal(str(round(float(row.avg_max_drawdown), 6))) if row.avg_max_drawdown else None,
                avg_win_rate=Decimal(str(round(float(row.avg_win_rate), 4))) if row.avg_win_rate else None,
                profitable_count=row.profitable_count or 0,
            )

    # Build response with summaries
    items = []
    for job in jobs:
        job_response = BacktestJobResponse.model_validate(job)
        if job.id in job_summaries:
            job_response.summary = job_summaries[job.id]
        items.append(job_response)

    return BacktestListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=BacktestJobResponse, status_code=status.HTTP_201_CREATED)
async def create_backtest(
    backtest_in: BacktestCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new backtest job."""
    # Validate date range
    if backtest_in.start_date >= backtest_in.end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be before end_date",
        )

    # Calculate total backtests
    total_backtests = len(backtest_in.strategy_ids) * len(backtest_in.stock_codes)

    # Create job
    job = BacktestJob(
        user_id=MOCK_USER_ID,
        name=backtest_in.name,
        description=backtest_in.description,
        strategy_ids=[str(sid) for sid in backtest_in.strategy_ids],
        stock_codes=backtest_in.stock_codes,
        start_date=backtest_in.start_date,
        end_date=backtest_in.end_date,
        initial_capital=backtest_in.initial_capital,
        commission_rate=backtest_in.commission_rate,
        slippage=backtest_in.slippage,
        position_sizing=backtest_in.position_sizing.model_dump(),
        total_backtests=total_backtests,
        status=BacktestStatus.QUEUED,
    )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Queue ARQ task
    try:
        arq_pool = await get_arq_pool()
        await arq_pool.enqueue_job('run_backtest_job', str(job.id))
    except Exception as e:
        # Log error but don't fail the request
        print(f"Failed to enqueue backtest job: {e}")

    return BacktestJobResponse.model_validate(job)


@router.get("/{job_id}", response_model=BacktestJobResponse)
async def get_backtest(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific backtest job by ID."""
    result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    return BacktestJobResponse.model_validate(job)


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_backtest(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Cancel a pending or running backtest job."""
    result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    if job.status in [BacktestStatus.COMPLETED, BacktestStatus.FAILED]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel a completed or failed job",
        )

    job.status = BacktestStatus.CANCELLED
    await db.commit()

    # TODO: Cancel ARQ task if running


@router.get("/{job_id}/results", response_model=List[BacktestResultResponse])
async def get_backtest_results(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get all results for a backtest job."""
    # Verify job exists and belongs to user
    job_result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    if not job_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    # Get results
    result = await db.execute(
        select(BacktestResult)
        .where(BacktestResult.job_id == job_id)
        .order_by(BacktestResult.sharpe_ratio.desc().nullslast())
    )
    results = result.scalars().all()

    return [BacktestResultResponse.model_validate(r) for r in results]


@router.get("/{job_id}/results/{result_id}", response_model=BacktestResultDetailResponse)
async def get_backtest_result_detail(
    job_id: UUID,
    result_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get detailed result including equity curve and trades."""
    result = await db.execute(
        select(BacktestResult).where(
            BacktestResult.id == result_id,
            BacktestResult.job_id == job_id,
        )
    )
    backtest_result = result.scalar_one_or_none()

    if not backtest_result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest result not found",
        )

    return BacktestResultDetailResponse.model_validate(backtest_result)


@router.get("/{job_id}/results/{result_id}/equity", response_model=List[EquityCurvePointResponse])
async def get_backtest_equity_curve(
    job_id: UUID,
    result_id: UUID,
    start_date: Optional[date] = Query(default=None, description="Filter by start date"),
    end_date: Optional[date] = Query(default=None, description="Filter by end date"),
    db: AsyncSession = Depends(get_db),
):
    """Get equity curve data for a backtest result."""
    # Verify result exists and belongs to job
    result = await db.execute(
        select(BacktestResult).where(
            BacktestResult.id == result_id,
            BacktestResult.job_id == job_id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest result not found",
        )

    # Query equity curve data
    query = select(BacktestEquity).where(BacktestEquity.result_id == result_id)

    if start_date:
        query = query.where(BacktestEquity.date >= start_date)
    if end_date:
        query = query.where(BacktestEquity.date <= end_date)

    query = query.order_by(BacktestEquity.date.asc())

    equity_result = await db.execute(query)
    equity_points = equity_result.scalars().all()

    return [EquityCurvePointResponse.model_validate(p) for p in equity_points]


@router.get("/{job_id}/results/{result_id}/trades", response_model=PaginatedTradesResponse)
async def get_backtest_trades(
    job_id: UUID,
    result_id: UUID,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get trade records for a backtest result with pagination."""
    # Verify result exists and belongs to job
    result = await db.execute(
        select(BacktestResult).where(
            BacktestResult.id == result_id,
            BacktestResult.job_id == job_id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest result not found",
        )

    # Get total count
    count_query = select(func.count()).where(BacktestTrade.result_id == result_id)
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Query trades with pagination
    query = (
        select(BacktestTrade)
        .where(BacktestTrade.result_id == result_id)
        .order_by(BacktestTrade.entry_date.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    trades_result = await db.execute(query)
    trades = trades_result.scalars().all()

    return PaginatedTradesResponse(
        items=[TradeRecordResponse.model_validate(t) for t in trades],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if total > 0 else 0,
    )


@router.get("/{job_id}/compare", response_model=ComparisonResponse)
async def compare_strategies(
    job_id: UUID,
    metric: str = Query(default="sharpe_ratio", description="Metric to rank by"),
    db: AsyncSession = Depends(get_db),
):
    """Compare strategy performance within a backtest job."""
    # Verify job exists
    job_result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    if not job_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    # Get all results
    result = await db.execute(
        select(BacktestResult).where(BacktestResult.job_id == job_id)
    )
    results = result.scalars().all()

    # Group by strategy
    strategy_metrics = {}
    for r in results:
        sid = str(r.strategy_id)
        if sid not in strategy_metrics:
            strategy_metrics[sid] = {
                "strategy_id": sid,
                "total_return": [],
                "sharpe_ratio": [],
                "max_drawdown": [],
                "win_rate": [],
            }

        if r.total_return is not None:
            strategy_metrics[sid]["total_return"].append(float(r.total_return))
        if r.sharpe_ratio is not None:
            strategy_metrics[sid]["sharpe_ratio"].append(float(r.sharpe_ratio))
        if r.max_drawdown is not None:
            strategy_metrics[sid]["max_drawdown"].append(float(r.max_drawdown))
        if r.win_rate is not None:
            strategy_metrics[sid]["win_rate"].append(float(r.win_rate))

    # Calculate averages
    strategies = []
    for sid, metrics in strategy_metrics.items():
        avg_metrics = {
            "strategy_id": sid,
            "avg_return": sum(metrics["total_return"]) / len(metrics["total_return"]) if metrics["total_return"] else 0,
            "avg_sharpe": sum(metrics["sharpe_ratio"]) / len(metrics["sharpe_ratio"]) if metrics["sharpe_ratio"] else 0,
            "avg_drawdown": sum(metrics["max_drawdown"]) / len(metrics["max_drawdown"]) if metrics["max_drawdown"] else 0,
            "avg_win_rate": sum(metrics["win_rate"]) / len(metrics["win_rate"]) if metrics["win_rate"] else 0,
            "num_backtests": len(metrics["total_return"]),
        }
        strategies.append(avg_metrics)

    # Sort by metric
    metric_map = {
        "sharpe_ratio": "avg_sharpe",
        "total_return": "avg_return",
        "max_drawdown": "avg_drawdown",
        "win_rate": "avg_win_rate",
    }
    sort_key = metric_map.get(metric, "avg_sharpe")
    reverse = sort_key != "avg_drawdown"  # Lower drawdown is better
    strategies.sort(key=lambda x: x[sort_key], reverse=reverse)

    # Add rankings
    rankings = {s["strategy_id"]: i + 1 for i, s in enumerate(strategies)}

    return ComparisonResponse(
        strategies=strategies,
        rankings=rankings,
    )


@router.get("/{job_id}/events")
async def stream_backtest_events(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Stream real-time backtest events via Server-Sent Events (SSE).

    Events include:
    - progress: Job progress updates
    - result: Individual backtest completions
    - log: Execution logs
    - job_complete: Job completion notification
    """
    # Verify job exists and belongs to user
    result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    return StreamingResponse(
        subscribe_events(str(job_id)),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )
