"""Backtest execution API endpoints."""

from datetime import date
from typing import List, Optional
from uuid import UUID
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.backtest import BacktestJob, BacktestResult, BacktestStatus

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
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]

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
    created_at: str

    class Config:
        from_attributes = True


class BacktestResultDetailResponse(BacktestResultResponse):
    """Detailed backtest result with equity curve and trades."""
    equity_curve: Optional[List[dict]]
    trades: Optional[List[dict]]
    monthly_returns: Optional[dict]


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

    return BacktestListResponse(
        items=[BacktestJobResponse.model_validate(j) for j in jobs],
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

    # TODO: Queue ARQ task here
    # await arq_redis.enqueue_job('run_backtest_job', job.id)

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
