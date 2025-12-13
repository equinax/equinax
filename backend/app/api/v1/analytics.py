"""Analytics API for backtest result analysis."""

from typing import List, Optional
from uuid import UUID
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.backtest import BacktestJob, BacktestResult, BacktestStatus

router = APIRouter()

# Temporary mock user ID
MOCK_USER_ID = "00000000-0000-0000-0000-000000000001"


# ============================================
# Response Schemas
# ============================================

class DistributionBucket(BaseModel):
    """A single bucket in the distribution."""
    range_min: float
    range_max: float
    count: int
    stock_codes: List[str] = Field(default_factory=list)


class DistributionStatistics(BaseModel):
    """Distribution statistics."""
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    p10: Optional[float] = None
    p25: Optional[float] = None
    p75: Optional[float] = None
    p90: Optional[float] = None


class OutlierStocks(BaseModel):
    """Best and worst performing stocks."""
    best: List[dict] = Field(default_factory=list)
    worst: List[dict] = Field(default_factory=list)


class DistributionResponse(BaseModel):
    """Response for distribution analysis."""
    buckets: List[DistributionBucket]
    statistics: DistributionStatistics
    outliers: OutlierStocks
    total_count: int
    metric: str


class RankingItem(BaseModel):
    """A single item in the ranking."""
    result_id: UUID
    strategy_id: UUID
    stock_code: str
    total_return: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    win_rate: Optional[float] = None
    total_trades: Optional[int] = None
    rank: int


class RankingResponse(BaseModel):
    """Response for ranking analysis."""
    items: List[RankingItem]
    total: int
    page: int
    page_size: int
    pages: int
    sort_by: str
    sort_order: str


# ============================================
# API Endpoints
# ============================================

@router.get("/{job_id}/distribution", response_model=DistributionResponse)
async def get_distribution(
    job_id: UUID,
    metric: str = Query(default="total_return", description="Metric to analyze (total_return, sharpe_ratio, max_drawdown)"),
    buckets: int = Query(default=20, ge=5, le=50, description="Number of buckets"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get return distribution analysis for a backtest job.

    Analyzes the distribution of a specified metric across all backtest results.
    """
    # Verify job exists and belongs to user
    job_result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    job = job_result.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    # Map metric name to column
    metric_column_map = {
        "total_return": BacktestResult.total_return,
        "sharpe_ratio": BacktestResult.sharpe_ratio,
        "max_drawdown": BacktestResult.max_drawdown,
        "win_rate": BacktestResult.win_rate,
    }

    if metric not in metric_column_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid metric. Choose from: {list(metric_column_map.keys())}",
        )

    metric_column = metric_column_map[metric]

    # Get all results with non-null metric values
    query = (
        select(
            BacktestResult.id,
            BacktestResult.strategy_id,
            BacktestResult.stock_code,
            metric_column.label("metric_value"),
        )
        .where(
            BacktestResult.job_id == job_id,
            BacktestResult.status == BacktestStatus.COMPLETED,
            metric_column.isnot(None),
        )
    )

    result = await db.execute(query)
    rows = result.all()

    if not rows:
        return DistributionResponse(
            buckets=[],
            statistics=DistributionStatistics(),
            outliers=OutlierStocks(),
            total_count=0,
            metric=metric,
        )

    # Extract metric values
    values = [float(r.metric_value) for r in rows]
    total_count = len(values)

    # Calculate statistics
    import statistics
    sorted_values = sorted(values)

    stats = DistributionStatistics(
        mean=statistics.mean(values),
        median=statistics.median(values),
        std=statistics.stdev(values) if len(values) > 1 else 0,
        min=min(values),
        max=max(values),
        p10=sorted_values[int(len(sorted_values) * 0.1)] if len(sorted_values) >= 10 else sorted_values[0],
        p25=sorted_values[int(len(sorted_values) * 0.25)],
        p75=sorted_values[int(len(sorted_values) * 0.75)],
        p90=sorted_values[int(len(sorted_values) * 0.9)] if len(sorted_values) >= 10 else sorted_values[-1],
    )

    # Create buckets
    min_val, max_val = min(values), max(values)
    bucket_size = (max_val - min_val) / buckets if max_val > min_val else 1

    bucket_list = []
    for i in range(buckets):
        range_min = min_val + i * bucket_size
        range_max = min_val + (i + 1) * bucket_size

        # Find stocks in this bucket
        stocks_in_bucket = [
            r.stock_code for r in rows
            if range_min <= float(r.metric_value) < range_max or (i == buckets - 1 and float(r.metric_value) == range_max)
        ]

        bucket_list.append(DistributionBucket(
            range_min=range_min,
            range_max=range_max,
            count=len(stocks_in_bucket),
            stock_codes=stocks_in_bucket[:10],  # Limit to 10 for response size
        ))

    # Get outliers (best and worst 5)
    sorted_rows = sorted(rows, key=lambda r: float(r.metric_value), reverse=(metric != "max_drawdown"))

    best = [
        {"stock_code": r.stock_code, "value": float(r.metric_value), "strategy_id": str(r.strategy_id)}
        for r in sorted_rows[:5]
    ]
    worst = [
        {"stock_code": r.stock_code, "value": float(r.metric_value), "strategy_id": str(r.strategy_id)}
        for r in sorted_rows[-5:]
    ]

    return DistributionResponse(
        buckets=bucket_list,
        statistics=stats,
        outliers=OutlierStocks(best=best, worst=worst),
        total_count=total_count,
        metric=metric,
    )


@router.get("/{job_id}/ranking", response_model=RankingResponse)
async def get_ranking(
    job_id: UUID,
    sort_by: str = Query(default="total_return", description="Sort by metric"),
    sort_order: str = Query(default="desc", description="Sort order (asc or desc)"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    strategy_id: Optional[UUID] = Query(default=None, description="Filter by strategy"),
    min_return: Optional[float] = Query(default=None, description="Minimum return filter"),
    max_return: Optional[float] = Query(default=None, description="Maximum return filter"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get ranked backtest results for a job.

    Supports sorting by various metrics and filtering.
    """
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

    # Map sort column
    sort_column_map = {
        "total_return": BacktestResult.total_return,
        "sharpe_ratio": BacktestResult.sharpe_ratio,
        "max_drawdown": BacktestResult.max_drawdown,
        "win_rate": BacktestResult.win_rate,
        "total_trades": BacktestResult.total_trades,
    }

    if sort_by not in sort_column_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid sort_by. Choose from: {list(sort_column_map.keys())}",
        )

    sort_column = sort_column_map[sort_by]

    # Build base query
    query = (
        select(BacktestResult)
        .where(
            BacktestResult.job_id == job_id,
            BacktestResult.status == BacktestStatus.COMPLETED,
        )
    )

    # Apply filters
    if strategy_id:
        query = query.where(BacktestResult.strategy_id == strategy_id)
    if min_return is not None:
        query = query.where(BacktestResult.total_return >= min_return)
    if max_return is not None:
        query = query.where(BacktestResult.total_return <= max_return)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply sorting
    if sort_order == "desc":
        query = query.order_by(sort_column.desc().nullslast())
    else:
        query = query.order_by(sort_column.asc().nullsfirst())

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    results = result.scalars().all()

    # Build response items with rank
    items = []
    base_rank = (page - 1) * page_size + 1
    for i, r in enumerate(results):
        items.append(RankingItem(
            result_id=r.id,
            strategy_id=r.strategy_id,
            stock_code=r.stock_code,
            total_return=float(r.total_return) if r.total_return else None,
            sharpe_ratio=float(r.sharpe_ratio) if r.sharpe_ratio else None,
            max_drawdown=float(r.max_drawdown) if r.max_drawdown else None,
            win_rate=float(r.win_rate) if r.win_rate else None,
            total_trades=r.total_trades,
            rank=base_rank + i,
        ))

    return RankingResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if total > 0 else 0,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/{job_id}/summary")
async def get_job_summary(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Get summary statistics for a backtest job.

    Returns aggregated metrics across all results.
    """
    # Verify job exists and belongs to user
    job_result = await db.execute(
        select(BacktestJob).where(
            BacktestJob.id == job_id,
            BacktestJob.user_id == MOCK_USER_ID,
        )
    )
    job = job_result.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backtest job not found",
        )

    # Get aggregated metrics
    summary_query = (
        select(
            func.count().label("total_results"),
            func.count().filter(BacktestResult.total_return > 0).label("profitable_count"),
            func.count().filter(BacktestResult.total_return <= 0).label("unprofitable_count"),
            func.avg(BacktestResult.total_return).label("avg_return"),
            func.min(BacktestResult.total_return).label("min_return"),
            func.max(BacktestResult.total_return).label("max_return"),
            func.avg(BacktestResult.sharpe_ratio).label("avg_sharpe"),
            func.avg(BacktestResult.max_drawdown).label("avg_max_drawdown"),
            func.avg(BacktestResult.win_rate).label("avg_win_rate"),
            func.avg(BacktestResult.total_trades).label("avg_trades"),
        )
        .where(
            BacktestResult.job_id == job_id,
            BacktestResult.status == BacktestStatus.COMPLETED,
        )
    )

    result = await db.execute(summary_query)
    row = result.one()

    # Calculate win rate (profitable results / total)
    win_rate = row.profitable_count / row.total_results if row.total_results > 0 else 0

    return {
        "job_id": str(job_id),
        "job_name": job.name,
        "job_status": job.status.value,
        "total_results": row.total_results,
        "profitable_count": row.profitable_count,
        "unprofitable_count": row.unprofitable_count,
        "pool_win_rate": win_rate,
        "metrics": {
            "avg_return": float(row.avg_return) if row.avg_return else None,
            "min_return": float(row.min_return) if row.min_return else None,
            "max_return": float(row.max_return) if row.max_return else None,
            "avg_sharpe": float(row.avg_sharpe) if row.avg_sharpe else None,
            "avg_max_drawdown": float(row.avg_max_drawdown) if row.avg_max_drawdown else None,
            "avg_win_rate": float(row.avg_win_rate) if row.avg_win_rate else None,
            "avg_trades": float(row.avg_trades) if row.avg_trades else None,
        },
        "pool_snapshot": job.pool_snapshot,
    }
