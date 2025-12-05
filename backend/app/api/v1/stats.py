"""Dashboard statistics API endpoints."""

from typing import Optional
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.strategy import Strategy
from app.db.models.backtest import BacktestJob, BacktestResult, BacktestStatus

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class DashboardStats(BaseModel):
    """Dashboard statistics response."""
    total_strategies: int = Field(description="Total number of strategies")
    active_strategies: int = Field(description="Number of active strategies")
    total_backtests: int = Field(description="Total number of backtest jobs")
    completed_backtests: int = Field(description="Number of completed backtests")
    running_backtests: int = Field(description="Number of running backtests")
    best_return: Optional[Decimal] = Field(default=None, description="Best backtest return")
    best_sharpe: Optional[Decimal] = Field(default=None, description="Best Sharpe ratio")
    avg_sharpe: Optional[Decimal] = Field(default=None, description="Average Sharpe ratio")
    total_stocks: int = Field(default=0, description="Total number of stocks in database")


# Temporary: Mock user ID until auth is implemented
MOCK_USER_ID = "00000000-0000-0000-0000-000000000001"


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=DashboardStats, summary="Get Dashboard Statistics")
async def get_dashboard_stats(
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregated statistics for the dashboard.

    Returns counts and best metrics for strategies and backtests.
    """
    # Strategy counts
    total_strategies_query = select(func.count()).select_from(Strategy).where(
        Strategy.user_id == MOCK_USER_ID
    )
    total_strategies_result = await db.execute(total_strategies_query)
    total_strategies = total_strategies_result.scalar() or 0

    active_strategies_query = select(func.count()).select_from(Strategy).where(
        Strategy.user_id == MOCK_USER_ID,
        Strategy.is_active == True
    )
    active_strategies_result = await db.execute(active_strategies_query)
    active_strategies = active_strategies_result.scalar() or 0

    # Backtest counts
    total_backtests_query = select(func.count()).select_from(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID
    )
    total_backtests_result = await db.execute(total_backtests_query)
    total_backtests = total_backtests_result.scalar() or 0

    completed_backtests_query = select(func.count()).select_from(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID,
        BacktestJob.status == BacktestStatus.COMPLETED
    )
    completed_backtests_result = await db.execute(completed_backtests_query)
    completed_backtests = completed_backtests_result.scalar() or 0

    running_backtests_query = select(func.count()).select_from(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID,
        BacktestJob.status == BacktestStatus.RUNNING
    )
    running_backtests_result = await db.execute(running_backtests_query)
    running_backtests = running_backtests_result.scalar() or 0

    # Best metrics from backtest results
    best_return = None
    best_sharpe = None
    avg_sharpe = None

    # Get best return
    best_return_query = select(func.max(BacktestResult.total_return)).select_from(
        BacktestResult
    ).join(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID,
        BacktestResult.status == 'completed'
    )
    best_return_result = await db.execute(best_return_query)
    best_return = best_return_result.scalar()

    # Get best Sharpe
    best_sharpe_query = select(func.max(BacktestResult.sharpe_ratio)).select_from(
        BacktestResult
    ).join(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID,
        BacktestResult.status == 'completed'
    )
    best_sharpe_result = await db.execute(best_sharpe_query)
    best_sharpe = best_sharpe_result.scalar()

    # Get average Sharpe
    avg_sharpe_query = select(func.avg(BacktestResult.sharpe_ratio)).select_from(
        BacktestResult
    ).join(BacktestJob).where(
        BacktestJob.user_id == MOCK_USER_ID,
        BacktestResult.status == 'completed'
    )
    avg_sharpe_result = await db.execute(avg_sharpe_query)
    avg_sharpe_value = avg_sharpe_result.scalar()
    if avg_sharpe_value is not None:
        avg_sharpe = Decimal(str(round(float(avg_sharpe_value), 4)))

    # Get total stocks count
    from app.db.models.stock import StockBasic
    total_stocks_query = select(func.count()).select_from(StockBasic)
    total_stocks_result = await db.execute(total_stocks_query)
    total_stocks = total_stocks_result.scalar() or 0

    return DashboardStats(
        total_strategies=total_strategies,
        active_strategies=active_strategies,
        total_backtests=total_backtests,
        completed_backtests=completed_backtests,
        running_backtests=running_backtests,
        best_return=best_return,
        best_sharpe=best_sharpe,
        avg_sharpe=avg_sharpe,
        total_stocks=total_stocks,
    )
