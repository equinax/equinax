"""Dashboard statistics API endpoints."""

from typing import Optional
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.strategy import Strategy
from app.db.models.backtest import BacktestJob, BacktestResult, BacktestStatus

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class BestStrategy(BaseModel):
    """Best performing strategy based on average return."""
    strategy_id: str = Field(description="Strategy UUID")
    strategy_name: str = Field(description="Strategy name")
    strategy_type: Optional[str] = Field(default=None, description="Strategy type")
    avg_return: Decimal = Field(description="Average return across all backtests")
    avg_sharpe: Optional[Decimal] = Field(default=None, description="Average Sharpe ratio")
    backtest_count: int = Field(description="Number of backtests using this strategy")
    avg_win_rate: Optional[Decimal] = Field(default=None, description="Average win rate")


class BestBacktest(BaseModel):
    """Best single backtest result by total return."""
    result_id: str = Field(description="Backtest result UUID")
    stock_code: str = Field(description="Stock code")
    strategy_id: str = Field(description="Strategy UUID")
    strategy_name: str = Field(description="Strategy name")
    total_return: Decimal = Field(description="Total return")
    annual_return: Optional[Decimal] = Field(default=None, description="Annualized return")
    final_value: Optional[Decimal] = Field(default=None, description="Final portfolio value")
    sharpe_ratio: Optional[Decimal] = Field(default=None, description="Sharpe ratio")
    max_drawdown: Optional[Decimal] = Field(default=None, description="Maximum drawdown")
    volatility: Optional[Decimal] = Field(default=None, description="Volatility")
    total_trades: Optional[int] = Field(default=None, description="Total number of trades")
    win_rate: Optional[Decimal] = Field(default=None, description="Win rate")
    profit_factor: Optional[Decimal] = Field(default=None, description="Profit factor")
    created_at: datetime = Field(description="Backtest creation time")


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
    top_strategies: list[BestStrategy] = Field(default_factory=list, description="Top 3 performing strategies")
    top_backtests: list[BestBacktest] = Field(default_factory=list, description="Top 3 backtests by return")


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

    # Get top 3 strategies (by average return)
    top_strategies_list = []
    top_strategies_query = (
        select(
            BacktestResult.strategy_id,
            func.avg(BacktestResult.total_return).label('avg_return'),
            func.avg(BacktestResult.sharpe_ratio).label('avg_sharpe'),
            func.count().label('backtest_count'),
            func.avg(BacktestResult.win_rate).label('avg_win_rate'),
        )
        .join(BacktestJob, BacktestResult.job_id == BacktestJob.id)
        .where(
            BacktestJob.user_id == MOCK_USER_ID,
            BacktestResult.status == 'completed',
            BacktestResult.total_return.isnot(None),
        )
        .group_by(BacktestResult.strategy_id)
        .order_by(desc('avg_return'))
        .limit(3)
    )
    top_strategies_result = await db.execute(top_strategies_query)
    top_strategies_rows = top_strategies_result.all()

    for row in top_strategies_rows:
        # Get strategy details
        strategy_query = select(Strategy).where(Strategy.id == row.strategy_id)
        strategy_result = await db.execute(strategy_query)
        strategy = strategy_result.scalar_one_or_none()

        if strategy:
            top_strategies_list.append(BestStrategy(
                strategy_id=str(row.strategy_id),
                strategy_name=strategy.name,
                strategy_type=strategy.strategy_type,
                avg_return=Decimal(str(round(float(row.avg_return), 6))),
                avg_sharpe=Decimal(str(round(float(row.avg_sharpe), 4))) if row.avg_sharpe else None,
                backtest_count=row.backtest_count,
                avg_win_rate=Decimal(str(round(float(row.avg_win_rate), 4))) if row.avg_win_rate else None,
            ))

    # Get top 3 backtests (by total return)
    top_backtests_list = []
    top_backtests_query = (
        select(BacktestResult)
        .join(BacktestJob, BacktestResult.job_id == BacktestJob.id)
        .where(
            BacktestJob.user_id == MOCK_USER_ID,
            BacktestResult.status == 'completed',
            BacktestResult.total_return.isnot(None),
        )
        .order_by(desc(BacktestResult.total_return))
        .limit(3)
    )
    top_backtests_result = await db.execute(top_backtests_query)
    top_backtests_rows = top_backtests_result.scalars().all()

    for row in top_backtests_rows:
        # Get strategy name
        strategy_query = select(Strategy).where(Strategy.id == row.strategy_id)
        strategy_result = await db.execute(strategy_query)
        strategy = strategy_result.scalar_one_or_none()
        strategy_name = strategy.name if strategy else "Unknown"

        top_backtests_list.append(BestBacktest(
            result_id=str(row.id),
            stock_code=row.stock_code,
            strategy_id=str(row.strategy_id),
            strategy_name=strategy_name,
            total_return=row.total_return,
            annual_return=row.annual_return,
            final_value=row.final_value,
            sharpe_ratio=row.sharpe_ratio,
            max_drawdown=row.max_drawdown,
            volatility=row.volatility,
            total_trades=row.total_trades,
            win_rate=row.win_rate,
            profit_factor=row.profit_factor,
            created_at=row.created_at,
        ))

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
        top_strategies=top_strategies_list,
        top_backtests=top_backtests_list,
    )
