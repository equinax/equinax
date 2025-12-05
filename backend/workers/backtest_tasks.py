"""Backtest execution tasks for ARQ."""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import UUID
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import settings
from app.db.models.backtest import BacktestJob, BacktestResult as BacktestResultModel, BacktestStatus
from app.db.models.strategy import Strategy
from app.db.models.stock import DailyKData, AdjustFactor
from app.domain.engine import BacktraderEngine, BacktestConfig


# Create engine for worker (separate from main app)
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

# Thread pool for running synchronous backtrader code
_executor = ThreadPoolExecutor(max_workers=4)


async def run_backtest_job(ctx: dict, job_id: str) -> Dict[str, Any]:
    """
    Master task that orchestrates a batch backtest job.
    Runs individual backtests sequentially or in parallel.
    """
    async with worker_session_maker() as db:
        # Get job
        result = await db.execute(
            select(BacktestJob).where(BacktestJob.id == job_id)
        )
        job = result.scalar_one_or_none()

        if not job:
            return {"error": "Job not found"}

        # Update status to running
        job.status = BacktestStatus.RUNNING
        job.started_at = datetime.utcnow()
        await db.commit()

        try:
            total = len(job.strategy_ids) * len(job.stock_codes)
            completed = 0
            successful = 0
            failed = 0

            # Run backtests
            for strategy_id in job.strategy_ids:
                for stock_code in job.stock_codes:
                    try:
                        # Run single backtest
                        backtest_result = await execute_single_backtest(
                            db=db,
                            job=job,
                            strategy_id=UUID(strategy_id),
                            stock_code=stock_code,
                        )

                        if backtest_result.status == BacktestStatus.COMPLETED:
                            successful += 1
                        else:
                            failed += 1

                    except Exception as e:
                        failed += 1
                        # Create failed result
                        failed_result = BacktestResultModel(
                            job_id=job.id,
                            strategy_id=UUID(strategy_id),
                            stock_code=stock_code,
                            parameters={},
                            status=BacktestStatus.FAILED,
                            error_message=str(e),
                        )
                        db.add(failed_result)

                    completed += 1

                    # Update progress
                    job.progress = (completed / total) * 100
                    job.successful_backtests = successful
                    job.failed_backtests = failed
                    await db.commit()

            # Mark job as completed
            job.status = BacktestStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            await db.commit()

            return {
                "job_id": str(job_id),
                "status": "completed",
                "total": total,
                "successful": successful,
                "failed": failed,
            }

        except Exception as e:
            job.status = BacktestStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            await db.commit()
            raise


async def load_stock_data(
    db: AsyncSession,
    stock_code: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load stock OHLCV data and adjustment factors from database."""

    # Build query for daily k-line data
    query = select(DailyKData).where(DailyKData.code == stock_code)
    if start_date:
        query = query.where(DailyKData.date >= start_date.date())
    if end_date:
        query = query.where(DailyKData.date <= end_date.date())
    query = query.order_by(DailyKData.date)

    result = await db.execute(query)
    rows = result.scalars().all()

    if not rows:
        raise ValueError(f"No data found for stock {stock_code}")

    # Convert to DataFrame
    data = []
    for row in rows:
        data.append({
            'date': row.date,
            'open': float(row.open),
            'high': float(row.high),
            'low': float(row.low),
            'close': float(row.close),
            'volume': float(row.volume),
            'amount': float(row.amount) if row.amount else 0,
            'turn': float(row.turn) if row.turn else 0,
            'pctChg': float(row.pctChg) if row.pctChg else 0,
        })

    data_df = pd.DataFrame(data)

    # Load adjustment factors
    adjust_query = select(AdjustFactor).where(AdjustFactor.code == stock_code)
    if start_date:
        adjust_query = adjust_query.where(AdjustFactor.date >= start_date.date())
    if end_date:
        adjust_query = adjust_query.where(AdjustFactor.date <= end_date.date())

    adjust_result = await db.execute(adjust_query)
    adjust_rows = adjust_result.scalars().all()

    adjust_data = []
    for row in adjust_rows:
        adjust_data.append({
            'date': row.date,
            'foreAdjustFactor': float(row.foreAdjustFactor) if row.foreAdjustFactor else 1.0,
            'backAdjustFactor': float(row.backAdjustFactor) if row.backAdjustFactor else 1.0,
        })

    adjust_df = pd.DataFrame(adjust_data) if adjust_data else pd.DataFrame()

    return data_df, adjust_df


def run_backtest_sync(
    strategy_code: str,
    data_df: pd.DataFrame,
    stock_code: str,
    config: BacktestConfig,
    parameters: Dict[str, Any],
    adjust_df: pd.DataFrame,
):
    """Synchronous wrapper for running backtrader (must run in thread pool)."""
    return BacktraderEngine.run(
        strategy_code=strategy_code,
        data_df=data_df,
        stock_code=stock_code,
        config=config,
        parameters=parameters,
        adjust_factors_df=adjust_df,
    )


async def execute_single_backtest(
    db: AsyncSession,
    job: BacktestJob,
    strategy_id: UUID,
    stock_code: str,
) -> BacktestResultModel:
    """Execute a single backtest for one strategy-stock combination."""

    # Get strategy
    strategy_result = await db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    )
    strategy = strategy_result.scalar_one_or_none()

    if not strategy:
        raise ValueError(f"Strategy {strategy_id} not found")

    # Load stock data
    data_df, adjust_df = await load_stock_data(
        db=db,
        stock_code=stock_code,
        start_date=job.start_date,
        end_date=job.end_date,
    )

    # Create backtest config
    config = BacktestConfig(
        initial_capital=float(job.initial_capital),
        commission=float(job.commission_rate),
        slippage_perc=float(job.slippage),
        stake_type='percent',
        stake_value=95.0,
        start_date=job.start_date,
        end_date=job.end_date,
        adjust_type='forward',
    )

    # Run backtest in thread pool (backtrader is synchronous)
    loop = asyncio.get_event_loop()
    bt_result = await loop.run_in_executor(
        _executor,
        run_backtest_sync,
        strategy.code,
        data_df,
        stock_code,
        config,
        strategy.parameters or {},
        adjust_df,
    )

    # Create result model
    if bt_result.success:
        result = BacktestResultModel(
            job_id=job.id,
            strategy_id=strategy_id,
            stock_code=stock_code,
            parameters=strategy.parameters or {},
            total_return=bt_result.total_return,
            annual_return=bt_result.annual_return,
            sharpe_ratio=bt_result.sharpe_ratio,
            max_drawdown=bt_result.max_drawdown,
            total_trades=bt_result.total_trades,
            winning_trades=bt_result.winning_trades,
            losing_trades=bt_result.losing_trades,
            win_rate=bt_result.win_rate,
            final_value=bt_result.final_value,
            execution_time_ms=bt_result.execution_time_ms,
            status=BacktestStatus.COMPLETED,
            equity_curve=bt_result.equity_curve or [],
            trades=bt_result.trades or [],
        )
    else:
        result = BacktestResultModel(
            job_id=job.id,
            strategy_id=strategy_id,
            stock_code=stock_code,
            parameters=strategy.parameters or {},
            status=BacktestStatus.FAILED,
            error_message=bt_result.error_message,
            execution_time_ms=bt_result.execution_time_ms,
        )

    db.add(result)
    await db.commit()
    await db.refresh(result)

    return result


async def run_single_backtest(
    ctx: dict,
    job_id: str,
    strategy_id: str,
    stock_code: str,
) -> Dict[str, Any]:
    """
    Run a single backtest as a standalone task.
    Used for re-running individual failed backtests.
    """
    async with worker_session_maker() as db:
        # Get job
        result = await db.execute(
            select(BacktestJob).where(BacktestJob.id == job_id)
        )
        job = result.scalar_one_or_none()

        if not job:
            return {"error": "Job not found"}

        try:
            backtest_result = await execute_single_backtest(
                db=db,
                job=job,
                strategy_id=UUID(strategy_id),
                stock_code=stock_code,
            )

            return {
                "result_id": str(backtest_result.id),
                "status": backtest_result.status.value,
            }

        except Exception as e:
            return {
                "error": str(e),
                "status": "failed",
            }
