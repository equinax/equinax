"""Backtest execution tasks for ARQ."""

import asyncio
import logging
import traceback
from datetime import datetime, date
from typing import Dict, Any, Optional, Union, List
from uuid import UUID
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.config import settings
from app.db.models.backtest import (
    BacktestJob,
    BacktestResult as BacktestResultModel,
    BacktestEquity,
    BacktestTrade,
    BacktestStatus,
)
from app.db.models.strategy import Strategy
from app.db.models.stock import DailyKData, AdjustFactor
from app.domain.engine import BacktraderEngine, BacktestConfig
from app.core.redis_pubsub import publish_event

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


def calculate_monthly_returns(equity_curve: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Calculate monthly returns from equity curve data.

    Args:
        equity_curve: List of {date: "YYYY-MM-DD", value: float}

    Returns:
        Dict mapping "YYYY-MM" -> monthly return (as decimal, e.g., 0.05 for 5%)
    """
    if not equity_curve or len(equity_curve) < 2:
        return {}

    # Group by month, keep last value of each month
    monthly_last = {}
    for point in equity_curve:
        date_str = point.get('date', '')
        value = point.get('value', 0)
        if date_str and value:
            month_key = date_str[:7]  # "YYYY-MM"
            monthly_last[month_key] = value

    # Calculate monthly returns
    returns = {}
    months = sorted(monthly_last.keys())
    for i, month in enumerate(months):
        if i == 0:
            continue  # First month has no previous month to compare
        prev_value = monthly_last[months[i - 1]]
        curr_value = monthly_last[month]
        if prev_value > 0:
            returns[month] = (curr_value - prev_value) / prev_value

    return returns


async def run_backtest_job(ctx: dict, job_id: str) -> Dict[str, Any]:
    """
    Master task that orchestrates a batch backtest job.
    Runs individual backtests sequentially or in parallel.
    """
    logger.info(f"Starting backtest job: {job_id}")

    async with worker_session_maker() as db:
        # Get job
        result = await db.execute(
            select(BacktestJob).where(BacktestJob.id == job_id)
        )
        job = result.scalar_one_or_none()

        if not job:
            logger.error(f"Job not found: {job_id}")
            return {"error": "Job not found"}

        logger.info(f"Job {job_id}: {len(job.strategy_ids)} strategies x {len(job.stock_codes)} stocks")

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
                    logger.info(f"Running backtest: strategy={strategy_id}, stock={stock_code}")

                    # Publish log event
                    await publish_event("log", job_id, {
                        "level": "info",
                        "message": f"开始回测: {stock_code}",
                    })

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
                            logger.info(f"Backtest completed: {stock_code}, return={backtest_result.total_return}")

                            # Publish result event
                            await publish_event("result", job_id, {
                                "result_id": str(backtest_result.id),
                                "stock_code": stock_code,
                                "status": "completed",
                                "total_return": float(backtest_result.total_return) if backtest_result.total_return else None,
                                "sharpe_ratio": float(backtest_result.sharpe_ratio) if backtest_result.sharpe_ratio else None,
                            })

                            # Publish log event for completion
                            await publish_event("log", job_id, {
                                "level": "info",
                                "message": f"回测完成: {stock_code}, 收益率={float(backtest_result.total_return)*100:.2f}%" if backtest_result.total_return else f"回测完成: {stock_code}",
                            })
                        else:
                            failed += 1
                            logger.warning(f"Backtest failed: {stock_code}, error={backtest_result.error_message}")

                            # Publish result event for failure
                            await publish_event("result", job_id, {
                                "result_id": str(backtest_result.id),
                                "stock_code": stock_code,
                                "status": "failed",
                                "error_message": backtest_result.error_message,
                            })

                            # Publish log event for failure
                            await publish_event("log", job_id, {
                                "level": "error",
                                "message": f"回测失败: {stock_code}",
                            })

                    except Exception as e:
                        failed += 1
                        error_msg = f"{str(e)}\n{traceback.format_exc()}"
                        logger.error(f"Backtest exception for {stock_code}:\n{error_msg}")

                        # Publish log event for exception
                        await publish_event("log", job_id, {
                            "level": "error",
                            "message": f"回测异常: {stock_code} - {str(e)[:100]}",
                        })

                        # Create failed result
                        failed_result = BacktestResultModel(
                            job_id=job.id,
                            strategy_id=UUID(strategy_id),
                            stock_code=stock_code,
                            parameters={},
                            status=BacktestStatus.FAILED,
                            error_message=error_msg[:2000],  # Truncate to fit DB
                        )
                        db.add(failed_result)

                    completed += 1

                    # Update progress
                    job.progress = (completed / total) * 100
                    job.successful_backtests = successful
                    job.failed_backtests = failed
                    await db.commit()

                    # Publish progress event
                    await publish_event("progress", job_id, {
                        "progress": float(job.progress),
                        "completed": completed,
                        "total": total,
                        "successful": successful,
                        "failed": failed,
                    })

            # Mark job as completed
            job.status = BacktestStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            await db.commit()

            logger.info(f"Job {job_id} completed: {successful} successful, {failed} failed")

            # Publish job_complete event
            await publish_event("job_complete", job_id, {
                "status": "completed",
                "successful": successful,
                "failed": failed,
                "total": total,
            })

            # Publish final log
            await publish_event("log", job_id, {
                "level": "info",
                "message": f"任务完成: 成功 {successful}, 失败 {failed}",
            })

            return {
                "job_id": str(job_id),
                "status": "completed",
                "total": total,
                "successful": successful,
                "failed": failed,
            }

        except Exception as e:
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            logger.error(f"Job {job_id} failed:\n{error_msg}")
            job.status = BacktestStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            await db.commit()

            # Publish job_complete event for failure
            await publish_event("job_complete", job_id, {
                "status": "failed",
                "error_message": str(e)[:200],
            })

            # Publish error log
            await publish_event("log", job_id, {
                "level": "error",
                "message": f"任务失败: {str(e)[:100]}",
            })

            raise


async def load_stock_data(
    db: AsyncSession,
    stock_code: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load stock OHLCV data and adjustment factors from database."""
    from datetime import date as date_type

    # Build query for daily k-line data
    query = select(DailyKData).where(DailyKData.code == stock_code)
    if start_date:
        # Handle both date and datetime objects
        sd = start_date if isinstance(start_date, date_type) else start_date.date()
        query = query.where(DailyKData.date >= sd)
    if end_date:
        ed = end_date if isinstance(end_date, date_type) else end_date.date()
        query = query.where(DailyKData.date <= ed)
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
            'pctChg': float(row.pct_chg) if row.pct_chg else 0,
        })

    data_df = pd.DataFrame(data)

    # Load adjustment factors
    adjust_query = select(AdjustFactor).where(AdjustFactor.code == stock_code)
    if start_date:
        adjust_query = adjust_query.where(AdjustFactor.divid_operate_date >= sd)
    if end_date:
        adjust_query = adjust_query.where(AdjustFactor.divid_operate_date <= ed)

    adjust_result = await db.execute(adjust_query)
    adjust_rows = adjust_result.scalars().all()

    adjust_data = []
    for row in adjust_rows:
        adjust_data.append({
            'date': row.divid_operate_date,
            'foreAdjustFactor': float(row.fore_adjust_factor) if row.fore_adjust_factor else 1.0,
            'backAdjustFactor': float(row.back_adjust_factor) if row.back_adjust_factor else 1.0,
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
        adjust_type='backward',  # Use backward adjustment for backtesting (includes dividend returns)
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
        # Calculate monthly returns from equity curve
        monthly_returns = calculate_monthly_returns(bt_result.equity_curve or [])

        # Create main result record (without equity_curve and trades - stored in separate tables)
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
            monthly_returns=monthly_returns,
        )
        db.add(result)
        await db.flush()  # Get result.id

        # Batch insert equity curve points
        if bt_result.equity_curve:
            equity_records = []
            prev_value = None
            for point in bt_result.equity_curve:
                value = float(point.get('value', 0))
                daily_return = None
                if prev_value and prev_value > 0:
                    daily_return = (value - prev_value) / prev_value
                equity_records.append(BacktestEquity(
                    result_id=result.id,
                    date=datetime.strptime(point['date'], '%Y-%m-%d').date(),
                    value=value,
                    drawdown=point.get('drawdown'),
                    daily_return=daily_return,
                ))
                prev_value = value
            db.add_all(equity_records)

        # Batch insert trade records
        if bt_result.trades:
            trade_records = []
            for trade in bt_result.trades:
                entry_date_str = trade.get('entry_date') or trade.get('open_datetime', '')
                exit_date_str = trade.get('exit_date') or trade.get('close_datetime', '')

                entry_date = None
                if entry_date_str:
                    entry_date = datetime.strptime(entry_date_str.split(' ')[0], '%Y-%m-%d').date()

                exit_date = None
                if exit_date_str:
                    exit_date = datetime.strptime(exit_date_str.split(' ')[0], '%Y-%m-%d').date()

                trade_records.append(BacktestTrade(
                    result_id=result.id,
                    stock_code=stock_code,
                    direction=trade.get('direction', 'long'),
                    entry_date=entry_date,
                    entry_price=trade.get('entry_price') or trade.get('open_price', 0),
                    exit_date=exit_date,
                    exit_price=trade.get('exit_price') or trade.get('close_price'),
                    size=int(trade.get('size', 0)),
                    pnl=trade.get('pnl'),
                    commission=trade.get('commission'),
                    net_pnl=trade.get('net_pnl'),
                    pnl_percent=trade.get('pnl_percent'),
                    bars_held=trade.get('bars_held'),
                ))
            db.add_all(trade_records)
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
