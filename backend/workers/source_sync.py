"""
数据源批量同步模块

使用抽象数据源接口进行批量数据同步。
支持 TuShare 和 AkShare 数据源切换。
"""

import asyncio
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Callable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .data_sources import get_data_source, BaseDataSource

logger = logging.getLogger(__name__)


async def sync_daily_data_with_source(
    session: AsyncSession,
    trade_date: date,
    asset_types: List[str] = None,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    使用数据源接口同步指定日期的市场数据

    这是一个简化的同步函数，使用 TuShare 等数据源的批量 API，
    一次请求获取全市场数据，效率极高。

    Args:
        session: 数据库会话
        trade_date: 要同步的交易日期
        asset_types: 要同步的资产类型列表，可选 ['stock', 'etf', 'index']，默认全部
        progress_callback: 进度回调

    Returns:
        同步结果统计
    """
    if asset_types is None:
        asset_types = ["stock", "etf", "index"]

    # 获取数据源
    source = get_data_source()
    logger.info(f"Using data source: {source.name}")

    results = {
        "status": "success",
        "trade_date": str(trade_date),
        "data_source": source.name,
        "stock_count": 0,
        "etf_count": 0,
        "index_count": 0,
        "valuation_count": 0,
        "errors": [],
    }

    total_steps = len(asset_types) + (1 if "stock" in asset_types else 0)  # +1 for valuation
    current_step = 0

    # 1. 同步股票数据
    if "stock" in asset_types:
        current_step += 1
        if progress_callback:
            await progress_callback(
                f"[{source.name}] 获取股票数据 {trade_date}...",
                int(current_step / total_steps * 100),
                {"action": "fetch_stock", "date": str(trade_date)},
            )

        try:
            df = source.fetch_stock_daily_by_date(trade_date)
            if not df.empty:
                count = await _insert_market_daily(session, df)
                results["stock_count"] = count
                logger.info(f"Inserted {count} stock records for {trade_date}")
        except Exception as e:
            error_msg = f"Stock sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

    # 2. 同步 ETF 数据
    if "etf" in asset_types:
        current_step += 1
        if progress_callback:
            await progress_callback(
                f"[{source.name}] 获取 ETF 数据 {trade_date}...",
                int(current_step / total_steps * 100),
                {"action": "fetch_etf", "date": str(trade_date)},
            )

        try:
            df = source.fetch_etf_daily_by_date(trade_date)
            if not df.empty:
                count = await _insert_market_daily(session, df)
                results["etf_count"] = count
                logger.info(f"Inserted {count} ETF records for {trade_date}")
        except Exception as e:
            error_msg = f"ETF sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

    # 3. 同步指数数据
    if "index" in asset_types:
        current_step += 1
        if progress_callback:
            await progress_callback(
                f"[{source.name}] 获取指数数据 {trade_date}...",
                int(current_step / total_steps * 100),
                {"action": "fetch_index", "date": str(trade_date)},
            )

        try:
            df = source.fetch_index_daily_by_date(trade_date)
            if not df.empty:
                count = await _insert_market_daily(session, df)
                results["index_count"] = count
                logger.info(f"Inserted {count} index records for {trade_date}")
        except Exception as e:
            error_msg = f"Index sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

    # 4. 同步估值数据（股票）
    if "stock" in asset_types:
        current_step += 1
        if progress_callback:
            await progress_callback(
                f"[{source.name}] 获取估值数据 {trade_date}...",
                int(current_step / total_steps * 100),
                {"action": "fetch_valuation", "date": str(trade_date)},
            )

        try:
            df = source.fetch_valuation_by_date(trade_date)
            if not df.empty:
                count = await _insert_valuation(session, df)
                results["valuation_count"] = count
                logger.info(f"Inserted {count} valuation records for {trade_date}")
        except Exception as e:
            error_msg = f"Valuation sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

    await session.commit()

    if results["errors"]:
        results["status"] = "partial"

    total_count = results["stock_count"] + results["etf_count"] + results["index_count"]
    logger.info(
        f"Sync complete for {trade_date}: {total_count} market records, {results['valuation_count']} valuation records"
    )

    return results


async def backfill_missing_dates(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    asset_types: List[str] = None,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    补全日期范围内的缺失数据

    Args:
        session: 数据库会话
        start_date: 开始日期（包含）
        end_date: 结束日期（包含）
        asset_types: 要同步的资产类型
        progress_callback: 进度回调

    Returns:
        补全结果统计
    """
    source = get_data_source()

    # 获取日期范围内的交易日
    trading_days = source.get_trading_days(start_date, end_date)

    if not trading_days:
        return {
            "status": "skip",
            "message": f"No trading days between {start_date} and {end_date}",
            "dates_synced": 0,
        }

    logger.info(f"Backfilling {len(trading_days)} trading days: {start_date} to {end_date}")

    total_results = {
        "status": "success",
        "data_source": source.name,
        "dates_synced": 0,
        "stock_count": 0,
        "etf_count": 0,
        "index_count": 0,
        "valuation_count": 0,
        "errors": [],
    }

    for i, trade_date in enumerate(trading_days):
        if progress_callback:
            await progress_callback(
                f"补全数据 {trade_date} ({i + 1}/{len(trading_days)})...",
                int((i + 1) / len(trading_days) * 100),
                {
                    "action": "backfill",
                    "date": str(trade_date),
                    "progress": f"{i + 1}/{len(trading_days)}",
                },
            )

        try:
            result = await sync_daily_data_with_source(
                session, trade_date, asset_types, progress_callback=None
            )

            total_results["dates_synced"] += 1
            total_results["stock_count"] += result.get("stock_count", 0)
            total_results["etf_count"] += result.get("etf_count", 0)
            total_results["index_count"] += result.get("index_count", 0)
            total_results["valuation_count"] += result.get("valuation_count", 0)
            total_results["errors"].extend(result.get("errors", []))

        except Exception as e:
            error_msg = f"Failed to sync {trade_date}: {e}"
            logger.error(error_msg)
            total_results["errors"].append(error_msg)

        # 短暂延迟，避免 API 限流
        await asyncio.sleep(0.2)

    if total_results["errors"]:
        total_results["status"] = "partial"

    logger.info(f"Backfill complete: {total_results['dates_synced']} dates synced")
    return total_results


async def _insert_market_daily(session: AsyncSession, df: pd.DataFrame) -> int:
    """
    批量插入 market_daily 表

    Args:
        session: 数据库会话
        df: 标准化的 DataFrame (columns: code, trade_date, open, high, low, close, etc.)

    Returns:
        插入的记录数
    """
    if df.empty:
        return 0

    def safe_value(val):
        """Convert NaN/None to None for DB insertion"""
        if pd.isna(val):
            return None
        return val

    def safe_int(val):
        """Convert to int, handling NaN"""
        if pd.isna(val):
            return None
        try:
            return int(val)
        except:
            return None

    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "code": row["code"],
                "date": row["trade_date"],
                "open": safe_value(row.get("open")),
                "high": safe_value(row.get("high")),
                "low": safe_value(row.get("low")),
                "close": safe_value(row.get("close")),
                "preclose": safe_value(row.get("pre_close")),
                "volume": safe_int(row.get("volume")),
                "amount": safe_value(row.get("amount")),
                "turn": safe_value(row.get("turn")),
                "pct_chg": safe_value(row.get("pct_chg")),
                "trade_status": 1,
            }
        )

    sql = text("""
        INSERT INTO market_daily (code, date, open, high, low, close, preclose, volume, amount, turn, pct_chg, trade_status)
        VALUES (:code, :date, :open, :high, :low, :close, :preclose, :volume, :amount, :turn, :pct_chg, :trade_status)
        ON CONFLICT (code, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            preclose = EXCLUDED.preclose,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            turn = EXCLUDED.turn,
            pct_chg = EXCLUDED.pct_chg,
            trade_status = EXCLUDED.trade_status
    """)

    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await session.execute(sql, batch)
        total_inserted += len(batch)

    return total_inserted


async def _insert_valuation(session: AsyncSession, df: pd.DataFrame) -> int:
    """
    批量插入 indicator_valuation 表，并同步更新 market_daily.turn

    TuShare 的 daily_basic 接口返回换手率 (turnover_rate)，但 daily 接口不返回。
    因此需要在这里将换手率同步到 market_daily 表。
    """
    if df.empty:
        return 0

    def safe_value(val):
        """Convert NaN/None to None for DB insertion"""
        if pd.isna(val):
            return None
        return val

    def safe_int(val, default=0):
        """Convert to int, handling NaN"""
        if pd.isna(val):
            return default
        try:
            return int(val)
        except:
            return default

    records = []
    turnover_records = []
    for _, row in df.iterrows():
        code = row["code"]
        trade_date = row["trade_date"]
        turn = safe_value(row.get("turn"))

        records.append(
            {
                "code": code,
                "date": trade_date,
                "pe_ttm": safe_value(row.get("pe_ttm")),
                "pb_mrq": safe_value(row.get("pb_mrq")),
                "ps_ttm": None,
                "pcf_ncf_ttm": None,
                "total_mv": safe_value(row.get("total_mv")),
                "circ_mv": safe_value(row.get("circ_mv")),
                "is_st": safe_int(row.get("is_st"), 0),
            }
        )

        # Collect turnover data for market_daily update
        if turn is not None:
            turnover_records.append(
                {
                    "code": code,
                    "date": trade_date,
                    "turn": turn,
                }
            )

    sql = text("""
        INSERT INTO indicator_valuation (code, date, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, total_mv, circ_mv, is_st)
        VALUES (:code, :date, :pe_ttm, :pb_mrq, :ps_ttm, :pcf_ncf_ttm, :total_mv, :circ_mv, :is_st)
        ON CONFLICT (code, date) DO UPDATE SET
            pe_ttm = EXCLUDED.pe_ttm,
            pb_mrq = EXCLUDED.pb_mrq,
            ps_ttm = EXCLUDED.ps_ttm,
            pcf_ncf_ttm = EXCLUDED.pcf_ncf_ttm,
            total_mv = EXCLUDED.total_mv,
            circ_mv = EXCLUDED.circ_mv,
            is_st = EXCLUDED.is_st
    """)

    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await session.execute(sql, batch)
        total_inserted += len(batch)

    # Update market_daily.turn with turnover data from valuation
    # TuShare daily() doesn't return turnover, but daily_basic() does
    if turnover_records:
        update_turn_sql = text("""
            UPDATE market_daily 
            SET turn = :turn
            WHERE code = :code AND date = :date
        """)

        for i in range(0, len(turnover_records), batch_size):
            batch = turnover_records[i : i + batch_size]
            await session.execute(update_turn_sql, batch)

        logger.info(f"Updated {len(turnover_records)} turnover records in market_daily")

    return total_inserted
