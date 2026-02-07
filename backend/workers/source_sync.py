"""
数据源批量同步模块

使用 TuShare 数据源进行批量数据同步。
这是系统唯一的数据源模块。
"""

import asyncio
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Callable, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .data_sources import get_data_source, BaseDataSource
from .trading_days import get_latest_trading_day

logger = logging.getLogger(__name__)


# =============================================================================
# 核心指数代码（用于指数同步检测）
# =============================================================================

CORE_INDEX_CODES = [
    "sh.000001",  # 上证综指
    "sh.000016",  # 上证50
    "sh.000300",  # 沪深300
    "sh.000905",  # 中证500
    "sz.399001",  # 深证成指
    "sz.399006",  # 创业板指
]


# =============================================================================
# 数据库辅助函数
# =============================================================================


async def get_pg_max_date(session: AsyncSession, asset_type: str = "stock") -> Optional[date]:
    """
    获取 PostgreSQL 中的最新数据日期

    Args:
        session: 数据库会话
        asset_type: 'stock' 或 'etf'

    Returns:
        最新日期，如果无数据返回 None
    """
    if asset_type == "stock":
        # 股票代码格式: sh.600000, sz.000001, bj.430001
        # 排除指数: sh.000xxx (上证指数), sz.399xxx (深证指数)
        query = text("""
            SELECT MAX(date) FROM market_daily
            WHERE (code LIKE 'sh.6%' OR code LIKE 'sz.0%' OR code LIKE 'sz.3%' OR code LIKE 'bj.%')
            AND code NOT LIKE 'sh.000%' AND code NOT LIKE 'sz.399%'
        """)
    else:
        # ETF代码格式
        query = text("""
            SELECT MAX(date) FROM market_daily
            WHERE code LIKE 'sh.5%' OR code LIKE 'sz.1%'
        """)

    result = await session.execute(query)
    return result.scalar()


async def get_pg_index_max_date(session: AsyncSession) -> Optional[date]:
    """
    获取核心指数中最小的最大日期（只计算有完整 OHLC 数据的记录）。

    只检查核心指数（上证综指、沪深300等），避免因小众指数数据缺失
    而导致的反复同步问题。

    注意：TuShare index_dailybasic 只返回估值数据，不含 OHLC，
    因此需要检查 close IS NOT NULL 确保数据完整。

    Returns:
        核心指数中最小的最大日期，如果没有数据则返回 None
    """
    core_codes_str = ", ".join(f"'{c}'" for c in CORE_INDEX_CODES)
    query = text(f"""
        SELECT MIN(max_date) as min_max_date
        FROM (
            SELECT code, MAX(date) as max_date
            FROM market_daily
            WHERE code IN ({core_codes_str})
              AND close IS NOT NULL
            GROUP BY code
        ) sub
    """)
    result = await session.execute(query)
    return result.scalar()


# =============================================================================
# 复权因子同步（使用 baostock）
# =============================================================================


async def batch_insert_adjust_factors(
    session: AsyncSession,
    records: List[Dict],
) -> int:
    """批量插入复权因子数据 (upsert)"""
    if not records:
        return 0

    from app.db.models.asset import AdjustFactor
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(AdjustFactor).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["code", "divid_operate_date"],
        set_={
            "fore_adjust_factor": stmt.excluded.fore_adjust_factor,
            "back_adjust_factor": stmt.excluded.back_adjust_factor,
            "adjust_factor": stmt.excluded.adjust_factor,
        },
    )
    await session.execute(stmt)
    return len(records)


def _deduplicate_adjust_records(records: List[Dict]) -> List[Dict]:
    """去除重复的复权因子记录（baostock 有时返回重复数据）"""
    seen: set = set()
    deduped: List[Dict] = []
    for record in records:
        key = (record["code"], record["divid_operate_date"])
        if key not in seen:
            seen.add(key)
            deduped.append(record)
    return deduped


def _fetch_adjust_factors_batch_sync(
    codes_with_names: List[Tuple[str, str]],
    start_date: str,
) -> Tuple[List[Dict], Dict[str, str]]:
    """使用 baostock 批量获取复权因子（同步函数，在线程池中调用）"""
    import baostock as bs

    all_records: List[Dict] = []
    errors: Dict[str, str] = {}

    lg = bs.login()
    if lg.error_code != "0":
        return [], {"_login": f"baostock login failed: {lg.error_msg}"}

    try:
        for code, name in codes_with_names:
            try:
                rs = bs.query_adjust_factor(code=code, start_date=start_date)

                while (rs.error_code == "0") and rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 5:
                        all_records.append(
                            {
                                "code": row[0],
                                "divid_operate_date": date.fromisoformat(row[1]),
                                "fore_adjust_factor": Decimal(row[2]) if row[2] else None,
                                "back_adjust_factor": Decimal(row[3]) if row[3] else None,
                                "adjust_factor": Decimal(row[4]) if row[4] else None,
                            }
                        )

            except Exception as e:
                errors[code] = str(e)

    finally:
        bs.logout()

    all_records = _deduplicate_adjust_records(all_records)
    return all_records, errors


async def sync_adjust_factors(
    session: AsyncSession,
    progress_callback: Optional[Callable[[str, int, Dict], Any]] = None,
) -> Dict[str, Any]:
    """
    同步复权因子数据 - 分批增量模式

    使用 baostock query_adjust_factor 接口获取复权因子。
    优化策略：3天内已同步则跳过（复权因子是稀疏事件）。
    """
    max_date_query = text("SELECT MAX(divid_operate_date) FROM adjust_factor")
    result = await session.execute(max_date_query)
    max_date = result.scalar()

    latest_trading_day = get_latest_trading_day()
    today = date.today()

    if max_date and max_date >= latest_trading_day:
        logger.info(f"Adjust factor data is up to date (max_date={max_date})")
        return {"status": "skip", "message": "复权因子已是最新", "records": 0}

    if max_date:
        days_since_last = (today - max_date).days
        if days_since_last <= 3:
            logger.info(f"Adjust factor recently synced ({days_since_last} days ago), skipping")
            return {
                "status": "skip",
                "message": f"复权因子已是最新（{days_since_last}天前已同步）",
                "records": 0,
            }

    lookback_days = 7
    start_date_str = (
        (max_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        if max_date
        else "2020-01-01"
    )

    active_stocks_query = text("""
        SELECT DISTINCT am.code, am.name
        FROM asset_meta am
        INNER JOIN market_daily md ON am.code = md.code
        WHERE am.asset_type = 'STOCK'
          AND am.status = 1
          AND md.date >= :since_date
        ORDER BY am.code
    """)

    since_date = max_date - timedelta(days=30) if max_date else date(2020, 1, 1)
    active_result = await session.execute(active_stocks_query, {"since_date": since_date})
    active_stocks = [(row[0], row[1]) for row in active_result]

    if not active_stocks:
        return {"status": "skip", "message": "没有活跃股票需要同步", "records": 0}

    total_stocks = len(active_stocks)
    logger.info(
        f"Starting adjust factor sync for {total_stocks} active stocks (since {since_date})"
    )

    if progress_callback:
        await progress_callback(
            "开始同步复权因子...",
            0,
            {"action": "adjust_factor_start", "total_stocks": total_stocks},
        )

    BATCH_SIZE = 100
    total_records = 0
    total_errors: Dict[str, str] = {}
    loop = asyncio.get_event_loop()

    total_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_idx in range(0, total_stocks, BATCH_SIZE):
        batch = active_stocks[batch_idx : batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1

        records, batch_errors = await loop.run_in_executor(
            None,
            _fetch_adjust_factors_batch_sync,
            batch,
            start_date_str,
        )

        total_errors.update(batch_errors)

        if records:
            try:
                await batch_insert_adjust_factors(session, records)
                await session.flush()
                total_records += len(records)
            except Exception as e:
                await session.rollback()
                logger.warning(f"Failed to insert adjust factors for batch {batch_num}: {e}")
                total_errors[f"batch_{batch_num}"] = str(e)

        done_count = min(batch_idx + BATCH_SIZE, total_stocks)
        progress_pct = int(done_count / total_stocks * 100)

        last_code, last_name = batch[-1] if batch else ("", "")
        records_str = f"+{len(records)}条" if records else ""
        msg = f"复权因子 [{done_count}/{total_stocks}]: {last_code} {last_name} {records_str}"

        logger.info(f"Adjust factor batch {batch_num}/{total_batches}: {len(records)} records")

        if progress_callback:
            await progress_callback(
                msg,
                progress_pct,
                {
                    "action": "adjust_factor_progress",
                    "batch": batch_num,
                    "total_batches": total_batches,
                    "done": done_count,
                    "total": total_stocks,
                },
            )

    await session.commit()

    error_count = len(total_errors)
    logger.info(f"Adjust factor sync complete: {total_records} records, {error_count} errors")

    return {
        "status": "success" if error_count == 0 else "partial",
        "message": f"复权因子同步完成 ({total_stocks}只股票, {total_records}条记录)",
        "records": total_records,
        "stocks_processed": total_stocks,
        "error_count": error_count,
    }


async def sync_daily_data_with_source(
    session: AsyncSession,
    trade_date: date,
    asset_types: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[str, int, Dict], Any]] = None,
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
    asset_types: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[str, int, Dict], Any]] = None,
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
