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
    "sh.000906",  # 中证800
    "sh.000852",  # 中证1000
    "sh.000688",  # 科创50
    "sz.399001",  # 深证成指
    "sz.399006",  # 创业板指
    "sz.399005",  # 中小100
    "sz.399673",  # 创业板50
]

SW_L1_INDEX_CODES = [
    "sw.801010",  # 农林牧渔
    "sw.801030",  # 基础化工
    "sw.801040",  # 钢铁
    "sw.801050",  # 有色金属
    "sw.801080",  # 电子
    "sw.801110",  # 家用电器
    "sw.801120",  # 食品饮料
    "sw.801130",  # 纺织服饰
    "sw.801140",  # 轻工制造
    "sw.801150",  # 医药生物
    "sw.801160",  # 公用事业
    "sw.801170",  # 交通运输
    "sw.801180",  # 房地产
    "sw.801200",  # 商贸零售
    "sw.801210",  # 社会服务
    "sw.801230",  # 综合
    "sw.801710",  # 建筑材料
    "sw.801720",  # 建筑装饰
    "sw.801730",  # 电力设备
    "sw.801740",  # 国防军工
    "sw.801750",  # 计算机
    "sw.801760",  # 传媒
    "sw.801770",  # 通信
    "sw.801780",  # 银行
    "sw.801790",  # 非银金融
    "sw.801880",  # 汽车
    "sw.801890",  # 机械设备
    "sw.801950",  # 煤炭
    "sw.801960",  # 石油石化
    "sw.801970",  # 环保
    "sw.801980",  # 美容护理
]

ALL_INDEX_CODES = CORE_INDEX_CODES + SW_L1_INDEX_CODES


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
# 复权因子同步（使用 TuShare 按日期批量获取）
# =============================================================================


async def batch_insert_adjust_factors(
    session: AsyncSession,
    records: List[Dict],
) -> int:
    """批量插入复权因子数据 (upsert)

    asyncpg 限制单次查询参数不超过 32767 个,
    每条记录约 5 个字段, 所以每批最多 ~6000 条记录.
    """
    if not records:
        return 0

    from app.db.models.asset import AdjustFactor
    from sqlalchemy.dialects.postgresql import insert

    CHUNK_SIZE = 8000  # 8000 * 3 columns = 24000 params, safely under 32767
    total = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        stmt = insert(AdjustFactor).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code", "divid_operate_date"],
            set_={
                "adjust_factor": stmt.excluded.adjust_factor,
            },
        )
        await session.execute(stmt)
        total += len(chunk)
    return total


def _deduplicate_adjust_records(records: List[Dict]) -> List[Dict]:
    """去除重复的复权因子记录"""
    seen: set = set()
    deduped: List[Dict] = []
    for record in records:
        key = (record["code"], record["divid_operate_date"])
        if key not in seen:
            seen.add(key)
            deduped.append(record)
    return deduped


async def sync_adjust_factors(
    session: AsyncSession,
    progress_callback: Optional[Callable[[str, int, Dict], Any]] = None,
) -> Dict[str, Any]:
    """同步复权因子数据 - TuShare 按日期批量获取（股票+ETF）"""
    from .data_sources import get_data_source

    source = get_data_source()

    max_date_query = text("SELECT MAX(divid_operate_date) FROM adjust_factor")
    result = await session.execute(max_date_query)
    max_date = result.scalar()

    latest_trading_day = get_latest_trading_day()

    if max_date and max_date >= latest_trading_day:
        logger.info(f"Adjust factor data is up to date (max_date={max_date})")
        return {"status": "skip", "message": "复权因子已是最新", "records": 0}

    # 确定需要同步的日期范围
    if max_date:
        start_date = max_date - timedelta(days=3)
    else:
        start_date = date(2020, 1, 1)

    end_date = latest_trading_day

    trading_days = source.get_trading_days(start_date, end_date)

    if not trading_days:
        return {"status": "skip", "message": "没有需要同步的交易日", "records": 0}

    total_days = len(trading_days)
    logger.info(
        f"Starting adjust factor sync via TuShare: {total_days} trading days "
        f"({start_date} ~ {end_date})"
    )

    if progress_callback:
        await progress_callback(
            f"开始同步复权因子（{total_days}个交易日）...",
            0,
            {"action": "adjust_factor_start", "total_days": total_days},
        )

    total_records = 0
    total_errors: Dict[str, str] = {}

    for i, trade_date in enumerate(trading_days):
        date_str = trade_date.strftime("%Y%m%d")
        batch_records: List[Dict] = []

        try:
            stock_df = source.fetch_stock_adj_factor_by_date(trade_date)
            if not stock_df.empty:
                for _, row in stock_df.iterrows():
                    adj_val = row.get("adj_factor")
                    batch_records.append(
                        {
                            "code": row["code"],
                            "divid_operate_date": trade_date,
                            "adjust_factor": adj_val,
                        }
                    )
        except Exception as e:
            total_errors[f"stock_{date_str}"] = str(e)
            logger.warning(f"Failed to fetch stock adj_factor for {date_str}: {e}")

        try:
            etf_df = source.fetch_etf_adj_factor_by_date(trade_date)
            if not etf_df.empty:
                for _, row in etf_df.iterrows():
                    adj_val = row.get("adj_factor")
                    batch_records.append(
                        {
                            "code": row["code"],
                            "divid_operate_date": row["trade_date"],
                            "adjust_factor": adj_val,
                        }
                    )
        except Exception as e:
            total_errors[f"etf_{date_str}"] = str(e)
            logger.warning(f"Failed to fetch ETF adj_factor for {date_str}: {e}")

        # 3. 去重并批量插入
        if batch_records:
            batch_records = _deduplicate_adjust_records(batch_records)
            try:
                await batch_insert_adjust_factors(session, batch_records)
                await session.flush()
                total_records += len(batch_records)
            except Exception as e:
                await session.rollback()
                logger.warning(f"Failed to insert adjust factors for {date_str}: {e}")
                total_errors[f"insert_{date_str}"] = str(e)

        progress_pct = int((i + 1) / total_days * 100)
        msg = f"复权因子 [{i + 1}/{total_days}]: {trade_date} +{len(batch_records)}条（股票+ETF）"

        if (i + 1) % 10 == 0 or i == total_days - 1:
            logger.info(
                f"Adjust factor progress: {i + 1}/{total_days} days, {total_records} total records"
            )

        if progress_callback:
            await progress_callback(
                msg,
                progress_pct,
                {
                    "action": "adjust_factor_progress",
                    "day": i + 1,
                    "total_days": total_days,
                    "date": str(trade_date),
                    "records": len(batch_records),
                },
            )

        # TuShare rate limit: 2 API calls per day iteration
        if i < total_days - 1:
            await asyncio.sleep(0.3)

    await session.commit()

    error_count = len(total_errors)
    logger.info(f"Adjust factor sync complete: {total_records} records, {error_count} errors")

    return {
        "status": "success" if error_count == 0 else "partial",
        "message": f"复权因子同步完成（{total_days}个交易日, {total_records}条记录, 含ETF）",
        "records": total_records,
        "days_processed": total_days,
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

    # 3. 同步指数数据（核心市场指数 + 申万一级行业指数）
    if "index" in asset_types:
        current_step += 1
        if progress_callback:
            await progress_callback(
                f"[{source.name}] 获取指数数据 {trade_date}...",
                int(current_step / total_steps * 100),
                {"action": "fetch_index", "date": str(trade_date)},
            )

        index_count = 0
        # 3a. 核心市场指数 (via index_daily per code)
        try:
            df = source.fetch_index_daily_by_date(trade_date)
            if not df.empty:
                count = await _insert_market_daily(session, df)
                index_count += count
                logger.info(f"Inserted {count} core index records for {trade_date}")
        except Exception as e:
            error_msg = f"Core index sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        # 3b. 申万一级行业指数 (via sw_daily)
        try:
            df_sw = source.fetch_sw_daily_by_date(trade_date)
            if not df_sw.empty:
                count_sw = await _insert_market_daily(session, df_sw)
                index_count += count_sw
                logger.info(f"Inserted {count_sw} SW L1 index records for {trade_date}")
        except Exception as e:
            error_msg = f"SW index sync failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)

        results["index_count"] = index_count

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


async def sync_index_backfill(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    """
    按指数代码纵向补全 index 行情数据。

    使用 ALL_INDEX_CODES（11 核心 + 31 申万一级）作为代码源。
    核心指数走 index_daily API，申万指数走 sw_daily API。
    """
    from .data_sources.base import convert_standard_code_to_tushare

    source = get_data_source()
    index_codes = ALL_INDEX_CODES
    logger.info(
        f"[IndexBackfill] Starting vertical backfill {start_date} ~ {end_date}, "
        f"{len(index_codes)} codes"
    )

    total_records = 0
    errors: List[str] = []

    for i, std_code in enumerate(index_codes):
        ts_code = convert_standard_code_to_tushare(std_code)
        try:
            if std_code.startswith("sw."):
                df = source.fetch_sw_daily_by_code(ts_code, start_date, end_date)
            else:
                df = source.fetch_index_daily_by_code(ts_code, start_date, end_date)
            if not df.empty:
                count = await _insert_market_daily(session, df)
                total_records += count
        except Exception as e:
            err = f"{std_code}: {str(e)[:100]}"
            logger.warning(f"[IndexBackfill] Failed {err}")
            errors.append(err)

        if (i + 1) % 20 == 0:
            await session.commit()
            logger.info(
                f"[IndexBackfill] Progress {i + 1}/{len(index_codes)}, records={total_records}"
            )

        await asyncio.sleep(0.2)

    await session.commit()

    status_str = "success" if not errors else "partial"
    logger.info(
        f"[IndexBackfill] Complete: {len(index_codes)} codes, {total_records} records, "
        f"{len(errors)} errors"
    )

    return {
        "status": status_str,
        "total_codes": len(index_codes),
        "total_records": total_records,
        "errors": errors,
    }


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
                "pct_chg": safe_value(row.get("pct_chg")),
                "change": safe_value(row.get("change")),
            }
        )

    sql = text("""
        INSERT INTO market_daily (code, date, open, high, low, close, preclose, volume, amount, pct_chg, change)
        VALUES (:code, :date, :open, :high, :low, :close, :preclose, :volume, :amount, :pct_chg, :change)
        ON CONFLICT (code, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            preclose = EXCLUDED.preclose,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            pct_chg = EXCLUDED.pct_chg,
            change = EXCLUDED.change
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
    批量插入 indicator_valuation 表

    TuShare daily_basic 接口返回的所有估值与换手率数据直接存入此表。
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
    for _, row in df.iterrows():
        records.append(
            {
                "code": row["code"],
                "date": row["trade_date"],
                "close": safe_value(row.get("close")),
                "turnover_rate": safe_value(row.get("turnover_rate")),
                "turnover_rate_f": safe_value(row.get("turnover_rate_f")),
                "volume_ratio": safe_value(row.get("volume_ratio")),
                "pe": safe_value(row.get("pe")),
                "pe_ttm": safe_value(row.get("pe_ttm")),
                "pb_mrq": safe_value(row.get("pb_mrq")),
                "ps": safe_value(row.get("ps")),
                "ps_ttm": safe_value(row.get("ps_ttm")),
                "dv_ratio": safe_value(row.get("dv_ratio")),
                "dv_ttm": safe_value(row.get("dv_ttm")),
                "total_mv": safe_value(row.get("total_mv")),
                "circ_mv": safe_value(row.get("circ_mv")),
                "total_share": safe_value(row.get("total_share")),
                "float_share": safe_value(row.get("float_share")),
                "free_share": safe_value(row.get("free_share")),
                "is_st": safe_int(row.get("is_st"), 0),
            }
        )

    sql = text("""
        INSERT INTO indicator_valuation (
            code, date, close, turnover_rate, turnover_rate_f, volume_ratio,
            pe, pe_ttm, pb_mrq, ps, ps_ttm, dv_ratio, dv_ttm,
            total_mv, circ_mv, total_share, float_share, free_share, is_st
        )
        VALUES (
            :code, :date, :close, :turnover_rate, :turnover_rate_f, :volume_ratio,
            :pe, :pe_ttm, :pb_mrq, :ps, :ps_ttm, :dv_ratio, :dv_ttm,
            :total_mv, :circ_mv, :total_share, :float_share, :free_share, :is_st
        )
        ON CONFLICT (code, date) DO UPDATE SET
            close = EXCLUDED.close,
            turnover_rate = EXCLUDED.turnover_rate,
            turnover_rate_f = EXCLUDED.turnover_rate_f,
            volume_ratio = EXCLUDED.volume_ratio,
            pe = EXCLUDED.pe,
            pe_ttm = EXCLUDED.pe_ttm,
            pb_mrq = EXCLUDED.pb_mrq,
            ps = EXCLUDED.ps,
            ps_ttm = EXCLUDED.ps_ttm,
            dv_ratio = EXCLUDED.dv_ratio,
            dv_ttm = EXCLUDED.dv_ttm,
            total_mv = EXCLUDED.total_mv,
            circ_mv = EXCLUDED.circ_mv,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            free_share = EXCLUDED.free_share,
            is_st = EXCLUDED.is_st
    """)

    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        await session.execute(sql, batch)
        total_inserted += len(batch)

    return total_inserted
