"""
批量数据同步模块

使用 akshare 批量 API 直接同步到 PostgreSQL，跳过 SQLite 缓存。
支持智能历史补全、并行下载和实时进度展示。
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple, Callable

import pandas as pd
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# =============================================================================
# 配置常量
# =============================================================================

# 最多自动补全的交易日数 (约 3 个月)
MAX_BACKFILL_DAYS = 60

# 进度报告间隔 (每 N 只股票报告一次)
PROGRESS_REPORT_INTERVAL = 1

# 并行下载的线程数
PARALLEL_WORKERS = 10


# =============================================================================
# 交易日检测
# =============================================================================

def _fetch_latest_trading_day_from_baostock() -> date:
    """
    从 baostock 获取最近的交易日（内部函数，不带缓存）

    如果当前时间 < 17:00，不考虑今天（盘中数据不完整）
    """
    import baostock as bs
    from datetime import datetime

    today = date.today()
    now = datetime.now()

    # 如果当前时间 < 17:00，不考虑今天（盘中数据不完整）
    if now.hour < 17:
        query_end_date = today - timedelta(days=1)
    else:
        query_end_date = today

    # 登录 baostock
    lg = bs.login()
    if lg.error_code != '0':
        logger.warning(f"baostock login failed: {lg.error_msg}, using {query_end_date} as fallback")
        return query_end_date

    try:
        # 查询最近30天的交易日历
        start_date = (query_end_date - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = query_end_date.strftime('%Y-%m-%d')

        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)

        trading_days = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == '1':  # is_trading_day
                trading_days.append(row[0])

        if trading_days:
            return date.fromisoformat(trading_days[-1])
        else:
            logger.warning(f"No trading days found in last 30 days, using {query_end_date}")
            return query_end_date

    finally:
        bs.logout()


def get_latest_trading_day() -> date:
    """
    获取最近的交易日（今天或之前）

    使用 Redis 缓存，每天只查询一次 baostock。
    缓存 key 区分盘中(before17)/盘后(after17)，避免早上的缓存影响下午的查询。
    """
    import redis
    from datetime import datetime
    import os

    today = date.today()
    now = datetime.now()

    # 缓存 key 区分盘中/盘后
    time_segment = "after17" if now.hour >= 17 else "before17"
    cache_key = f"latest_trading_day:{today.isoformat()}:{time_segment}"

    # 获取 Redis 连接
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    try:
        r = redis.from_url(redis_url, decode_responses=True)

        # 尝试从缓存获取
        cached = r.get(cache_key)
        if cached:
            logger.debug(f"Using cached latest trading day: {cached}")
            return date.fromisoformat(cached)

        # 缓存未命中，从 baostock 获取
        latest_trading_day = _fetch_latest_trading_day_from_baostock()

        # 计算到今天结束的秒数
        end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59)
        ttl_seconds = int((end_of_day - now).total_seconds()) + 1

        # 缓存结果
        r.setex(cache_key, ttl_seconds, latest_trading_day.isoformat())
        logger.info(f"Cached latest trading day: {latest_trading_day} (TTL: {ttl_seconds}s)")

        return latest_trading_day

    except redis.RedisError as e:
        logger.warning(f"Redis error, falling back to direct baostock call: {e}")
        return _fetch_latest_trading_day_from_baostock()


def get_trading_days_between(start_date: date, end_date: date) -> List[date]:
    """
    获取两个日期之间的所有交易日（不包含 start_date，包含 end_date）

    Args:
        start_date: 起始日期（不包含）
        end_date: 结束日期（包含）

    Returns:
        交易日列表
    """
    import baostock as bs

    if start_date >= end_date:
        return []

    lg = bs.login()
    if lg.error_code != '0':
        logger.warning(f"baostock login failed: {lg.error_msg}")
        return []

    try:
        # 查询从 start_date+1 到 end_date 的交易日
        query_start = (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
        query_end = end_date.strftime('%Y-%m-%d')

        rs = bs.query_trade_dates(start_date=query_start, end_date=query_end)

        trading_days = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == '1':  # is_trading_day
                trading_days.append(date.fromisoformat(row[0]))

        return trading_days

    finally:
        bs.logout()


async def get_pg_max_date(session: AsyncSession, asset_type: str = 'stock') -> Optional[date]:
    """
    获取 PostgreSQL 中的最新数据日期

    Args:
        session: 数据库会话
        asset_type: 'stock' 或 'etf'

    Returns:
        最新日期，如果无数据返回 None
    """
    if asset_type == 'stock':
        # 股票代码格式: sh.600000, sz.000001, bj.430001
        # 排除指数: sh.000xxx (上证指数), sz.399xxx (深证指数)
        query = text("""
            SELECT MAX(date) FROM market_daily
            WHERE (code LIKE 'sh.6%' OR code LIKE 'sz.0%' OR code LIKE 'sz.3%' OR code LIKE 'bj.%')
            AND code NOT LIKE 'sh.000%' AND code NOT LIKE 'sz.399%'
        """)
    else:
        # ETF代码格式: 待确认
        query = text("""
            SELECT MAX(date) FROM market_daily
            WHERE code LIKE 'sh.5%' OR code LIKE 'sz.1%'
        """)

    result = await session.execute(query)
    return result.scalar()


# =============================================================================
# 批量数据获取
# =============================================================================

def fetch_all_stocks_batch() -> Tuple[pd.DataFrame, date]:
    """
    批量获取全部 A 股当日数据

    使用 akshare 的 stock_zh_a_spot_em() 一次性获取全市场数据。

    Returns:
        Tuple of (DataFrame, trading_date)
    """
    import akshare as ak

    logger.info("Fetching all A-stock data using akshare batch API...")
    df = ak.stock_zh_a_spot_em()

    if df.empty:
        raise ValueError("akshare returned empty DataFrame for stocks")

    logger.info(f"Fetched {len(df)} stocks from akshare")

    # 从数据中获取实际的交易日期（如果有的话）
    # akshare spot_em 返回的是实时数据，日期即为当天（交易日）
    trading_date = date.today()

    return df, trading_date


def get_etf_list() -> List[str]:
    """
    获取全部 ETF 代码列表

    Returns:
        ETF 代码列表 (纯数字，如 ['510050', '159919', ...])
    """
    import akshare as ak

    logger.info("Fetching ETF list...")

    # 尝试获取 ETF 列表
    for attempt in range(3):
        try:
            df = ak.fund_etf_spot_em()
            codes = df['代码'].tolist()
            logger.info(f"Found {len(codes)} ETFs")
            return codes
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed to get ETF list: {e}")
            if attempt < 2:
                import time
                time.sleep(2)

    # 备用方案：返回常见 ETF 代码范围
    logger.warning("Using fallback ETF code ranges")
    codes = []
    # 上交所 ETF: 510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 516xxx, 517xxx, 518xxx
    for prefix in ['510', '511', '512', '513', '515', '516', '517', '518', '560', '561', '562', '563']:
        for i in range(1000):
            codes.append(f"{prefix}{i:03d}")
    # 深交所 ETF: 159xxx
    for i in range(1000):
        codes.append(f"159{i:03d}")
    return codes


def _fetch_etf_history_sync(code: str, start_str: str, end_str: str, missing_days_set: set) -> List[Dict]:
    """
    同步下载单只 ETF 的历史数据（供并行调用）

    Args:
        code: ETF 代码 (纯数字，如 '510050')
        start_str: 开始日期 YYYYMMDD
        end_str: 结束日期 YYYYMMDD
        missing_days_set: 需要的日期集合

    Returns:
        市场数据记录列表
    """
    import akshare as ak

    records = []
    try:
        df = ak.fund_etf_hist_em(
            symbol=code,
            period='daily',
            start_date=start_str,
            end_date=end_str,
            adjust=''
        )

        if df is not None and not df.empty:
            # 确定交易所前缀
            if code.startswith('5'):
                full_code = f"sh.{code}"
            else:
                full_code = f"sz.{code}"

            for _, row in df.iterrows():
                row_date = pd.to_datetime(row['日期']).date()
                if row_date not in missing_days_set:
                    continue

                records.append({
                    'code': full_code,
                    'date': row_date,
                    'open': safe_decimal(row.get('开盘')),
                    'high': safe_decimal(row.get('最高')),
                    'low': safe_decimal(row.get('最低')),
                    'close': safe_decimal(row.get('收盘')),
                    'preclose': None,
                    'volume': safe_int(row.get('成交量')),
                    'amount': safe_decimal(row.get('成交额')),
                    'turn': safe_decimal(row.get('换手率')),
                    'pct_chg': safe_decimal(row.get('涨跌幅')),
                    'trade_status': 1,
                })
    except Exception:
        pass  # 静默处理失败（可能是无效代码）

    return records


def fetch_all_etfs_batch() -> Tuple[pd.DataFrame, date]:
    """
    批量获取全部 ETF 当日数据（备用方法，可能超时）

    使用 akshare 的 fund_etf_spot_em() 一次性获取全市场 ETF 数据。

    Returns:
        Tuple of (DataFrame, trading_date)
    """
    import akshare as ak

    logger.info("Fetching all ETF data using akshare batch API...")
    df = ak.fund_etf_spot_em()

    if df.empty:
        raise ValueError("akshare returned empty DataFrame for ETFs")

    logger.info(f"Fetched {len(df)} ETFs from akshare")

    trading_date = date.today()

    return df, trading_date


# =============================================================================
# 数据转换
# =============================================================================

def convert_stock_code(code: str) -> str:
    """
    转换股票代码格式

    akshare 返回: 600000, 000001, 430001
    目标格式: sh.600000, sz.000001, bj.430001
    """
    code = str(code).zfill(6)

    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    elif code.startswith('4') or code.startswith('8'):
        return f"bj.{code}"
    else:
        # 其他情况默认上海
        return f"sh.{code}"


def convert_etf_code(code: str) -> str:
    """
    转换 ETF 代码格式

    akshare 返回: 510050, 159915
    目标格式: sh.510050, sz.159915
    """
    code = str(code).zfill(6)

    if code.startswith('5'):
        return f"sh.{code}"
    elif code.startswith('1'):
        return f"sz.{code}"
    else:
        return f"sh.{code}"


def safe_decimal(value, default=None) -> Optional[Decimal]:
    """安全转换为 Decimal"""
    if pd.isna(value) or value == '' or value == '-':
        return default
    try:
        return Decimal(str(value))
    except:
        return default


def safe_int(value, default=None) -> Optional[int]:
    """安全转换为 int"""
    if pd.isna(value) or value == '' or value == '-':
        return default
    try:
        return int(float(value))
    except:
        return default


def transform_stock_data(df: pd.DataFrame, trading_date: date) -> Tuple[List[Dict], List[Dict]]:
    """
    转换股票数据为数据库格式

    Args:
        df: akshare 返回的 DataFrame
        trading_date: 交易日期

    Returns:
        Tuple of (market_daily_records, indicator_valuation_records)
    """
    market_daily_records = []
    valuation_records = []

    # akshare stock_zh_a_spot_em 字段映射
    # 实际字段名需要根据 akshare 返回确认
    for _, row in df.iterrows():
        code = convert_stock_code(row.get('代码', row.get('股票代码', '')))

        if not code or code == 'sh.' or code == 'sz.':
            continue

        # market_daily 记录
        market_record = {
            'code': code,
            'date': trading_date,
            'open': safe_decimal(row.get('今开')),
            'high': safe_decimal(row.get('最高')),
            'low': safe_decimal(row.get('最低')),
            'close': safe_decimal(row.get('最新价')),
            'preclose': safe_decimal(row.get('昨收')),
            # 成交量: akshare 返回的是"手"，需要转为"股" (*100)
            'volume': safe_int(row.get('成交量', 0)) * 100 if safe_int(row.get('成交量')) else None,
            'amount': safe_decimal(row.get('成交额')),
            'turn': safe_decimal(row.get('换手率')),
            'pct_chg': safe_decimal(row.get('涨跌幅')),
            'trade_status': 1,  # 有数据说明在交易
        }
        market_daily_records.append(market_record)

        # indicator_valuation 记录
        # 市值单位转换: akshare 返回的是元，需要转为亿元 (/100000000)
        total_mv = safe_decimal(row.get('总市值'))
        circ_mv_raw = safe_decimal(row.get('流通市值'))

        # 优先使用 akshare 返回的流通市值，如果没有则从 amount/turn 计算
        # Formula: circ_mv = amount / (turn / 100) = amount * 100 / turn
        amount = safe_decimal(row.get('成交额'))
        turn = safe_decimal(row.get('换手率'))
        if circ_mv_raw:
            circ_mv = Decimal(str(circ_mv_raw / 100000000))  # 转为亿元
        elif amount and turn and turn > 0:
            circ_mv = Decimal(str((float(amount) * 100 / float(turn)) / 100000000))
        else:
            circ_mv = None

        valuation_record = {
            'code': code,
            'date': trading_date,
            'pe_ttm': safe_decimal(row.get('市盈率-动态')),
            'pb_mrq': safe_decimal(row.get('市净率')),
            'ps_ttm': None,  # akshare spot 不提供
            'pcf_ncf_ttm': None,  # akshare spot 不提供
            'total_mv': Decimal(str(total_mv / 100000000)) if total_mv else None,
            'circ_mv': circ_mv,
            # 从名称判断 ST 状态
            'is_st': 1 if 'ST' in str(row.get('名称', '')) else 0,
        }
        valuation_records.append(valuation_record)

    return market_daily_records, valuation_records


def transform_etf_data(df: pd.DataFrame, trading_date: date) -> List[Dict]:
    """
    转换 ETF 数据为数据库格式

    Args:
        df: akshare 返回的 DataFrame
        trading_date: 交易日期

    Returns:
        market_daily_records
    """
    market_daily_records = []

    for _, row in df.iterrows():
        code = convert_etf_code(row.get('代码', row.get('基金代码', '')))

        if not code:
            continue

        market_record = {
            'code': code,
            'date': trading_date,
            'open': safe_decimal(row.get('今开')),
            'high': safe_decimal(row.get('最高')),
            'low': safe_decimal(row.get('最低')),
            'close': safe_decimal(row.get('最新价')),
            'preclose': safe_decimal(row.get('昨收')),
            'volume': safe_int(row.get('成交量', 0)) * 100 if safe_int(row.get('成交量')) else None,
            'amount': safe_decimal(row.get('成交额')),
            'turn': safe_decimal(row.get('换手率')),
            'pct_chg': safe_decimal(row.get('涨跌幅')),
            'trade_status': 1,
        }
        market_daily_records.append(market_record)

    return market_daily_records


# =============================================================================
# 批量插入数据库
# =============================================================================

async def batch_insert_market_daily(
    session: AsyncSession,
    records: List[Dict],
) -> int:
    """
    批量插入 market_daily 表

    使用 ON CONFLICT DO UPDATE 实现 upsert。

    Args:
        session: 数据库会话
        records: 要插入的记录列表

    Returns:
        插入/更新的记录数
    """
    if not records:
        return 0

    # 使用原生 SQL 进行批量 upsert
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

    # 分批插入，每批 1000 条
    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        await session.execute(sql, batch)
        total_inserted += len(batch)

        if (i + batch_size) % 5000 == 0:
            logger.info(f"Inserted {total_inserted}/{len(records)} market_daily records...")

    return total_inserted


async def batch_insert_valuation(
    session: AsyncSession,
    records: List[Dict],
) -> int:
    """
    批量插入 indicator_valuation 表

    使用 ON CONFLICT DO UPDATE 实现 upsert。

    Args:
        session: 数据库会话
        records: 要插入的记录列表

    Returns:
        插入/更新的记录数
    """
    if not records:
        return 0

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
        batch = records[i:i + batch_size]
        await session.execute(sql, batch)
        total_inserted += len(batch)

    return total_inserted


# =============================================================================
# 主同步函数
# =============================================================================

async def sync_stocks_batch(
    session: AsyncSession,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    批量同步股票数据 - 智能补全 + 进度展示

    策略：
    - 仅缺 1 天（最新一天）：用批量 API（秒级完成）
    - 缺多天：用历史 API 一次性下载所有天（包括最新一天）

    Args:
        session: 数据库会话
        progress_callback: 进度回调函数 (message, progress_pct, detail) -> awaitable

    Returns:
        同步结果
    """
    # 1. 检查是否需要更新
    pg_max_date = await get_pg_max_date(session, 'stock')
    latest_trading_day = get_latest_trading_day()

    logger.info(f"Stock sync check: PG max date = {pg_max_date}, latest trading day = {latest_trading_day}")

    if pg_max_date and pg_max_date >= latest_trading_day:
        return {
            "status": "skip",
            "message": f"数据已是最新 (PG: {pg_max_date}, 最近交易日: {latest_trading_day})",
            "records": 0,
        }

    # 2. 计算缺失的交易日
    missing_days = get_trading_days_between(pg_max_date, latest_trading_day) if pg_max_date else [latest_trading_day]

    total_market_count = 0
    total_valuation_count = 0
    messages = []

    # 3. 检查是否超过阈值
    if len(missing_days) > MAX_BACKFILL_DAYS:
        # 超过阈值，只同步最新一天
        skip_msg = f"跳过历史补全 (缺失 {len(missing_days)} 天 > {MAX_BACKFILL_DAYS} 天阈值，请使用手动导入)"
        messages.append(skip_msg)
        logger.warning(skip_msg)
        missing_days = [latest_trading_day]

    # 4. 根据缺失天数选择同步策略
    if len(missing_days) == 1 and missing_days[0] == latest_trading_day:
        # 仅缺最新一天：使用批量 API（秒级完成）
        if progress_callback:
            await progress_callback(f"同步 {latest_trading_day} (批量模式)...", 50, {
                "action": "batch_sync",
                "date": str(latest_trading_day),
            })

        logger.info(f"Syncing {latest_trading_day} using batch API (single day mode)...")

        df, _ = fetch_all_stocks_batch()
        market_records, valuation_records = transform_stock_data(df, latest_trading_day)

        market_count = await batch_insert_market_daily(session, market_records)
        valuation_count = await batch_insert_valuation(session, valuation_records)

        total_market_count = market_count
        total_valuation_count = valuation_count
        messages.append(f"最新数据: {market_count} 条")

        await session.commit()

    else:
        # 缺多天：使用历史 API 一次性下载所有天（包括最新一天）
        if progress_callback:
            await progress_callback(f"开始补全 {len(missing_days)} 个交易日...", 10, {
                "action": "backfill_start",
                "days_to_backfill": len(missing_days),
            })

        backfill_result = await backfill_stock_history_with_progress(
            session,
            missing_days,  # 包括最新一天
            progress_callback,
        )

        if backfill_result.get("status") == "success":
            total_market_count = backfill_result.get("records", 0)
            messages.append(f"补全 {len(missing_days)} 天: {total_market_count} 条")

        # 补充获取最新一天的 PE/PB 数据（历史 API 不返回 PE/PB）
        if latest_trading_day in missing_days:
            if progress_callback:
                await progress_callback("补充获取 PE/PB 数据...", 95, {
                    "action": "fetch_pe_pb",
                })
            logger.info(f"Fetching PE/PB data for {latest_trading_day} from spot API...")
            df, _ = fetch_all_stocks_batch()
            _, valuation_records = transform_stock_data(df, latest_trading_day)
            valuation_count = await batch_insert_valuation(session, valuation_records)
            total_valuation_count = valuation_count
            logger.info(f"Updated {valuation_count} valuation records with PE/PB")
            await session.commit()

    return {
        "status": "success",
        "message": " | ".join(messages) if messages else "同步完成",
        "trading_date": str(latest_trading_day),
        "market_daily_count": total_market_count,
        "valuation_count": total_valuation_count,
        "days_backfilled": len(missing_days),
    }


async def backfill_etf_history(
    session: AsyncSession,
    missing_days: List[date],
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    并行下载 ETF 历史数据

    使用 fund_etf_hist_em 接口，比 fund_etf_spot_em 更稳定。

    Args:
        session: 数据库会话
        missing_days: 需要补全的交易日列表
        progress_callback: 进度回调函数 (message, progress_pct, detail) -> awaitable

    Returns:
        补全结果
    """
    if not missing_days:
        return {"status": "skip", "message": "没有需要补全的日期", "records": 0}

    # 计算日期范围
    start_date = min(missing_days)
    end_date = max(missing_days)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    missing_days_set = set(missing_days)

    logger.info(f"Starting ETF backfill for {len(missing_days)} days: {start_date} to {end_date}")

    # 获取 ETF 列表
    etf_codes = get_etf_list()
    total_etfs = len(etf_codes)
    logger.info(f"Found {total_etfs} ETFs to backfill")

    total_records = 0
    all_records = []
    completed_count = 0
    lock = asyncio.Lock()

    loop = asyncio.get_event_loop()

    # 使用线程池并行下载
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = [
            loop.run_in_executor(
                executor,
                _fetch_etf_history_sync,
                code, start_str, end_str, missing_days_set
            )
            for code in etf_codes
        ]

        for future in asyncio.as_completed(futures):
            records = await future
            async with lock:
                all_records.extend(records)
                completed_count += 1

                # 每个 ETF 都更新进度
                if completed_count % PROGRESS_REPORT_INTERVAL == 0 or completed_count == total_etfs:
                    # 进度: 60-75% 区间给 ETF
                    overall_pct = 60 + (completed_count / total_etfs) * 15
                    msg = f"ETF 下载中 [{completed_count}/{total_etfs}]"

                    if completed_count % 100 == 0 or completed_count == total_etfs:
                        logger.info(msg)

                    if progress_callback:
                        await progress_callback(msg, int(overall_pct), {
                            "action": "etf_backfill_progress",
                            "etfs_done": completed_count,
                            "etfs_total": total_etfs,
                            "days_count": len(missing_days),
                            "date_range": f"{start_date} ~ {end_date}",
                        })

    # 批量写入
    if all_records:
        count = await batch_insert_market_daily(session, all_records)
        total_records = count
        await session.commit()

    logger.info(f"ETF backfill complete: {total_records} records for {len(missing_days)} days")

    return {
        "status": "success",
        "message": f"ETF补全了 {len(missing_days)} 个交易日的数据",
        "records": total_records,
        "days_backfilled": len(missing_days),
    }


def fetch_etf_valuation_batch() -> List[Dict]:
    """
    从 fund_etf_spot_em 获取 ETF 市值数据

    Returns:
        indicator_valuation 记录列表
    """
    import akshare as ak

    try:
        df = ak.fund_etf_spot_em()
    except Exception as e:
        logger.warning(f"Failed to fetch ETF valuation data: {e}")
        return []

    records = []
    # 使用最新交易日而非今日，确保与 market_daily 日期一致
    valuation_date = get_latest_trading_day()

    for _, row in df.iterrows():
        code = str(row.get('代码', ''))
        if not code:
            continue

        # 转换代码格式: 510050 -> sh.510050 / 159xxx -> sz.159xxx
        if code.startswith('51') or code.startswith('56'):
            db_code = f'sh.{code}'
        elif code.startswith('15') or code.startswith('16'):
            db_code = f'sz.{code}'
        else:
            continue  # 跳过非主流 ETF

        total_mv = safe_decimal(row.get('总市值'))
        circ_mv = safe_decimal(row.get('流通市值'))

        if total_mv or circ_mv:
            records.append({
                'code': db_code,
                'date': valuation_date,
                'pe_ttm': None,
                'pb_mrq': None,
                'ps_ttm': None,
                'pcf_ncf_ttm': None,
                'total_mv': Decimal(str(float(total_mv) / 100000000)) if total_mv else None,  # 转为亿元
                'circ_mv': Decimal(str(float(circ_mv) / 100000000)) if circ_mv else None,
                'is_st': 0,
            })

    logger.info(f"Fetched ETF valuation data: {len(records)} records")
    return records


async def sync_etfs_batch(
    session: AsyncSession,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    同步 ETF 数据 - 使用历史 API（更稳定）

    Args:
        session: 数据库会话
        progress_callback: 进度回调函数 (message, progress_pct, detail) -> awaitable

    Returns:
        同步结果
    """
    # 1. 检查是否需要更新
    pg_max_date = await get_pg_max_date(session, 'etf')
    latest_trading_day = get_latest_trading_day()

    logger.info(f"ETF sync check: PG max date = {pg_max_date}, latest trading day = {latest_trading_day}")

    if pg_max_date and pg_max_date >= latest_trading_day:
        # 市值数据仍需每日更新（即使行情数据已是最新）
        etf_valuation_records = await asyncio.to_thread(fetch_etf_valuation_batch)
        valuation_count = 0
        if etf_valuation_records:
            valuation_count = await batch_insert_valuation(session, etf_valuation_records)
            await session.commit()
            logger.info(f"ETF valuation synced (skip mode): {valuation_count} records")

        return {
            "status": "skip",
            "message": f"ETF数据已是最新 (PG: {pg_max_date}, 最近交易日: {latest_trading_day})",
            "records": 0,
            "valuation_count": valuation_count,
        }

    # 2. 计算缺失的交易日
    missing_days = get_trading_days_between(pg_max_date, latest_trading_day) if pg_max_date else [latest_trading_day]

    # 3. 使用历史 API 并行下载（比 fund_etf_spot_em 更稳定）
    result = await backfill_etf_history(session, missing_days, progress_callback)

    # 4. 获取并保存 ETF 市值数据（每日更新）
    etf_valuation_records = await asyncio.to_thread(fetch_etf_valuation_batch)
    valuation_count = 0
    if etf_valuation_records:
        valuation_count = await batch_insert_valuation(session, etf_valuation_records)
        await session.commit()
        logger.info(f"ETF valuation synced: {valuation_count} records")

    return {
        "status": result.get("status", "success"),
        "message": f"ETF数据同步完成",
        "trading_date": str(latest_trading_day),
        "market_daily_count": result.get("records", 0),
        "total_etfs": result.get("records", 0),
        "valuation_count": valuation_count,
    }


# =============================================================================
# 历史数据补全（用于缺失多个交易日的情况）
# =============================================================================

def fetch_stock_history_for_dates(start_date: date, end_date: date) -> pd.DataFrame:
    """
    获取所有股票在指定日期范围内的历史数据

    使用 akshare 的 stock_zh_a_hist 接口，按股票遍历获取。
    这比实时 API 慢，但可以获取历史数据。

    Args:
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        包含所有股票历史数据的 DataFrame
    """
    import akshare as ak

    logger.info(f"Fetching stock history from {start_date} to {end_date}...")

    # 首先获取股票列表
    stock_list = ak.stock_zh_a_spot_em()[['代码', '名称']]
    logger.info(f"Found {len(stock_list)} stocks to fetch history for")

    all_data = []
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    for idx, row in stock_list.iterrows():
        code = row['代码']
        name = row['名称']

        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period='daily',
                start_date=start_str,
                end_date=end_str,
                adjust=''
            )
            if not df.empty:
                df['代码'] = code
                df['名称'] = name
                all_data.append(df)

        except Exception as e:
            logger.debug(f"Failed to fetch history for {code}: {e}")

        # 进度日志
        if (idx + 1) % 500 == 0:
            logger.info(f"Fetched history for {idx + 1}/{len(stock_list)} stocks...")

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total historical records fetched: {len(result)}")
        return result
    else:
        return pd.DataFrame()


def transform_history_data(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    转换历史数据为数据库格式

    Args:
        df: akshare stock_zh_a_hist 返回的 DataFrame

    Returns:
        Tuple of (market_daily_records, indicator_valuation_records)
    """
    market_daily_records = []
    valuation_records = []

    for _, row in df.iterrows():
        code = convert_stock_code(row.get('代码', row.get('股票代码', '')))
        trading_date = pd.to_datetime(row['日期']).date()

        if not code:
            continue

        # market_daily 记录
        market_record = {
            'code': code,
            'date': trading_date,
            'open': safe_decimal(row.get('开盘')),
            'high': safe_decimal(row.get('最高')),
            'low': safe_decimal(row.get('最低')),
            'close': safe_decimal(row.get('收盘')),
            'preclose': None,  # 历史 API 可能没有昨收
            'volume': safe_int(row.get('成交量')),
            'amount': safe_decimal(row.get('成交额')),
            'turn': safe_decimal(row.get('换手率')),
            'pct_chg': safe_decimal(row.get('涨跌幅')),
            'trade_status': 1,
        }
        market_daily_records.append(market_record)

        # indicator_valuation 记录（历史数据可能没有 PE/PB）
        valuation_record = {
            'code': code,
            'date': trading_date,
            'pe_ttm': None,
            'pb_mrq': None,
            'ps_ttm': None,
            'pcf_ncf_ttm': None,
            'total_mv': None,
            'circ_mv': None,
            'is_st': 1 if 'ST' in str(row.get('名称', '')) else 0,
        }
        valuation_records.append(valuation_record)

    return market_daily_records, valuation_records


def _fetch_stock_history_sync(code: str, start_str: str, end_str: str, missing_days_set: set) -> Tuple[List[Dict], List[Dict]]:
    """
    同步下载单只股票的历史数据（供并行调用）

    Args:
        code: 股票代码
        start_str: 开始日期 YYYYMMDD
        end_str: 结束日期 YYYYMMDD
        missing_days_set: 需要的日期集合

    Returns:
        Tuple of (market_records, valuation_records)
    """
    import akshare as ak

    market_records = []
    valuation_records = []
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period='daily',
            start_date=start_str,
            end_date=end_str,
            adjust=''
        )

        if df is not None and not df.empty:
            db_code = convert_stock_code(code)
            for _, row in df.iterrows():
                row_date = pd.to_datetime(row['日期']).date()
                if row_date not in missing_days_set:
                    continue

                amount = safe_decimal(row.get('成交额'))
                turn = safe_decimal(row.get('换手率'))

                market_records.append({
                    'code': db_code,
                    'date': row_date,
                    'open': safe_decimal(row.get('开盘')),
                    'high': safe_decimal(row.get('最高')),
                    'low': safe_decimal(row.get('最低')),
                    'close': safe_decimal(row.get('收盘')),
                    'preclose': None,
                    # 成交量: akshare 历史API返回的是"手"，需要转为"股" (*100)
                    'volume': safe_int(row.get('成交量', 0)) * 100 if safe_int(row.get('成交量')) else None,
                    'amount': amount,
                    'turn': turn,
                    'pct_chg': safe_decimal(row.get('涨跌幅')),
                    'trade_status': 1,
                })

                # 计算 circ_mv: circ_mv = amount * 100 / turn (转为亿元)
                circ_mv = None
                if amount and turn and turn > 0:
                    circ_mv = Decimal(str((float(amount) * 100 / float(turn)) / 100000000))

                valuation_records.append({
                    'code': db_code,
                    'date': row_date,
                    'pe_ttm': None,       # 历史API不提供
                    'pb_mrq': None,       # 历史API不提供
                    'ps_ttm': None,       # 历史API不提供
                    'pcf_ncf_ttm': None,  # 历史API不提供
                    'total_mv': circ_mv,  # 使用 circ_mv 作为 total_mv 近似值
                    'circ_mv': circ_mv,
                    'is_st': 0,  # 历史数据无法判断 ST
                })
    except Exception:
        pass  # 静默处理失败

    return market_records, valuation_records


async def backfill_stock_history_with_progress(
    session: AsyncSession,
    missing_days: List[date],
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    并行下载历史数据，支持进度回调

    优化策略：
    1. 每只股票只调用一次 API，一次性下载所有缺失日期
    2. 使用线程池并行下载，提高速度 10 倍
    3. 同时保存 market_daily 和 indicator_valuation 数据

    Args:
        session: 数据库会话
        missing_days: 需要补全的交易日列表
        progress_callback: 进度回调函数 (message, progress_pct, detail) -> awaitable

    Returns:
        补全结果
    """
    import akshare as ak

    if not missing_days:
        return {"status": "skip", "message": "没有需要补全的日期", "records": 0}

    # 计算日期范围
    start_date = min(missing_days)
    end_date = max(missing_days)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    missing_days_set = set(missing_days)

    logger.info(f"Starting parallel backfill for {len(missing_days)} days: {start_date} to {end_date}")

    # 获取股票列表
    logger.info("Fetching stock list for backfill...")
    stock_list_df = ak.stock_zh_a_spot_em()
    codes = stock_list_df['代码'].tolist()
    total_stocks = len(codes)
    logger.info(f"Found {total_stocks} stocks to backfill ({len(missing_days)} days, {PARALLEL_WORKERS} workers)")

    total_records = 0
    total_valuation_records = 0
    all_market_records = []
    all_valuation_records = []
    completed_count = 0
    lock = asyncio.Lock()

    # 使用线程池并行下载
    loop = asyncio.get_event_loop()

    async def process_batch(batch_codes: List[str], batch_start_idx: int):
        """处理一批股票的下载"""
        nonlocal completed_count, all_market_records, all_valuation_records, total_records

        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            # 并行下载这批股票
            futures = [
                loop.run_in_executor(
                    executor,
                    _fetch_stock_history_sync,
                    code, start_str, end_str, missing_days_set
                )
                for code in batch_codes
            ]

            for i, future in enumerate(asyncio.as_completed(futures)):
                market_records, valuation_records = await future
                async with lock:
                    all_market_records.extend(market_records)
                    all_valuation_records.extend(valuation_records)
                    completed_count += 1

                    # 进度报告
                    if completed_count % PROGRESS_REPORT_INTERVAL == 0 or completed_count == total_stocks:
                        overall_pct = 10 + (completed_count / total_stocks) * 80

                        msg = f"下载中 [{completed_count}/{total_stocks}]"

                        if completed_count % 100 == 0 or completed_count == total_stocks:
                            logger.info(msg)

                        if progress_callback:
                            await progress_callback(msg, int(overall_pct), {
                                "action": "backfill_progress",
                                "stocks_done": completed_count,
                                "stocks_total": total_stocks,
                                "days_count": len(missing_days),
                                "date_range": f"{start_date} ~ {end_date}",
                            })

    # 分批处理（每批 500 只股票）
    batch_size = 500
    for batch_start in range(0, total_stocks, batch_size):
        batch_codes = codes[batch_start:batch_start + batch_size]
        await process_batch(batch_codes, batch_start)

        # 每批处理完后写入数据库
        if all_market_records:
            count = await batch_insert_market_daily(session, all_market_records)
            total_records += count
            all_market_records = []

        if all_valuation_records:
            val_count = await batch_insert_valuation(session, all_valuation_records)
            total_valuation_records += val_count
            all_valuation_records = []

        await session.commit()
        logger.info(f"Batch committed: {total_records} market, {total_valuation_records} valuation records")

    # 写入剩余记录
    if all_market_records:
        count = await batch_insert_market_daily(session, all_market_records)
        total_records += count

    if all_valuation_records:
        val_count = await batch_insert_valuation(session, all_valuation_records)
        total_valuation_records += val_count

    await session.commit()

    logger.info(f"Backfill complete: {total_records} market, {total_valuation_records} valuation records for {len(missing_days)} days")

    return {
        "status": "success",
        "message": f"补全了 {len(missing_days)} 个交易日的数据",
        "records": total_records,
        "valuation_records": total_valuation_records,
        "days_backfilled": len(missing_days),
    }


async def backfill_missing_stock_days(
    session: AsyncSession,
    missing_days: List[date],
) -> Dict[str, Any]:
    """
    补全缺失的股票交易日数据（无进度回调版本，兼容旧代码）

    Args:
        session: 数据库会话
        missing_days: 缺失的交易日列表

    Returns:
        补全结果
    """
    return await backfill_stock_history_with_progress(session, missing_days, None)


# =============================================================================
# 指数数据同步
# =============================================================================

def fetch_all_indices_batch() -> Tuple[pd.DataFrame, date]:
    """
    批量获取指数实时数据

    Returns:
        Tuple of (DataFrame, trading_date)
    """
    import akshare as ak

    logger.info("Fetching all index data using akshare batch API...")

    all_indices = []

    # 获取上证系列指数
    try:
        df_sh = ak.stock_zh_index_spot_em(symbol='上证系列指数')
        all_indices.append(df_sh)
        logger.info(f"Fetched {len(df_sh)} Shanghai indices")
    except Exception as e:
        logger.warning(f"Failed to fetch Shanghai indices: {e}")

    # 获取深证系列指数
    try:
        df_sz = ak.stock_zh_index_spot_em(symbol='深证系列指数')
        all_indices.append(df_sz)
        logger.info(f"Fetched {len(df_sz)} Shenzhen indices")
    except Exception as e:
        logger.warning(f"Failed to fetch Shenzhen indices: {e}")

    if all_indices:
        df = pd.concat(all_indices, ignore_index=True)
        logger.info(f"Total indices fetched: {len(df)}")
        return df, date.today()
    else:
        raise ValueError("Failed to fetch any index data")


def get_index_list() -> List[str]:
    """
    获取全部指数代码列表

    Returns:
        指数代码列表 (akshare格式，如 ['sh000001', 'sz399001', ...])
    """
    import akshare as ak

    logger.info("Fetching index list...")
    codes = []

    try:
        # 上证系列指数
        df_sh = ak.stock_zh_index_spot_em(symbol='上证系列指数')
        for code in df_sh['代码'].tolist():
            codes.append(f"sh{str(code).zfill(6)}")
        logger.info(f"Found {len(df_sh)} Shanghai indices")
    except Exception as e:
        logger.warning(f"Failed to get Shanghai index list: {e}")

    try:
        # 深证系列指数
        df_sz = ak.stock_zh_index_spot_em(symbol='深证系列指数')
        for code in df_sz['代码'].tolist():
            codes.append(f"sz{str(code).zfill(6)}")
        logger.info(f"Found {len(df_sz)} Shenzhen indices")
    except Exception as e:
        logger.warning(f"Failed to get Shenzhen index list: {e}")

    logger.info(f"Total indices: {len(codes)}")
    return codes


def _fetch_index_history_sync(index_code: str, start_str: str, end_str: str, missing_days_set: set) -> List[Dict]:
    """
    同步下载单个指数的历史数据（供并行调用）

    Args:
        index_code: 指数代码 (akshare格式，如 'sh000001')
        start_str: 开始日期 YYYYMMDD
        end_str: 结束日期 YYYYMMDD
        missing_days_set: 需要的日期集合

    Returns:
        市场数据记录列表
    """
    import akshare as ak

    records = []
    try:
        df = ak.stock_zh_index_daily_em(
            symbol=index_code,
            start_date=start_str,
            end_date=end_str
        )

        if df is not None and not df.empty:
            # 转换代码格式: sh000001 -> sh.000001
            db_code = f"{index_code[:2]}.{index_code[2:]}"

            for _, row in df.iterrows():
                row_date = pd.to_datetime(row['date']).date()
                if row_date not in missing_days_set:
                    continue

                records.append({
                    'code': db_code,
                    'date': row_date,
                    'open': safe_decimal(row.get('open')),
                    'high': safe_decimal(row.get('high')),
                    'low': safe_decimal(row.get('low')),
                    'close': safe_decimal(row.get('close')),
                    'preclose': None,
                    # akshare stock_zh_index_daily_em 返回的成交量单位是"手"，需要转为"股" (*100)
                    'volume': safe_int(row.get('volume')) * 100 if row.get('volume') else None,
                    'amount': safe_decimal(row.get('amount')),
                    'turn': None,
                    'pct_chg': None,
                    'trade_status': 1,
                })
    except Exception:
        pass  # 静默处理失败

    return records


def fetch_index_history(index_code: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    获取单个指数的历史数据

    Args:
        index_code: 指数代码，如 "sh000001"
        start_date: 起始日期
        end_date: 结束日期

    Returns:
        历史数据 DataFrame
    """
    import akshare as ak

    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    try:
        df = ak.stock_zh_index_daily_em(
            symbol=index_code,
            start_date=start_str,
            end_date=end_str
        )
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch history for index {index_code}: {e}")
        return pd.DataFrame()


def convert_index_code(code: str) -> str:
    """
    转换指数代码格式

    akshare 返回: 000001
    目标格式: sh.000001
    """
    code = str(code).zfill(6)

    # 上证指数以 000 开头，深证指数以 399 开头
    if code.startswith('000'):
        return f"sh.{code}"
    elif code.startswith('399'):
        return f"sz.{code}"
    else:
        # 其他情况根据代码特征判断
        return f"sh.{code}"


def transform_index_data(df: pd.DataFrame, trading_date: date) -> List[Dict]:
    """
    转换指数实时数据为数据库格式

    Args:
        df: akshare 返回的指数 DataFrame
        trading_date: 交易日期

    Returns:
        market_daily_records
    """
    market_daily_records = []

    for _, row in df.iterrows():
        code = convert_index_code(row.get('代码', ''))

        if not code:
            continue

        market_record = {
            'code': code,
            'date': trading_date,
            'open': safe_decimal(row.get('今开')),
            'high': safe_decimal(row.get('最高')),
            'low': safe_decimal(row.get('最低')),
            'close': safe_decimal(row.get('最新价')),
            'preclose': safe_decimal(row.get('昨收')),
            'volume': safe_int(row.get('成交量')),
            'amount': safe_decimal(row.get('成交额')),
            'turn': None,  # 指数没有换手率
            'pct_chg': safe_decimal(row.get('涨跌幅')),
            'trade_status': 1,
        }
        market_daily_records.append(market_record)

    return market_daily_records


def transform_index_history(df: pd.DataFrame, index_code: str) -> List[Dict]:
    """
    转换指数历史数据为数据库格式

    Args:
        df: akshare stock_zh_index_daily_em 返回的 DataFrame
        index_code: 原始指数代码（如 sh000001）

    Returns:
        market_daily_records
    """
    market_daily_records = []

    # 转换代码格式: sh000001 -> sh.000001
    db_code = f"{index_code[:2]}.{index_code[2:]}"

    for _, row in df.iterrows():
        trading_date = pd.to_datetime(row['date']).date()

        market_record = {
            'code': db_code,
            'date': trading_date,
            'open': safe_decimal(row.get('open')),
            'high': safe_decimal(row.get('high')),
            'low': safe_decimal(row.get('low')),
            'close': safe_decimal(row.get('close')),
            'preclose': None,
            'volume': safe_int(row.get('volume')),
            'amount': safe_decimal(row.get('amount')),
            'turn': None,
            'pct_chg': None,
            'trade_status': 1,
        }
        market_daily_records.append(market_record)

    return market_daily_records


async def get_pg_index_max_date(session: AsyncSession) -> Optional[date]:
    """
    获取所有指数中最小的最大日期，确保所有指数都被同步到相同日期。

    使用 MIN(MAX(date)) 策略：
    - 对每个指数取其最大日期
    - 然后取所有指数中最小的那个日期
    - 这样可以确保所有指数都被同步，而不是只检查 sh.000001
    """
    query = text("""
        SELECT MIN(max_date) as min_max_date
        FROM (
            SELECT code, MAX(date) as max_date
            FROM market_daily
            WHERE code LIKE 'sh.0%' OR code LIKE 'sz.39%'
            GROUP BY code
            HAVING COUNT(*) > 10
        ) sub
    """)
    result = await session.execute(query)
    return result.scalar()


async def backfill_index_history(
    session: AsyncSession,
    missing_days: List[date],
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    并行下载指数历史数据

    Args:
        session: 数据库会话
        missing_days: 需要补全的交易日列表
        progress_callback: 进度回调函数

    Returns:
        补全结果
    """
    if not missing_days:
        return {"status": "skip", "message": "没有需要补全的日期", "records": 0}

    # 计算日期范围
    start_date = min(missing_days)
    end_date = max(missing_days)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    missing_days_set = set(missing_days)

    logger.info(f"Starting index backfill for {len(missing_days)} days: {start_date} to {end_date}")

    # 获取指数列表
    index_codes = get_index_list()
    total_indices = len(index_codes)
    logger.info(f"Found {total_indices} indices to backfill")

    total_records = 0
    all_records = []
    completed_count = 0
    lock = asyncio.Lock()

    loop = asyncio.get_event_loop()

    # 使用线程池并行下载
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = [
            loop.run_in_executor(
                executor,
                _fetch_index_history_sync,
                code, start_str, end_str, missing_days_set
            )
            for code in index_codes
        ]

        for future in asyncio.as_completed(futures):
            records = await future
            async with lock:
                all_records.extend(records)
                completed_count += 1

                # 每个指数都更新进度
                if completed_count % PROGRESS_REPORT_INTERVAL == 0 or completed_count == total_indices:
                    # 进度: 75-90% 区间给指数
                    overall_pct = 75 + (completed_count / total_indices) * 15
                    msg = f"指数下载中 [{completed_count}/{total_indices}]"

                    if completed_count % 50 == 0 or completed_count == total_indices:
                        logger.info(msg)

                    if progress_callback:
                        await progress_callback(msg, int(overall_pct), {
                            "action": "index_backfill_progress",
                            "indices_done": completed_count,
                            "indices_total": total_indices,
                            "days_count": len(missing_days),
                            "date_range": f"{start_date} ~ {end_date}",
                        })

    # 批量写入
    if all_records:
        count = await batch_insert_market_daily(session, all_records)
        total_records = count
        await session.commit()

    logger.info(f"Index backfill complete: {total_records} records for {len(missing_days)} days")

    return {
        "status": "success",
        "message": f"指数补全了 {len(missing_days)} 个交易日的数据",
        "records": total_records,
        "days_backfilled": len(missing_days),
    }


async def calculate_index_pct_chg(session: AsyncSession) -> int:
    """
    计算指数的 pct_chg (涨跌幅)

    由于历史 API (stock_zh_index_daily_em) 不返回 pct_chg，
    我们从连续交易日的 close 价格计算。

    使用窗口函数 LAG() 代替子查询，大幅提升性能。

    Returns:
        更新的记录数
    """
    # 设置 TimescaleDB 元组解压限制为无限，避免大批量更新时出错
    await session.execute(text("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0"))

    # 使用 CTE + 窗口函数 LAG() 获取前一天收盘价，比子查询快 100 倍
    sql = text("""
        WITH prev_close AS (
            SELECT
                code,
                date,
                close,
                LAG(close) OVER (PARTITION BY code ORDER BY date) as prev_close
            FROM market_daily
            WHERE (code LIKE 'sh.0%' OR code LIKE 'sz.39%')
        )
        UPDATE market_daily m
        SET pct_chg = ROUND((pc.close - pc.prev_close) / pc.prev_close * 100, 2)
        FROM prev_close pc
        WHERE m.code = pc.code
          AND m.date = pc.date
          AND m.pct_chg IS NULL
          AND pc.prev_close IS NOT NULL
          AND pc.prev_close > 0
    """)
    result = await session.execute(sql)
    updated = result.rowcount
    logger.info(f"Calculated pct_chg for {updated} index records")
    return updated


async def sync_indices_batch(
    session: AsyncSession,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    同步指数数据 - 使用历史 API（更稳定）

    Args:
        session: 数据库会话
        progress_callback: 进度回调函数

    Returns:
        同步结果
    """
    # 1. 检查是否需要更新
    pg_max_date = await get_pg_index_max_date(session)
    latest_trading_day = get_latest_trading_day()

    logger.info(f"Index sync check: PG max date = {pg_max_date}, latest trading day = {latest_trading_day}")

    if pg_max_date and pg_max_date >= latest_trading_day:
        return {
            "status": "skip",
            "message": f"指数数据已是最新 (PG: {pg_max_date}, 最近交易日: {latest_trading_day})",
            "records": 0,
        }

    # 2. 计算缺失的交易日
    missing_days = get_trading_days_between(pg_max_date, latest_trading_day) if pg_max_date else [latest_trading_day]

    # 3. 使用历史 API 并行下载所有指数
    result = await backfill_index_history(session, missing_days, progress_callback)

    # 4. 计算指数的 pct_chg (历史 API 不返回此字段)
    pct_chg_updated = await calculate_index_pct_chg(session)
    await session.commit()

    return {
        "status": result.get("status", "success"),
        "message": f"指数数据同步完成",
        "trading_date": str(latest_trading_day),
        "market_daily_count": result.get("records", 0),
        "total_indices": result.get("records", 0),
        "pct_chg_calculated": pct_chg_updated,
    }


async def _old_sync_indices_batch(session: AsyncSession) -> Dict[str, Any]:
    """
    旧版同步指数数据（保留备用）
    """
    pg_max_date = await get_pg_index_max_date(session)
    latest_trading_day = get_latest_trading_day()

    if pg_max_date and pg_max_date >= latest_trading_day:
        return {
            "status": "skip",
            "message": f"指数数据已是最新",
            "records": 0,
        }

    missing_days = []
    if pg_max_date:
        missing_days = get_trading_days_between(pg_max_date, latest_trading_day)

    total_records = 0

    if len(missing_days) > 1:
        main_indices = ['sh000001', 'sh000300', 'sz399001', 'sz399006']

        for index_code in main_indices:
            start_date = min(missing_days)
            end_date = max(missing_days)

            df = fetch_index_history(index_code, start_date, end_date)
            if not df.empty:
                records = transform_index_history(df, index_code)
                count = await batch_insert_market_daily(session, records)
                total_records += count
                logger.info(f"Backfilled {count} records for index {index_code}")

    # 4. 获取实时指数数据（当天）
    df, trading_date = fetch_all_indices_batch()
    trading_date = latest_trading_day

    # 5. 转换并插入
    market_records = transform_index_data(df, trading_date)
    market_count = await batch_insert_market_daily(session, market_records)
    total_records += market_count

    await session.commit()

    return {
        "status": "success",
        "message": f"指数数据同步完成",
        "trading_date": str(trading_date),
        "market_daily_count": total_records,
        "total_indices": len(df),
        "days_backfilled": len(missing_days) if missing_days else 0,
    }


# =============================================================================
# 复权因子同步
# =============================================================================

async def batch_insert_adjust_factors(
    session: AsyncSession,
    records: List[Dict],
) -> int:
    """
    批量插入复权因子数据 (upsert)

    Args:
        session: 数据库会话
        records: 复权因子记录列表

    Returns:
        插入/更新的记录数
    """
    if not records:
        return 0

    from app.db.models.asset import AdjustFactor
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(AdjustFactor).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=['code', 'divid_operate_date'],
        set_={
            'fore_adjust_factor': stmt.excluded.fore_adjust_factor,
            'back_adjust_factor': stmt.excluded.back_adjust_factor,
            'adjust_factor': stmt.excluded.adjust_factor,
        }
    )
    await session.execute(stmt)
    return len(records)


def _fetch_adjust_factor_batch(codes: List[str], start_date: str) -> List[Dict]:
    """
    批量获取复权因子（使用单个 baostock 会话）

    baostock 不是线程安全的，所以使用单线程顺序处理。

    Args:
        codes: 股票代码列表 (sh.xxxxxx 或 sz.xxxxxx 格式)
        start_date: 开始日期 YYYY-MM-DD

    Returns:
        复权因子记录列表
    """
    import baostock as bs

    all_records = []
    try:
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"baostock login failed: {lg.error_msg}")
            return all_records

        for code in codes:
            try:
                rs = bs.query_adjust_factor(code=code, start_date=start_date)

                while (rs.error_code == '0') and rs.next():
                    row = rs.get_row_data()
                    # row: [code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor]
                    if len(row) >= 5:
                        all_records.append({
                            'code': row[0],
                            'divid_operate_date': date.fromisoformat(row[1]),
                            'fore_adjust_factor': Decimal(row[2]) if row[2] else None,
                            'back_adjust_factor': Decimal(row[3]) if row[3] else None,
                            'adjust_factor': Decimal(row[4]) if row[4] else None,
                        })
            except Exception as e:
                logger.debug(f"Failed to fetch adjust factor for {code}: {e}")

        bs.logout()
    except Exception as e:
        logger.error(f"baostock session error: {e}")

    return all_records


async def sync_adjust_factors(
    session: AsyncSession,
    progress_callback: Callable[[str, int, Dict], Any] = None,
) -> Dict[str, Any]:
    """
    同步复权因子数据

    使用 baostock query_adjust_factor 接口获取复权因子，
    用于计算复权价格进行回测。

    Args:
        session: 数据库会话
        progress_callback: 进度回调函数

    Returns:
        同步结果
    """
    from app.db.models.asset import AssetMeta, AssetType

    # 获取所有股票代码
    stocks_query = select(AssetMeta.code).where(
        AssetMeta.asset_type == AssetType.STOCK,
        AssetMeta.status == 1
    )
    stocks_result = await session.execute(stocks_query)
    stock_codes = [row[0] for row in stocks_result]

    if not stock_codes:
        return {"status": "skip", "message": "没有股票需要同步", "records": 0}

    total_stocks = len(stock_codes)
    logger.info(f"Starting adjust factor sync for {total_stocks} stocks")

    if progress_callback:
        await progress_callback("开始同步复权因子...", 0, {
            "action": "adjust_factor_start",
            "total_stocks": total_stocks,
        })

    # 获取数据库中已有的最新复权日期
    max_date_query = text("""
        SELECT MAX(divid_operate_date) FROM adjust_factor
    """)
    result = await session.execute(max_date_query)
    max_date = result.scalar()

    # 如果有历史数据，从最新日期开始；否则从 2020-01-01 开始
    start_date = (max_date - timedelta(days=30)).strftime('%Y-%m-%d') if max_date else '2020-01-01'
    logger.info(f"Fetching adjust factors from {start_date}")

    total_records = 0
    loop = asyncio.get_event_loop()

    # 分批处理 - 使用单线程顺序处理以避免 baostock 线程安全问题
    batch_size = 200  # 每批处理的股票数
    for batch_start in range(0, total_stocks, batch_size):
        batch_codes = stock_codes[batch_start:batch_start + batch_size]
        batch_end = min(batch_start + batch_size, total_stocks)

        # 在线程池中执行同步代码
        records = await loop.run_in_executor(
            None,
            _fetch_adjust_factor_batch,
            batch_codes, start_date
        )

        # 写入数据库
        if records:
            count = await batch_insert_adjust_factors(session, records)
            total_records += count

        pct = int(batch_end / total_stocks * 100)
        logger.info(f"Adjust factor progress: {batch_end}/{total_stocks} ({len(records)} records)")

        if progress_callback:
            await progress_callback(
                f"复权因子 [{batch_end}/{total_stocks}]",
                pct,
                {"action": "adjust_factor_progress", "done": batch_end}
            )

    await session.commit()

    logger.info(f"Adjust factor sync complete: {total_records} records")

    return {
        "status": "success",
        "message": f"复权因子同步完成",
        "records": total_records,
        "stocks_processed": total_stocks,
    }
