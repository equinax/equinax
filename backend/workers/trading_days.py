"""
交易日工具模块

提供交易日检测和查询功能。使用 baostock 作为主要数据源，
并带有 Redis 缓存以避免频繁查询。

注意：不再使用 akshare 作为 fallback，改用规则推断。
"""

import json
import logging
import os
import redis
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def _fetch_latest_trading_day_from_baostock() -> date:
    """
    从 baostock 获取最近的交易日（内部函数，不带缓存）

    如果当前时间 < 17:00，不考虑今天（盘中数据不完整）
    """
    import baostock as bs

    # 使用中国时区
    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)
    today = now.date()

    # 如果当前时间 < 17:00（中国时间），不考虑今天（盘中数据不完整）
    if now.hour < 17:
        query_end_date = today - timedelta(days=1)
    else:
        query_end_date = today

    # 登录 baostock
    lg = bs.login()
    if lg.error_code != "0":
        logger.warning(f"baostock login failed: {lg.error_msg}, using {query_end_date} as fallback")
        return query_end_date

    try:
        # 查询最近30天的交易日历
        start_date = (query_end_date - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = query_end_date.strftime("%Y-%m-%d")

        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)

        trading_days = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == "1":  # is_trading_day
                trading_days.append(row[0])

        if trading_days:
            return date.fromisoformat(trading_days[-1])
        else:
            logger.warning(f"No trading days found in last 30 days, using {query_end_date}")
            return query_end_date

    finally:
        bs.logout()


def _get_latest_trading_day_with_fallback() -> Tuple[date, str]:
    """
    获取最近交易日，带 fallback

    Returns:
        (交易日, 数据来源)
        来源可能是: "baostock", "rule-based"
    """
    # 使用中国时区
    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)
    today = now.date()

    # 1. 先尝试 baostock
    try:
        baostock_result = _fetch_latest_trading_day_from_baostock()

        # 验证: 如果 baostock 返回的日期在合理范围内（7天内），直接使用
        if baostock_result and (today - baostock_result).days <= 7:
            return baostock_result, "baostock"

        # baostock 数据可能过期（例如年初没有新年数据）
        logger.warning(f"baostock returned stale date: {baostock_result}, trying fallback...")
    except Exception as e:
        logger.warning(f"baostock failed: {e}, using rule-based fallback")

    # 2. Fallback: 规则推断（排除周末）
    candidate = today if now.hour >= 17 else today - timedelta(days=1)
    for _ in range(10):  # 最多回溯10天
        if candidate.weekday() < 5:  # 非周末
            logger.info(f"Using rule-based trading day: {candidate}")
            return candidate, "rule-based"
        candidate -= timedelta(days=1)

    # 最后保底返回今天
    logger.warning(f"All fallbacks exhausted, using {today}")
    return today, "rule-based"


def get_latest_trading_day_with_source() -> Tuple[date, str]:
    """
    获取最近的交易日及其数据来源

    使用 Redis 缓存，每天只查询一次。
    缓存 key 区分盘中(before17)/盘后(after17)。

    Returns:
        (交易日, 数据来源)
        来源可能是: "baostock", "rule-based"
    """
    # 使用中国时区（Asia/Shanghai）而不是系统时区
    china_tz = ZoneInfo("Asia/Shanghai")
    now_china = datetime.now(china_tz)
    today = now_china.date()

    # 缓存 key 区分盘中/盘后 (基于中国时间)
    time_segment = "after17" if now_china.hour >= 17 else "before17"
    cache_key = f"latest_trading_day_v3:{today.isoformat()}:{time_segment}"

    # 获取 Redis 连接
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    try:
        r = redis.from_url(redis_url, decode_responses=True)

        # 尝试从缓存获取
        cached = r.get(cache_key)
        if cached:
            data = json.loads(cached)
            logger.debug(
                f"Using cached latest trading day: {data['date']} (source: {data['source']})"
            )
            return date.fromisoformat(data["date"]), data["source"]

        # 缓存未命中，使用 fallback 机制获取
        latest_trading_day, source = _get_latest_trading_day_with_fallback()

        # 计算到今天结束的秒数（基于中国时间）
        end_of_day = datetime(
            now_china.year, now_china.month, now_china.day, 23, 59, 59, tzinfo=china_tz
        )
        ttl_seconds = int((end_of_day - now_china).total_seconds()) + 1

        # 缓存结果（包含日期和来源）
        cache_data = json.dumps({"date": latest_trading_day.isoformat(), "source": source})
        r.setex(cache_key, ttl_seconds, cache_data)
        logger.info(
            f"Cached latest trading day: {latest_trading_day} (source: {source}, TTL: {ttl_seconds}s)"
        )

        return latest_trading_day, source

    except redis.RedisError as e:
        logger.warning(f"Redis error, falling back to direct detection: {e}")
        return _get_latest_trading_day_with_fallback()


def get_latest_trading_day() -> date:
    """
    获取最近的交易日（今天或之前）

    使用 Redis 缓存，每天只查询一次。
    缓存 key 区分盘中(before17)/盘后(after17)。

    注意：如果需要知道数据来源，请使用 get_latest_trading_day_with_source()
    """
    trading_day, _ = get_latest_trading_day_with_source()
    return trading_day


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
    if lg.error_code != "0":
        logger.warning(f"baostock login failed: {lg.error_msg}")
        return []

    try:
        # 查询从 start_date+1 到 end_date 的交易日
        query_start = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
        query_end = end_date.strftime("%Y-%m-%d")

        rs = bs.query_trade_dates(start_date=query_start, end_date=query_end)

        trading_days = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == "1":  # is_trading_day
                trading_days.append(date.fromisoformat(row[0]))

        return trading_days

    finally:
        bs.logout()


def is_trading_day(check_date: date) -> bool:
    """
    检查指定日期是否为交易日

    Args:
        check_date: 要检查的日期

    Returns:
        是否为交易日
    """
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        logger.warning(f"baostock login failed: {lg.error_msg}")
        # 保守估计：工作日返回 True
        return check_date.weekday() < 5

    try:
        date_str = check_date.strftime("%Y-%m-%d")
        rs = bs.query_trade_dates(start_date=date_str, end_date=date_str)

        if rs.next():
            row = rs.get_row_data()
            return row[1] == "1"

        return False

    finally:
        bs.logout()
