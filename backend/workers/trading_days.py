"""
交易日工具模块

提供交易日检测和查询功能。使用 TuShare Pro (pro.trade_cal) 作为唯一数据源，
带 Redis 缓存避免重复网络调用，并带规则推断 fallback。

注意：项目原则是 TuShare 为唯一数据源。本模块原先使用 baostock + akshare 已全部移除。
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import redis as _redis

logger = logging.getLogger(__name__)


def _get_redis() -> Optional[_redis.Redis]:
    """获取 sync Redis 连接（失败返回 None，不抛异常）。"""
    try:
        return _redis.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        return None


def _tushare_trading_days(start_date: date, end_date: date) -> List[date]:
    """
    从 TuShare 拉取 [start_date, end_date]（含两端）之间的交易日。

    失败返回空列表，不抛异常 — 由调用方决定 fallback。
    """
    if start_date > end_date:
        return []

    try:
        # 延迟导入以避免在不需要时初始化 TuShare client
        from .data_sources import get_data_source

        source = get_data_source()
        return source.get_trading_days(start_date, end_date)
    except Exception as e:
        logger.warning(f"TuShare get_trading_days failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Latest trading day (cached daily)
# ---------------------------------------------------------------------------


def _fetch_latest_trading_day_from_tushare() -> date | None:
    """
    从 TuShare 获取最近的交易日（不带缓存）。

    如果当前时间 < 17:00（中国时间），不考虑今天（盘中数据不完整）。
    返回 None 表示 TuShare 不可用。
    """
    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)
    today = now.date()
    query_end = today if now.hour >= 17 else today - timedelta(days=1)
    query_start = query_end - timedelta(days=30)

    days = _tushare_trading_days(query_start, query_end)
    if not days:
        return None
    return max(days)


def _rule_based_latest_trading_day() -> date:
    """规则推断: 回溯到最近的工作日（不考虑节假日）。"""
    china_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(china_tz)
    today = now.date()
    candidate = today if now.hour >= 17 else today - timedelta(days=1)
    for _ in range(10):
        if candidate.weekday() < 5:
            return candidate
        candidate -= timedelta(days=1)
    return today


def _get_latest_trading_day_with_fallback() -> Tuple[date, str]:
    """
    获取最近交易日 + 数据来源标签。

    Returns:
        (date, source) — source ∈ {"tushare", "rule-based"}
    """
    china_tz = ZoneInfo("Asia/Shanghai")
    today = datetime.now(china_tz).date()

    try:
        ts_result = _fetch_latest_trading_day_from_tushare()
        # 合理性校验: TuShare 返回的日期必须在最近 7 天内
        if ts_result and (today - ts_result).days <= 7:
            return ts_result, "tushare"
        if ts_result:
            logger.warning(f"TuShare returned stale trading day: {ts_result}, using rule-based")
    except Exception as e:
        logger.warning(f"TuShare lookup failed: {e}, using rule-based")

    return _rule_based_latest_trading_day(), "rule-based"


def get_latest_trading_day_with_source() -> Tuple[date, str]:
    """
    获取最近的交易日及其数据来源（带 Redis 每日缓存）。

    缓存 key 按 (日期, 盘中/盘后) 分段。
    """
    china_tz = ZoneInfo("Asia/Shanghai")
    now_china = datetime.now(china_tz)
    today = now_china.date()
    time_segment = "after17" if now_china.hour >= 17 else "before17"
    cache_key = f"latest_trading_day_v4:{today.isoformat()}:{time_segment}"

    r = _get_redis()
    if r is not None:
        try:
            cached = r.get(cache_key)
            if cached:
                data = json.loads(cached)  # type: ignore[arg-type]
                return date.fromisoformat(data["date"]), data["source"]
        except Exception as e:
            logger.warning(f"Redis read failed: {e}")

    trading_day, source = _get_latest_trading_day_with_fallback()

    if r is not None:
        try:
            end_of_day = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=china_tz)
            ttl = int((end_of_day - now_china).total_seconds()) + 1
            r.setex(
                cache_key,
                ttl,
                json.dumps({"date": trading_day.isoformat(), "source": source}),
            )
        except Exception as e:
            logger.warning(f"Redis write failed: {e}")

    return trading_day, source


def get_latest_trading_day() -> date:
    """获取最近的交易日（today 或更早）。"""
    trading_day, _ = get_latest_trading_day_with_source()
    return trading_day


# ---------------------------------------------------------------------------
# Range queries
# ---------------------------------------------------------------------------


def get_trading_days_between(start_date: date, end_date: date) -> List[date]:
    """
    获取 (start_date, end_date] 之间的所有交易日（**不包含 start_date，包含 end_date**）。

    带 Redis 缓存（1 小时 TTL），按 (start, end) 唯一。
    TuShare 不可用时返回空列表。
    """
    if start_date >= end_date:
        return []

    cache_key = f"trading_days_between_v1:{start_date.isoformat()}:{end_date.isoformat()}"
    r = _get_redis()
    if r is not None:
        try:
            cached = r.get(cache_key)
            if cached:
                return [date.fromisoformat(d) for d in json.loads(cached)]  # type: ignore[arg-type]
        except Exception as e:
            logger.warning(f"Redis read failed: {e}")

    # 半开区间: TuShare 返回 [start+1, end] 闭区间
    query_start = start_date + timedelta(days=1)
    days = _tushare_trading_days(query_start, end_date)

    if r is not None and days:
        try:
            r.setex(cache_key, 3600, json.dumps([d.isoformat() for d in days]))
        except Exception as e:
            logger.warning(f"Redis write failed: {e}")

    return days


def is_trading_day(check_date: date) -> bool:
    """
    检查指定日期是否为交易日（TuShare 查询单天）。

    TuShare 不可用时退化为「非周末即交易日」的保守估计。
    """
    days = _tushare_trading_days(check_date, check_date)
    if days:
        return check_date in days
    # Fallback: 非周末视为交易日
    return check_date.weekday() < 5
