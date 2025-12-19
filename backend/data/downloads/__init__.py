"""
Data Download Package

提供统一的数据下载接口，供 data_cli.py 调用。

Usage:
    from data.downloads import download_stocks, download_market_cap

Scripts:
- download_a_stock_data.py      - A股日线K线数据 (BaoStock)
- download_etf_data.py          - ETF日线数据 (AKShare)
- download_market_cap.py        - 市值数据 (AKShare)
- download_index_constituents.py - 指数成分 (AKShare)
- download_industry_data.py     - 行业分类 (AKShare)
- download_northbound_holdings.py - 北向持仓 (AKShare)
- download_institutional_holdings.py - 机构持仓 (AKShare)
"""

import sqlite3
from pathlib import Path
from typing import Optional, List
from datetime import date

# 缓存目录
CACHE_DIR = Path(__file__).parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 导出速率控制配置
from .base import RATE_LIMITS, RateLimiter, CheckpointManager, normalize_code


def download_stocks(years: Optional[List[int]] = None, mode: str = 'all', force: bool = False) -> int:
    """
    下载 A 股日线数据

    Args:
        years: 年份列表，如 [2024] 或 [2023, 2024]
        mode: 下载模式 - 'all', 'basic', 'daily', 'adjust'
        force: 是否强制重新下载

    Returns:
        下载的记录数
    """
    import baostock as bs
    from .download_a_stock_data import download_year, get_db_path

    if years is None:
        years = [date.today().year]

    # 检查本地缓存，跳过已有完整数据的年份
    if not force:
        years_to_download = []
        for year in years:
            db_path = Path(get_db_path(year))  # get_db_path 已返回完整路径
            if db_path.exists():
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(DISTINCT code) FROM daily_k_data")
                count = cursor.fetchone()[0]
                conn.close()
                if count >= 4000:  # 有足够多的股票数据，认为已完成
                    print(f"[stocks] {year} 年数据已存在 ({count} 只股票)，跳过")
                    continue
            years_to_download.append(year)
        years = years_to_download

    if not years:
        print("[stocks] 所有年份数据已存在，无需下载")
        return 0

    # 登录 BaoStock
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"BaoStock 登录失败: {lg.error_msg}")
    print(f"BaoStock 登录成功")

    try:
        total = 0
        for year in years:
            count = download_year(year=year, mode=mode, force=force)
            total += count if count else 0
        return total
    finally:
        bs.logout()
        print("BaoStock 已登出")


def download_etfs(years: Optional[List[int]] = None, mode: str = 'all', force: bool = False) -> int:
    """
    下载 ETF 日线数据

    Args:
        years: 年份列表
        mode: 下载模式 - 'all', 'basic', 'daily', 'adjust'
        force: 是否强制重新下载

    Returns:
        下载的记录数
    """
    from .download_etf_data import download_year, get_db_path

    if years is None:
        years = [date.today().year]

    # 检查本地缓存，跳过已有完整数据的年份
    if not force:
        years_to_download = []
        for year in years:
            db_path = Path(get_db_path(year))  # get_db_path 已返回完整路径
            if db_path.exists():
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(DISTINCT code) FROM daily_k_data")  # ETF 表名也是 daily_k_data
                count = cursor.fetchone()[0]
                conn.close()
                if count >= 500:  # 有足够多的 ETF 数据，认为已完成
                    print(f"[etfs] {year} 年数据已存在 ({count} 只 ETF)，跳过")
                    continue
            years_to_download.append(year)
        years = years_to_download

    if not years:
        print("[etfs] 所有年份数据已存在，无需下载")
        return 0

    total = 0
    for year in years:
        count = download_year(year=year, mode=mode, force=force)
        total += count if count else 0
    return total


def download_market_cap(recent: int = 30, full_history: bool = False) -> int:
    """
    下载市值数据

    Args:
        recent: 下载最近 N 天的数据
        full_history: 是否下载完整历史

    Returns:
        下载的记录数
    """
    from .download_market_cap import download_recent_days, download_all_market_cap, create_database, get_db_path

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    create_database(conn)

    try:
        if full_history:
            download_all_market_cap(conn, full_history=True)
        else:
            download_recent_days(conn, days=recent)

        # 返回记录数
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_market_cap")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def download_northbound(today_only: bool = True, start_date: str = None) -> int:
    """
    下载北向持仓数据

    Args:
        today_only: 只下载今天的数据
        start_date: 历史数据开始日期 (如果 today_only=False)

    Returns:
        下载的记录数
    """
    from .download_northbound_holdings import download_today, download_history, create_database

    db_path = CACHE_DIR / "northbound_holdings.db"
    conn = sqlite3.connect(str(db_path))
    create_database(conn)

    try:
        if today_only:
            download_today(conn)
        else:
            download_history(conn, start_date=start_date)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM northbound_holdings")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def download_institutional(quarters: Optional[List[str]] = None) -> int:
    """
    下载机构持仓数据

    Args:
        quarters: 季度列表，如 ['20240331', '20240630']，默认下载最新一期

    Returns:
        下载的记录数
    """
    from .download_institutional_holdings import download_latest, download_quarters, create_database

    db_path = CACHE_DIR / "institutional_holdings.db"
    conn = sqlite3.connect(str(db_path))
    create_database(conn)

    try:
        if quarters:
            download_quarters(conn, quarters=quarters)
        else:
            download_latest(conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM institutional_holdings")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def download_indices(indices: Optional[List[str]] = None) -> int:
    """
    下载指数成分股

    Args:
        indices: 指数列表，如 ['000300', '000905']，默认下载全部主要指数

    Returns:
        下载的记录数
    """
    from .download_index_constituents import download_all_indices, create_database

    db_path = CACHE_DIR / "index_constituents.db"
    conn = sqlite3.connect(str(db_path))
    create_database(conn)

    try:
        download_all_indices(conn, indices=indices)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM index_constituents")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def download_industries() -> int:
    """
    下载行业分类数据

    Returns:
        下载的记录数
    """
    from .download_industry_data import download_em_industries, download_sw_industries, create_database

    db_path = CACHE_DIR / "industry_classification.db"
    conn = sqlite3.connect(str(db_path))
    create_database(conn)

    try:
        # 下载东方财富行业分类
        download_em_industries(conn)
        # 下载申万行业分类
        download_sw_industries(conn)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM industry_classification")
        return cursor.fetchone()[0]
    finally:
        conn.close()


# 导出所有公共接口
__all__ = [
    # 目录配置
    'CACHE_DIR',
    'RATE_LIMITS',

    # 工具类
    'RateLimiter',
    'CheckpointManager',
    'normalize_code',

    # 下载函数
    'download_stocks',
    'download_etfs',
    'download_market_cap',
    'download_northbound',
    'download_institutional',
    'download_indices',
    'download_industries',
]
