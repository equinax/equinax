"""
Data Download Package

提供统一的数据下载接口，供 data_cli.py 调用。

Usage:
    from data.downloads import download_stocks, download_etfs

Scripts:
- download_a_stock_data.py      - A股日线K线数据 (BaoStock)
- download_etf_data.py          - ETF日线数据 (AKShare)
- download_index_constituents.py - 指数成分 (AKShare)
- download_industry_data.py     - 行业分类 (AKShare)

Note:
- 市值数据 (circ_mv) 改为在导入 PostgreSQL 时自动计算: circ_mv = amount / (turn / 100)
- 北向/机构持仓数据已取消 (API 只支持当日快照，不支持历史数据)
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


def download_indices(indices: Optional[List[str]] = None, force: bool = False) -> int:
    """
    下载指数成分股（当前日期快照）

    注意：指数成分数据只支持当前日期，不支持历史数据。
    AKShare API 只能获取实时的指数成分股，无法获取历史任意日期的成分股。
    --years 参数对此数据类型无效。

    Args:
        indices: 指数列表，如 ['000300', '000905']，默认下载全部主要指数
        force: 是否强制重新下载

    Returns:
        下载的记录数
    """
    from .download_index_constituents import download_all_indices, create_database

    print("[indices] 注意: 指数成分股只支持当前日期快照，--years 参数无效")

    db_path = CACHE_DIR / "index_constituents.db"
    conn = sqlite3.connect(str(db_path))
    create_database(conn)

    try:
        download_all_indices(conn, indices=indices, force=force)

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM index_constituents")
        return cursor.fetchone()[0]
    finally:
        conn.close()


def download_industries(force: bool = False) -> int:
    """
    下载行业分类数据

    Args:
        force: 是否强制重新下载（忽略缓存）

    Returns:
        下载的记录数
    """
    from .download_industry_data import download_em_industries, download_sw_industries, create_database

    db_path = CACHE_DIR / "industry_classification.db"
    # create_database 需要 Path，返回 conn
    conn = create_database(db_path)

    try:
        # 下载东方财富行业分类（传递 force 参数，启用缓存检测）
        download_em_industries(conn, force=force)
        # 下载申万行业分类（传递 force 参数，启用缓存检测）
        download_sw_industries(conn, force=force)

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
    'download_indices',
    'download_industries',
]
