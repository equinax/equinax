#!/usr/bin/env python3
"""
A股日线数据下载脚本 (多年版)
使用 baostock 下载全部证券的日线数据和复权因子，存入 SQLite 数据库
支持 2020-2025 年，每年一个数据库文件
支持增量下载、断点续传、失败重试
"""

import sqlite3
import baostock as bs
from tqdm import tqdm
from datetime import datetime, date
import time
import argparse
import json
import os

# 限流配置
REQUEST_DELAY = 0.05  # 每次请求后等待时间（秒）
BATCH_DELAY = 1.0     # 每批次后额外等待时间

# 日线数据字段
DAILY_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"

# 缓存目录
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")


def get_db_path(year: int) -> str:
    """获取指定年份的数据库路径（cache 目录）"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"a_stock_{year}.db")


def get_failed_log_path(year: int) -> str:
    """获取指定年份的失败记录文件路径（cache 目录）"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"failed_stocks_{year}.json")


def get_date_range(year: int) -> tuple:
    """获取指定年份的日期范围"""
    start_date = f"{year}-01-01"
    if year == 2025:
        # 2025年使用当前日期
        end_date = date.today().strftime("%Y-%m-%d")
    else:
        end_date = f"{year}-12-31"
    return start_date, end_date


def get_stock_list_date(year: int) -> str:
    """获取用于查询证券列表的日期

    baostock 的 query_all_stock 对某些日期可能没有数据（比如节假日）
    这里使用一个工作日作为查询日期
    """
    # 使用年末最后一个交易周的周五作为查询日期
    stock_list_dates = {
        2019: "2019-12-31",
        2020: "2020-12-31",
        2021: "2021-12-31",
        2022: "2022-12-30",  # 12-31 是周六
        2023: "2023-12-29",  # 12-31 是周日
        2024: "2024-12-31",
        2025: "2025-12-05",  # 最近的交易日
    }
    return stock_list_dates.get(year, f"{year}-12-31")


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # 证券基本信息表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY,
            code_name TEXT,
            ipo_date TEXT,
            out_date TEXT,
            type INTEGER,
            status INTEGER
        )
    """)

    # 日线交易数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_k_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            preclose REAL,
            volume REAL,
            amount REAL,
            turn REAL,
            tradestatus INTEGER,
            pctChg REAL,
            peTTM REAL,
            pbMRQ REAL,
            psTTM REAL,
            pcfNcfTTM REAL,
            isST INTEGER,
            UNIQUE(code, date)
        )
    """)

    # 复权因子表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS adjust_factor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            dividOperateDate TEXT,
            foreAdjustFactor REAL,
            backAdjustFactor REAL,
            adjustFactor REAL,
            UNIQUE(code, dividOperateDate)
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_k_data(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_code ON daily_k_data(code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_adjust_code ON adjust_factor(code)")

    conn.commit()
    print("数据库表结构创建完成")


def get_all_stocks(end_date: str) -> list:
    """获取所有证券列表"""
    print(f"正在获取 {end_date} 的证券列表...")
    rs = bs.query_all_stock(day=end_date)

    if rs.error_code != '0':
        print(f"获取证券列表失败: {rs.error_msg}")
        return []

    stocks = []
    while rs.next():
        row = rs.get_row_data()
        stocks.append({
            'code': row[0],
            'tradeStatus': row[1],
            'code_name': row[2]
        })

    print(f"共获取 {len(stocks)} 只证券")
    return stocks


def get_stock_basic(code: str) -> dict:
    """获取单只证券的基本信息"""
    rs = bs.query_stock_basic(code=code)
    if rs.error_code != '0' or not rs.next():
        return None

    row = rs.get_row_data()
    return {
        'code': row[0] if len(row) > 0 else None,
        'code_name': row[1] if len(row) > 1 else None,
        'ipo_date': row[2] if len(row) > 2 else None,
        'out_date': row[3] if len(row) > 3 else None,
        'type': int(row[4]) if len(row) > 4 and row[4] else None,
        'status': int(row[5]) if len(row) > 5 and row[5] else None
    }


def get_downloaded_basic_codes(conn: sqlite3.Connection) -> set:
    """获取已下载基本信息的证券代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stock_basic")
    return {row[0] for row in cursor.fetchall()}


def get_downloaded_daily_codes(conn: sqlite3.Connection) -> set:
    """获取已有日线数据的证券代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM daily_k_data")
    return {row[0] for row in cursor.fetchall()}


def get_outdated_daily_codes(conn: sqlite3.Connection, target_date: str) -> set:
    """获取日线数据未更新到目标日期的证券代码

    Args:
        conn: 数据库连接
        target_date: 目标日期 (YYYY-MM-DD)

    Returns:
        需要更新的证券代码集合
    """
    cursor = conn.cursor()
    # 获取每只股票的最新日期，筛选出落后于目标日期的
    cursor.execute("""
        SELECT code, MAX(date) as max_date
        FROM daily_k_data
        GROUP BY code
        HAVING max_date < ?
    """, (target_date,))
    return {row[0] for row in cursor.fetchall()}


def get_downloaded_adjust_codes(conn: sqlite3.Connection) -> set:
    """获取已有复权因子的证券代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM adjust_factor")
    return {row[0] for row in cursor.fetchall()}


def load_failed_stocks(year: int) -> dict:
    """加载失败记录"""
    failed_log = get_failed_log_path(year)
    if os.path.exists(failed_log):
        with open(failed_log, 'r') as f:
            return json.load(f)
    return {"basic": [], "daily": [], "adjust": []}


def save_failed_stocks(year: int, failed: dict):
    """保存失败记录"""
    failed_log = get_failed_log_path(year)
    with open(failed_log, 'w') as f:
        json.dump(failed, f, indent=2)


def download_stock_basic(conn: sqlite3.Connection, stocks: list, skip_existing: bool = True):
    """下载并保存证券基本信息（支持增量）"""
    cursor = conn.cursor()

    # 获取已下载的代码
    existing_codes = get_downloaded_basic_codes(conn) if skip_existing else set()

    # 过滤需要下载的
    to_download = [s for s in stocks if s['code'] not in existing_codes]

    if not to_download:
        print(f"\n证券基本信息已全部下载完成 ({len(existing_codes)} 条)")
        return []

    print(f"\n正在下载证券基本信息... (已有 {len(existing_codes)}, 待下载 {len(to_download)})")

    success_count = 0
    failed_stocks = []

    for i, stock in enumerate(tqdm(to_download, desc="下载基本信息")):
        info = get_stock_basic(stock['code'])
        if info and info['code']:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_basic
                    (code, code_name, ipo_date, out_date, type, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (info['code'], info['code_name'], info['ipo_date'],
                      info['out_date'], info['type'], info['status']))
                success_count += 1
            except Exception as e:
                failed_stocks.append(stock['code'])
        else:
            failed_stocks.append(stock['code'])

        # 限流
        time.sleep(REQUEST_DELAY)

        # 每100条提交一次并额外等待
        if (i + 1) % 100 == 0:
            conn.commit()
            time.sleep(BATCH_DELAY)

    conn.commit()
    print(f"证券基本信息下载完成: 本次 {success_count} 条, 累计 {len(existing_codes) + success_count} 条")
    if failed_stocks:
        print(f"失败: {len(failed_stocks)} 只")

    return failed_stocks


def safe_float(value):
    """安全转换为浮点数"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_int(value):
    """安全转换为整数"""
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def download_daily_data(conn: sqlite3.Connection, stocks: list, start_date: str, end_date: str, skip_existing: bool = True):
    """下载日线数据（支持增量）"""
    cursor = conn.cursor()

    if skip_existing:
        # 获取已下载的代码
        existing_codes = get_downloaded_daily_codes(conn)
        # 获取数据落后的代码（需要更新）
        outdated_codes = get_outdated_daily_codes(conn, end_date)

        # 需要下载的：1) 完全没有的  2) 数据落后的
        new_codes = {s['code'] for s in stocks} - existing_codes
        to_download = [s for s in stocks if s['code'] in new_codes or s['code'] in outdated_codes]

        if not to_download:
            print(f"\n日线数据已全部更新完成 ({len(existing_codes)} 只证券，数据已到 {end_date})")
            return []

        print(f"\n正在下载 {start_date} 至 {end_date} 日线数据...")
        print(f"已有 {len(existing_codes)} 只, 新增 {len(new_codes)} 只, 待更新 {len(outdated_codes)} 只")
    else:
        to_download = stocks
        print(f"\n正在下载 {start_date} 至 {end_date} 日线数据...")
        print(f"强制模式: 下载全部 {len(to_download)} 只")

    total_records = 0
    failed_stocks = []

    for i, stock in enumerate(tqdm(to_download, desc="下载日线数据")):
        code = stock['code']
        try:
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=DAILY_FIELDS,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"  # 不复权
            )

            if rs.error_code != '0':
                failed_stocks.append(code)
                time.sleep(REQUEST_DELAY)
                continue

            batch = []
            while rs.next():
                row = rs.get_row_data()
                batch.append((
                    row[0],                  # date
                    row[1],                  # code
                    safe_float(row[2]),      # open
                    safe_float(row[3]),      # high
                    safe_float(row[4]),      # low
                    safe_float(row[5]),      # close
                    safe_float(row[6]),      # preclose
                    safe_float(row[7]),      # volume
                    safe_float(row[8]),      # amount
                    safe_float(row[9]),      # turn
                    safe_int(row[10]),       # tradestatus
                    safe_float(row[11]),     # pctChg
                    safe_float(row[12]),     # peTTM
                    safe_float(row[13]),     # pbMRQ
                    safe_float(row[14]),     # psTTM
                    safe_float(row[15]),     # pcfNcfTTM
                    safe_int(row[16])        # isST
                ))

            if batch:
                cursor.executemany("""
                    INSERT OR IGNORE INTO daily_k_data
                    (date, code, open, high, low, close, preclose, volume, amount,
                     turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                total_records += len(batch)

            # 限流
            time.sleep(REQUEST_DELAY)

            # 每100只股票提交一次并额外等待
            if (i + 1) % 100 == 0:
                conn.commit()
                time.sleep(BATCH_DELAY)

        except Exception as e:
            failed_stocks.append(code)

    conn.commit()
    print(f"日线数据下载完成: 本次 {total_records} 条记录")
    if failed_stocks:
        print(f"失败股票: {len(failed_stocks)} 只")

    return failed_stocks


def download_adjust_factors(conn: sqlite3.Connection, stocks: list, start_date: str, end_date: str, skip_existing: bool = True):
    """下载复权因子（支持增量）"""
    cursor = conn.cursor()

    # 只下载股票的复权因子（sh.6/sz.0/sz.3 开头）
    stock_codes = [s['code'] for s in stocks
                   if s['code'].startswith(('sh.6', 'sz.0', 'sz.3'))]

    # 获取已下载的代码
    existing_codes = get_downloaded_adjust_codes(conn) if skip_existing else set()

    # 过滤需要下载的
    to_download = [c for c in stock_codes if c not in existing_codes]

    if not to_download:
        print(f"\n复权因子已全部下载完成 ({len(existing_codes)} 只股票)")
        return []

    print(f"\n正在下载 {start_date} 至 {end_date} 复权因子...")
    print(f"已有 {len(existing_codes)} 只, 待下载 {len(to_download)} 只")

    total_records = 0
    failed_stocks = []

    for i, code in enumerate(tqdm(to_download, desc="下载复权因子")):
        try:
            rs = bs.query_adjust_factor(
                code=code,
                start_date=start_date,
                end_date=end_date
            )

            if rs.error_code != '0':
                failed_stocks.append(code)
                time.sleep(REQUEST_DELAY)
                continue

            batch = []
            while rs.next():
                row = rs.get_row_data()
                if len(row) >= 5:
                    batch.append((
                        row[0],                  # code
                        row[1],                  # dividOperateDate
                        safe_float(row[2]),      # foreAdjustFactor
                        safe_float(row[3]),      # backAdjustFactor
                        safe_float(row[4])       # adjustFactor
                    ))

            if batch:
                cursor.executemany("""
                    INSERT OR IGNORE INTO adjust_factor
                    (code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor)
                    VALUES (?, ?, ?, ?, ?)
                """, batch)
                total_records += len(batch)

            # 限流
            time.sleep(REQUEST_DELAY)

            # 每100只股票提交一次并额外等待
            if (i + 1) % 100 == 0:
                conn.commit()
                time.sleep(BATCH_DELAY)

        except Exception as e:
            failed_stocks.append(code)

    conn.commit()
    print(f"复权因子下载完成: 本次 {total_records} 条记录")
    if failed_stocks:
        print(f"失败股票: {len(failed_stocks)} 只")

    return failed_stocks


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("数据库统计信息")
    print("=" * 50)

    cursor.execute("SELECT COUNT(*) FROM stock_basic")
    print(f"证券基本信息: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(*) FROM daily_k_data")
    print(f"日线数据: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM daily_k_data")
    print(f"有日线数据的证券: {cursor.fetchone()[0]} 只")

    cursor.execute("SELECT MIN(date), MAX(date) FROM daily_k_data")
    row = cursor.fetchone()
    if row[0]:
        print(f"日期范围: {row[0]} 至 {row[1]}")

    cursor.execute("SELECT COUNT(*) FROM adjust_factor")
    print(f"复权因子: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM adjust_factor")
    print(f"有复权因子的证券: {cursor.fetchone()[0]} 只")

    print("=" * 50)


def download_year(year: int, mode: str = 'all', force: bool = False, delay: float = 0.05):
    """下载指定年份的数据"""
    global REQUEST_DELAY
    REQUEST_DELAY = delay

    start_date, end_date = get_date_range(year)
    stock_list_date = get_stock_list_date(year)
    db_path = get_db_path(year)

    start_time = time.time()
    print("\n" + "=" * 60)
    print(f"A股 {year} 年日线数据下载")
    print(f"日期范围: {start_date} 至 {end_date}")
    print(f"证券列表查询日期: {stock_list_date}")
    print(f"数据库: {db_path}")
    print(f"模式: {mode}, 强制: {force}, 请求间隔: {REQUEST_DELAY}s")
    print("=" * 60)

    # 仅查看状态
    if mode == 'status':
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            print_statistics(conn)
            conn.close()
        else:
            print(f"数据库 {db_path} 不存在")
        return

    skip_existing = not force
    failed_record = load_failed_stocks(year)

    try:
        # 创建数据库连接
        conn = sqlite3.connect(db_path)

        # 创建表结构
        create_database(conn)

        # 获取证券列表 (使用专门的查询日期)
        stocks = get_all_stocks(stock_list_date)
        if not stocks:
            print("获取证券列表失败，退出")
            return

        # 根据模式下载
        if mode == 'retry':
            # 重试失败的股票
            print("\n=== 重试失败的股票 ===")
            if failed_record['basic']:
                print(f"重试基本信息: {len(failed_record['basic'])} 只")
                retry_stocks = [{'code': c} for c in failed_record['basic']]
                new_failed = download_stock_basic(conn, retry_stocks, skip_existing=False)
                failed_record['basic'] = new_failed

            if failed_record['daily']:
                print(f"重试日线数据: {len(failed_record['daily'])} 只")
                retry_stocks = [{'code': c} for c in failed_record['daily']]
                new_failed = download_daily_data(conn, retry_stocks, start_date, end_date, skip_existing=False)
                failed_record['daily'] = new_failed

            if failed_record['adjust']:
                print(f"重试复权因子: {len(failed_record['adjust'])} 只")
                retry_stocks = [{'code': c} for c in failed_record['adjust']]
                new_failed = download_adjust_factors(conn, retry_stocks, start_date, end_date, skip_existing=False)
                failed_record['adjust'] = new_failed

        elif mode in ['all', 'basic']:
            failed = download_stock_basic(conn, stocks, skip_existing)
            failed_record['basic'] = failed

        if mode in ['all', 'daily']:
            failed = download_daily_data(conn, stocks, start_date, end_date, skip_existing)
            failed_record['daily'] = failed

        if mode in ['all', 'adjust']:
            failed = download_adjust_factors(conn, stocks, start_date, end_date, skip_existing)
            failed_record['adjust'] = failed

        # 保存失败记录
        save_failed_stocks(year, failed_record)

        # 打印统计信息
        print_statistics(conn)

        conn.close()

    except Exception as e:
        print(f"下载出错: {e}")

    elapsed = time.time() - start_time
    print(f"\n{year}年数据下载耗时: {elapsed/60:.1f} 分钟")
    print(f"数据库已保存至: {db_path}")

    # 显示失败统计
    total_failed = len(failed_record['basic']) + len(failed_record['daily']) + len(failed_record['adjust'])
    if total_failed > 0:
        print(f"\n失败统计: 基本信息 {len(failed_record['basic'])}, 日线 {len(failed_record['daily'])}, 复权因子 {len(failed_record['adjust'])}")
        print(f"可运行 --year {year} --mode retry 重试失败的股票")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='A股日线数据下载工具（多年版）')
    parser.add_argument('--year', type=int, help='下载指定年份 (2020-2025)')
    parser.add_argument('--years', type=str, help='下载多个年份，逗号分隔 (如: 2020,2021,2022)')
    parser.add_argument('--recent', type=int, help='增量更新模式: 更新最近 N 天内有数据缺失的股票')
    parser.add_argument('--mode', choices=['all', 'basic', 'daily', 'adjust', 'retry', 'status'],
                        default='all', help='下载模式: all=全部, basic=基本信息, daily=日线, adjust=复权因子, retry=重试失败, status=查看状态')
    parser.add_argument('--force', action='store_true', help='强制重新下载（忽略已有数据）')
    parser.add_argument('--delay', type=float, default=0.05, help='请求间隔（秒），默认0.05')
    return parser.parse_args()


def download_recent(recent_days: int, delay: float = 0.05):
    """增量更新模式：只下载数据落后的股票

    Args:
        recent_days: 检查最近 N 天的数据
        delay: 请求间隔
    """
    global REQUEST_DELAY
    REQUEST_DELAY = delay

    year = date.today().year
    end_date = date.today().strftime("%Y-%m-%d")
    start_date = f"{year}-01-01"
    db_path = get_db_path(year)

    print("\n" + "=" * 60)
    print(f"A股增量更新模式")
    print(f"数据库: {db_path}")
    print("=" * 60)

    # 检查数据库是否存在
    if not os.path.exists(db_path):
        print(f"数据库 {db_path} 不存在，请先运行 --year {year} 下载完整数据")
        return

    conn = sqlite3.connect(db_path, timeout=30)  # 30秒超时防止锁定

    # 获取数据库中的最新日期
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM daily_k_data")
    db_max_date = cursor.fetchone()[0]

    if not db_max_date:
        print("数据库中没有日线数据")
        conn.close()
        return

    print(f"数据库最新日期: {db_max_date}")
    print(f"目标更新日期: {end_date}")

    # 获取需要更新的股票（数据落后于数据库最新日期的）
    try:
        outdated_codes = get_outdated_daily_codes(conn, db_max_date)
    except sqlite3.OperationalError as e:
        print(f"数据库访问错误: {e}")
        print("可能有其他进程正在使用数据库，请稍后重试")
        conn.close()
        return

    if not outdated_codes:
        print(f"\n所有股票数据已是最新 (最新日期: {db_max_date})")
        # 检查是否需要从数据源获取更新的数据
        print("正在检查数据源是否有更新...")
        conn.close()

        # 尝试下载一只股票看看有没有新数据
        lg = bs.login()
        if lg.error_code != '0':
            print(f"登录失败: {lg.error_msg}")
            return

        rs = bs.query_history_k_data_plus(
            code="sh.000001",
            fields="date",
            start_date=db_max_date,
            end_date=end_date,
            frequency="d"
        )
        new_dates = []
        while rs.next():
            d = rs.get_row_data()[0]
            if d > db_max_date:
                new_dates.append(d)
        bs.logout()

        if not new_dates:
            print(f"数据源也没有新数据 (最新: {db_max_date})")
            return
        else:
            print(f"数据源有新数据: {new_dates}")
            # 重新连接并下载所有股票的新数据
            conn = sqlite3.connect(db_path, timeout=30)
            # 所有股票都需要更新
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT code FROM daily_k_data")
            outdated_codes = {row[0] for row in cursor.fetchall()}

    print(f"\n发现 {len(outdated_codes)} 只股票数据需要更新")

    # 登录 baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        conn.close()
        return
    print(f"登录成功: {lg.error_msg}")

    try:
        cursor = conn.cursor()
        total_records = 0
        failed_stocks = []

        # 下载需要更新的股票
        for i, code in enumerate(tqdm(list(outdated_codes), desc="更新日线数据")):
            try:
                rs = bs.query_history_k_data_plus(
                    code=code,
                    fields=DAILY_FIELDS,
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3"
                )

                if rs.error_code != '0':
                    failed_stocks.append(code)
                    time.sleep(REQUEST_DELAY)
                    continue

                batch = []
                while rs.next():
                    row = rs.get_row_data()
                    batch.append((
                        row[0], row[1],
                        safe_float(row[2]), safe_float(row[3]),
                        safe_float(row[4]), safe_float(row[5]),
                        safe_float(row[6]), safe_float(row[7]),
                        safe_float(row[8]), safe_float(row[9]),
                        safe_int(row[10]), safe_float(row[11]),
                        safe_float(row[12]), safe_float(row[13]),
                        safe_float(row[14]), safe_float(row[15]),
                        safe_int(row[16])
                    ))

                if batch:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO daily_k_data
                        (date, code, open, high, low, close, preclose, volume, amount,
                         turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    total_records += len(batch)

                time.sleep(REQUEST_DELAY)

                if (i + 1) % 100 == 0:
                    conn.commit()
                    time.sleep(BATCH_DELAY)

            except Exception as e:
                failed_stocks.append(code)
                continue

        conn.commit()
        print(f"\n更新完成: 共更新 {total_records} 条记录")
        if failed_stocks:
            print(f"失败: {len(failed_stocks)} 只")

    finally:
        conn.close()
        bs.logout()
        print("已登出 baostock")


def main():
    args = parse_args()

    # 增量更新模式
    if args.recent is not None:
        download_recent(args.recent, args.delay)
        return

    # 确定要下载的年份
    years_to_download = []

    if args.years:
        # 解析逗号分隔的年份列表
        years_to_download = [int(y.strip()) for y in args.years.split(',')]
    elif args.year:
        years_to_download = [args.year]
    else:
        # 默认下载所有年份 (跳过2024，因为已下载)
        print("未指定年份，请使用 --year, --years 或 --recent 参数")
        print("示例:")
        print("  python download_a_stock_data.py --year 2023")
        print("  python download_a_stock_data.py --years 2020,2021,2022,2023,2025")
        print("  python download_a_stock_data.py --recent 1  # 增量更新")
        print("  python download_a_stock_data.py --mode status --years 2020,2021,2022,2023,2024,2025")
        return

    # 验证年份范围
    valid_years = []
    for year in years_to_download:
        if 2019 <= year <= 2025:
            valid_years.append(year)
        else:
            print(f"警告: 年份 {year} 不在有效范围 (2019-2025)，已跳过")

    if not valid_years:
        print("没有有效的年份")
        return

    # 如果只是查看状态，不需要登录
    if args.mode == 'status':
        for year in valid_years:
            download_year(year, args.mode, args.force, args.delay)
        return

    # 登录 baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return
    print(f"登录成功: {lg.error_msg}")

    try:
        # 依次下载每个年份
        for year in valid_years:
            download_year(year, args.mode, args.force, args.delay)
    finally:
        bs.logout()
        print("\n已登出 baostock")

    print("\n" + "=" * 60)
    print("全部下载完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
