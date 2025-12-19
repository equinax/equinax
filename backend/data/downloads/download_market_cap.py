#!/usr/bin/env python3
"""
A股市值数据下载脚本
使用 AKShare 下载 A 股的总市值和流通市值数据，存入 SQLite 数据库
用于补全 baostock 日线数据中缺失的 total_mv 字段
"""

import sqlite3
import akshare as ak
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date, timedelta
import time
import argparse
import os

# 限流配置
REQUEST_DELAY = 0.3  # 每次请求后等待时间（秒）
BATCH_DELAY = 2.0    # 每批次后额外等待时间

# 数据库文件（存放在 cache 目录）
DB_FILE = "market_cap.db"
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")


def get_db_path() -> str:
    """获取数据库路径（cache 目录）"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, DB_FILE)


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # 市值数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_market_cap (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            total_mv REAL,           -- 总市值 (亿元)
            circ_mv REAL,            -- 流通市值 (亿元)
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, date)
        )
    """)

    # 下载日志表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS download_log (
            download_date TEXT PRIMARY KEY,
            stock_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_cap_date ON stock_market_cap(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_cap_code ON stock_market_cap(code)")

    conn.commit()
    print("数据库表结构创建完成")


def normalize_code(code: str) -> str:
    """
    标准化股票代码为 sh.XXXXXX 或 sz.XXXXXX 格式

    AKShare 返回的代码格式可能是:
    - 600000 (无前缀)
    - 000001 (无前缀)
    - 300001 (无前缀)
    - sh600000 / sz000001 (带前缀无点号)
    """
    code = str(code).strip()

    # 移除可能存在的前缀
    if code.startswith(('sh', 'sz', 'bj')):
        prefix = code[:2]
        code_num = code[2:]
        if code_num.startswith('.'):
            code_num = code_num[1:]
        return f"{prefix}.{code_num}"

    # 根据代码判断市场
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith(('0', '3')):
        return f"sz.{code}"
    elif code.startswith(('4', '8')):
        return f"bj.{code}"
    else:
        return f"sz.{code}"


def get_downloaded_dates(conn: sqlite3.Connection) -> set:
    """获取已下载的日期"""
    cursor = conn.cursor()
    cursor.execute("SELECT download_date FROM download_log")
    return {row[0] for row in cursor.fetchall()}


def get_a_stock_list() -> list:
    """获取 A 股股票列表"""
    print("正在获取 A 股股票列表...")
    try:
        # 使用 akshare 获取 A 股实时行情，从中提取股票代码
        df = ak.stock_zh_a_spot_em()
        codes = df['代码'].tolist()
        print(f"共获取 {len(codes)} 只 A 股")
        return codes
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []


def download_market_cap_by_stock(code: str) -> pd.DataFrame:
    """
    下载单只股票的市值数据
    使用 ak.stock_a_lg_indicator(symbol) 获取个股指标数据

    返回 DataFrame 包含: trade_date, total_mv, circ_mv
    """
    try:
        # 获取个股指标数据
        df = ak.stock_a_lg_indicator(symbol=code)

        if df is None or df.empty:
            return pd.DataFrame()

        # 提取需要的列
        # ak.stock_a_lg_indicator 返回的列通常包括:
        # trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv
        result = pd.DataFrame()

        if 'trade_date' in df.columns:
            result['date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        elif '日期' in df.columns:
            result['date'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        else:
            return pd.DataFrame()

        # 总市值（亿元）
        if 'total_mv' in df.columns:
            result['total_mv'] = df['total_mv'].astype(float) / 100000000  # 元 -> 亿元
        elif '总市值' in df.columns:
            result['total_mv'] = df['总市值'].astype(float) / 100000000
        else:
            result['total_mv'] = None

        # 流通市值（亿元）
        if 'circ_mv' in df.columns:
            result['circ_mv'] = df['circ_mv'].astype(float) / 100000000  # 元 -> 亿元
        elif '流通市值' in df.columns:
            result['circ_mv'] = df['流通市值'].astype(float) / 100000000
        else:
            result['circ_mv'] = None

        result['code'] = normalize_code(code)

        return result[['code', 'date', 'total_mv', 'circ_mv']]

    except Exception as e:
        # print(f"下载 {code} 市值数据失败: {e}")
        return pd.DataFrame()


def download_all_market_cap(conn: sqlite3.Connection, target_date: str = None, full_history: bool = False):
    """
    下载所有 A 股的市值数据

    Args:
        conn: 数据库连接
        target_date: 目标日期，默认为今天
        full_history: 是否下载完整历史数据
    """
    cursor = conn.cursor()

    # 获取股票列表
    stock_codes = get_a_stock_list()
    if not stock_codes:
        print("获取股票列表失败")
        return

    # 如果是下载单日，先检查是否已下载
    if not full_history and target_date:
        downloaded_dates = get_downloaded_dates(conn)
        if target_date in downloaded_dates:
            print(f"日期 {target_date} 的市值数据已下载")
            return

    print(f"\n正在下载 {len(stock_codes)} 只股票的市值数据...")
    if full_history:
        print("模式: 完整历史数据")
    else:
        print(f"模式: 单日数据 ({target_date or date.today().strftime('%Y-%m-%d')})")

    total_records = 0
    failed_stocks = []

    for i, code in enumerate(tqdm(stock_codes, desc="下载市值数据")):
        try:
            df = download_market_cap_by_stock(code)

            if df.empty:
                failed_stocks.append(code)
                time.sleep(REQUEST_DELAY)
                continue

            # 如果不是全量下载，只保留目标日期的数据
            if not full_history and target_date:
                df = df[df['date'] == target_date]

            if df.empty:
                time.sleep(REQUEST_DELAY)
                continue

            # 写入数据库
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO stock_market_cap
                    (code, date, total_mv, circ_mv)
                    VALUES (?, ?, ?, ?)
                """, (row['code'], row['date'], row['total_mv'], row['circ_mv']))
                total_records += 1

            # 限流
            time.sleep(REQUEST_DELAY)

            # 每 50 只股票提交一次
            if (i + 1) % 50 == 0:
                conn.commit()
                time.sleep(BATCH_DELAY)

        except Exception as e:
            failed_stocks.append(code)

    conn.commit()

    # 记录下载日志
    if target_date and not full_history:
        cursor.execute("""
            INSERT OR REPLACE INTO download_log (download_date, stock_count)
            VALUES (?, ?)
        """, (target_date, total_records))
        conn.commit()

    print(f"\n市值数据下载完成: {total_records} 条记录")
    if failed_stocks:
        print(f"失败股票: {len(failed_stocks)} 只")


def download_recent_days(conn: sqlite3.Connection, days: int = 30):
    """下载最近 N 天的市值数据"""
    print(f"\n下载最近 {days} 天的市值数据...")

    today = date.today()
    downloaded_dates = get_downloaded_dates(conn)

    dates_to_download = []
    for i in range(days):
        d = today - timedelta(days=i)
        d_str = d.strftime('%Y-%m-%d')
        if d_str not in downloaded_dates:
            dates_to_download.append(d_str)

    if not dates_to_download:
        print("所有日期都已下载")
        return

    print(f"待下载日期: {len(dates_to_download)} 天")

    for target_date in dates_to_download:
        print(f"\n下载 {target_date} 的市值数据...")
        download_all_market_cap(conn, target_date=target_date, full_history=False)


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("市值数据库统计信息")
    print("=" * 50)

    cursor.execute("SELECT COUNT(*) FROM stock_market_cap")
    print(f"市值记录总数: {cursor.fetchone()[0]:,} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM stock_market_cap")
    print(f"股票数量: {cursor.fetchone()[0]:,} 只")

    cursor.execute("SELECT COUNT(DISTINCT date) FROM stock_market_cap")
    print(f"日期数量: {cursor.fetchone()[0]:,} 天")

    cursor.execute("SELECT MIN(date), MAX(date) FROM stock_market_cap")
    row = cursor.fetchone()
    if row[0]:
        print(f"日期范围: {row[0]} 至 {row[1]}")

    cursor.execute("SELECT COUNT(*) FROM download_log")
    print(f"下载日志: {cursor.fetchone()[0]} 条")

    print("=" * 50)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='A股市值数据下载工具')
    parser.add_argument('--date', type=str, help='下载指定日期 (YYYY-MM-DD)')
    parser.add_argument('--recent', type=int, help='下载最近 N 天的数据')
    parser.add_argument('--full', action='store_true', help='下载完整历史数据')
    parser.add_argument('--status', action='store_true', help='查看数据库状态')
    return parser.parse_args()


def main():
    args = parse_args()

    db_path = get_db_path()
    print(f"数据库路径: {db_path}")

    conn = sqlite3.connect(db_path)
    create_database(conn)

    try:
        if args.status:
            print_statistics(conn)
        elif args.full:
            download_all_market_cap(conn, full_history=True)
            print_statistics(conn)
        elif args.recent:
            download_recent_days(conn, days=args.recent)
            print_statistics(conn)
        elif args.date:
            download_all_market_cap(conn, target_date=args.date)
            print_statistics(conn)
        else:
            # 默认下载今天的数据
            today = date.today().strftime('%Y-%m-%d')
            download_all_market_cap(conn, target_date=today)
            print_statistics(conn)
    finally:
        conn.close()

    print("\n完成!")


if __name__ == "__main__":
    main()
