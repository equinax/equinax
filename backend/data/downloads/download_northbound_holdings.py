#!/usr/bin/env python3
"""
北向资金持仓数据下载脚本
使用 AKShare 下载陆股通持仓数据，存入 SQLite 数据库
用于 4+1 分类系统的微观结构维度 (is_northbound_heavy)
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
REQUEST_DELAY = 0.5  # 每次请求后等待时间（秒）

# 数据库文件
DB_FILE = "northbound_holdings.db"


def get_db_path() -> str:
    """获取数据库路径"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # 北向持仓数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS northbound_holdings (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            name TEXT,
            holding_shares REAL,          -- 持股数量 (万股)
            holding_ratio REAL,           -- 持股比例 (%)
            holding_change REAL,          -- 持股变动 (万股)
            market_value REAL,            -- 持股市值 (亿)
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
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_northbound_date ON northbound_holdings(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_northbound_code ON northbound_holdings(code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_northbound_ratio ON northbound_holdings(holding_ratio)")

    conn.commit()
    print("数据库表结构创建完成")


def normalize_code(code: str) -> str:
    """
    标准化股票代码为 sh.XXXXXX 或 sz.XXXXXX 格式
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


def download_northbound_holdings_em() -> pd.DataFrame:
    """
    下载北向资金持仓数据
    使用 ak.stock_hsgt_hold_stock_em(market="北向") 获取当日持仓排名

    Returns:
        DataFrame with columns: code, name, holding_shares, holding_ratio, etc.
    """
    print("\n正在下载北向资金持仓数据...")

    try:
        # 获取北向持仓数据 (沪股通 + 深股通)
        df = ak.stock_hsgt_hold_stock_em(market="北向")

        if df is None or df.empty:
            print("  未获取到数据")
            return pd.DataFrame()

        print(f"  原始列名: {df.columns.tolist()}")
        print(f"  获取 {len(df)} 条持仓记录")

        # 构建结果DataFrame
        result = pd.DataFrame()

        # 解析代码 - 尝试不同的列名
        code_col = None
        for col in ['代码', '证券代码', 'code']:
            if col in df.columns:
                code_col = col
                break

        if code_col is None:
            print(f"  无法找到代码列, 可用列: {df.columns.tolist()}")
            return pd.DataFrame()

        result['code'] = df[code_col].apply(normalize_code)

        # 解析名称
        name_col = None
        for col in ['名称', '证券名称', '证券简称', 'name']:
            if col in df.columns:
                name_col = col
                break
        result['name'] = df[name_col] if name_col else None

        # 解析持股数量 (万股)
        shares_col = None
        for col in ['今日持股-股数', '持股数量', '持股数量(万股)', '持股']:
            if col in df.columns:
                shares_col = col
                break
        if shares_col:
            # 可能单位是股,需要转换为万股
            result['holding_shares'] = pd.to_numeric(df[shares_col], errors='coerce')
            # 如果数值很大,可能单位是股,转为万股 (AKShare返回万股单位)
            # 检查是否需要转换 - 如果中位数 > 1亿说明单位是股
            if result['holding_shares'].median() > 100000000:
                result['holding_shares'] = result['holding_shares'] / 10000
        else:
            result['holding_shares'] = None

        # 解析持股比例 (%)
        ratio_col = None
        for col in ['今日持股-占总股本比', '今日持股-占流通股比', '持股占比', '持股比例', '占总股本比']:
            if col in df.columns:
                ratio_col = col
                break
        if ratio_col:
            result['holding_ratio'] = pd.to_numeric(df[ratio_col].astype(str).str.rstrip('%'), errors='coerce')
        else:
            result['holding_ratio'] = None

        # 解析持股变动 (万股) - 使用5日增持估计
        change_col = None
        for col in ['5日增持估计-股数', '持股变动', '持股数量变动', '今日变动']:
            if col in df.columns:
                change_col = col
                break
        if change_col:
            result['holding_change'] = pd.to_numeric(df[change_col], errors='coerce')
            # 如果数值很大,可能单位是股,转为万股
            if result['holding_change'].abs().median() > 100000000:
                result['holding_change'] = result['holding_change'] / 10000
        else:
            result['holding_change'] = None

        # 解析市值 (亿)
        mv_col = None
        for col in ['今日持股-市值', '持股市值', '持股市值(亿)', '市值']:
            if col in df.columns:
                mv_col = col
                break
        if mv_col:
            result['market_value'] = pd.to_numeric(df[mv_col], errors='coerce')
            # 如果单位是万元,转为亿 (AKShare 返回万元单位)
            if result['market_value'].median() > 10000:
                result['market_value'] = result['market_value'] / 10000
        else:
            result['market_value'] = None

        # 添加日期
        result['date'] = date.today().strftime('%Y-%m-%d')

        return result[['code', 'date', 'name', 'holding_shares', 'holding_ratio',
                       'holding_change', 'market_value']]

    except Exception as e:
        print(f"  下载失败: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def download_stock_northbound_history(code: str, start_date: str = None) -> pd.DataFrame:
    """
    下载单只股票的北向历史持仓
    使用 ak.stock_hsgt_individual_em(symbol, indicator="北向资金持股")

    Args:
        code: 股票代码 (6位数字)
        start_date: 开始日期

    Returns:
        DataFrame with historical holdings
    """
    try:
        # 提取6位数字代码
        if '.' in code:
            code_num = code.split('.')[-1]
        else:
            code_num = code

        df = ak.stock_hsgt_individual_em(symbol=code_num, indicator="北向资金持股")

        if df is None or df.empty:
            return pd.DataFrame()

        result = pd.DataFrame()

        # 解析日期
        date_col = None
        for col in ['日期', 'date', '交易日']:
            if col in df.columns:
                date_col = col
                break

        if date_col:
            result['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        else:
            return pd.DataFrame()

        # 标准化代码
        result['code'] = normalize_code(code_num)

        # 持股数量
        for col in ['持股数量', '持股']:
            if col in df.columns:
                result['holding_shares'] = pd.to_numeric(df[col], errors='coerce') / 10000  # 转为万股
                break

        # 持股比例
        for col in ['持股占比', '占总股本比']:
            if col in df.columns:
                result['holding_ratio'] = pd.to_numeric(df[col], errors='coerce')
                break

        # 持股变动
        for col in ['持股变动', '较上日增减']:
            if col in df.columns:
                result['holding_change'] = pd.to_numeric(df[col], errors='coerce') / 10000  # 转为万股
                break

        # 市值
        for col in ['持股市值', '市值']:
            if col in df.columns:
                result['market_value'] = pd.to_numeric(df[col], errors='coerce') / 100000000  # 转为亿
                break

        # 过滤日期
        if start_date:
            result = result[result['date'] >= start_date]

        return result

    except Exception as e:
        return pd.DataFrame()


def save_to_database(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """保存数据到数据库"""
    if df.empty:
        return 0

    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO northbound_holdings
            (code, date, name, holding_shares, holding_ratio, holding_change, market_value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['code'],
            row['date'],
            row.get('name'),
            row.get('holding_shares'),
            row.get('holding_ratio'),
            row.get('holding_change'),
            row.get('market_value'),
        ))
        count += 1

    conn.commit()
    return count


def download_today(conn: sqlite3.Connection):
    """下载今日北向持仓数据"""
    today = date.today().strftime('%Y-%m-%d')

    # 检查是否已下载
    downloaded_dates = get_downloaded_dates(conn)
    if today in downloaded_dates:
        print(f"今日 ({today}) 数据已下载")
        return

    df = download_northbound_holdings_em()
    if df.empty:
        print("未获取到数据")
        return

    count = save_to_database(conn, df)

    # 记录下载日志
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO download_log (download_date, stock_count)
        VALUES (?, ?)
    """, (today, count))
    conn.commit()

    print(f"保存 {count} 条持仓记录")


def download_history(conn: sqlite3.Connection, start_date: str, stocks: list = None):
    """
    下载历史北向持仓数据

    Args:
        conn: 数据库连接
        start_date: 开始日期
        stocks: 股票列表,如果为空则从当日持仓中获取
    """
    print(f"\n下载 {start_date} 以来的历史数据...")

    # 如果没有指定股票列表,先下载今日数据获取股票列表
    if not stocks:
        df_today = download_northbound_holdings_em()
        if df_today.empty:
            print("无法获取股票列表")
            return
        stocks = df_today['code'].unique().tolist()
        save_to_database(conn, df_today)

    print(f"共 {len(stocks)} 只股票需要下载历史数据")

    for code in tqdm(stocks, desc="下载历史数据"):
        try:
            df = download_stock_northbound_history(code, start_date)
            if not df.empty:
                save_to_database(conn, df)
        except Exception as e:
            pass

        time.sleep(REQUEST_DELAY)

    conn.commit()


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("北向资金持仓数据库统计信息")
    print("=" * 50)

    cursor.execute("SELECT COUNT(*) FROM northbound_holdings")
    print(f"持仓记录总数: {cursor.fetchone()[0]:,} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM northbound_holdings")
    print(f"股票数量: {cursor.fetchone()[0]:,} 只")

    cursor.execute("SELECT COUNT(DISTINCT date) FROM northbound_holdings")
    print(f"日期数量: {cursor.fetchone()[0]:,} 天")

    cursor.execute("SELECT MIN(date), MAX(date) FROM northbound_holdings")
    row = cursor.fetchone()
    if row[0]:
        print(f"日期范围: {row[0]} 至 {row[1]}")

    # 最新一天统计
    cursor.execute("""
        SELECT date, COUNT(*) as cnt, AVG(holding_ratio) as avg_ratio
        FROM northbound_holdings
        WHERE date = (SELECT MAX(date) FROM northbound_holdings)
        GROUP BY date
    """)
    row = cursor.fetchone()
    if row:
        print(f"\n最新数据 ({row[0]}):")
        print(f"  持仓股票数: {row[1]} 只")
        avg_ratio = row[2] if row[2] is not None else 0
        print(f"  平均持股比例: {avg_ratio:.2f}%")

    # 北向重仓股 (持股比例 > 5%)
    cursor.execute("""
        SELECT COUNT(*) FROM northbound_holdings
        WHERE date = (SELECT MAX(date) FROM northbound_holdings)
        AND holding_ratio > 5
    """)
    heavy_count = cursor.fetchone()[0]
    print(f"  北向重仓股 (>5%): {heavy_count} 只")

    print("=" * 50)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='北向资金持仓数据下载工具')
    parser.add_argument('--today', action='store_true', help='下载今日数据')
    parser.add_argument('--history', type=str, help='下载历史数据 (开始日期 YYYY-MM-DD)')
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
        elif args.history:
            download_history(conn, args.history)
            print_statistics(conn)
        else:
            # 默认下载今日数据
            download_today(conn)
            print_statistics(conn)
    finally:
        conn.close()

    print("\n完成!")


if __name__ == "__main__":
    main()
