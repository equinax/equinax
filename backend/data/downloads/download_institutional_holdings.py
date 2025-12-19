#!/usr/bin/env python3
"""
机构持仓数据下载脚本
使用 AKShare 下载公募基金持仓数据，存入 SQLite 数据库
用于 4+1 分类系统的微观结构维度 (is_institutional)
"""

import sqlite3
import akshare as ak
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
import time
import argparse
import os

# 限流配置
REQUEST_DELAY = 0.5  # 每次请求后等待时间（秒）

# 数据库文件
DB_FILE = "institutional_holdings.db"


def get_db_path() -> str:
    """获取数据库路径"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # 机构持仓数据表 (按季度)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutional_holdings (
            code TEXT NOT NULL,
            report_date TEXT NOT NULL,   -- 报告期 (e.g., '2024-09-30')
            name TEXT,
            fund_count INTEGER,          -- 持仓基金数量
            holding_shares REAL,         -- 持股数量 (万股)
            holding_ratio REAL,          -- 持股占流通股比例 (%)
            holding_change REAL,         -- 较上期变动 (万股)
            market_value REAL,           -- 持股市值 (亿)
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, report_date)
        )
    """)

    # 下载日志表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS download_log (
            report_date TEXT PRIMARY KEY,
            stock_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst_report_date ON institutional_holdings(report_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst_code ON institutional_holdings(code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst_ratio ON institutional_holdings(holding_ratio)")

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


def get_downloaded_quarters(conn: sqlite3.Connection) -> set:
    """获取已下载的季度"""
    cursor = conn.cursor()
    cursor.execute("SELECT report_date FROM download_log")
    return {row[0] for row in cursor.fetchall()}


def get_recent_quarters(count: int = 4) -> list:
    """获取最近N个季度的报告期日期"""
    today = date.today()
    quarters = []

    # 当前年份的季度
    year = today.year
    quarter_ends = [f"{year}-03-31", f"{year}-06-30", f"{year}-09-30", f"{year}-12-31"]

    # 找到当前或之前的季度
    for qe in quarter_ends:
        if qe <= today.strftime('%Y-%m-%d'):
            quarters.append(qe)

    # 如果不够，加上前一年的季度
    while len(quarters) < count:
        year -= 1
        for qe in reversed([f"{year}-03-31", f"{year}-06-30", f"{year}-09-30", f"{year}-12-31"]):
            quarters.insert(0, qe)
            if len(quarters) >= count:
                break

    # 返回最近的N个
    return quarters[-count:]


def download_fund_holdings_by_stock() -> pd.DataFrame:
    """
    下载公募基金重仓股数据
    使用 ak.stock_report_fund_hold() 或类似函数

    Returns:
        DataFrame with columns: code, report_date, fund_count, holding_shares, holding_ratio, etc.
    """
    print("\n正在下载公募基金重仓股数据...")

    try:
        # 尝试获取基金重仓股数据
        # ak.stock_report_fund_hold(symbol="基金重仓股", date="20240930")
        df = ak.stock_report_fund_hold(symbol="基金重仓股")

        if df is None or df.empty:
            print("  未获取到数据")
            return pd.DataFrame()

        print(f"  原始列名: {df.columns.tolist()}")
        print(f"  获取 {len(df)} 条持仓记录")

        # 构建结果DataFrame
        result = pd.DataFrame()

        # 解析代码
        code_col = None
        for col in ['代码', '证券代码', 'code', '股票代码']:
            if col in df.columns:
                code_col = col
                break

        if code_col is None:
            print(f"  无法找到代码列, 可用列: {df.columns.tolist()}")
            return pd.DataFrame()

        result['code'] = df[code_col].apply(normalize_code)

        # 解析名称
        name_col = None
        for col in ['名称', '证券名称', '股票名称', 'name']:
            if col in df.columns:
                name_col = col
                break
        result['name'] = df[name_col] if name_col else None

        # 解析报告期
        date_col = None
        for col in ['报告期', '日期', 'report_date', '截止日期']:
            if col in df.columns:
                date_col = col
                break
        if date_col:
            result['report_date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        else:
            # 默认使用最近季度
            quarters = get_recent_quarters(1)
            result['report_date'] = quarters[0] if quarters else date.today().strftime('%Y-%m-%d')

        # 解析基金数量
        fund_count_col = None
        for col in ['基金家数', '持仓基金数', '机构数', '家数']:
            if col in df.columns:
                fund_count_col = col
                break
        if fund_count_col:
            result['fund_count'] = pd.to_numeric(df[fund_count_col], errors='coerce')
        else:
            result['fund_count'] = None

        # 解析持股数量 (万股)
        shares_col = None
        for col in ['持股数量', '持仓股数', '持股']:
            if col in df.columns:
                shares_col = col
                break
        if shares_col:
            result['holding_shares'] = pd.to_numeric(df[shares_col], errors='coerce')
            # 如果数值很大，可能单位是股，转为万股
            if result['holding_shares'].median() > 1000000:
                result['holding_shares'] = result['holding_shares'] / 10000
        else:
            result['holding_shares'] = None

        # 解析持股比例 (%)
        ratio_col = None
        for col in ['持股占比', '持仓占比', '占流通股比', '占总股本比']:
            if col in df.columns:
                ratio_col = col
                break
        if ratio_col:
            result['holding_ratio'] = pd.to_numeric(df[ratio_col].astype(str).str.rstrip('%'), errors='coerce')
        else:
            result['holding_ratio'] = None

        # 解析持股变动 (万股)
        change_col = None
        for col in ['持股变动', '增持股数', '变动股数']:
            if col in df.columns:
                change_col = col
                break
        if change_col:
            result['holding_change'] = pd.to_numeric(df[change_col], errors='coerce')
            if result['holding_change'].abs().median() > 1000000:
                result['holding_change'] = result['holding_change'] / 10000
        else:
            result['holding_change'] = None

        # 解析市值 (亿)
        mv_col = None
        for col in ['持股市值', '市值', '持仓市值']:
            if col in df.columns:
                mv_col = col
                break
        if mv_col:
            result['market_value'] = pd.to_numeric(df[mv_col], errors='coerce')
            if result['market_value'].median() > 100000000:
                result['market_value'] = result['market_value'] / 100000000
        else:
            result['market_value'] = None

        return result[['code', 'report_date', 'name', 'fund_count', 'holding_shares',
                       'holding_ratio', 'holding_change', 'market_value']]

    except Exception as e:
        print(f"  下载失败: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def download_fund_holdings_by_quarter(quarter: str) -> pd.DataFrame:
    """
    下载指定季度的公募基金重仓股数据

    Args:
        quarter: 报告期 (e.g., '2024-09-30')

    Returns:
        DataFrame with holdings data
    """
    print(f"\n正在下载 {quarter} 的基金持仓数据...")

    try:
        # 转换日期格式 (2024-09-30 -> 20240930)
        date_str = quarter.replace('-', '')

        df = ak.stock_report_fund_hold(symbol="基金重仓股", date=date_str)

        if df is None or df.empty:
            print(f"  未获取到 {quarter} 的数据")
            return pd.DataFrame()

        print(f"  获取 {len(df)} 条记录")

        # 处理数据 (复用上面的逻辑)
        result = pd.DataFrame()

        # 解析代码
        code_col = None
        for col in ['代码', '证券代码', 'code', '股票代码']:
            if col in df.columns:
                code_col = col
                break

        if code_col is None:
            return pd.DataFrame()

        result['code'] = df[code_col].apply(normalize_code)
        result['report_date'] = quarter

        # 名称
        for col in ['名称', '证券名称', '股票名称']:
            if col in df.columns:
                result['name'] = df[col]
                break

        # 基金数量
        for col in ['基金家数', '持仓基金数', '机构数']:
            if col in df.columns:
                result['fund_count'] = pd.to_numeric(df[col], errors='coerce')
                break

        # 持股数量
        for col in ['持股数量', '持仓股数']:
            if col in df.columns:
                result['holding_shares'] = pd.to_numeric(df[col], errors='coerce')
                if result['holding_shares'].median() > 1000000:
                    result['holding_shares'] = result['holding_shares'] / 10000
                break

        # 持股比例
        for col in ['持股占比', '占流通股比']:
            if col in df.columns:
                result['holding_ratio'] = pd.to_numeric(df[col].astype(str).str.rstrip('%'), errors='coerce')
                break

        # 持股变动
        for col in ['持股变动', '增持股数']:
            if col in df.columns:
                result['holding_change'] = pd.to_numeric(df[col], errors='coerce')
                if result['holding_change'].abs().median() > 1000000:
                    result['holding_change'] = result['holding_change'] / 10000
                break

        # 市值
        for col in ['持股市值', '市值']:
            if col in df.columns:
                result['market_value'] = pd.to_numeric(df[col], errors='coerce')
                if result['market_value'].median() > 100000000:
                    result['market_value'] = result['market_value'] / 100000000
                break

        return result

    except Exception as e:
        print(f"  下载 {quarter} 失败: {e}")
        return pd.DataFrame()


def save_to_database(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """保存数据到数据库"""
    if df.empty:
        return 0

    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO institutional_holdings
            (code, report_date, name, fund_count, holding_shares, holding_ratio, holding_change, market_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['code'],
            row.get('report_date'),
            row.get('name'),
            row.get('fund_count'),
            row.get('holding_shares'),
            row.get('holding_ratio'),
            row.get('holding_change'),
            row.get('market_value'),
        ))
        count += 1

    conn.commit()
    return count


def download_latest(conn: sqlite3.Connection):
    """下载最新一期的机构持仓数据"""
    df = download_fund_holdings_by_stock()
    if df.empty:
        print("未获取到数据")
        return

    count = save_to_database(conn, df)

    # 记录下载日志
    if count > 0 and 'report_date' in df.columns:
        report_date = df['report_date'].iloc[0]
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO download_log (report_date, stock_count)
            VALUES (?, ?)
        """, (report_date, count))
        conn.commit()

    print(f"保存 {count} 条持仓记录")


def download_quarters(conn: sqlite3.Connection, quarters: list):
    """下载指定季度的数据"""
    for quarter in quarters:
        downloaded = get_downloaded_quarters(conn)
        if quarter in downloaded:
            print(f"季度 {quarter} 已下载，跳过")
            continue

        df = download_fund_holdings_by_quarter(quarter)
        if not df.empty:
            count = save_to_database(conn, df)

            # 记录下载日志
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO download_log (report_date, stock_count)
                VALUES (?, ?)
            """, (quarter, count))
            conn.commit()

            print(f"  保存 {count} 条记录")

        time.sleep(REQUEST_DELAY)


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("机构持仓数据库统计信息")
    print("=" * 50)

    cursor.execute("SELECT COUNT(*) FROM institutional_holdings")
    print(f"持仓记录总数: {cursor.fetchone()[0]:,} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM institutional_holdings")
    print(f"股票数量: {cursor.fetchone()[0]:,} 只")

    cursor.execute("SELECT COUNT(DISTINCT report_date) FROM institutional_holdings")
    print(f"季度数量: {cursor.fetchone()[0]:,} 期")

    cursor.execute("SELECT MIN(report_date), MAX(report_date) FROM institutional_holdings")
    row = cursor.fetchone()
    if row[0]:
        print(f"日期范围: {row[0]} 至 {row[1]}")

    # 最新一期统计
    cursor.execute("""
        SELECT report_date, COUNT(*) as cnt, AVG(holding_ratio) as avg_ratio
        FROM institutional_holdings
        WHERE report_date = (SELECT MAX(report_date) FROM institutional_holdings)
        GROUP BY report_date
    """)
    row = cursor.fetchone()
    if row:
        print(f"\n最新数据 ({row[0]}):")
        print(f"  持仓股票数: {row[1]} 只")
        if row[2]:
            print(f"  平均持股比例: {row[2]:.2f}%")

    # 机构重仓股 (持股比例 > 10%)
    cursor.execute("""
        SELECT COUNT(*) FROM institutional_holdings
        WHERE report_date = (SELECT MAX(report_date) FROM institutional_holdings)
        AND holding_ratio > 10
    """)
    heavy_count = cursor.fetchone()[0]
    print(f"  机构重仓股 (>10%): {heavy_count} 只")

    print("=" * 50)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='机构持仓数据下载工具')
    parser.add_argument('--latest', action='store_true', help='下载最新数据')
    parser.add_argument('--quarter', type=str, help='下载指定季度 (YYYY-MM-DD, e.g., 2024-09-30)')
    parser.add_argument('--recent', type=int, default=4, help='下载最近N个季度 (默认4)')
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
        elif args.quarter:
            download_quarters(conn, [args.quarter])
            print_statistics(conn)
        elif args.latest:
            download_latest(conn)
            print_statistics(conn)
        else:
            # 默认下载最近几个季度
            quarters = get_recent_quarters(args.recent)
            print(f"将下载以下季度: {quarters}")
            download_quarters(conn, quarters)
            print_statistics(conn)
    finally:
        conn.close()

    print("\n完成!")


if __name__ == "__main__":
    main()
