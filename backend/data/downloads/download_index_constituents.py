#!/usr/bin/env python3
"""
指数成分股数据下载脚本
使用 AKShare 下载沪深300、中证500、中证1000成分股数据，存入 SQLite 数据库
用于 4+1 分类系统的指数成分判断
"""

import sqlite3
import akshare as ak
import pandas as pd
from datetime import datetime, date
import argparse
import os

# 数据库文件
DB_FILE = "index_constituents.db"

# 支持的指数
SUPPORTED_INDICES = {
    "000300": {"name": "沪深300", "short": "hs300"},
    "000905": {"name": "中证500", "short": "zz500"},
    "000852": {"name": "中证1000", "short": "zz1000"},
    "000016": {"name": "上证50", "short": "sz50"},
    "399006": {"name": "创业板指", "short": "cyb"},
    "000688": {"name": "科创50", "short": "kc50"},
}


def get_db_path() -> str:
    """获取数据库路径"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILE)


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # 成分股数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_code TEXT NOT NULL,      -- 指数代码 (000300, 000905, etc)
            index_name TEXT,               -- 指数名称
            stock_code TEXT NOT NULL,      -- 成分股代码 (sh.600000, sz.000001)
            stock_name TEXT,               -- 成分股名称
            weight REAL,                   -- 权重 (%)
            effective_date TEXT NOT NULL,  -- 生效日期
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, stock_code, effective_date)
        )
    """)

    # 下载日志表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS download_log (
            index_code TEXT NOT NULL,
            download_date TEXT NOT NULL,
            stock_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, download_date)
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_constituents_index ON index_constituents(index_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_constituents_stock ON index_constituents(stock_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_constituents_date ON index_constituents(effective_date)")

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


def download_index_constituents(index_code: str) -> pd.DataFrame:
    """
    下载指数成分股数据
    使用 ak.index_stock_cons(symbol) 获取成分股列表

    Args:
        index_code: 指数代码 (000300, 000905, 000852)

    Returns:
        DataFrame with columns: index_code, stock_code, stock_name, weight
    """
    if index_code not in SUPPORTED_INDICES:
        print(f"不支持的指数: {index_code}")
        return pd.DataFrame()

    index_info = SUPPORTED_INDICES[index_code]
    print(f"\n正在下载 {index_info['name']} ({index_code}) 成分股...")

    try:
        # 获取成分股列表
        df = ak.index_stock_cons(symbol=index_code)

        if df is None or df.empty:
            print(f"  未获取到数据")
            return pd.DataFrame()

        print(f"  原始列名: {df.columns.tolist()}")

        # 构建结果DataFrame
        result = pd.DataFrame()

        # 解析股票代码 - 尝试不同的列名 (must do this first to have rows)
        code_col = None
        for col in ['品种代码', '证券代码', '成份券代码', 'code', '代码']:
            if col in df.columns:
                code_col = col
                break

        if code_col is None:
            print(f"  无法找到代码列, 可用列: {df.columns.tolist()}")
            return pd.DataFrame()

        result['stock_code'] = df[code_col].apply(normalize_code)

        # Now add index_code and index_name (after stock_code so DataFrame has rows)
        result['index_code'] = index_code
        result['index_name'] = index_info['name']

        # 解析股票名称
        name_col = None
        for col in ['品种名称', '证券名称', '成份券名称', 'name', '名称']:
            if col in df.columns:
                name_col = col
                break

        result['stock_name'] = df[name_col] if name_col else None

        # 解析权重 (如果有)
        weight_col = None
        for col in ['权重', 'weight', '比例']:
            if col in df.columns:
                weight_col = col
                break

        if weight_col:
            result['weight'] = pd.to_numeric(df[weight_col], errors='coerce')
        else:
            result['weight'] = None

        print(f"  获取 {len(result)} 只成分股")
        return result

    except Exception as e:
        print(f"  下载失败: {e}")
        return pd.DataFrame()


def download_index_weights(index_code: str) -> pd.DataFrame:
    """
    下载指数成分股权重数据 (如果 index_stock_cons 没有权重)
    使用 ak.index_stock_info(symbol) 或其他方法
    """
    try:
        # 尝试使用 index_stock_info 获取更详细信息
        df = ak.index_stock_info(symbol=index_code)
        if df is not None and not df.empty:
            return df
    except:
        pass

    return pd.DataFrame()


def save_to_database(conn: sqlite3.Connection, df: pd.DataFrame, effective_date: str):
    """保存成分股数据到数据库"""
    if df.empty:
        return 0

    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO index_constituents
            (index_code, index_name, stock_code, stock_name, weight, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row['index_code'],
            row.get('index_name'),
            row['stock_code'],
            row.get('stock_name'),
            row.get('weight'),
            effective_date
        ))
        count += 1

    # 记录下载日志
    if count > 0:
        cursor.execute("""
            INSERT OR REPLACE INTO download_log (index_code, download_date, stock_count)
            VALUES (?, ?, ?)
        """, (df['index_code'].iloc[0], effective_date, count))

    conn.commit()
    return count


def download_all_indices(conn: sqlite3.Connection, indices: list = None):
    """
    下载所有指定指数的成分股

    Args:
        conn: 数据库连接
        indices: 要下载的指数列表，默认下载主要指数
    """
    if indices is None:
        indices = ["000300", "000905", "000852"]  # 默认: 沪深300, 中证500, 中证1000

    effective_date = date.today().strftime('%Y-%m-%d')
    total_count = 0

    for index_code in indices:
        df = download_index_constituents(index_code)
        if not df.empty:
            count = save_to_database(conn, df, effective_date)
            total_count += count
            print(f"  保存 {count} 条记录")

    print(f"\n总计下载 {total_count} 条成分股记录")
    return total_count


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("指数成分股数据库统计信息")
    print("=" * 50)

    # 总记录数
    cursor.execute("SELECT COUNT(*) FROM index_constituents")
    print(f"成分股记录总数: {cursor.fetchone()[0]:,} 条")

    # 按指数统计
    cursor.execute("""
        SELECT index_code, index_name, COUNT(DISTINCT stock_code) as stocks,
               MAX(effective_date) as latest_date
        FROM index_constituents
        GROUP BY index_code
        ORDER BY index_code
    """)

    print("\n按指数统计:")
    for row in cursor.fetchall():
        print(f"  {row[1] or row[0]}: {row[2]:,} 只成分股 (截至 {row[3]})")

    # 下载日志
    cursor.execute("""
        SELECT index_code, download_date, stock_count
        FROM download_log
        ORDER BY download_date DESC
        LIMIT 10
    """)

    logs = cursor.fetchall()
    if logs:
        print("\n最近下载记录:")
        for row in logs:
            index_name = SUPPORTED_INDICES.get(row[0], {}).get('name', row[0])
            print(f"  {row[1]} - {index_name}: {row[2]} 只")

    print("=" * 50)


def list_supported_indices():
    """列出支持的指数"""
    print("\n支持的指数:")
    for code, info in SUPPORTED_INDICES.items():
        print(f"  {code} - {info['name']} ({info['short']})")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='指数成分股数据下载工具')
    parser.add_argument('--index', type=str, nargs='+',
                        help='指定指数代码 (如 000300 000905), 默认下载沪深300/中证500/中证1000')
    parser.add_argument('--all', action='store_true',
                        help='下载所有支持的指数')
    parser.add_argument('--status', action='store_true', help='查看数据库状态')
    parser.add_argument('--list', action='store_true', help='列出支持的指数')
    return parser.parse_args()


def main():
    args = parse_args()

    if args.list:
        list_supported_indices()
        return

    db_path = get_db_path()
    print(f"数据库路径: {db_path}")

    conn = sqlite3.connect(db_path)
    create_database(conn)

    try:
        if args.status:
            print_statistics(conn)
        elif args.all:
            download_all_indices(conn, list(SUPPORTED_INDICES.keys()))
            print_statistics(conn)
        elif args.index:
            download_all_indices(conn, args.index)
            print_statistics(conn)
        else:
            # 默认下载主要三个指数
            download_all_indices(conn)
            print_statistics(conn)
    finally:
        conn.close()

    print("\n完成!")


if __name__ == "__main__":
    main()
