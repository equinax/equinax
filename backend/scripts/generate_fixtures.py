#!/usr/bin/env python3
"""
Generate Fixture Data for Development

Creates small sample databases from full dataset:
- 100 representative stocks x 30 days
- 20 ETFs x 30 days
- Index constituents (HS300/ZZ500/ZZ1000)
- Industry classification (Shenwan L1)

Usage:
    python -m scripts.generate_fixtures
    python -m scripts.generate_fixtures --stocks 50 --days 15
"""

import sqlite3
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional

# Paths
BACKEND_DIR = Path(__file__).parent.parent
DATA_DIR = BACKEND_DIR / "data"
FIXTURES_DIR = DATA_DIR / "fixtures"
TRADING_DATA_DIR = Path("/Users/dan/Code/q/trading_data")


# Representative stock codes for each category
# These are well-known stocks that are likely to have complete data

# Large cap - HS300 representatives
HS300_SAMPLES = [
    "sh.600519",  # 贵州茅台
    "sh.601318",  # 中国平安
    "sh.600036",  # 招商银行
    "sz.000858",  # 五粮液
    "sh.600276",  # 恒瑞医药
    "sz.000333",  # 美的集团
    "sh.601899",  # 紫金矿业
    "sz.002415",  # 海康威视
    "sh.600900",  # 长江电力
    "sh.601012",  # 隆基绿能
    "sz.300750",  # 宁德时代
    "sh.600030",  # 中信证券
    "sh.600887",  # 伊利股份
    "sz.000001",  # 平安银行
    "sh.601166",  # 兴业银行
    "sz.002304",  # 洋河股份
    "sh.600809",  # 山西汾酒
    "sz.000568",  # 泸州老窖
    "sh.600048",  # 保利发展
    "sh.601668",  # 中国建筑
    "sz.002594",  # 比亚迪
    "sh.600585",  # 海螺水泥
    "sh.603259",  # 药明康德
    "sz.300059",  # 东方财富
    "sh.600438",  # 通威股份
    "sh.601888",  # 中国中免
    "sz.002352",  # 顺丰控股
    "sh.600309",  # 万华化学
    "sz.000725",  # 京东方A
    "sh.601919",  # 中远海控
]

# Mid cap - ZZ500 representatives
ZZ500_SAMPLES = [
    "sh.600196",  # 复星医药
    "sz.002241",  # 歌尔股份
    "sz.000063",  # 中兴通讯
    "sh.600016",  # 民生银行
    "sz.002049",  # 紫光国微
    "sh.600183",  # 生益科技
    "sz.000661",  # 长春高新
    "sz.002032",  # 苏泊尔
    "sh.603288",  # 海天味业
    "sz.000009",  # 中国宝安
    "sh.600763",  # 通策医疗
    "sz.002008",  # 大族激光
    "sz.000538",  # 云南白药
    "sh.600031",  # 三一重工
    "sz.002230",  # 科大讯飞
    "sh.600089",  # 特变电工
    "sz.000100",  # TCL科技
    "sz.002001",  # 新和成
    "sh.600346",  # 恒力石化
    "sz.002138",  # 顺络电子
    "sh.603899",  # 晨光股份
    "sz.002027",  # 分众传媒
    "sh.600079",  # 人福医药
    "sz.000423",  # 东阿阿胶
    "sz.002074",  # 国轩高科
    "sh.600104",  # 上汽集团
    "sz.000651",  # 格力电器
    "sz.002142",  # 宁波银行
    "sh.600570",  # 恒生电子
    "sz.000166",  # 申万宏源
]

# Small cap - ZZ1000 representatives
ZZ1000_SAMPLES = [
    "sz.300014",  # 亿纬锂能
    "sz.300124",  # 汇川技术
    "sz.300015",  # 爱尔眼科
    "sz.300274",  # 阳光电源
    "sz.300033",  # 同花顺
    "sz.300122",  # 智飞生物
    "sz.300142",  # 沃森生物
    "sz.300760",  # 迈瑞医疗
    "sz.300347",  # 泰格医药
    "sz.300003",  # 乐普医疗
    "sz.300413",  # 芒果超媒
    "sz.300502",  # 新易盛
    "sz.300433",  # 蓝思科技
    "sz.300496",  # 中科创达
    "sz.300308",  # 中际旭创
    "sz.300012",  # 华测检测
    "sz.300285",  # 国瓷材料
    "sz.300136",  # 信维通信
    "sz.300017",  # 网宿科技
    "sz.300024",  # 机器人
]

# Northbound heavy stocks (overlap with above is OK)
NORTHBOUND_HEAVY = [
    "sh.600519",  # 贵州茅台
    "sz.000858",  # 五粮液
    "sh.600036",  # 招商银行
    "sz.000333",  # 美的集团
    "sz.002415",  # 海康威视
    "sh.601318",  # 中国平安
    "sz.000651",  # 格力电器
    "sh.600276",  # 恒瑞医药
    "sh.600887",  # 伊利股份
    "sz.002352",  # 顺丰控股
]

# ST stocks
ST_STOCKS = [
    "sh.600225",  # *ST松江
    "sz.000932",  # *ST华菱
    "sh.600634",  # *ST富控
    "sz.002509",  # *ST天沃
    "sz.002450",  # *ST康得
]

# New stocks (IPO within last year) - these may change, will be filtered
NEW_STOCKS = [
    "sh.688041",  # 海光信息
    "sh.688256",  # 寒武纪
    "sh.688111",  # 金山办公
    "sh.688012",  # 中微公司
    "sh.688396",  # 华润微
]

# Sample ETFs
SAMPLE_ETFS = [
    "sh.510050",  # 50ETF
    "sh.510300",  # 300ETF
    "sh.510500",  # 500ETF
    "sh.512100",  # 1000ETF
    "sh.512010",  # 医药ETF
    "sh.512880",  # 证券ETF
    "sh.512690",  # 酒ETF
    "sh.512660",  # 军工ETF
    "sz.159915",  # 创业板ETF
    "sz.159919",  # 300ETF
    "sh.518880",  # 黄金ETF
    "sh.511010",  # 国债ETF
    "sh.511260",  # 十年国债
    "sh.513050",  # 中概互联
    "sh.513100",  # 纳指ETF
    "sh.513500",  # 标普ETF
    "sz.159941",  # 纳指ETF
    "sz.159920",  # 恒生ETF
    "sh.510330",  # 华夏300
    "sz.159901",  # 深100ETF
]


def get_latest_date(conn: sqlite3.Connection, table: str = "daily_k_data") -> Optional[str]:
    """Get the latest date in the database."""
    try:
        cursor = conn.execute(f"SELECT MAX(date) FROM {table}")
        return cursor.fetchone()[0]
    except:
        return None


def get_date_range(latest_date: str, days: int) -> Tuple[str, str]:
    """Calculate date range from latest date."""
    end_date = datetime.strptime(latest_date, "%Y-%m-%d")
    start_date = end_date - timedelta(days=days + 10)  # Extra buffer for weekends
    return start_date.strftime("%Y-%m-%d"), latest_date


def get_available_codes(conn: sqlite3.Connection, target_codes: List[str]) -> List[str]:
    """Filter codes that actually exist in the database."""
    cursor = conn.execute("SELECT DISTINCT code FROM stock_basic")
    available = {row[0] for row in cursor.fetchall()}
    return [code for code in target_codes if code in available]


def extract_stock_data(
    source_path: Path,
    target_path: Path,
    codes: List[str],
    start_date: str,
    end_date: str,
) -> int:
    """Extract stock data for specified codes and date range."""
    if not source_path.exists():
        print(f"  Source not found: {source_path}")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    # Get available codes
    available_codes = get_available_codes(source_conn, codes)
    print(f"  Available codes: {len(available_codes)}/{len(codes)}")

    if not available_codes:
        source_conn.close()
        target_conn.close()
        return 0

    # Create target tables
    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY,
            code_name TEXT,
            ipoDate TEXT,
            outDate TEXT,
            type TEXT,
            status TEXT
        )
    """)

    target_conn.execute("""
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

    target_conn.execute("""
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

    # Copy stock_basic
    code_list = "','".join(available_codes)
    cursor = source_conn.execute(f"""
        SELECT * FROM stock_basic WHERE code IN ('{code_list}')
    """)
    rows = cursor.fetchall()
    target_conn.executemany(
        "INSERT OR REPLACE INTO stock_basic VALUES (?,?,?,?,?,?)",
        rows
    )
    print(f"  Copied {len(rows)} stock_basic records")

    # Copy daily_k_data (select specific columns to match target schema)
    cursor = source_conn.execute(f"""
        SELECT id, date, code, open, high, low, close, preclose, volume, amount,
               turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM, pcfNcfTTM, isST
        FROM daily_k_data
        WHERE code IN ('{code_list}')
        AND date >= '{start_date}' AND date <= '{end_date}'
    """)
    rows = cursor.fetchall()
    target_conn.executemany(
        "INSERT OR REPLACE INTO daily_k_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    print(f"  Copied {len(rows)} daily_k_data records")

    # Copy adjust_factor
    try:
        cursor = source_conn.execute(f"""
            SELECT id, code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor
            FROM adjust_factor WHERE code IN ('{code_list}')
        """)
        rows = cursor.fetchall()
        if rows:
            target_conn.executemany(
                "INSERT OR REPLACE INTO adjust_factor VALUES (?,?,?,?,?,?)",
                rows
            )
            print(f"  Copied {len(rows)} adjust_factor records")
    except Exception as e:
        print(f"  No adjust_factor table or error: {e}")

    # Create indexes
    target_conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_code ON daily_k_data(code)")
    target_conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_k_data(date)")

    target_conn.commit()
    count = target_conn.execute("SELECT COUNT(*) FROM daily_k_data").fetchone()[0]

    source_conn.close()
    target_conn.close()

    return count


def extract_etf_data(
    source_path: Path,
    target_path: Path,
    codes: List[str],
    start_date: str,
    end_date: str,
) -> int:
    """Extract ETF data for specified codes and date range."""
    if not source_path.exists():
        print(f"  Source not found: {source_path}")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    # Check available codes
    cursor = source_conn.execute("SELECT DISTINCT code FROM etf_basic")
    available = {row[0] for row in cursor.fetchall()}
    available_codes = [code for code in codes if code in available]
    print(f"  Available ETF codes: {len(available_codes)}/{len(codes)}")

    if not available_codes:
        source_conn.close()
        target_conn.close()
        return 0

    # Create target tables (matching source schema)
    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_basic (
            code TEXT PRIMARY KEY,
            code_name TEXT,
            ipo_date TEXT,
            out_date TEXT,
            type INTEGER DEFAULT 5,
            status INTEGER
        )
    """)

    target_conn.execute("""
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
            UNIQUE(code, date)
        )
    """)

    # Copy etf_basic
    code_list = "','".join(available_codes)
    cursor = source_conn.execute(f"""
        SELECT * FROM etf_basic WHERE code IN ('{code_list}')
    """)
    rows = cursor.fetchall()
    if rows:
        target_conn.executemany(
            "INSERT OR REPLACE INTO etf_basic VALUES (?,?,?,?,?,?)",
            rows
        )
        print(f"  Copied {len(rows)} etf_basic records")

    # Copy daily_k_data (select all columns)
    cursor = source_conn.execute(f"""
        SELECT id, date, code, open, high, low, close, preclose, volume, amount, turn, tradestatus, pctChg
        FROM daily_k_data
        WHERE code IN ('{code_list}')
        AND date >= '{start_date}' AND date <= '{end_date}'
    """)
    rows = cursor.fetchall()
    target_conn.executemany(
        "INSERT OR REPLACE INTO daily_k_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    print(f"  Copied {len(rows)} ETF daily_k_data records")

    # Create indexes
    target_conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_code ON daily_k_data(code)")
    target_conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_k_data(date)")

    target_conn.commit()
    count = target_conn.execute("SELECT COUNT(*) FROM daily_k_data").fetchone()[0]

    source_conn.close()
    target_conn.close()

    return count


def extract_index_data(source_path: Path, target_path: Path) -> int:
    """Extract index constituent data."""
    if not source_path.exists():
        print(f"  Source not found: {source_path}")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    # Create target table
    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_code TEXT NOT NULL,
            index_name TEXT,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            weight REAL,
            effective_date TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, stock_code, effective_date)
        )
    """)

    # Copy all index data (it's small)
    cursor = source_conn.execute("""
        SELECT index_code, index_name, stock_code, stock_name, weight, effective_date, created_at
        FROM index_constituents
    """)
    rows = cursor.fetchall()
    if rows:
        target_conn.executemany(
            "INSERT OR REPLACE INTO index_constituents VALUES (?,?,?,?,?,?,?)",
            rows
        )
        print(f"  Copied {len(rows)} index_constituents records")

    target_conn.commit()
    count = len(rows) if rows else 0

    source_conn.close()
    target_conn.close()

    return count


def extract_industry_data(source_path: Path, target_path: Path) -> int:
    """Extract industry classification data."""
    if not source_path.exists():
        print(f"  Source not found: {source_path}")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    # Create target tables (matching source schema)
    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS industry_classification (
            industry_code TEXT NOT NULL,
            industry_name TEXT NOT NULL,
            industry_level INTEGER NOT NULL,
            parent_code TEXT,
            classification_system TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (industry_code, classification_system)
        )
    """)

    target_conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_industry_mapping (
            stock_code TEXT NOT NULL,
            industry_code TEXT NOT NULL,
            classification_system TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            expire_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_code, industry_code, classification_system, effective_date)
        )
    """)

    # Copy industry_classification
    try:
        cursor = source_conn.execute("""
            SELECT industry_code, industry_name, industry_level, parent_code,
                   classification_system, created_at
            FROM industry_classification
        """)
        rows = cursor.fetchall()
        if rows:
            target_conn.executemany(
                "INSERT OR REPLACE INTO industry_classification VALUES (?,?,?,?,?,?)",
                rows
            )
            print(f"  Copied {len(rows)} industry_classification records")
    except Exception as e:
        print(f"  Error copying industry_classification: {e}")

    # Copy stock_industry_mapping
    try:
        cursor = source_conn.execute("""
            SELECT stock_code, industry_code, classification_system,
                   effective_date, expire_date, created_at
            FROM stock_industry_mapping
        """)
        rows = cursor.fetchall()
        if rows:
            target_conn.executemany(
                "INSERT OR REPLACE INTO stock_industry_mapping VALUES (?,?,?,?,?,?)",
                rows
            )
            print(f"  Copied {len(rows)} stock_industry_mapping records")
    except Exception as e:
        print(f"  Error copying stock_industry_mapping: {e}")

    target_conn.commit()

    source_conn.close()
    target_conn.close()

    return len(rows) if rows else 0


def generate_fixtures(
    stocks: int = 100,
    days: int = 30,
    source_dir: Path = TRADING_DATA_DIR,
) -> dict:
    """
    Generate fixture files from full dataset.

    Args:
        stocks: Number of sample stocks
        days: Number of days of history
        source_dir: Source data directory

    Returns:
        dict: Statistics about generated fixtures
    """
    print(f"\nGenerating fixtures from: {source_dir}")
    print(f"Parameters: {stocks} stocks, {days} days\n")

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    stats = {}

    # 1. Generate stock fixture
    print("=" * 50)
    print("Generating sample_stocks.db...")
    print("=" * 50)

    # Find latest stock database
    stock_dbs = sorted(source_dir.glob("a_stock_*.db"), reverse=True)
    if not stock_dbs:
        print("No stock databases found!")
        return stats

    latest_stock_db = stock_dbs[0]
    print(f"Using: {latest_stock_db.name}")

    # Get date range
    conn = sqlite3.connect(str(latest_stock_db))
    latest_date = get_latest_date(conn)
    conn.close()

    if not latest_date:
        print("Could not determine latest date")
        return stats

    start_date, end_date = get_date_range(latest_date, days)
    print(f"Date range: {start_date} to {end_date}")

    # Combine all sample codes
    all_codes = list(set(
        HS300_SAMPLES[:30] +
        ZZ500_SAMPLES[:30] +
        ZZ1000_SAMPLES[:20] +
        NORTHBOUND_HEAVY[:10] +
        ST_STOCKS[:5] +
        NEW_STOCKS[:5]
    ))[:stocks]

    print(f"Sample codes: {len(all_codes)}")

    target_path = FIXTURES_DIR / "sample_stocks.db"
    count = extract_stock_data(
        latest_stock_db,
        target_path,
        all_codes,
        start_date,
        end_date
    )
    stats["stocks"] = count
    print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")

    # 2. Generate ETF fixture
    print("\n" + "=" * 50)
    print("Generating sample_etfs.db...")
    print("=" * 50)

    etf_dbs = sorted(source_dir.glob("etf_*.db"), reverse=True)
    if etf_dbs:
        latest_etf_db = etf_dbs[0]
        print(f"Using: {latest_etf_db.name}")

        target_path = FIXTURES_DIR / "sample_etfs.db"
        count = extract_etf_data(
            latest_etf_db,
            target_path,
            SAMPLE_ETFS,
            start_date,
            end_date
        )
        stats["etfs"] = count
        print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")
    else:
        print("No ETF databases found!")

    # 3. Generate index fixture
    print("\n" + "=" * 50)
    print("Generating sample_indices.db...")
    print("=" * 50)

    index_db = source_dir / "index_constituents.db"
    target_path = FIXTURES_DIR / "sample_indices.db"
    count = extract_index_data(index_db, target_path)
    stats["indices"] = count
    if target_path.exists():
        print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")

    # 4. Generate industry fixture
    print("\n" + "=" * 50)
    print("Generating sample_industries.db...")
    print("=" * 50)

    industry_db = source_dir / "industry_classification.db"
    target_path = FIXTURES_DIR / "sample_industries.db"
    count = extract_industry_data(industry_db, target_path)
    stats["industries"] = count
    if target_path.exists():
        print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")

    # Summary
    print("\n" + "=" * 50)
    print("Summary")
    print("=" * 50)

    total_size = 0
    for f in FIXTURES_DIR.glob("*.db"):
        size = f.stat().st_size / 1024 / 1024
        total_size += size
        print(f"  {f.name}: {size:.2f} MB")

    print(f"\nTotal size: {total_size:.2f} MB")
    print(f"Stats: {stats}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate fixture data")
    parser.add_argument("--stocks", type=int, default=100, help="Number of sample stocks")
    parser.add_argument("--days", type=int, default=30, help="Days of history")
    parser.add_argument("--source", type=str, default=str(TRADING_DATA_DIR), help="Source directory")

    args = parser.parse_args()

    generate_fixtures(
        stocks=args.stocks,
        days=args.days,
        source_dir=Path(args.source)
    )
