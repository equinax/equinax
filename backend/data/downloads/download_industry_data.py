#!/usr/bin/env python3
"""Download industry classification data to SQLite database.

Downloads Shenwan (申万), EastMoney (东方财富), and optionally CSRC industry classifications
and stores them in a local SQLite database for offline use.

Industry classification data changes infrequently (quarterly at most), so this script
should only need to be run periodically.

Usage:
    # Download all classification systems
    python download_industry_data.py

    # Download only Shenwan classification
    python download_industry_data.py --system sw

    # Download only EastMoney classification
    python download_industry_data.py --system em

Output:
    industry_classification.db - SQLite database with industry classifications
"""

import argparse
import sqlite3
import time
from datetime import date
from pathlib import Path
from typing import Optional

import akshare as ak
import pandas as pd


# Database path
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "industry_classification.db"


def create_database(db_path: Path) -> sqlite3.Connection:
    """Create SQLite database with required tables."""
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Industry classification table
    cursor.execute("""
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

    # Stock-industry mapping table
    cursor.execute("""
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

    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_industry_level
        ON industry_classification(industry_level)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_industry_parent
        ON industry_classification(parent_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_industry_system
        ON industry_classification(classification_system)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mapping_stock
        ON stock_industry_mapping(stock_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mapping_industry
        ON stock_industry_mapping(industry_code)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_mapping_system
        ON stock_industry_mapping(classification_system)
    """)

    conn.commit()
    return conn


def normalize_stock_code(code: str) -> str:
    """Normalize stock code to sh.XXXXXX or sz.XXXXXX format."""
    # Remove any existing suffix
    code = str(code).strip()

    if code.startswith("sh.") or code.startswith("sz.") or code.startswith("bj."):
        return code

    # Handle codes like 600000.SH, 000001.SZ
    if ".SH" in code.upper():
        return f"sh.{code.upper().replace('.SH', '')}"
    if ".SZ" in code.upper():
        return f"sz.{code.upper().replace('.SZ', '')}"
    if ".BJ" in code.upper():
        return f"bj.{code.upper().replace('.BJ', '')}"

    # Handle plain codes like 600000, 000001
    code_num = code.zfill(6)
    if code_num.startswith(("6", "9")):
        return f"sh.{code_num}"
    elif code_num.startswith(("0", "3")):
        return f"sz.{code_num}"
    elif code_num.startswith(("4", "8")):
        return f"bj.{code_num}"
    else:
        return f"sz.{code_num}"


def download_em_industries(conn: sqlite3.Connection) -> tuple[int, int]:
    """Download EastMoney (东方财富) industry classification.

    Returns:
        Tuple of (industries_count, mappings_count)
    """
    print("\n=== Downloading EastMoney Industry Classification ===\n")

    cursor = conn.cursor()
    today = date.today().isoformat()

    # Clear existing EM data
    cursor.execute("DELETE FROM stock_industry_mapping WHERE classification_system = 'em'")
    cursor.execute("DELETE FROM industry_classification WHERE classification_system = 'em'")
    conn.commit()

    industries_count = 0
    mappings_count = 0

    try:
        # Fetch industry list
        print("Fetching industry list...")
        industry_df = ak.stock_board_industry_name_em()
        print(f"Found {len(industry_df)} industries")
        print(f"Columns: {industry_df.columns.tolist()}")

        # Process each industry
        for idx, row in industry_df.iterrows():
            industry_name = row.get("板块名称") or row.get("行业名称")
            industry_code = row.get("板块代码") or row.get("行业代码") or f"EM{idx:04d}"

            if not industry_name:
                continue

            print(f"[{idx+1}/{len(industry_df)}] Processing {industry_name} ({industry_code})...")

            # Insert industry
            cursor.execute("""
                INSERT OR REPLACE INTO industry_classification
                (industry_code, industry_name, industry_level, parent_code, classification_system)
                VALUES (?, ?, ?, ?, ?)
            """, (industry_code, industry_name, 1, None, "em"))
            industries_count += 1

            # Fetch constituents
            try:
                time.sleep(0.3)  # Rate limiting
                cons_df = ak.stock_board_industry_cons_em(symbol=industry_name)

                if cons_df is not None and len(cons_df) > 0:
                    print(f"  Found {len(cons_df)} stocks")

                    for _, stock_row in cons_df.iterrows():
                        stock_code = stock_row.get("代码") or stock_row.get("股票代码")
                        if stock_code:
                            stock_code = normalize_stock_code(stock_code)

                            cursor.execute("""
                                INSERT OR REPLACE INTO stock_industry_mapping
                                (stock_code, industry_code, classification_system, effective_date, expire_date)
                                VALUES (?, ?, ?, ?, ?)
                            """, (stock_code, industry_code, "em", today, None))
                            mappings_count += 1

            except Exception as e:
                print(f"  Error fetching constituents: {e}")

            # Commit in batches
            if (idx + 1) % 10 == 0:
                conn.commit()
                print(f"  Committed batch, total: {industries_count} industries, {mappings_count} mappings")

        conn.commit()

    except Exception as e:
        print(f"Error downloading EM industries: {e}")
        import traceback
        traceback.print_exc()

    return industries_count, mappings_count


def download_sw_industries(conn: sqlite3.Connection, levels: list = None) -> tuple[int, int]:
    """Download Shenwan (申万) industry classification.

    Args:
        conn: SQLite connection
        levels: List of levels to download [1, 2, 3], default [1]

    Returns:
        Tuple of (industries_count, mappings_count)
    """
    if levels is None:
        levels = [1]

    print("\n=== Downloading Shenwan Industry Classification ===\n")
    print(f"Levels to download: {levels}")

    cursor = conn.cursor()
    today = date.today().isoformat()

    # Clear existing SW data
    cursor.execute("DELETE FROM stock_industry_mapping WHERE classification_system = 'sw'")
    cursor.execute("DELETE FROM industry_classification WHERE classification_system = 'sw'")
    conn.commit()

    industries_count = 0
    mappings_count = 0

    # Download L1 industries
    if 1 in levels:
        l1_count, l1_map = _download_sw_level(conn, cursor, 1, today)
        industries_count += l1_count
        mappings_count += l1_map

    # Download L2 industries
    if 2 in levels:
        l2_count, l2_map = _download_sw_level(conn, cursor, 2, today)
        industries_count += l2_count
        mappings_count += l2_map

    # Download L3 industries
    if 3 in levels:
        l3_count, l3_map = _download_sw_level(conn, cursor, 3, today)
        industries_count += l3_count
        mappings_count += l3_map

    conn.commit()
    return industries_count, mappings_count


def _download_sw_level(conn: sqlite3.Connection, cursor, level: int, today: str) -> tuple[int, int]:
    """Download a specific level of Shenwan industries."""
    print(f"\n--- Downloading Shenwan L{level} industries ---")

    industries_count = 0
    mappings_count = 0

    try:
        # Fetch industry list based on level
        if level == 1:
            try:
                df = ak.sw_index_first_info()
            except:
                df = ak.sw_index_spot()
            prefix = "801"
        elif level == 2:
            try:
                df = ak.sw_index_second_info()
            except Exception as e:
                print(f"sw_index_second_info failed: {e}")
                return 0, 0
            prefix = "801"  # L2 codes also start with 801 but have different patterns
        elif level == 3:
            try:
                df = ak.sw_index_third_info()
            except Exception as e:
                print(f"sw_index_third_info failed: {e}")
                return 0, 0
            prefix = "851"  # L3 codes start with 851
        else:
            return 0, 0

        print(f"Found {len(df)} Shenwan L{level} industries")
        print(f"Columns: {df.columns.tolist()}")

        # Process industries
        for idx, row in df.iterrows():
            code = row.get("行业代码") or row.get("指数代码") or row.get("代码")
            name = row.get("行业名称") or row.get("指数名称") or row.get("名称")
            parent_code = row.get("父级行业代码") or row.get("一级行业代码") or None

            if not code or not name:
                continue

            code = str(code)
            # 去掉可能的重复名称后缀
            name = name.replace("(申万)", "").strip()

            # For L2, try to extract parent from code pattern or data
            if level == 2 and not parent_code:
                # L2 code pattern: 801XXY where XX is L1 code suffix
                # E.g., 801011 belongs to 801010
                try:
                    code_num = code.replace(".SI", "")
                    if len(code_num) >= 6:
                        parent_num = code_num[:5] + "0"
                        parent_code = parent_num
                except:
                    pass

            # For L3, try to determine parent L2 code
            if level == 3 and not parent_code:
                # This may need adjustment based on actual data structure
                try:
                    # Try to find parent_code from row data
                    parent_code = row.get("二级行业代码") or row.get("所属二级")
                except:
                    pass

            print(f"[{idx+1}/{len(df)}] Processing L{level}: {name} ({code})...")

            # Insert industry
            cursor.execute("""
                INSERT OR REPLACE INTO industry_classification
                (industry_code, industry_name, industry_level, parent_code, classification_system)
                VALUES (?, ?, ?, ?, ?)
            """, (code, name, level, parent_code, "sw"))
            industries_count += 1

            # Fetch constituents
            try:
                time.sleep(0.8)  # Rate limiting
                code_num = code.replace(".SI", "")
                cons_df = ak.index_component_sw(symbol=code_num)

                if cons_df is not None and len(cons_df) > 0:
                    print(f"  Found {len(cons_df)} stocks")

                    for _, stock_row in cons_df.iterrows():
                        stock_code = stock_row.get("证券代码") or stock_row.get("stock_code") or stock_row.get("代码")
                        if stock_code:
                            stock_code = normalize_stock_code(stock_code)

                            cursor.execute("""
                                INSERT OR REPLACE INTO stock_industry_mapping
                                (stock_code, industry_code, classification_system, effective_date, expire_date)
                                VALUES (?, ?, ?, ?, ?)
                            """, (stock_code, code, "sw", today, None))
                            mappings_count += 1
                else:
                    print(f"  No stocks found")

            except Exception as e:
                print(f"  Error fetching constituents: {e}")
                time.sleep(2)

            # Commit in batches
            if (idx + 1) % 10 == 0:
                conn.commit()
                print(f"  Committed batch, total L{level}: {industries_count} industries, {mappings_count} mappings")

        conn.commit()

    except Exception as e:
        print(f"Error downloading SW L{level} industries: {e}")
        import traceback
        traceback.print_exc()

    print(f"L{level} complete: {industries_count} industries, {mappings_count} mappings")
    return industries_count, mappings_count


def show_summary(conn: sqlite3.Connection):
    """Show database summary."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("Database Summary")
    print("=" * 60)

    # Industry counts by system
    cursor.execute("""
        SELECT classification_system, COUNT(*) as count
        FROM industry_classification
        GROUP BY classification_system
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]} industries: {row[1]}")

    # Mapping counts by system
    cursor.execute("""
        SELECT classification_system, COUNT(*) as count
        FROM stock_industry_mapping
        GROUP BY classification_system
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]} mappings: {row[1]}")

    # Total unique stocks
    cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_industry_mapping")
    total_stocks = cursor.fetchone()[0]
    print(f"  Total unique stocks: {total_stocks}")


def main():
    parser = argparse.ArgumentParser(
        description="Download industry classification data to SQLite"
    )
    parser.add_argument(
        "--system", "-s",
        choices=["all", "em", "sw"],
        default="all",
        help="Classification system to download (default: all)"
    )
    parser.add_argument(
        "--level", "-l",
        type=str,
        default="1",
        help="Shenwan industry levels to download, comma-separated (e.g., '1,2,3' for all levels, default: '1')"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DB_PATH,
        help=f"Output database path (default: {DB_PATH})"
    )

    args = parser.parse_args()

    # Parse levels
    levels = [int(x.strip()) for x in args.level.split(",") if x.strip().isdigit()]
    if not levels:
        levels = [1]

    print("=" * 60)
    print("Industry Classification Download")
    print("=" * 60)
    print(f"Output database: {args.output}")
    print(f"Classification system: {args.system}")
    print(f"Shenwan levels: {levels}")

    # Create/open database
    conn = create_database(args.output)

    try:
        if args.system in ("all", "em"):
            em_industries, em_mappings = download_em_industries(conn)
            print(f"\nEastMoney: {em_industries} industries, {em_mappings} mappings")

        if args.system in ("all", "sw"):
            sw_industries, sw_mappings = download_sw_industries(conn, levels=levels)
            print(f"\nShenwan: {sw_industries} industries, {sw_mappings} mappings")

        show_summary(conn)

    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
