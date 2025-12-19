#!/usr/bin/env python3
"""
ETF 日线数据下载脚本
使用 AkShare 获取 ETF 列表和日线数据（baostock 不支持 ETF K线数据）
存入独立的 SQLite 数据库: etf_{year}.db
支持增量下载、断点续传、失败重试
"""

import sqlite3
import akshare as ak
from tqdm import tqdm
from datetime import datetime, date
import time
import argparse
import json
import os

# 限流配置
REQUEST_DELAY = 0.3   # 每次请求后等待时间（秒），AkShare 需要更大间隔
BATCH_DELAY = 2.0     # 每批次后额外等待时间

# 缓存目录
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")


def get_db_path(year: int) -> str:
    """获取指定年份的数据库路径（cache 目录）"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"etf_{year}.db")


def get_failed_log_path(year: int) -> str:
    """获取指定年份的失败记录文件路径（cache 目录）"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"failed_etf_{year}.json")


def get_date_range(year: int) -> tuple:
    """获取指定年份的日期范围"""
    start_date = f"{year}-01-01"
    if year == 2025:
        end_date = date.today().strftime("%Y-%m-%d")
    else:
        end_date = f"{year}-12-31"
    return start_date, end_date


def get_etf_codes():
    """从 AkShare 获取所有 ETF 代码，带重试和备用方案"""
    print("正在从 AkShare 获取 ETF 列表...")

    # 方案1: fund_etf_spot_em (带重试)
    for attempt in range(3):
        try:
            time.sleep(2)  # 请求前等待
            df = ak.fund_etf_spot_em()
            codes = []
            for _, row in df.iterrows():
                code = str(row['代码'])
                name = str(row['名称'])
                if code.startswith('5'):
                    codes.append({'code': f'sh.{code}', 'code_name': name, 'tradeStatus': '1'})
                elif code.startswith('1'):
                    codes.append({'code': f'sz.{code}', 'code_name': name, 'tradeStatus': '1'})
            if codes:
                print(f"获取 ETF 共 {len(codes)} 只 (方案1)")
                return codes
        except Exception as e:
            print(f"方案1 第{attempt+1}次尝试失败: {e}")
            time.sleep(5)

    # 方案2: fund_etf_category_sina
    print("尝试备用方案2...")
    try:
        time.sleep(2)
        df = ak.fund_etf_category_sina(symbol="ETF基金")
        codes = []
        for _, row in df.iterrows():
            code = str(row['代码'])
            name = str(row['名称']) if '名称' in df.columns else ''
            if code.startswith('5'):
                codes.append({'code': f'sh.{code}', 'code_name': name, 'tradeStatus': '1'})
            elif code.startswith('1'):
                codes.append({'code': f'sz.{code}', 'code_name': name, 'tradeStatus': '1'})
        if codes:
            print(f"获取 ETF 共 {len(codes)} 只 (方案2)")
            return codes
    except Exception as e:
        print(f"方案2 失败: {e}")

    # 方案3: 使用代码模式生成常见 ETF 代码范围
    print("使用备用方案3: 代码模式生成...")
    codes = []
    # 上交所 ETF: 510000-519999
    for i in range(510000, 520000):
        codes.append({'code': f'sh.{i}', 'code_name': '', 'tradeStatus': '1'})
    # 深交所 ETF: 159000-159999
    for i in range(159000, 160000):
        codes.append({'code': f'sz.{i}', 'code_name': '', 'tradeStatus': '1'})
    print(f"生成 ETF 代码范围共 {len(codes)} 个 (方案3，将过滤无效代码)")
    return codes


def create_database(conn: sqlite3.Connection):
    """创建数据库表结构"""
    cursor = conn.cursor()

    # ETF 基本信息表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etf_basic (
            code TEXT PRIMARY KEY,
            code_name TEXT,
            ipo_date TEXT,
            out_date TEXT,
            type INTEGER DEFAULT 5,
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


def get_etf_basic(code: str, code_name: str = '') -> dict:
    """获取单只 ETF 的基本信息（从 AkShare 列表中已获取）"""
    return {
        'code': code,
        'code_name': code_name,
        'ipo_date': None,
        'out_date': None,
        'type': 5,  # ETF 类型
        'status': 1  # 正常交易
    }


def get_downloaded_basic_codes(conn: sqlite3.Connection) -> set:
    """获取已下载基本信息的 ETF 代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM etf_basic")
    return {row[0] for row in cursor.fetchall()}


def get_downloaded_daily_codes(conn: sqlite3.Connection) -> set:
    """获取已有日线数据的 ETF 代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM daily_k_data")
    return {row[0] for row in cursor.fetchall()}


def get_downloaded_adjust_codes(conn: sqlite3.Connection) -> set:
    """获取已有复权因子的 ETF 代码"""
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM adjust_factor")
    return {row[0] for row in cursor.fetchall()}


def load_failed_etfs(year: int) -> dict:
    """加载失败记录"""
    failed_log = get_failed_log_path(year)
    if os.path.exists(failed_log):
        with open(failed_log, 'r') as f:
            return json.load(f)
    return {"basic": [], "daily": [], "adjust": []}


def save_failed_etfs(year: int, failed: dict):
    """保存失败记录"""
    failed_log = get_failed_log_path(year)
    with open(failed_log, 'w') as f:
        json.dump(failed, f, indent=2)


def download_etf_basic(conn: sqlite3.Connection, etfs: list, skip_existing: bool = True):
    """保存 ETF 基本信息（从 AkShare 列表直接获取）"""
    cursor = conn.cursor()

    existing_codes = get_downloaded_basic_codes(conn) if skip_existing else set()
    to_download = [e for e in etfs if e['code'] not in existing_codes]

    if not to_download:
        print(f"\nETF 基本信息已全部下载完成 ({len(existing_codes)} 条)")
        return []

    print(f"\n正在保存 ETF 基本信息... (已有 {len(existing_codes)}, 待保存 {len(to_download)})")

    success_count = 0
    failed_etfs = []

    for etf in tqdm(to_download, desc="保存基本信息"):
        try:
            info = get_etf_basic(etf['code'], etf.get('code_name', ''))
            cursor.execute("""
                INSERT OR REPLACE INTO etf_basic
                (code, code_name, ipo_date, out_date, type, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (info['code'], info['code_name'], info['ipo_date'],
                  info['out_date'], info['type'], info['status']))
            success_count += 1
        except Exception as e:
            failed_etfs.append(etf['code'])

    conn.commit()
    print(f"ETF 基本信息保存完成: 本次 {success_count} 条, 累计 {len(existing_codes) + success_count} 条")
    if failed_etfs:
        print(f"失败: {len(failed_etfs)} 只")

    return failed_etfs


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


def download_daily_data(conn: sqlite3.Connection, etfs: list, start_date: str, end_date: str, skip_existing: bool = True):
    """下载日线数据（使用 AkShare fund_etf_hist_em）"""
    cursor = conn.cursor()

    existing_codes = get_downloaded_daily_codes(conn) if skip_existing else set()
    to_download = [e for e in etfs if e['code'] not in existing_codes]

    if not to_download:
        print(f"\n日线数据已全部下载完成 ({len(existing_codes)} 只 ETF)")
        return []

    print(f"\n正在下载 {start_date} 至 {end_date} 日线数据...")
    print(f"已有 {len(existing_codes)} 只, 待下载 {len(to_download)} 只")
    print("使用 AkShare fund_etf_hist_em API (baostock 不支持 ETF K线)")

    # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
    start_date_fmt = start_date.replace('-', '')
    end_date_fmt = end_date.replace('-', '')

    total_records = 0
    failed_etfs = []

    for i, etf in enumerate(tqdm(to_download, desc="下载日线数据")):
        code = etf['code']
        # 提取纯数字代码 (sh.510050 -> 510050)
        symbol = code.split('.')[1] if '.' in code else code

        try:
            # 使用 AkShare 获取 ETF 历史数据
            df = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                adjust=""  # 不复权
            )

            if df is None or df.empty:
                time.sleep(REQUEST_DELAY)
                continue

            batch = []
            for _, row in df.iterrows():
                # AkShare 返回字段: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
                date_str = str(row['日期'])
                batch.append((
                    date_str,                    # date
                    code,                        # code (保留 sh./sz. 前缀)
                    safe_float(row['开盘']),      # open
                    safe_float(row['最高']),      # high
                    safe_float(row['最低']),      # low
                    safe_float(row['收盘']),      # close
                    None,                        # preclose (AkShare 不直接提供)
                    safe_float(row['成交量']),    # volume
                    safe_float(row['成交额']),    # amount
                    safe_float(row.get('换手率')), # turn
                    1,                           # tradestatus (默认正常交易)
                    safe_float(row['涨跌幅'])     # pctChg
                ))

            if batch:
                cursor.executemany("""
                    INSERT OR IGNORE INTO daily_k_data
                    (date, code, open, high, low, close, preclose, volume, amount,
                     turn, tradestatus, pctChg)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                total_records += len(batch)

            time.sleep(REQUEST_DELAY)

            if (i + 1) % 50 == 0:
                conn.commit()
                time.sleep(BATCH_DELAY)

        except Exception as e:
            failed_etfs.append(code)
            time.sleep(REQUEST_DELAY)

    conn.commit()
    print(f"日线数据下载完成: 本次 {total_records} 条记录")
    if failed_etfs:
        print(f"失败 ETF: {len(failed_etfs)} 只")

    return failed_etfs


def download_adjust_factors(conn: sqlite3.Connection, etfs: list, start_date: str, end_date: str, skip_existing: bool = True):
    """下载复权因子（通过比较不复权和前复权数据计算）"""
    cursor = conn.cursor()

    etf_codes = [e['code'] for e in etfs]
    existing_codes = get_downloaded_adjust_codes(conn) if skip_existing else set()
    to_download = [c for c in etf_codes if c not in existing_codes]

    if not to_download:
        print(f"\n复权因子已全部下载完成 ({len(existing_codes)} 只 ETF)")
        return []

    print(f"\n正在计算 {start_date} 至 {end_date} 复权因子...")
    print(f"已有 {len(existing_codes)} 只, 待下载 {len(to_download)} 只")
    print("通过比较不复权和前复权数据计算复权因子")

    # 将日期格式从 YYYY-MM-DD 转换为 YYYYMMDD
    start_date_fmt = start_date.replace('-', '')
    end_date_fmt = end_date.replace('-', '')

    total_records = 0
    failed_etfs = []

    for i, code in enumerate(tqdm(to_download, desc="计算复权因子")):
        symbol = code.split('.')[1] if '.' in code else code

        try:
            # 获取不复权数据
            df_raw = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                adjust=""
            )

            time.sleep(REQUEST_DELAY / 3)

            # 获取前复权数据
            df_qfq = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                adjust="qfq"
            )

            time.sleep(REQUEST_DELAY / 3)

            # 获取后复权数据
            df_hfq = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                adjust="hfq"
            )

            if df_raw is None or df_qfq is None or df_hfq is None or df_raw.empty or df_qfq.empty or df_hfq.empty:
                time.sleep(REQUEST_DELAY)
                continue

            # 计算复权因子
            # 前复权因子: 前复权价格 / 不复权价格
            # 后复权因子: 后复权价格 / 不复权价格
            batch = []
            for idx, row_raw in df_raw.iterrows():
                date_str = str(row_raw['日期'])
                close_raw = safe_float(row_raw['收盘'])

                # 找到对应日期的前复权和后复权收盘价
                qfq_row = df_qfq[df_qfq['日期'] == row_raw['日期']]
                hfq_row = df_hfq[df_hfq['日期'] == row_raw['日期']]

                if not qfq_row.empty and not hfq_row.empty and close_raw and close_raw > 0:
                    close_qfq = safe_float(qfq_row.iloc[0]['收盘'])
                    close_hfq = safe_float(hfq_row.iloc[0]['收盘'])

                    if close_qfq and close_hfq:
                        fore_factor = close_qfq / close_raw
                        back_factor = close_hfq / close_raw

                        # 只保存复权因子有变化的日期（或首日）
                        if len(batch) == 0 or abs(fore_factor - batch[-1][2]) > 0.0001 or abs(back_factor - batch[-1][3]) > 0.0001:
                            batch.append((
                                code,           # code
                                date_str,       # dividOperateDate
                                fore_factor,    # foreAdjustFactor
                                back_factor,    # backAdjustFactor
                                fore_factor     # adjustFactor (使用前复权)
                            ))

            if batch:
                cursor.executemany("""
                    INSERT OR IGNORE INTO adjust_factor
                    (code, dividOperateDate, foreAdjustFactor, backAdjustFactor, adjustFactor)
                    VALUES (?, ?, ?, ?, ?)
                """, batch)
                total_records += len(batch)

            time.sleep(REQUEST_DELAY)

            if (i + 1) % 30 == 0:
                conn.commit()
                time.sleep(BATCH_DELAY)

        except Exception as e:
            failed_etfs.append(code)
            time.sleep(REQUEST_DELAY)

    conn.commit()
    print(f"复权因子计算完成: 本次 {total_records} 条记录")
    if failed_etfs:
        print(f"失败 ETF: {len(failed_etfs)} 只")

    return failed_etfs


def print_statistics(conn: sqlite3.Connection):
    """打印数据库统计信息"""
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("数据库统计信息")
    print("=" * 50)

    cursor.execute("SELECT COUNT(*) FROM etf_basic")
    print(f"ETF 基本信息: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(*) FROM daily_k_data")
    print(f"日线数据: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM daily_k_data")
    print(f"有日线数据的 ETF: {cursor.fetchone()[0]} 只")

    cursor.execute("SELECT MIN(date), MAX(date) FROM daily_k_data")
    row = cursor.fetchone()
    if row[0]:
        print(f"日期范围: {row[0]} 至 {row[1]}")

    cursor.execute("SELECT COUNT(*) FROM adjust_factor")
    print(f"复权因子: {cursor.fetchone()[0]} 条")

    cursor.execute("SELECT COUNT(DISTINCT code) FROM adjust_factor")
    print(f"有复权因子的 ETF: {cursor.fetchone()[0]} 只")

    print("=" * 50)


def download_year(year: int, mode: str = 'all', force: bool = False, delay: float = 0.05):
    """下载指定年份的数据"""
    global REQUEST_DELAY
    REQUEST_DELAY = delay

    start_date, end_date = get_date_range(year)
    db_path = get_db_path(year)

    start_time = time.time()
    print("\n" + "=" * 60)
    print(f"ETF {year} 年日线数据下载")
    print(f"日期范围: {start_date} 至 {end_date}")
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
    failed_record = load_failed_etfs(year)

    try:
        conn = sqlite3.connect(db_path)
        create_database(conn)

        # 获取 ETF 列表
        etfs = get_etf_codes()
        if not etfs:
            print("获取 ETF 列表失败，退出")
            return

        # 根据模式下载
        if mode == 'retry':
            print("\n=== 重试失败的 ETF ===")
            if failed_record['basic']:
                print(f"重试基本信息: {len(failed_record['basic'])} 只")
                retry_etfs = [{'code': c, 'code_name': ''} for c in failed_record['basic']]
                new_failed = download_etf_basic(conn, retry_etfs, skip_existing=False)
                failed_record['basic'] = new_failed

            if failed_record['daily']:
                print(f"重试日线数据: {len(failed_record['daily'])} 只")
                retry_etfs = [{'code': c, 'code_name': ''} for c in failed_record['daily']]
                new_failed = download_daily_data(conn, retry_etfs, start_date, end_date, skip_existing=False)
                failed_record['daily'] = new_failed

            if failed_record['adjust']:
                print(f"重试复权因子: {len(failed_record['adjust'])} 只")
                retry_etfs = [{'code': c} for c in failed_record['adjust']]
                new_failed = download_adjust_factors(conn, retry_etfs, start_date, end_date, skip_existing=False)
                failed_record['adjust'] = new_failed

        elif mode in ['all', 'basic']:
            failed = download_etf_basic(conn, etfs, skip_existing)
            failed_record['basic'] = failed

        if mode in ['all', 'daily']:
            failed = download_daily_data(conn, etfs, start_date, end_date, skip_existing)
            failed_record['daily'] = failed

        if mode in ['all', 'adjust']:
            failed = download_adjust_factors(conn, etfs, start_date, end_date, skip_existing)
            failed_record['adjust'] = failed

        save_failed_etfs(year, failed_record)
        print_statistics(conn)
        conn.close()

    except Exception as e:
        print(f"下载出错: {e}")

    elapsed = time.time() - start_time
    print(f"\n{year}年 ETF 数据下载耗时: {elapsed/60:.1f} 分钟")
    print(f"数据库已保存至: {db_path}")

    total_failed = len(failed_record['basic']) + len(failed_record['daily']) + len(failed_record['adjust'])
    if total_failed > 0:
        print(f"\n失败统计: 基本信息 {len(failed_record['basic'])}, 日线 {len(failed_record['daily'])}, 复权因子 {len(failed_record['adjust'])}")
        print(f"可运行 --year {year} --mode retry 重试失败的 ETF")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='ETF 日线数据下载工具')
    parser.add_argument('--year', type=int, help='下载指定年份')
    parser.add_argument('--years', type=str, help='下载多个年份，逗号分隔 (如: 2020,2021,2022)')
    parser.add_argument('--mode', choices=['all', 'basic', 'daily', 'adjust', 'retry', 'status'],
                        default='all', help='下载模式: all=全部, basic=基本信息, daily=日线, adjust=复权因子, retry=重试失败, status=查看状态')
    parser.add_argument('--force', action='store_true', help='强制重新下载（忽略已有数据）')
    parser.add_argument('--delay', type=float, default=0.05, help='请求间隔（秒），默认0.05')
    return parser.parse_args()


def main():
    args = parse_args()

    years_to_download = []

    if args.years:
        years_to_download = [int(y.strip()) for y in args.years.split(',')]
    elif args.year:
        years_to_download = [args.year]
    else:
        print("未指定年份，请使用 --year 或 --years 参数")
        print("示例:")
        print("  python download_etf_data.py --year 2024")
        print("  python download_etf_data.py --years 2019,2020,2021,2022,2023,2024,2025")
        print("  python download_etf_data.py --mode status --years 2019,2020,2021,2022,2023,2024,2025")
        return

    valid_years = []
    for year in years_to_download:
        if 2010 <= year <= 2025:
            valid_years.append(year)
        else:
            print(f"警告: 年份 {year} 不在有效范围 (2010-2025)，已跳过")

    if not valid_years:
        print("没有有效的年份")
        return

    if args.mode == 'status':
        for year in valid_years:
            download_year(year, args.mode, args.force, args.delay)
        return

    # AkShare 无需登录
    print("使用 AkShare 获取 ETF 数据（无需登录）")

    for year in valid_years:
        download_year(year, args.mode, args.force, args.delay)

    print("\n" + "=" * 60)
    print("全部下载完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
