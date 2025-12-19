#!/usr/bin/env python3
"""
Fixture Data Management - Unified script for generating and downloading fixtures.

Combines the functionality of:
- generate_fixtures.py (extract from cache)
- download_fixtures.py (download from API)

Usage:
    python -m scripts.fixtures generate --stocks 100 --days 30
    python -m scripts.fixtures download market_cap
    python -m scripts.fixtures download all
    python -m scripts.fixtures status
"""

import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import typer
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

# Paths
BACKEND_DIR = Path(__file__).parent.parent
DATA_DIR = BACKEND_DIR / "data"
FIXTURES_DIR = DATA_DIR / "fixtures"
CACHE_DIR = DATA_DIR / "cache"

console = Console()
app = typer.Typer(
    name="fixtures",
    help="Fixture data management for development",
    add_completion=False,
)


# =============================================================================
# Sample Stock Lists (for generate and download)
# =============================================================================

# 20 sample codes used by download_fixtures
SAMPLE_CODES = [
    "sh.600036", "sh.600309", "sh.600519", "sh.600585", "sh.600900",
    "sh.601088", "sh.601318", "sh.601668", "sh.601899", "sh.603259",
    "sz.000002", "sz.000333", "sz.000858", "sz.002049", "sz.002415",
    "sz.002475", "sz.002594", "sz.300059", "sz.300124", "sz.300750",
]

# Large cap - HS300 representatives
HS300_SAMPLES = [
    "sh.600519", "sh.601318", "sh.600036", "sz.000858", "sh.600276",
    "sz.000333", "sh.601899", "sz.002415", "sh.600900", "sh.601012",
    "sz.300750", "sh.600030", "sh.600887", "sz.000001", "sh.601166",
    "sz.002304", "sh.600809", "sz.000568", "sh.600048", "sh.601668",
    "sz.002594", "sh.600585", "sh.603259", "sz.300059", "sh.600438",
    "sh.601888", "sz.002352", "sh.600309", "sz.000725", "sh.601919",
]

# Mid cap - ZZ500 representatives
ZZ500_SAMPLES = [
    "sh.600196", "sz.002241", "sz.000063", "sh.600016", "sz.002049",
    "sh.600183", "sz.000661", "sz.002032", "sh.603288", "sz.000009",
    "sh.600763", "sz.002008", "sz.000538", "sh.600031", "sz.002230",
    "sh.600089", "sz.000100", "sz.002001", "sh.600346", "sz.002138",
    "sh.603899", "sz.002027", "sh.600079", "sz.000423", "sz.002074",
    "sh.600104", "sz.000651", "sz.002142", "sh.600570", "sz.000166",
]

# Small cap - ZZ1000 representatives
ZZ1000_SAMPLES = [
    "sz.300014", "sz.300124", "sz.300015", "sz.300274", "sz.300033",
    "sz.300122", "sz.300142", "sz.300760", "sz.300347", "sz.300003",
    "sz.300413", "sz.300502", "sz.300433", "sz.300496", "sz.300308",
    "sz.300012", "sz.300285", "sz.300136", "sz.300017", "sz.300024",
]

# Sample ETFs
SAMPLE_ETFS = [
    "sh.510050", "sh.510300", "sh.510500", "sh.512100", "sh.512010",
    "sh.512880", "sh.512690", "sh.512660", "sz.159915", "sz.159919",
    "sh.518880", "sh.511010", "sh.511260", "sh.513050", "sh.513100",
    "sh.513500", "sz.159941", "sz.159920", "sh.510330", "sz.159901",
]


# =============================================================================
# Helper Functions
# =============================================================================

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
    start_date = end_date - timedelta(days=days + 10)
    return start_date.strftime("%Y-%m-%d"), latest_date


def get_available_codes(conn: sqlite3.Connection, target_codes: List[str]) -> List[str]:
    """Filter codes that actually exist in the database."""
    cursor = conn.execute("SELECT DISTINCT code FROM stock_basic")
    available = {row[0] for row in cursor.fetchall()}
    return [code for code in target_codes if code in available]


def code_to_akshare(code: str) -> str:
    """Convert sh.XXXXXX/sz.XXXXXX to XXXXXX (pure number)."""
    return code.split(".")[-1]


# =============================================================================
# Generate Functions (from cache)
# =============================================================================

def extract_stock_data(
    source_path: Path,
    target_path: Path,
    codes: List[str],
    start_date: str,
    end_date: str,
) -> int:
    """Extract stock data for specified codes and date range."""
    if not source_path.exists():
        console.print(f"  [yellow]Source not found: {source_path}[/yellow]")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    available_codes = get_available_codes(source_conn, codes)
    console.print(f"  Available codes: {len(available_codes)}/{len(codes)}")

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

    # Copy stock_basic
    code_list = "','".join(available_codes)
    cursor = source_conn.execute(f"SELECT * FROM stock_basic WHERE code IN ('{code_list}')")
    rows = cursor.fetchall()
    target_conn.executemany("INSERT OR REPLACE INTO stock_basic VALUES (?,?,?,?,?,?)", rows)
    console.print(f"  Copied {len(rows)} stock_basic records")

    # Copy daily_k_data
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
    console.print(f"  Copied {len(rows)} daily_k_data records")

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
        console.print(f"  [yellow]Source not found: {source_path}[/yellow]")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

    cursor = source_conn.execute("SELECT DISTINCT code FROM etf_basic")
    available = {row[0] for row in cursor.fetchall()}
    available_codes = [code for code in codes if code in available]
    console.print(f"  Available ETF codes: {len(available_codes)}/{len(codes)}")

    if not available_codes:
        source_conn.close()
        target_conn.close()
        return 0

    # Create target tables
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
    cursor = source_conn.execute(f"SELECT * FROM etf_basic WHERE code IN ('{code_list}')")
    rows = cursor.fetchall()
    if rows:
        target_conn.executemany("INSERT OR REPLACE INTO etf_basic VALUES (?,?,?,?,?,?)", rows)
        console.print(f"  Copied {len(rows)} etf_basic records")

    # Copy daily_k_data
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
    console.print(f"  Copied {len(rows)} ETF daily_k_data records")

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
        console.print(f"  [yellow]Source not found: {source_path}[/yellow]")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

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
        console.print(f"  Copied {len(rows)} index_constituents records")

    target_conn.commit()

    source_conn.close()
    target_conn.close()

    return len(rows) if rows else 0


def extract_industry_data(source_path: Path, target_path: Path) -> int:
    """Extract industry classification data."""
    if not source_path.exists():
        console.print(f"  [yellow]Source not found: {source_path}[/yellow]")
        return 0

    source_conn = sqlite3.connect(str(source_path))
    target_conn = sqlite3.connect(str(target_path))

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
            console.print(f"  Copied {len(rows)} industry_classification records")
    except Exception as e:
        console.print(f"  [yellow]Error copying industry_classification: {e}[/yellow]")

    # Copy stock_industry_mapping
    mapping_count = 0
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
            mapping_count = len(rows)
            console.print(f"  Copied {len(rows)} stock_industry_mapping records")
    except Exception as e:
        console.print(f"  [yellow]Error copying stock_industry_mapping: {e}[/yellow]")

    target_conn.commit()

    source_conn.close()
    target_conn.close()

    return mapping_count


# =============================================================================
# Download Functions (from API)
# =============================================================================

def download_market_cap_fixture(trading_dates: list) -> int:
    """Download market cap data from API for sample stocks."""
    import akshare as ak

    console.print("\n[bold cyan]Downloading Market Cap Fixture Data[/bold cyan]")

    target_path = FIXTURES_DIR / "sample_market_cap.db"
    if target_path.exists():
        target_path.unlink()

    conn = sqlite3.connect(str(target_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_market_cap (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            total_mv REAL,
            circ_mv REAL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_cap_date ON stock_market_cap(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_cap_code ON stock_market_cap(code)")

    total_records = 0

    with Progress(console=console) as progress:
        task = progress.add_task("Downloading...", total=len(SAMPLE_CODES))

        for code in SAMPLE_CODES:
            ak_code = code_to_akshare(code)
            progress.update(task, description=f"Downloading {code}...")

            try:
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    start_date=trading_dates[0].replace("-", ""),
                    end_date=trading_dates[-1].replace("-", ""),
                    adjust="qfq"
                )

                if df is None or df.empty:
                    progress.advance(task)
                    continue

                # Get current market cap
                try:
                    spot_df = ak.stock_zh_a_spot_em()
                    spot_row = spot_df[spot_df['代码'] == ak_code]
                    if not spot_row.empty:
                        current_total_mv = float(spot_row['总市值'].values[0]) / 1e8
                        current_circ_mv = float(spot_row['流通市值'].values[0]) / 1e8
                        current_close = float(spot_row['最新价'].values[0])
                    else:
                        current_close = df['收盘'].iloc[-1]
                        current_total_mv = 1000
                        current_circ_mv = 800
                except Exception:
                    current_close = df['收盘'].iloc[-1]
                    current_total_mv = 1000
                    current_circ_mv = 800

                records = []
                for _, row in df.iterrows():
                    date_str = str(row['日期'])[:10]
                    if date_str not in trading_dates:
                        continue

                    hist_close = row['收盘']
                    if current_close > 0:
                        ratio = hist_close / current_close
                        hist_total_mv = current_total_mv * ratio
                        hist_circ_mv = current_circ_mv * ratio
                    else:
                        hist_total_mv = current_total_mv
                        hist_circ_mv = current_circ_mv

                    records.append((code, date_str, round(hist_total_mv, 2), round(hist_circ_mv, 2)))

                if records:
                    conn.executemany(
                        "INSERT OR REPLACE INTO stock_market_cap (code, date, total_mv, circ_mv) VALUES (?, ?, ?, ?)",
                        records
                    )
                    conn.commit()
                    total_records += len(records)

                time.sleep(0.3)  # Rate limit

            except Exception as e:
                console.print(f"  [red]Error for {code}: {e}[/red]")

            progress.advance(task)

    conn.close()
    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    return total_records


def download_northbound_fixture(trading_dates: list) -> int:
    """Download northbound holdings data from API for sample stocks."""
    import akshare as ak

    console.print("\n[bold cyan]Downloading Northbound Holdings Fixture Data[/bold cyan]")

    target_path = FIXTURES_DIR / "sample_northbound.db"
    if target_path.exists():
        target_path.unlink()

    conn = sqlite3.connect(str(target_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS northbound_holdings (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            holding_ratio REAL,
            holding_change REAL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_northbound_date ON northbound_holdings(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_northbound_code ON northbound_holdings(code)")

    total_records = 0
    trading_dates_set = set(trading_dates)

    with Progress(console=console) as progress:
        task = progress.add_task("Downloading...", total=len(SAMPLE_CODES))

        for code in SAMPLE_CODES:
            ak_code = code_to_akshare(code)
            progress.update(task, description=f"Fetching {code}...")

            try:
                df = ak.stock_hsgt_individual_em(symbol=ak_code)

                if df is None or df.empty:
                    progress.advance(task)
                    continue

                df = df.sort_values('持股日期')
                prev_ratio = None

                for _, row in df.iterrows():
                    date_str = str(row.get('持股日期', ''))[:10]
                    if date_str not in trading_dates_set:
                        continue

                    try:
                        holding_ratio = float(row.get('持股数量占A股百分比', 0) or 0)
                    except (ValueError, TypeError):
                        holding_ratio = 0

                    holding_change = holding_ratio - prev_ratio if prev_ratio is not None else 0
                    prev_ratio = holding_ratio

                    conn.execute(
                        "INSERT OR REPLACE INTO northbound_holdings (code, date, holding_ratio, holding_change) VALUES (?, ?, ?, ?)",
                        (code, date_str, holding_ratio, holding_change)
                    )
                    total_records += 1

                conn.commit()
                time.sleep(0.3)  # Rate limit

            except Exception as e:
                console.print(f"[yellow]Warning: {code} - {e}[/yellow]")

            progress.advance(task)

    conn.close()
    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    return total_records


def download_institutional_fixture() -> int:
    """Download institutional holdings data from API for sample stocks."""
    import akshare as ak
    import pandas as pd

    console.print("\n[bold cyan]Downloading Institutional Holdings Fixture Data[/bold cyan]")

    target_path = FIXTURES_DIR / "sample_institutional.db"
    if target_path.exists():
        target_path.unlink()

    conn = sqlite3.connect(str(target_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS institutional_holdings (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            fund_holding_ratio REAL,
            fund_holding_change REAL,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_institutional_date ON institutional_holdings(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_institutional_code ON institutional_holdings(code)")

    total_records = 0
    sample_codes_ak = [code_to_akshare(c) for c in SAMPLE_CODES]
    quarter_dates = ["20240930", "20240630", "20240331", "20231231"]

    for quarter_date in quarter_dates:
        console.print(f"\nTrying quarter: {quarter_date}")

        try:
            df = ak.stock_report_fund_hold(symbol="基金持仓", date=quarter_date)

            if df is None or df.empty:
                continue

            code_col = None
            for col_name in ['股票代码', '代码', 'code']:
                if col_name in df.columns:
                    code_col = col_name
                    break

            if code_col is None:
                continue

            df_filtered = df[df[code_col].astype(str).isin(sample_codes_ak)]
            date_str = f"{quarter_date[:4]}-{quarter_date[4:6]}-{quarter_date[6:]}"

            for _, row in df_filtered.iterrows():
                ak_code = str(row[code_col])
                code = f"sh.{ak_code}" if ak_code.startswith(('6', '5')) else f"sz.{ak_code}"

                try:
                    holding_ratio = float(row.get('持有基金家数', 0) or 0) if '持有基金家数' in row.index and pd.notna(row.get('持有基金家数')) else 0
                    holding_change = float(row.get('持股变动比例', 0) or 0) if '持股变动比例' in row.index and pd.notna(row.get('持股变动比例')) else 0
                except (ValueError, TypeError):
                    holding_ratio, holding_change = 0, 0

                conn.execute(
                    "INSERT OR REPLACE INTO institutional_holdings (code, date, fund_holding_ratio, fund_holding_change) VALUES (?, ?, ?, ?)",
                    (code, date_str, holding_ratio, holding_change)
                )
                total_records += 1

            conn.commit()

            if total_records > 0:
                console.print(f"  [green]Successfully loaded {total_records} records[/green]")
                break

        except Exception as e:
            console.print(f"  [yellow]Error for {quarter_date}: {e}[/yellow]")

    conn.close()
    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    return total_records


# =============================================================================
# CLI Commands
# =============================================================================

@app.command()
def generate(
    stocks: int = typer.Option(100, "--stocks", "-s", help="Number of sample stocks"),
    days: int = typer.Option(30, "--days", "-d", help="Number of days of history"),
    source: str = typer.Option(None, "--source", help="Source data directory (default: data/cache/)"),
):
    """
    Generate fixture files from cache dataset.

    Creates sample databases for development:
    - sample_stocks.db (100 stocks x 30 days)
    - sample_etfs.db (20 ETFs x 30 days)
    - sample_indices.db (index constituents)
    - sample_industries.db (industry classification)
    """
    console.print("\n[bold blue]Generating Fixture Files from Cache[/bold blue]\n")

    source_dir = Path(source) if source else CACHE_DIR
    console.print(f"Source: {source_dir}")
    console.print(f"Parameters: {stocks} stocks, {days} days\n")

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    stats = {}

    # 1. Generate stock fixture
    console.print("[cyan]Generating sample_stocks.db...[/cyan]")
    stock_dbs = sorted(source_dir.glob("a_stock_*.db"), reverse=True)
    if stock_dbs:
        latest_stock_db = stock_dbs[0]
        console.print(f"Using: {latest_stock_db.name}")

        conn = sqlite3.connect(str(latest_stock_db))
        latest_date = get_latest_date(conn)
        conn.close()

        if latest_date:
            start_date, end_date = get_date_range(latest_date, days)
            console.print(f"Date range: {start_date} to {end_date}")

            all_codes = list(set(HS300_SAMPLES[:30] + ZZ500_SAMPLES[:30] + ZZ1000_SAMPLES[:20]))[:stocks]

            target_path = FIXTURES_DIR / "sample_stocks.db"
            count = extract_stock_data(latest_stock_db, target_path, all_codes, start_date, end_date)
            stats["stocks"] = count
            if target_path.exists():
                console.print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")
    else:
        console.print("  [yellow]No stock databases found![/yellow]")

    # 2. Generate ETF fixture
    console.print("\n[cyan]Generating sample_etfs.db...[/cyan]")
    etf_dbs = sorted(source_dir.glob("etf_*.db"), reverse=True)
    if etf_dbs and latest_date:
        latest_etf_db = etf_dbs[0]
        target_path = FIXTURES_DIR / "sample_etfs.db"
        count = extract_etf_data(latest_etf_db, target_path, SAMPLE_ETFS, start_date, end_date)
        stats["etfs"] = count
        if target_path.exists():
            console.print(f"Created: {target_path.name} ({target_path.stat().st_size / 1024 / 1024:.2f} MB)")
    else:
        console.print("  [yellow]No ETF databases found![/yellow]")

    # 3. Generate index fixture
    console.print("\n[cyan]Generating sample_indices.db...[/cyan]")
    index_db = source_dir / "index_constituents.db"
    target_path = FIXTURES_DIR / "sample_indices.db"
    count = extract_index_data(index_db, target_path)
    stats["indices"] = count

    # 4. Generate industry fixture
    console.print("\n[cyan]Generating sample_industries.db...[/cyan]")
    industry_db = source_dir / "industry_classification.db"
    target_path = FIXTURES_DIR / "sample_industries.db"
    count = extract_industry_data(industry_db, target_path)
    stats["industries"] = count

    # Summary
    console.print("\n[bold green]Generation Complete![/bold green]")
    console.print(f"Stats: {stats}")


@app.command()
def download(
    fixture_type: str = typer.Argument("all", help="Fixture type: market_cap, northbound, institutional, all"),
):
    """
    Download fixture data from API (market cap, northbound, institutional).

    Uses AKShare to download real data for the 20 sample stocks.
    Includes rate limiting to avoid being throttled.
    """
    console.print("\n[bold blue]Downloading Fixture Data from API[/bold blue]\n")

    # Get trading dates from sample_stocks.db
    sample_stocks_db = FIXTURES_DIR / "sample_stocks.db"
    if not sample_stocks_db.exists():
        console.print("[red]sample_stocks.db not found![/red]")
        console.print("Run 'fixtures generate' first to create sample_stocks.db")
        raise typer.Exit(1)

    conn = sqlite3.connect(str(sample_stocks_db))
    cursor = conn.execute("SELECT DISTINCT date FROM daily_k_data ORDER BY date")
    trading_dates = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not trading_dates:
        console.print("[red]No trading dates found in sample_stocks.db[/red]")
        raise typer.Exit(1)

    console.print(f"Trading dates: {trading_dates[0]} to {trading_dates[-1]} ({len(trading_dates)} days)")

    results = {}

    if fixture_type == "all":
        results['market_cap'] = download_market_cap_fixture(trading_dates)
        results['northbound'] = download_northbound_fixture(trading_dates)
        results['institutional'] = download_institutional_fixture()
    elif fixture_type == "market_cap":
        results['market_cap'] = download_market_cap_fixture(trading_dates)
    elif fixture_type == "northbound":
        results['northbound'] = download_northbound_fixture(trading_dates)
    elif fixture_type == "institutional":
        results['institutional'] = download_institutional_fixture()
    else:
        console.print(f"[red]Unknown fixture type: {fixture_type}[/red]")
        console.print("Valid types: all, market_cap, northbound, institutional")
        raise typer.Exit(1)

    console.print("\n[bold green]Download Complete![/bold green]")
    for dtype, count in results.items():
        console.print(f"  {dtype}: {count:,} records")


@app.command()
def status():
    """Show fixture files status."""
    console.print("\n[bold blue]Fixture Files Status[/bold blue]\n")

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    fixture_files = list(FIXTURES_DIR.glob("*.db"))
    if not fixture_files:
        console.print("[yellow]No fixture files found[/yellow]")
        console.print(f"Directory: {FIXTURES_DIR}")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("File")
    table.add_column("Size", justify="right")
    table.add_column("Records", justify="right")

    total_size = 0
    for f in sorted(fixture_files):
        size = f.stat().st_size / 1024
        total_size += size

        # Get record count
        try:
            conn = sqlite3.connect(str(f))
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            total_records = 0
            for table_name in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_records += cursor.fetchone()[0]
                except:
                    pass
            conn.close()
            records = f"{total_records:,}"
        except:
            records = "-"

        size_str = f"{size:.1f} KB" if size < 1024 else f"{size/1024:.2f} MB"
        table.add_row(f.name, size_str, records)

    console.print(table)
    console.print(f"\nTotal size: {total_size/1024:.2f} MB")
    console.print(f"Directory: {FIXTURES_DIR}")


if __name__ == "__main__":
    app()
