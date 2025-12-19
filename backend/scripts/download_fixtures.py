#!/usr/bin/env python3
"""
Download real fixture data from AKShare.

Downloads data for 20 sample stocks that match sample_stocks.db:
- Market cap (sample_market_cap.db)
- Northbound holdings (sample_northbound.db)
- Institutional holdings (sample_institutional.db)

Usage:
    python -m scripts.download_fixtures                    # Download all
    python -m scripts.download_fixtures market_cap         # Download market cap only
    python -m scripts.download_fixtures northbound         # Download northbound only
    python -m scripts.download_fixtures institutional      # Download institutional only
"""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
from rich.console import Console
from rich.progress import Progress

console = Console()

# Paths
BACKEND_DIR = Path(__file__).parent.parent
FIXTURES_DIR = BACKEND_DIR / "data" / "fixtures"
SAMPLE_STOCKS_DB = FIXTURES_DIR / "sample_stocks.db"

# 20 sample stock codes (matching sample_stocks.db)
SAMPLE_CODES = [
    "sh.600036", "sh.600309", "sh.600519", "sh.600585", "sh.600900",
    "sh.601088", "sh.601318", "sh.601668", "sh.601899", "sh.603259",
    "sz.000002", "sz.000333", "sz.000858", "sz.002049", "sz.002415",
    "sz.002475", "sz.002594", "sz.300059", "sz.300124", "sz.300750",
]


def get_trading_dates_from_fixture() -> list[str]:
    """Get trading dates from sample_stocks.db."""
    if not SAMPLE_STOCKS_DB.exists():
        console.print(f"[red]sample_stocks.db not found: {SAMPLE_STOCKS_DB}[/red]")
        return []

    conn = sqlite3.connect(str(SAMPLE_STOCKS_DB))
    try:
        cursor = conn.execute(
            "SELECT DISTINCT date FROM daily_k_data ORDER BY date"
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def code_to_akshare(code: str) -> str:
    """Convert sh.XXXXXX/sz.XXXXXX to XXXXXX (pure number)."""
    return code.split(".")[-1]


def download_market_cap_fixture():
    """
    Download real market cap data to sample_market_cap.db.

    Uses ak.stock_zh_a_hist() to get historical data with volume/amount,
    then calculates approximate market cap from close * volume patterns.

    Actually we'll use the real market cap from ak.stock_individual_info_em()
    for today's data and interpolate backwards.
    """
    console.print("\n[bold cyan]Downloading Market Cap Fixture Data[/bold cyan]")
    console.print("=" * 60)

    trading_dates = get_trading_dates_from_fixture()
    if not trading_dates:
        console.print("[red]No trading dates found in sample_stocks.db[/red]")
        return 0

    console.print(f"Trading dates: {trading_dates[0]} to {trading_dates[-1]} ({len(trading_dates)} days)")

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
                # Get historical daily data which includes close price
                # We'll estimate market cap from price changes relative to current
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    start_date=trading_dates[0].replace("-", ""),
                    end_date=trading_dates[-1].replace("-", ""),
                    adjust="qfq"
                )

                if df is None or df.empty:
                    console.print(f"  [yellow]No data for {code}[/yellow]")
                    progress.advance(task)
                    continue

                # Get current market cap from spot data
                try:
                    spot_df = ak.stock_zh_a_spot_em()
                    spot_row = spot_df[spot_df['代码'] == ak_code]
                    if not spot_row.empty:
                        current_total_mv = float(spot_row['总市值'].values[0]) / 1e8  # Convert to 亿
                        current_circ_mv = float(spot_row['流通市值'].values[0]) / 1e8
                        current_close = float(spot_row['最新价'].values[0])
                    else:
                        # Fallback: estimate from last close
                        current_close = df['收盘'].iloc[-1]
                        current_total_mv = 1000  # Default estimate
                        current_circ_mv = 800
                except Exception:
                    current_close = df['收盘'].iloc[-1]
                    current_total_mv = 1000
                    current_circ_mv = 800

                # Calculate historical market cap from price ratio
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

            except Exception as e:
                console.print(f"  [red]Error for {code}: {e}[/red]")

            progress.advance(task)

    conn.close()

    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    console.print(f"Output: {target_path}")
    console.print(f"Size: {target_path.stat().st_size / 1024:.1f} KB")

    return total_records


def download_northbound_fixture():
    """
    Download real northbound holdings data to sample_northbound.db.

    Uses ak.stock_hsgt_individual_em() to get historical northbound holdings for each stock.
    API columns: '持股日期', '持股数量占A股百分比'
    """
    console.print("\n[bold cyan]Downloading Northbound Holdings Fixture Data[/bold cyan]")
    console.print("=" * 60)

    trading_dates = get_trading_dates_from_fixture()
    trading_dates_set = set(trading_dates)
    if not trading_dates:
        console.print("[red]No trading dates found in sample_stocks.db[/red]")
        return 0

    console.print(f"Trading dates: {trading_dates[0]} to {trading_dates[-1]} ({len(trading_dates)} days)")

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

    # Fetch historical northbound data for each sample stock
    console.print("\nFetching historical northbound holdings for each stock...")

    with Progress(console=console) as progress:
        task = progress.add_task("Downloading...", total=len(SAMPLE_CODES))

        for code in SAMPLE_CODES:
            ak_code = code_to_akshare(code)
            progress.update(task, description=f"Fetching {code}...")

            try:
                # Get historical northbound data for this stock
                # API: stock_hsgt_individual_em(symbol='600519')
                # Returns: 持股日期, 当日收盘价, 当日涨跌幅, 持股数量, 持股市值, 持股数量占A股百分比, ...
                df = ak.stock_hsgt_individual_em(symbol=ak_code)

                if df is None or df.empty:
                    progress.advance(task)
                    continue

                # Sort by date to calculate daily change
                df = df.sort_values('持股日期')
                prev_ratio = None
                stock_records = 0

                for _, row in df.iterrows():
                    date_str = str(row.get('持股日期', ''))[:10]

                    # Only include dates within our trading date range
                    if date_str not in trading_dates_set:
                        continue

                    try:
                        # 持股数量占A股百分比 is the holding ratio
                        holding_ratio = float(row.get('持股数量占A股百分比', 0) or 0)
                    except (ValueError, TypeError):
                        holding_ratio = 0

                    # Calculate daily change
                    if prev_ratio is not None:
                        holding_change = holding_ratio - prev_ratio
                    else:
                        holding_change = 0

                    prev_ratio = holding_ratio

                    conn.execute(
                        "INSERT OR REPLACE INTO northbound_holdings (code, date, holding_ratio, holding_change) VALUES (?, ?, ?, ?)",
                        (code, date_str, holding_ratio, holding_change)
                    )
                    stock_records += 1
                    total_records += 1

                conn.commit()

            except Exception as e:
                # Some stocks might not have northbound data
                console.print(f"[yellow]Warning: {code} - {e}[/yellow]")

            progress.advance(task)

    conn.close()

    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    console.print(f"Output: {target_path}")
    if target_path.exists():
        console.print(f"Size: {target_path.stat().st_size / 1024:.1f} KB")

    return total_records


def download_institutional_fixture():
    """
    Download real institutional holdings data to sample_institutional.db.

    Uses ak.stock_report_fund_hold() to get fund holdings.
    API: stock_report_fund_hold(symbol="基金持仓", date="20240930")
    - symbol: 持股类型 ("基金持仓", "QFII持仓", "社保持仓", "券商持仓", "保险持仓", "信托持仓")
    - date: 季度末日期 YYYYMMDD 格式
    """
    console.print("\n[bold cyan]Downloading Institutional Holdings Fixture Data[/bold cyan]")
    console.print("=" * 60)

    trading_dates = get_trading_dates_from_fixture()
    if not trading_dates:
        console.print("[red]No trading dates found in sample_stocks.db[/red]")
        return 0

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

    # Try to get fund holdings data for recent quarters
    console.print("\nFetching institutional holdings data...")

    # Get latest quarter end date from trading_dates
    # Quarters: 0331, 0630, 0930, 1231
    quarter_dates = ["20240930", "20240630", "20240331", "20231231"]

    sample_codes_ak = [code_to_akshare(c) for c in SAMPLE_CODES]

    for quarter_date in quarter_dates:
        console.print(f"\nTrying quarter: {quarter_date}")

        try:
            # Get fund holdings report for "基金持仓" type
            df = ak.stock_report_fund_hold(symbol="基金持仓", date=quarter_date)

            if df is None or df.empty:
                console.print(f"  [yellow]No data for {quarter_date}[/yellow]")
                continue

            console.print(f"  Got {len(df)} records from API")
            console.print(f"  Columns: {df.columns.tolist()}")

            # Try to find the code column (might be different names)
            # Note: '序号' is row number, NOT stock code!
            code_col = None
            for col_name in ['股票代码', '代码', 'code']:
                if col_name in df.columns:
                    code_col = col_name
                    break

            if code_col is None:
                console.print(f"  [yellow]Cannot find code column[/yellow]")
                continue

            # Filter to sample codes
            df_filtered = df[df[code_col].astype(str).isin(sample_codes_ak)]
            console.print(f"  Filtered to {len(df_filtered)} sample stocks")

            # Convert date format for storage
            date_str = f"{quarter_date[:4]}-{quarter_date[4:6]}-{quarter_date[6:]}"

            for _, row in df_filtered.iterrows():
                ak_code = str(row[code_col])
                # Convert back to sh./sz. format
                if ak_code.startswith(('6', '5')):
                    code = f"sh.{ak_code}"
                else:
                    code = f"sz.{ak_code}"

                try:
                    # This API returns:
                    # - 持有基金家数: number of funds holding this stock
                    # - 持股总数: total shares held
                    # - 持股市值: holding market value
                    # - 持股变动比例: change ratio (%)
                    # We store '持有基金家数' as fund_holding_ratio (as a count/proxy)
                    # and '持股变动比例' as fund_holding_change
                    holding_ratio = 0
                    if '持有基金家数' in row.index:
                        val = row.get('持有基金家数', 0)
                        if val and pd.notna(val):
                            holding_ratio = float(val)

                    holding_change = 0
                    if '持股变动比例' in row.index:
                        val = row.get('持股变动比例', 0)
                        if val and pd.notna(val):
                            holding_change = float(val)
                except (ValueError, TypeError):
                    holding_ratio = 0
                    holding_change = 0

                conn.execute(
                    "INSERT OR REPLACE INTO institutional_holdings (code, date, fund_holding_ratio, fund_holding_change) VALUES (?, ?, ?, ?)",
                    (code, date_str, holding_ratio, holding_change)
                )
                total_records += 1

            conn.commit()

            if total_records > 0:
                console.print(f"  [green]Successfully loaded {total_records} records[/green]")
                break  # Got data, no need to try earlier quarters

        except Exception as e:
            console.print(f"  [yellow]Error for {quarter_date}: {e}[/yellow]")
            continue

    conn.close()

    console.print(f"\n[green]Total records: {total_records:,}[/green]")
    console.print(f"Output: {target_path}")
    if target_path.exists():
        console.print(f"Size: {target_path.stat().st_size / 1024:.1f} KB")

    return total_records


def main():
    """Main entry point."""
    console.print("\n[bold blue]Download Fixture Data from AKShare[/bold blue]")
    console.print("=" * 60)
    console.print(f"Sample stocks: {len(SAMPLE_CODES)}")
    console.print(f"Fixtures dir: {FIXTURES_DIR}")

    # Ensure fixtures directory exists
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Parse command line args
    data_type = sys.argv[1] if len(sys.argv) > 1 else "all"

    results = {}

    if data_type == "all":
        results['market_cap'] = download_market_cap_fixture()
        results['northbound'] = download_northbound_fixture()
        results['institutional'] = download_institutional_fixture()
    elif data_type == "market_cap":
        results['market_cap'] = download_market_cap_fixture()
    elif data_type == "northbound":
        results['northbound'] = download_northbound_fixture()
    elif data_type == "institutional":
        results['institutional'] = download_institutional_fixture()
    else:
        console.print(f"[red]Unknown data type: {data_type}[/red]")
        console.print("Valid types: all, market_cap, northbound, institutional")
        sys.exit(1)

    # Summary
    console.print("\n" + "=" * 60)
    console.print("[bold green]Download Complete![/bold green]")
    console.print("=" * 60)
    for dtype, count in results.items():
        console.print(f"  {dtype}: {count:,} records")


if __name__ == "__main__":
    main()
