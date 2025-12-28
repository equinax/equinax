#!/usr/bin/env python3
"""
Compute Stock Classification Categories

This script computes style factor classifications for stocks:
1. size_category - Based on market cap thresholds (MEGA/LARGE/MID/SMALL/MICRO)
2. vol_category - Based on 20-day volatility percentiles (HIGH/NORMAL/LOW)
3. value_category - Based on E/P ratio percentiles (VALUE/NEUTRAL/GROWTH)

The computed values are stored in the stock_style_exposure table.

Usage:
    # Compute for latest date
    python -m scripts.compute_classifications

    # Compute for specific date
    python -m scripts.compute_classifications --date 2024-01-15

    # Compute for date range
    python -m scripts.compute_classifications --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import asyncio
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional, List

import asyncpg

# Default paths
SCRIPT_DIR = Path(__file__).parent
DEFAULT_POSTGRES_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://quant:quant_dev_password@localhost:5432/quantdb"
).replace("+asyncpg", "").replace("postgresql+asyncpg", "postgresql")


# =============================================================================
# Size Category Classification
# =============================================================================

def get_size_category(market_cap_yi: Optional[Decimal]) -> Optional[str]:
    """
    Classify stock by market cap (in 亿元).

    Categories:
    - MEGA: >= 1000亿
    - LARGE: 200-1000亿
    - MID: 50-200亿
    - SMALL: 10-50亿
    - MICRO: < 10亿
    """
    if market_cap_yi is None:
        return None

    if market_cap_yi >= 1000:
        return "MEGA"
    elif market_cap_yi >= 200:
        return "LARGE"
    elif market_cap_yi >= 50:
        return "MID"
    elif market_cap_yi >= 10:
        return "SMALL"
    else:
        return "MICRO"


# =============================================================================
# Value Category Classification
# =============================================================================

def get_value_category_by_percentile(percentile: Optional[float]) -> Optional[str]:
    """
    Classify stock by E/P ratio percentile.

    Categories:
    - VALUE: top 30% E/P (high E/P = low PE = value stock)
    - GROWTH: bottom 30% E/P (low E/P = high PE = growth stock)
    - NEUTRAL: middle 40%
    """
    if percentile is None:
        return None

    if percentile >= 0.7:
        return "VALUE"
    elif percentile <= 0.3:
        return "GROWTH"
    else:
        return "NEUTRAL"


# =============================================================================
# Volatility Category Classification
# =============================================================================

def get_vol_category_by_percentile(percentile: Optional[float]) -> Optional[str]:
    """
    Classify stock by volatility percentile.

    Categories:
    - HIGH: top 30% volatility
    - LOW: bottom 30% volatility
    - NORMAL: middle 40%
    """
    if percentile is None:
        return None

    if percentile >= 0.7:
        return "HIGH"
    elif percentile <= 0.3:
        return "LOW"
    else:
        return "NORMAL"


# =============================================================================
# Main Computation Function
# =============================================================================

async def compute_classifications_for_date(
    pg_conn: asyncpg.Connection,
    target_date: date
) -> int:
    """Compute all classifications for a given date."""
    print(f"\nComputing classifications for {target_date}...")

    # Get all stocks with market data for this date (excluding indices and ETFs)
    stocks = await pg_conn.fetch("""
        SELECT
            m.code,
            m.date,
            m.turn,
            m.pct_chg,
            v.circ_mv,
            v.pe_ttm
        FROM market_daily m
        JOIN asset_meta am ON m.code = am.code
        LEFT JOIN indicator_valuation v ON m.code = v.code AND m.date = v.date
        WHERE m.date = $1
          AND am.asset_type = 'STOCK'
          AND am.status = 1
    """, target_date)

    if not stocks:
        print(f"  No market data found for {target_date}")
        return 0

    print(f"  Found {len(stocks)} stocks with market data")

    # Collect market caps and E/P ratios for percentile calculation
    market_caps = []
    ep_ratios = []  # E/P = 1/PE

    for stock in stocks:
        if stock['circ_mv'] is not None:
            market_caps.append(float(stock['circ_mv']))

        if stock['pe_ttm'] is not None and float(stock['pe_ttm']) > 0:
            ep_ratios.append(1.0 / float(stock['pe_ttm']))

    # Calculate 20-day volatility for each stock
    # Get daily returns for the past 20 trading days
    volatilities = {}
    vol_list = []

    codes = [s['code'] for s in stocks]

    # Get returns for last 20 days
    returns_data = await pg_conn.fetch("""
        SELECT code, date, pct_chg
        FROM market_daily
        WHERE code = ANY($1)
          AND date <= $2
          AND date > $2 - INTERVAL '40 days'
        ORDER BY code, date DESC
    """, codes, target_date)

    # Group by code and calculate std dev of returns
    from collections import defaultdict
    code_returns = defaultdict(list)
    for r in returns_data:
        if r['pct_chg'] is not None:
            code_returns[r['code']].append(float(r['pct_chg']))

    for code, returns in code_returns.items():
        if len(returns) >= 10:  # Need at least 10 days of data
            returns_20d = returns[:20]
            if len(returns_20d) >= 10:
                mean = sum(returns_20d) / len(returns_20d)
                variance = sum((x - mean) ** 2 for x in returns_20d) / len(returns_20d)
                vol = variance ** 0.5
                volatilities[code] = vol
                vol_list.append(vol)

    # Sort for percentile calculation
    market_caps.sort()
    ep_ratios.sort()
    vol_list.sort()

    # Prepare records
    records = []

    for stock in stocks:
        code = stock['code']
        market_cap = stock['circ_mv']
        pe_ttm = stock['pe_ttm']

        # Size category (absolute thresholds)
        size_cat = get_size_category(market_cap)

        # Size percentile (for ranking)
        size_percentile = None
        if market_cap is not None and market_caps:
            mc = float(market_cap)
            rank = sum(1 for m in market_caps if m <= mc)
            size_percentile = rank / len(market_caps)

        # Value category (percentile-based)
        value_cat = None
        ep_ratio = None
        value_percentile = None
        if pe_ttm is not None and float(pe_ttm) > 0:
            ep_ratio = 1.0 / float(pe_ttm)
            if ep_ratios:
                rank = sum(1 for e in ep_ratios if e <= ep_ratio)
                value_percentile = rank / len(ep_ratios)
                value_cat = get_value_category_by_percentile(value_percentile)

        # Volatility category (percentile-based)
        vol_cat = None
        vol_20d = volatilities.get(code)
        vol_percentile = None
        if vol_20d is not None and vol_list:
            rank = sum(1 for v in vol_list if v <= vol_20d)
            vol_percentile = rank / len(vol_list)
            vol_cat = get_vol_category_by_percentile(vol_percentile)

        # Turnover category - based on 20-day average (simplified to current day for now)
        turnover_cat = None
        turn = stock['turn']
        if turn is not None:
            turn_val = float(turn)
            if turn_val >= 10:  # High turnover > 10%
                turnover_cat = "HOT"
            elif turn_val <= 1:  # Low turnover < 1%
                turnover_cat = "DEAD"
            else:
                turnover_cat = "NORMAL"

        records.append((
            code,
            target_date,
            market_cap,
            None,  # size_rank (could compute if needed)
            Decimal(str(size_percentile)) if size_percentile else None,
            size_cat,
            Decimal(str(vol_20d)) if vol_20d else None,
            None,  # vol_rank
            Decimal(str(vol_percentile)) if vol_percentile else None,
            vol_cat,
            Decimal(str(turn)) if turn else None,  # avg_turnover_20d
            None,  # turnover_rank
            None,  # turnover_percentile
            turnover_cat,
            Decimal(str(ep_ratio)) if ep_ratio else None,
            None,  # bp_ratio
            None,  # value_rank
            Decimal(str(value_percentile)) if value_percentile else None,
            value_cat,
            None,  # momentum_20d
            None,  # momentum_60d
            None,  # momentum_rank
            None,  # momentum_percentile
        ))

    # Insert/update stock_style_exposure
    await pg_conn.executemany(
        """
        INSERT INTO stock_style_exposure (
            code, date, market_cap, size_rank, size_percentile, size_category,
            volatility_20d, vol_rank, vol_percentile, vol_category,
            avg_turnover_20d, turnover_rank, turnover_percentile, turnover_category,
            ep_ratio, bp_ratio, value_rank, value_percentile, value_category,
            momentum_20d, momentum_60d, momentum_rank, momentum_percentile
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        ON CONFLICT (code, date) DO UPDATE SET
            market_cap = EXCLUDED.market_cap,
            size_percentile = EXCLUDED.size_percentile,
            size_category = EXCLUDED.size_category,
            volatility_20d = EXCLUDED.volatility_20d,
            vol_percentile = EXCLUDED.vol_percentile,
            vol_category = EXCLUDED.vol_category,
            avg_turnover_20d = EXCLUDED.avg_turnover_20d,
            turnover_category = EXCLUDED.turnover_category,
            ep_ratio = EXCLUDED.ep_ratio,
            value_percentile = EXCLUDED.value_percentile,
            value_category = EXCLUDED.value_category,
            created_at = CURRENT_TIMESTAMP
        """,
        records,
    )

    print(f"  Computed classifications for {len(records)} stocks")
    return len(records)


async def compute_classifications(
    postgres_url: str,
    target_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> int:
    """Main computation function."""
    print("=" * 60)
    print("Stock Classification Computation")
    print("=" * 60)

    pg_conn = await asyncpg.connect(postgres_url)

    try:
        if target_date:
            # Single date
            return await compute_classifications_for_date(pg_conn, target_date)

        elif start_date and end_date:
            # Date range
            total = 0
            current = start_date
            while current <= end_date:
                count = await compute_classifications_for_date(pg_conn, current)
                total += count
                current += timedelta(days=1)
            return total

        else:
            # Latest date from market_daily
            result = await pg_conn.fetchval(
                "SELECT MAX(date) FROM market_daily"
            )
            if result:
                return await compute_classifications_for_date(pg_conn, result)
            else:
                print("No market data found")
                return 0

    finally:
        await pg_conn.close()


def cli():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Compute stock classification categories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--date", "-d",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Compute for specific date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date for range computation"
    )

    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date for range computation"
    )

    parser.add_argument(
        "--database-url",
        type=str,
        default=DEFAULT_POSTGRES_URL,
        help="PostgreSQL connection URL"
    )

    args = parser.parse_args()

    count = asyncio.run(compute_classifications(
        postgres_url=args.database_url,
        target_date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
    ))

    print(f"\nTotal: {count} classification records computed")


if __name__ == "__main__":
    cli()
