"""Fast Alpha Radar backtest script.

Loads market data ONCE in bulk, then evaluates recommendations across
multiple dates and tabs. ~50x faster than calling ScreenerService per date.

Usage:
    docker compose exec api python -m scripts.alpha_radar_backtest
    docker compose exec api python -m scripts.alpha_radar_backtest --top-n 5 --period 5
    docker compose exec api python -m scripts.alpha_radar_backtest --dates 2026-01-05,2026-01-12
"""

import argparse
import asyncio
import datetime
import logging
import os
import random
import shutil
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import async_session_maker
from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine
from app.services.alpha_radar.engine import score_tab
from app.services.alpha_radar.engine.config_loader import VALID_TABS, load_strategy_config
from app.services.alpha_radar.engine.factors import compute_all_factors
from app.services.alpha_radar.engine.strategies.rally.scoring import RALLY_MIN_SCORE
from app.services.alpha_radar.engine.strategies.overnight.scoring import OVERNIGHT_MIN_SCORE
from app.services.alpha_radar.engine.factors.market_factors import (
    compute_ici,
    compute_sci,
    compute_dispersion,
    SW_INDUSTRY_NAME_TO_INDEX,
)

# Silence SQL logs
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

CACHE_DIR = Path("/app/data/cache/backtest")

# --- Date range for random sampling ---
SAMPLE_RANGE_START = datetime.date(2025, 1, 6)
SAMPLE_RANGE_END = datetime.date(2026, 2, 10)
# Margin: skip first/last N trading days so lookback and evaluation windows have data
SAMPLE_MARGIN_DAYS = 25


def get_trading_days(
    start: datetime.date = SAMPLE_RANGE_START,
    end: datetime.date = SAMPLE_RANGE_END,
) -> list[datetime.date]:
    """Fetch SSE trading calendar from TuShare."""
    import tushare as ts

    api_key = os.environ.get("TUSHARE_API_KEY")
    if not api_key:
        raise ValueError("TUSHARE_API_KEY environment variable not set")
    ts.set_token(api_key)
    pro = ts.pro_api()

    df = pro.trade_cal(
        exchange="SSE",
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
        is_open="1",
    )
    if df is None or df.empty:
        raise RuntimeError("No trading days returned from TuShare trade_cal")

    return sorted(datetime.datetime.strptime(d, "%Y%m%d").date() for d in df["cal_date"].tolist())


def sample_trading_days(
    n: int,
    seed: int | None = None,
    margin: int = SAMPLE_MARGIN_DAYS,
) -> list[datetime.date]:
    """Randomly sample N trading days from the available range.

    Trims `margin` days from both ends so lookback (60d) and evaluation
    windows (T+20) always have data.  Dates are returned sorted.

    Args:
        n: Number of dates to sample.
        seed: Random seed for reproducibility. None = different each run.
        margin: Number of trading days to skip at each end of the range.
    """
    all_days = get_trading_days()

    if margin * 2 >= len(all_days):
        raise ValueError(f"Margin {margin} too large for {len(all_days)} trading days")
    eligible = all_days[margin : len(all_days) - margin]

    if n > len(eligible):
        raise ValueError(f"Requested {n} dates but only {len(eligible)} eligible trading days")

    rng = random.Random(seed)
    sampled = sorted(rng.sample(eligible, n))
    log.info(f"Sampled {n} trading days (seed={seed}): {sampled[0]} .. {sampled[-1]}")
    return sampled


async def load_all_data(
    db,
    start_date: datetime.date,
    end_date: datetime.date,
    lookback_days: int = 60,
) -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """Load ALL market, valuation, style, profile, moneyflow, and limit data in one shot."""

    t0 = time.time()

    db_lookback = lookback_days

    # Get actual lookback start date (trading days)
    result = await db.execute(
        text(
            "SELECT DISTINCT date FROM market_daily "
            "WHERE date <= :start_date ORDER BY date DESC LIMIT :lookback"
        ),
        {"start_date": start_date, "lookback": db_lookback},
    )
    dates = [row[0] for row in result.fetchall()]
    data_start = dates[-1] if dates else start_date

    # 1. Market data (bulk)
    result = await db.execute(
        text("""
            SELECT md.code, md.date, md.open, md.high, md.low, md.close,
                   md.preclose, md.volume, md.amount, iv.turnover_rate AS turn, md.pct_chg,
                   am.name, am.asset_type, am.exchange
            FROM market_daily md
            JOIN asset_meta am ON md.code = am.code
            LEFT JOIN indicator_valuation iv ON md.code = iv.code AND md.date = iv.date
            WHERE md.date >= :data_start AND md.date <= :end_date
            AND am.asset_type = 'STOCK'
            ORDER BY md.code, md.date
        """),
        {"data_start": data_start, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    if not rows:
        raise RuntimeError("No market data found")

    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    market_df = pl.DataFrame(data).with_columns(
        [
            pl.col("date").cast(pl.Date),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("preclose").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("amount").cast(pl.Float64),
            pl.col("turn").cast(pl.Float64),
            pl.col("pct_chg").cast(pl.Float64),
        ]
    )
    log.info(f"  Market data: {market_df.height} rows, {market_df['code'].n_unique()} stocks")

    # 2. Valuation data - load all dates in range
    result = await db.execute(
        text("""
            SELECT code, date, pe_ttm, pb_mrq, ps_ttm, total_mv, circ_mv, is_st
            FROM indicator_valuation
            WHERE date >= :start_date AND date <= :end_date
        """),
        {"start_date": start_date, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    valuation_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not valuation_df.is_empty():
        valuation_df = valuation_df.with_columns(pl.col("date").cast(pl.Date))
    log.info(f"  Valuation data: {valuation_df.height} rows")

    # 3. Style factors - load all dates in range
    result = await db.execute(
        text("""
            SELECT code, date, market_cap, size_category, size_percentile,
                   volatility_20d, vol_category, vol_percentile,
                   avg_turnover_20d, turnover_category, turnover_percentile,
                   value_category, value_percentile, ep_ratio, bp_ratio,
                   momentum_20d, momentum_60d, momentum_percentile
            FROM stock_style_exposure
            WHERE date >= :start_date AND date <= :end_date
        """),
        {"start_date": start_date, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    style_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not style_df.is_empty():
        style_df = style_df.with_columns(pl.col("date").cast(pl.Date))
    log.info(f"  Style data: {style_df.height} rows")

    # 4. Stock profiles
    result = await db.execute(
        text(
            "SELECT code, sw_industry_l1, sw_industry_l2, sw_industry_l3, em_industry FROM stock_profile"
        )
    )
    rows = result.fetchall()
    columns = list(result.keys())
    profile_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    log.info(f"  Profiles: {profile_df.height} rows")

    # 5. Index data (sh.000001) for market regime
    result = await db.execute(
        text("""
            SELECT code, date, close, pct_chg
            FROM market_daily
            WHERE code = 'sh.000001'
            AND date >= :data_start AND date <= :end_date
            ORDER BY date
        """),
        {"data_start": data_start, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    index_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not index_df.is_empty():
        index_df = index_df.with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("close").cast(pl.Float64),
            ]
        )
    log.info(f"  Index data: {index_df.height} rows")

    # 6. Moneyflow data (for regime enhancement)
    result = await db.execute(
        text("""
            SELECT code, date, net_mf_amount,
                   buy_lg_amount, sell_lg_amount,
                   buy_elg_amount, sell_elg_amount
            FROM moneyflow_daily
            WHERE date >= :data_start AND date <= :end_date
        """),
        {"data_start": data_start, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    moneyflow_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not moneyflow_df.is_empty():
        moneyflow_df = moneyflow_df.with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("net_mf_amount").cast(pl.Float64),
                pl.col("buy_lg_amount").cast(pl.Float64),
                pl.col("sell_lg_amount").cast(pl.Float64),
                pl.col("buy_elg_amount").cast(pl.Float64),
                pl.col("sell_elg_amount").cast(pl.Float64),
            ]
        )
    log.info(f"  Moneyflow data: {moneyflow_df.height} rows")

    # 7. Limit list data (for regime enhancement)
    result = await db.execute(
        text("""
            SELECT code, date, limit_type
            FROM limit_list_daily
            WHERE date >= :data_start AND date <= :end_date
        """),
        {"data_start": data_start, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    limit_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not limit_df.is_empty():
        limit_df = limit_df.with_columns(pl.col("date").cast(pl.Date))
    log.info(f"  Limit list data: {limit_df.height} rows")

    # 8. Industry index data (SW L1 indices for ICI/SCI)
    result = await db.execute(
        text("""
            SELECT code, date, close, pct_chg
            FROM market_daily
            WHERE code LIKE 'sh.801%'
            AND date >= :data_start AND date <= :end_date
            ORDER BY code, date
        """),
        {"data_start": data_start, "end_date": end_date},
    )
    rows = result.fetchall()
    columns = list(result.keys())
    industry_index_df = (
        pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if rows
        else pl.DataFrame()
    )
    if not industry_index_df.is_empty():
        industry_index_df = industry_index_df.with_columns(
            [
                pl.col("date").cast(pl.Date),
                pl.col("close").cast(pl.Float64),
                pl.col("pct_chg").cast(pl.Float64),
            ]
        )
    log.info(f"  Industry index data: {industry_index_df.height} rows")

    elapsed = time.time() - t0
    log.info(f"  Data load: {elapsed:.1f}s total")
    return (
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
        industry_index_df,
    )


async def load_all_data_cached(
    db,
    start_date: datetime.date,
    end_date: datetime.date,
    lookback_days: int = 60,
    use_cache: bool = True,
) -> tuple[
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
    pl.DataFrame,
]:
    """Load data with Parquet caching."""
    if not use_cache:
        return await load_all_data(db, start_date, end_date, lookback_days)

    db_lookback = lookback_days

    result = await db.execute(
        text(
            "SELECT DISTINCT date FROM market_daily "
            "WHERE date <= :start_date ORDER BY date DESC LIMIT :lookback"
        ),
        {"start_date": start_date, "lookback": db_lookback},
    )
    dates = [row[0] for row in result.fetchall()]
    data_start = dates[-1] if dates else start_date

    cache_key = f"{data_start}_{end_date}"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    files = {
        "market": CACHE_DIR / f"{cache_key}_market.parquet",
        "valuation": CACHE_DIR / f"{cache_key}_valuation.parquet",
        "style": CACHE_DIR / f"{cache_key}_style.parquet",
        "profile": CACHE_DIR / f"{cache_key}_profile.parquet",
        "index": CACHE_DIR / f"{cache_key}_index.parquet",
        "moneyflow": CACHE_DIR / f"{cache_key}_moneyflow.parquet",
        "limit": CACHE_DIR / f"{cache_key}_limit.parquet",
        "industry_index": CACHE_DIR / f"{cache_key}_industry_index.parquet",
    }

    if all(f.exists() for f in files.values()):
        log.info(f"Loading from cache: {CACHE_DIR}/{cache_key}_*.parquet")
        t0 = time.time()
        market_df = pl.read_parquet(files["market"])
        valuation_df = pl.read_parquet(files["valuation"])
        style_df = pl.read_parquet(files["style"])
        profile_df = pl.read_parquet(files["profile"])
        index_df = pl.read_parquet(files["index"])
        moneyflow_df = pl.read_parquet(files["moneyflow"])
        limit_df = pl.read_parquet(files["limit"])
        industry_index_df = pl.read_parquet(files["industry_index"])
        log.info(f"  Cache load: {time.time() - t0:.1f}s")
        return (
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
            industry_index_df,
        )

    log.info("Cache miss. Loading from database...")
    (
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
        industry_index_df,
    ) = await load_all_data(db, start_date, end_date, lookback_days)

    log.info(f"Saving to cache: {CACHE_DIR}/{cache_key}_*.parquet")
    market_df.write_parquet(files["market"])
    valuation_df.write_parquet(files["valuation"])
    style_df.write_parquet(files["style"])
    profile_df.write_parquet(files["profile"])
    index_df.write_parquet(files["index"])
    moneyflow_df.write_parquet(files["moneyflow"])
    limit_df.write_parquet(files["limit"])
    industry_index_df.write_parquet(files["industry_index"])

    return (
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
        industry_index_df,
    )


def compute_regime_score(
    target_date: datetime.date,
    index_df: pl.DataFrame,
    market_df: "pl.DataFrame | None" = None,
    moneyflow_df: "pl.DataFrame | None" = None,
    limit_df: "pl.DataFrame | None" = None,
    industry_index_df: "pl.DataFrame | None" = None,
    profile_df: "pl.DataFrame | None" = None,
) -> tuple[float, int, dict]:
    """Compute market regime score from index + breadth + moneyflow + limits.

    Iter 13: Multi-signal regime with moneyflow/limit confirmation.
    Returns (regime_score, weak_days, regime_details) where regime_details
    contains diagnostic fields for the abstain decision.
    """
    details: dict = {}

    if index_df.is_empty():
        return 50.0, 0, details

    idx = index_df.filter(pl.col("date") <= target_date).sort("date")
    if idx.height < 10:
        return 50.0, 0, details

    closes = idx["close"].to_list()
    close_today = closes[-1]
    close_5d_ago = closes[-5] if len(closes) >= 5 else close_today

    ret_5d = (close_today / close_5d_ago - 1) * 100 if close_5d_ago else 0.0

    # Index component (0-100): same mapping as Iter 4
    if ret_5d > 3.0:
        index_component = max(10.0, 30.0 - (ret_5d - 3.0) * 5)
    elif ret_5d > 1.0:
        index_component = 50.0 - (ret_5d - 1.0) * 10
    elif ret_5d > -1.0:
        index_component = 60.0 + (-ret_5d) * 10
    elif ret_5d > -3.0:
        index_component = 70.0 + (-ret_5d - 1.0) * 7.5
    else:
        index_component = max(25.0, 55.0 + ret_5d * 5)
    index_component = max(0.0, min(100.0, index_component))

    # Breadth data
    breadth_values = []
    breadth_today_val = 50.0
    breadth_5d_avg = 50.0

    if market_df is not None and not market_df.is_empty():
        all_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") <= target_date)
            .sort("date", descending=True)
            .head(5)
        )
        date_list = all_dates["date"].to_list()

        if date_list:
            stocks = market_df.filter(
                (pl.col("date").is_in(date_list))
                & (~pl.col("code").str.starts_with("sh.000"))
                & (~pl.col("code").str.starts_with("sz.399"))
            )
            if not stocks.is_empty():
                breadth_per_day = (
                    stocks.group_by("date")
                    .agg((pl.col("pct_chg").fill_null(0.0) > 0).mean().alias("breadth_pct"))
                    .sort("date", descending=True)
                    .with_columns(pl.col("breadth_pct") * 100)
                )
                if breadth_per_day.height > 0:
                    breadth_values = breadth_per_day["breadth_pct"].to_list()
                    breadth_today_val = breadth_values[0]
                    breadth_5d_avg = sum(breadth_values) / len(breadth_values)

    # Breadth MODIFIER (divergence-based, synced with polars_engine.py)
    panic_days = sum(1 for v in breadth_values if v < 40) if breadth_values else 0
    # Iter 10: count weak breadth days (< 35%) for fragility detection
    weak_days = sum(1 for v in breadth_values if v < 35) if breadth_values else 0

    breadth_modifier = 0.0

    if ret_5d > 0 and breadth_5d_avg < 45:
        divergence_gap = ret_5d * (45 - breadth_5d_avg) / 45.0
        panic_penalty = panic_days * 3.0
        breadth_modifier = -(divergence_gap * 5.0 + panic_penalty)
        breadth_modifier = max(-25.0, breadth_modifier)
        # Iter 10: attenuate divergence penalty when target-day breadth is strong.
        # Strong today breadth (>60%) means the market is recovering from prior weakness.
        # The historical divergence was real but is being resolved now.
        if breadth_today_val > 60:
            attenuation = min(1.0, (breadth_today_val - 60) / 20.0)  # 0→1 over 60→80%
            breadth_modifier *= 1.0 - attenuation * 0.7  # reduce penalty by up to 70%

    elif ret_5d <= 0 and breadth_5d_avg < 35:
        capitulation_depth = (35 - breadth_5d_avg) / 35.0
        breadth_modifier = capitulation_depth * 8.0

    elif breadth_5d_avg > 55:
        breadth_modifier = min(10.0, (breadth_5d_avg - 55) * 0.5)

    elif breadth_5d_avg < 45 and panic_days >= 3:
        breadth_modifier = -panic_days * 2.0

    # --- Iter 10: Target-day breadth signal ---
    # Breadth on the target day itself is a strong contemporaneous signal.
    # High breadth (>65%) = broad bullish participation → regime boost.
    # Low breadth (<35%) = widespread selling → regime penalty.
    # Iter 10b: Strengthened breadth_today signal. Threshold 65→60, slope 0.5→1.0, cap 15→20.
    # 01-05 (76%): +16.0 boost pushes regime to ~56. 12-08 (61.6%): only +1.6, still penalized.
    breadth_today_modifier = 0.0
    if breadth_today_val > 60:
        breadth_today_modifier = min(20.0, (breadth_today_val - 60) * 1.0)
    elif breadth_today_val < 35:
        breadth_today_modifier = max(-15.0, (breadth_today_val - 35) * 0.5)

    # --- Iter 10 Change 2: Breadth fragility detection ---
    # When recent breadth is highly volatile (e.g. swinging 27%→80%→62%),
    # the average smooths away the instability signal.
    # 12-08: 27.1%, 27.3%, 79.9%, ?, 61.6% → avg ~49% looks neutral,
    # but 2 days below 35% = extreme fragility.
    # If ≥2 out of 5 days have breadth < 35%, apply additional fragility penalty.
    fragility_penalty = 0.0
    if weak_days >= 2:
        fragility_penalty = weak_days * 5.0  # -10 to -25
    elif weak_days == 1 and panic_days >= 2:
        fragility_penalty = 5.0  # mild penalty for borderline fragility

    # Iter 10b: Attenuate fragility penalty when target-day breadth is strong (>65%).
    # Historical fragility was real, but strong today breadth = market recovering.
    # 01-05: breadth_today=76%, weak_days=2 → penalty=10 → attenuated to ~3.0
    # 12-08: breadth_today=61.6% → no attenuation, penalty stays at 15.0
    if fragility_penalty > 0 and breadth_today_val > 65:
        frag_attenuation = min(1.0, (breadth_today_val - 65) / 15.0)  # 0→1 over 65→80%
        fragility_penalty *= 1.0 - frag_attenuation * 0.7  # reduce by up to 70%

    score = index_component + breadth_modifier + breadth_today_modifier - fragility_penalty
    score = max(0.0, min(100.0, round(score, 1)))

    # Iter 13: Moneyflow-based market-wide sentiment signal
    mf_pct_inflow = 50.0
    mf_avg_net = 0.0
    mf_elg_net = 0.0
    if moneyflow_df is not None and not moneyflow_df.is_empty():
        mf_today = moneyflow_df.filter(pl.col("date") == target_date)
        if not mf_today.is_empty():
            mf_pct_inflow = (
                mf_today.filter(pl.col("net_mf_amount") > 0).height / max(mf_today.height, 1) * 100
            )
            mf_avg_net = mf_today["net_mf_amount"].mean() or 0.0
            elg_net = (mf_today["buy_elg_amount"] - mf_today["sell_elg_amount"]).mean()
            mf_elg_net = elg_net if elg_net is not None else 0.0

    # Iter 13: Limit up/down ratio
    limit_up_count = 0
    limit_down_count = 0
    if limit_df is not None and not limit_df.is_empty():
        lim_today = limit_df.filter(pl.col("date") == target_date)
        if not lim_today.is_empty():
            limit_up_count = lim_today.filter(pl.col("limit_type") == "U").height
            limit_down_count = lim_today.filter(pl.col("limit_type") == "D").height

    details = {
        "index_component": index_component,
        "ret_5d": ret_5d,
        "breadth_today": breadth_today_val,
        "breadth_5d_avg": breadth_5d_avg,
        "breadth_modifier": breadth_modifier,
        "breadth_today_modifier": breadth_today_modifier,
        "fragility_penalty": fragility_penalty,
        "panic_days": panic_days,
        "mf_pct_inflow": mf_pct_inflow,
        "mf_avg_net": mf_avg_net,
        "mf_elg_net": mf_elg_net,
        "limit_up": limit_up_count,
        "limit_down": limit_down_count,
    }

    # ICI/SCI/Dispersion
    ici_20d = 0.0
    sci_by_industry = {}
    dispersion_std = 0.0
    market_regime_v2 = "个股分化"

    if market_df is not None and not market_df.is_empty():
        # Filter market_df to last 25 days for ICI/SCI
        ici_window_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") <= target_date)
            .sort("date", descending=True)
            .head(25)
        )
        min_date = ici_window_dates["date"].min()

        if min_date:
            stock_pct_df = market_df.filter(
                (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
            ).select(["date", "code", "pct_chg"])

            index_pct_df = index_df.filter(
                (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
            ).select(["date", "pct_chg"])

            ici_20d = compute_ici(stock_pct_df, index_pct_df, target_date)
            dispersion_std = compute_dispersion(stock_pct_df, index_pct_df, target_date)

            if (
                industry_index_df is not None
                and not industry_index_df.is_empty()
                and profile_df is not None
            ):
                ind_pct_df = industry_index_df.filter(
                    (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
                ).select(["date", "code", "pct_chg"])

                p_rows = profile_df.select(["code", "sw_industry_l1"]).rows()
                stock_industry_map = {}
                for r in p_rows:
                    if r[1] and r[1] in SW_INDUSTRY_NAME_TO_INDEX:
                        stock_industry_map[r[0]] = SW_INDUSTRY_NAME_TO_INDEX[r[1]]

                sci_by_industry = compute_sci(
                    stock_pct_df, ind_pct_df, stock_industry_map, target_date
                )

    if ici_20d > 0.5 and ret_5d > 0:
        market_regime_v2 = "系统性主导"
    elif ici_20d > 0.5 and ret_5d < 0:
        market_regime_v2 = "系统性风险"
    elif 0.25 <= ici_20d <= 0.5:
        market_regime_v2 = "行业轮动"

    # Phase 2: ICI → regime_discount enhancement
    ici_modifier = 0.0
    if ici_20d > 0.5 and ret_5d < -1.0:
        ici_modifier = -15.0
    elif ici_20d > 0.5 and ret_5d > 1.0:
        ici_modifier = 5.0
    elif ici_20d < 0.25:
        ici_modifier = 10.0
    score = max(0.0, min(100.0, round(score + ici_modifier, 1)))

    details.update(
        {
            "ici_20d": round(ici_20d, 4),
            "ici_modifier": round(ici_modifier, 1),
            "sci_by_industry": sci_by_industry,
            "dispersion_std": round(dispersion_std, 4),
            "market_regime_v2": market_regime_v2,
        }
    )

    return score, weak_days, details


def compute_scores_for_date(
    target_date: datetime.date,
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
    industry_index_df: pl.DataFrame,
    tab: str,
    top_n: int | None = None,
    lookback_days: int = 60,
    precomputed_factors: bool = False,
    engine: "PolarsEngine | None" = None,
    version: str | None = None,
) -> tuple[list[dict], float]:
    """Compute scores for a single date using vectorized operations.

    Returns:
        (recommendations, confidence) where confidence is 1.0 for normal days
        and 0.3-0.5 for days that would have been abstained under the old model.
    """

    # 1. Compute market regime score
    regime_score, weak_days, regime_details = compute_regime_score(
        target_date,
        index_df,
        market_df,
        moneyflow_df,
        limit_df,
        industry_index_df,
        profile_df,
    )

    # Confidence-weighted market gate (was: binary abstain gate)
    # Instead of returning [] on hostile conditions, we reduce confidence
    # so that every date still produces recommendations.
    confidence = 1.0

    breadth_today = regime_details.get("breadth_today", 50.0)
    mf_pct_inflow = regime_details.get("mf_pct_inflow", 50.0)
    mf_avg_net = regime_details.get("mf_avg_net", 0.0)
    limit_down = regime_details.get("limit_down", 0)

    is_capitulation = breadth_today < 5.0 or limit_down > 500
    hostile_breadth = breadth_today < 35.0
    hostile_moneyflow = mf_pct_inflow < 32.0 or mf_avg_net < -1200
    hostile_weakness = weak_days >= 2
    low_regime = regime_score < 45

    if not is_capitulation:
        hostile_signals = sum([hostile_breadth, hostile_moneyflow, hostile_weakness, low_regime])
        # Path 1: Hostile breadth with multi-signal confirmation → confidence 0.3
        if hostile_breadth and hostile_signals >= 3:
            confidence = min(confidence, 0.3)
        # Path 2: Sustained fragility (low regime + weak days) → confidence 0.3
        elif low_regime and weak_days >= 3:
            confidence = min(confidence, 0.3)
        # Path 3: Extreme dispersion with hostile confirmation → confidence 0.4
        elif (
            tab != "dragon"
            and regime_details.get("dispersion_std", 0.0) > 4.5
            and hostile_signals >= 2
        ):
            confidence = min(confidence, 0.4)

    # Rally-specific gate: weak 5-day breadth → confidence 0.4
    breadth_5d_avg = regime_details.get("breadth_5d_avg", 50.0)
    if tab == "rally" and breadth_5d_avg < 40.0:
        confidence = min(confidence, 0.4)

    # Overnight-specific gate: regime too weak → confidence 0.5
    if tab == "overnight" and regime_score > 50:
        confidence = min(confidence, 0.5)

    scoring = ScoringEngine(
        market_regime_score=regime_score,
        ici_20d=regime_details.get("ici_20d", 0.0),
        dispersion_std=regime_details.get("dispersion_std", 0.0),
    )

    # Get trading dates up to target_date
    all_dates = market_df.select("date").unique().sort("date")
    dates_before = all_dates.filter(pl.col("date") <= target_date)
    if dates_before.height < 20:
        return [], confidence

    # Get lookback start
    lookback_dates = dates_before.tail(lookback_days)
    lookback_start = lookback_dates["date"][0]

    # Filter market data to [lookback_start, target_date]
    df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= target_date))

    if df.is_empty():
        return [], confidence

    # Calculate technical indicators
    if not precomputed_factors:
        df = engine.calculate_technical_indicators(df)

    # Filter to target date only
    df = df.filter(pl.col("date") == target_date)
    if df.is_empty():
        return [], confidence

    # Join valuation (for target_date)
    if not valuation_df.is_empty():
        val_day = valuation_df.filter(pl.col("date") == target_date)
        if not val_day.is_empty():
            val_day = val_day.with_columns(
                [
                    (pl.col("pe_ttm").rank() / pl.len()).alias("pe_percentile"),
                ]
            )
            df = df.join(val_day, on="code", how="left", suffix="_val")
            # Filter ST stocks
            if "is_st" in df.columns:
                df = df.filter(pl.col("is_st").fill_null(0) != 1)
            elif "is_st_val" in df.columns:
                df = df.filter(pl.col("is_st_val").fill_null(0) != 1)

    # Join style factors (for target_date)
    if not style_df.is_empty():
        style_day = style_df.filter(pl.col("date") == target_date)
        if not style_day.is_empty():
            if "volatility_20d" in style_day.columns:
                style_day = style_day.with_columns(
                    [
                        (pl.col("volatility_20d").rank() / pl.len()).alias("vol_percentile"),
                    ]
                )
            style_select_cols = ["code", "size_category", "momentum_20d", "momentum_60d"]
            for col in [
                "vol_percentile",
                "value_percentile",
                "momentum_percentile",
                "turnover_percentile",
                "size_percentile",
                "ep_ratio",
                "bp_ratio",
            ]:
                if col in style_day.columns:
                    style_select_cols.append(col)
            df = df.join(
                style_day.select(style_select_cols), on="code", how="left", suffix="_style"
            )
            # Merge suffixed columns
            for scol in ["vol_percentile", "value_percentile", "momentum_percentile"]:
                suffixed = f"{scol}_style"
                if suffixed in df.columns:
                    df = df.with_columns(
                        pl.coalesce([pl.col(scol), pl.col(suffixed)]).alias(scol)
                    ).drop(suffixed)

    # Join profiles
    if not profile_df.is_empty():
        df = df.join(profile_df, on="code", how="left")

    # Map SCI to individual stocks
    sci_map = regime_details.get("sci_by_industry", {})
    if sci_map and "sw_industry_l1" in df.columns:
        sci_mapping_df = pl.DataFrame(
            {
                "sw_industry_l1": list(sci_map.keys()),
                "sector_coherence_sci": [float(v) for v in sci_map.values()],
            }
        )
        df = df.join(sci_mapping_df, on="sw_industry_l1", how="left")
        df = df.with_columns(pl.col("sector_coherence_sci").fill_null(0.0))

    # Iter 9: compute and join sector momentum
    sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, target_date)
    if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
        df = df.join(
            sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
            on="sw_industry_l1",
            how="left",
        )
        df = df.with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

    # Iter 14: Join per-stock moneyflow data for scoring signals
    # net_mf_amount = net moneyflow (positive = inflow); elg_net = extra-large order net (institutional)
    # Normalize to percentile ranks so they're comparable across dates with different market volumes
    if moneyflow_df is not None and not moneyflow_df.is_empty():
        mf_today = moneyflow_df.filter(pl.col("date") == target_date)
        if not mf_today.is_empty():
            mf_features = mf_today.with_columns(
                [
                    (pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw"),
                ]
            ).select(["code", "net_mf_amount", "elg_net_raw"])

            # Rank-normalize to 0-100 percentile (robust across different market-cap regimes)
            mf_features = mf_features.with_columns(
                [
                    (pl.col("net_mf_amount").rank() / pl.len() * 100).alias("mf_net_percentile"),
                    (pl.col("elg_net_raw").rank() / pl.len() * 100).alias("elg_net_percentile"),
                ]
            ).select(["code", "mf_net_percentile", "elg_net_percentile"])

            df = df.join(mf_features, on="code", how="left")
            df = df.with_columns(
                [
                    pl.col("mf_net_percentile").fill_null(50.0),
                    pl.col("elg_net_percentile").fill_null(50.0),
                ]
            )

    # Hard filter: exclude near_limit_up stocks (matches screener_service.py)
    if tab in ("weekly", "rally", "dragon"):
        if "near_limit_up" in df.columns:
            df = df.filter(pl.col("near_limit_up") == False)  # noqa: E712

    if tab == "weekly" and "pct_chg" in df.columns:
        df = df.filter(pl.col("pct_chg").fill_null(0.0).abs() <= 5.0)
    elif tab == "dragon" and "pct_chg" in df.columns:
        df = df.filter(pl.col("pct_chg").fill_null(0.0).abs() <= 5.0)

    # Calculate scores
    if tab in VALID_TABS:
        df, score_col = score_tab(tab, df, market_regime_score=regime_score, version=version)  # type: ignore[arg-type]
    else:
        return [], confidence

    # Sort and get top N with sector diversification cap (max 2 per sector)
    df = df.sort([score_col, "code"], descending=[True, False], nulls_last=True)
    if "sw_industry_l1" in df.columns and top_n <= 10:
        selected_indices = []
        sector_counts: dict[str, int] = {}
        max_per_sector = 2
        for i, row in enumerate(df.iter_rows(named=True)):
            sector = row.get("sw_industry_l1", "")
            if sector and sector_counts.get(sector, 0) >= max_per_sector:
                continue
            selected_indices.append(i)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            if len(selected_indices) >= top_n:
                break
        df = df[selected_indices] if selected_indices else df.head(top_n)
    else:
        df = df.head(top_n)

    results = []
    for row in df.iter_rows(named=True):
        score_val = row.get(score_col, 0)
        if tab == "rally" and score_val < RALLY_MIN_SCORE:
            continue
        if tab == "overnight" and score_val < OVERNIGHT_MIN_SCORE:
            continue
        results.append(
            {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "score": score_val,
                "close": row.get("close", 0),
                "pct_chg": row.get("pct_chg", 0),
            }
        )
    return results, confidence


def evaluate_t_plus_n(
    recommendations: list[dict],
    target_date: datetime.date,
    market_df: pl.DataFrame,
    period: int = 5,
    limit_df: "pl.DataFrame | None" = None,
    tab: str = "",
) -> dict:
    """Evaluate T+N performance using pre-loaded market data.

    For most strategies: close-to-close returns (T close → T+period close).
    For overnight: open-to-open returns (T+1 open → T+2 open), matching the
    actual execution model of buying at next-day open and selling at T+2 open.
    """

    codes = [r["code"] for r in recommendations]
    if not codes:
        return {
            "win_rate": None,
            "avg_return": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_loss_ratio": None,
            "stocks": [],
        }

    # Get trading dates after target_date
    future_dates = (
        market_df.select("date").unique().filter(pl.col("date") > target_date).sort("date")
    )

    if future_dates.height < period:
        return {
            "win_rate": None,
            "avg_return": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_loss_ratio": None,
            "stocks": [],
        }

    eval_date = future_dates["date"][period - 1]

    if tab == "overnight":
        # Overnight strategy: buy at T+1 open, sell at T+2 open
        entry_date = future_dates["date"][0]
        exit_date = future_dates["date"][1] if future_dates.height >= 2 else None
        if exit_date is None:
            return {
                "win_rate": None,
                "avg_return": None,
                "avg_win": None,
                "avg_loss": None,
                "profit_loss_ratio": None,
                "stocks": [],
            }
        ref_prices = (
            market_df.filter((pl.col("date") == entry_date) & (pl.col("code").is_in(codes)))
            .select(["code", "open"])
            .rename({"open": "ref_close"})
        )
        eval_prices = (
            market_df.filter((pl.col("date") == exit_date) & (pl.col("code").is_in(codes)))
            .select(["code", "open"])
            .rename({"open": "eval_close"})
        )
    else:
        # Default: close-to-close returns
        # Get ref prices (target_date close) and eval prices (eval_date close)
        ref_prices = (
            market_df.filter((pl.col("date") == target_date) & (pl.col("code").is_in(codes)))
            .select(["code", "close"])
            .rename({"close": "ref_close"})
        )

        eval_prices = (
            market_df.filter((pl.col("date") == eval_date) & (pl.col("code").is_in(codes)))
            .select(["code", "close"])
            .rename({"close": "eval_close"})
        )

    # Join and compute returns
    perf = ref_prices.join(eval_prices, on="code", how="inner")
    perf = perf.with_columns(
        [
            ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
            .round(2)
            .alias("return_pct"),
        ]
    )

    if perf.is_empty():
        return {
            "win_rate": None,
            "avg_return": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_loss_ratio": None,
            "stocks": [],
        }

    returns = [r for r in perf["return_pct"].to_list() if r is not None]
    if not returns:
        return {
            "win_rate": None,
            "avg_return": None,
            "avg_win": None,
            "avg_loss": None,
            "profit_loss_ratio": None,
            "stocks": [],
        }
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    total = len(returns)

    win_rate = round(len(wins) / total * 100, 1) if total > 0 else None
    avg_return = round(sum(returns) / total, 2) if total > 0 else None
    avg_win = round(sum(wins) / len(wins), 2) if wins else None
    avg_loss = round(sum(losses) / len(losses), 2) if losses else None
    pl_ratio = round(avg_win / abs(avg_loss), 2) if avg_win and avg_loss and avg_loss != 0 else None

    # Per-stock limit-up stats within evaluation window
    limit_up_stats: dict[str, dict] = {}
    if limit_df is not None and not limit_df.is_empty():
        eval_window_dates = future_dates.head(period)["date"].to_list()
        lu = limit_df.filter(
            (pl.col("code").is_in(codes))
            & (pl.col("date").is_in(eval_window_dates))
            & (pl.col("limit_type") == "U")
        ).sort(["code", "date"])
        for code in codes:
            code_dates = lu.filter(pl.col("code") == code)["date"].to_list()
            count = len(code_dates)
            max_consec = 0
            if count > 0:
                consec = 1
                for i in range(1, len(code_dates)):
                    idx_prev = eval_window_dates.index(code_dates[i - 1])
                    idx_curr = eval_window_dates.index(code_dates[i])
                    if idx_curr == idx_prev + 1:
                        consec += 1
                    else:
                        max_consec = max(max_consec, consec)
                        consec = 1
                max_consec = max(max_consec, consec)
            limit_up_stats[code] = {"limit_up_count": count, "max_consec_limit_up": max_consec}

    stock_details = []
    for row in perf.iter_rows(named=True):
        rec = next((r for r in recommendations if r["code"] == row["code"]), {})
        detail = {
            "code": row["code"],
            "name": rec.get("name", ""),
            "score": rec.get("score", 0),
            "return": row["return_pct"],
        }
        if row["code"] in limit_up_stats:
            detail.update(limit_up_stats[row["code"]])
        stock_details.append(detail)

    return {
        "win_rate": win_rate,
        "avg_return": avg_return,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_loss_ratio": pl_ratio,
        "eval_date": str(eval_date),
        "stocks": stock_details,
    }


async def run_backtest(
    test_dates: list[datetime.date],
    tabs: list[str],
    top_n: int | None = None,
    period: int = 5,
    verbose: bool = False,
    use_cache: bool = True,
    version: str | None = None,
):
    """Run the full backtest."""

    t_start = time.time()

    # Determine date range: earliest test date minus lookback, latest test date + period buffer
    earliest = min(test_dates)
    latest = max(test_dates)
    # Buffer for T+period evaluation (55 calendar days covers ~30 trading days for dragon T+20)
    end_date = latest + datetime.timedelta(days=55)

    log.info(f"=== Alpha Radar Backtest ===")
    log.info(f"Test dates: {len(test_dates)} dates ({earliest} to {latest})")
    log.info(f"Tabs: {', '.join(tabs)}")
    log.info(f"Top N: {top_n or 'per-strategy'}, Eval period: per-tab (from config)")
    log.info(f"Loading data (cache={'ON' if use_cache else 'OFF'})...")

    async with async_session_maker() as db:
        (
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
            industry_index_df,
        ) = await load_all_data_cached(
            db, earliest, end_date, lookback_days=60, use_cache=use_cache
        )

    log.info(f"\nPrecomputing factors on full dataset...")
    t_factors = time.time()
    market_df = market_df.sort(["code", "date"])
    market_df = compute_all_factors(market_df)
    log.info(f"Factor precomputation: {time.time() - t_factors:.1f}s")

    log.info(f"\nRunning backtest...")

    all_results = {}
    confidence_map: dict[datetime.date, float] = {}
    regime_data: dict[datetime.date, dict] = {}
    for d in test_dates:
        all_results[d] = {}
        regime_score, weak_days, regime_details = compute_regime_score(
            d,
            index_df,
            market_df,
            moneyflow_df,
            limit_df,
            industry_index_df,
            profile_df,
        )
        regime_data[d] = {
            "regime_score": regime_score,
            "weak_days": weak_days,
            **regime_details,
        }
        date_confidence = 1.0
        for tab in tabs:
            t0 = time.time()
            tab_top_n = top_n or load_strategy_config(tab, version=version).backtest_top_n
            recs, tab_confidence = compute_scores_for_date(
                d,
                market_df,
                valuation_df,
                style_df,
                profile_df,
                index_df,
                moneyflow_df,
                limit_df,
                industry_index_df,
                tab=tab,
                top_n=tab_top_n,
                precomputed_factors=True,
                version=version,
            )
            date_confidence = min(date_confidence, tab_confidence)
            tab_period = (
                load_strategy_config(tab, version=version).eval_period
                if tab in VALID_TABS
                else period
            )
            perf = evaluate_t_plus_n(recs, d, market_df, tab_period, limit_df=limit_df, tab=tab)
            elapsed = time.time() - t0
            all_results[d][tab] = {
                "recommendations": recs,
                "performance": perf,
                "elapsed": elapsed,
                "confidence": tab_confidence,
            }

            if verbose and recs:
                log.info(f"\n  {d} | {tab} | {elapsed:.2f}s | confidence={tab_confidence}")
                for r in recs:
                    ret_info = next((s for s in perf["stocks"] if s["code"] == r["code"]), {})
                    ret_val = ret_info.get("return", "N/A")
                    marker = (
                        "✅"
                        if isinstance(ret_val, (int, float)) and ret_val > 0
                        else "❌"
                        if isinstance(ret_val, (int, float))
                        else "?"
                    )
                    close_str = f"{r['close']:.2f}" if r.get("close") is not None else "N/A"
                    chg_str = f"{r['pct_chg']:.2f}" if r.get("pct_chg") is not None else "N/A"
                    score_str = f"{r['score']:.1f}" if r.get("score") is not None else "N/A"
                    log.info(
                        f"    {r['code']} {r['name']:<8} score={score_str} close={close_str} chg={chg_str}% → T+{tab_period}: {ret_val}% {marker}"
                    )
            elif verbose and date_confidence < 1.0 and tab == tabs[0]:
                rd = regime_data[d]
                log.info(
                    f"\n  {d} | LOW_CONF={date_confidence} (regime={rd['regime_score']:.1f}, weak_days={rd['weak_days']}, "
                    f"breadth={rd.get('breadth_today', 0):.1f}%, "
                    f"mf_inflow={rd.get('mf_pct_inflow', 0):.1f}%, "
                    f"mf_net={rd.get('mf_avg_net', 0):.0f})"
                )
        confidence_map[d] = date_confidence

    # Print summary table
    total_elapsed = time.time() - t_start
    col_width = 30
    header_parts = [f"{'Date':<14}"]
    for tab in tabs:
        tp = load_strategy_config(tab, version=version).eval_period if tab in VALID_TABS else period
        header_parts.append(f"{tab.upper()}(T+{tp})"[:col_width].ljust(col_width))
    separator_width = 14 + col_width * len(tabs)
    log.info(f"\n{'=' * separator_width}")
    log.info(" ".join(header_parts))
    log.info(f"{'=' * separator_width}")

    low_confidence_dates = {d for d, c in confidence_map.items() if c < 1.0}
    for d in test_dates:
        row_parts = [f"{d!s:<14}"]
        conf = confidence_map.get(d, 1.0)
        conf_tag = f" [{conf}]" if conf < 1.0 else ""
        for tab in tabs:
            perf = all_results[d][tab]["performance"]
            wr = perf["win_rate"]
            ar = perf["avg_return"]
            plr = perf["profit_loss_ratio"]
            if wr is not None:
                row_parts.append(f"WR={wr}% AR={ar}% PL={plr or 'N/A':<5}{conf_tag}")
            else:
                row_parts.append(f"NO_DATA{conf_tag:<23}")
        log.info(" ".join(row_parts))

    # Weighted averages
    log.info(f"\n{'--- WEIGHTED AVERAGES ---':^110}")
    if low_confidence_dates:
        log.info(f"  ({len(low_confidence_dates)} low-confidence date(s) with reduced weight)")
    log.info(f"{'Tab':<14} {'Avg WR':<12} {'Avg AR':<12} {'Avg P/L':<12} {'# Dates':<10}")
    log.info("-" * 60)

    for tab in tabs:
        wrs, ars, plrs, weights = [], [], [], []
        for d in test_dates:
            perf = all_results[d][tab]["performance"]
            conf = confidence_map.get(d, 1.0)
            if perf["win_rate"] is not None:
                wrs.append(perf["win_rate"])
                ars.append(perf["avg_return"])
                weights.append(conf)
                if perf["profit_loss_ratio"] is not None:
                    plrs.append((perf["profit_loss_ratio"], conf))

        if wrs:
            total_weight = sum(weights)
            avg_wr = sum(w * wr for w, wr in zip(weights, wrs)) / total_weight
            avg_ar = sum(w * ar for w, ar in zip(weights, ars)) / total_weight
            if plrs:
                plr_weight = sum(c for _, c in plrs)
                avg_plr = sum(v * c for v, c in plrs) / plr_weight
                plr_str = f"{avg_plr:.2f}"
            else:
                plr_str = "N/A"
            log.info(
                f"{tab:<14} {avg_wr:.1f}%{'':<7} {avg_ar:.2f}%{'':<7} {plr_str:<12} {len(wrs)}"
            )
        else:
            log.info(f"{tab:<14} NO DATA")

    log.info(f"\nTotal time: {total_elapsed:.1f}s")
    return all_results, confidence_map, regime_data


def parse_args():
    parser = argparse.ArgumentParser(description="Alpha Radar Backtest")
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Top N stocks per tab (default: per-strategy config)",
    )
    parser.add_argument("--period", type=int, default=5, help="Evaluation period T+N (default: 5)")
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated dates (YYYY-MM-DD). Default: 25 randomly sampled trading days",
    )
    parser.add_argument(
        "--tabs",
        type=str,
        default="weekly,rally,dragon",
        help="Comma-separated tabs to test",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-stock details")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for date sampling (default: random each run)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable Parquet caching (force DB load)",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear all cached Parquet files and exit",
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Strategy config version (e.g. 8.0). Default: head version.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.clear_cache:
        if CACHE_DIR.exists():
            log.info(f"Clearing cache directory: {CACHE_DIR}")
            shutil.rmtree(CACHE_DIR)
            log.info("Cache cleared.")
        else:
            log.info(f"Cache directory {CACHE_DIR} does not exist.")
        sys.exit(0)

    if args.dates:
        test_dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    else:
        test_dates = sample_trading_days(n=25, seed=args.seed)

    tabs = [t.strip() for t in args.tabs.split(",")]
    asyncio.run(
        run_backtest(
            test_dates,
            tabs,
            args.top_n,
            args.period,
            args.verbose,
            use_cache=not args.no_cache,
            version=args.version,
        )
    )


if __name__ == "__main__":
    main()
