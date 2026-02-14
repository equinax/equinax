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
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import polars as pl
from sqlalchemy import text

from app.db.session import async_session_maker
from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine

# Silence SQL logs
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


async def load_all_data(
    db,
    start_date: datetime.date,
    end_date: datetime.date,
    lookback_days: int = 60,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load ALL market, valuation, style, and profile data in one shot."""

    t0 = time.time()

    # Get actual lookback start date (trading days)
    result = await db.execute(
        text(
            "SELECT DISTINCT date FROM market_daily "
            "WHERE date <= :start_date ORDER BY date DESC LIMIT :lookback"
        ),
        {"start_date": start_date, "lookback": lookback_days},
    )
    dates = [row[0] for row in result.fetchall()]
    data_start = dates[-1] if dates else start_date

    # 1. Market data (bulk)
    result = await db.execute(
        text("""
            SELECT md.code, md.date, md.open, md.high, md.low, md.close,
                   md.preclose, md.volume, md.amount, md.turn, md.pct_chg,
                   am.name, am.asset_type, am.exchange
            FROM market_daily md
            JOIN asset_meta am ON md.code = am.code
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

    elapsed = time.time() - t0
    log.info(f"  Data load: {elapsed:.1f}s total")
    return market_df, valuation_df, style_df, profile_df, index_df


def compute_regime_score(
    target_date: datetime.date, index_df: pl.DataFrame, market_df: "pl.DataFrame | None" = None
) -> tuple[float, int]:
    """Compute market regime score from index + breadth divergence (no DB calls).

    Iter 7 v2: Index component as BASE + breadth MODIFIER (divergence-based).
    Must stay in sync with polars_engine.py load_market_regime().
    """
    if index_df.is_empty():
        return 50.0, 0

    idx = index_df.filter(pl.col("date") <= target_date).sort("date")
    if idx.height < 10:
        return 50.0, 0

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
    return score, weak_days


def compute_scores_for_date(
    target_date: datetime.date,
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    tab: str,
    top_n: int,
    lookback_days: int = 60,
) -> list[dict]:
    """Compute scores for a single date using pre-loaded data. Pure Polars, no DB calls."""

    engine = PolarsEngine.__new__(PolarsEngine)  # Skip __init__ (needs db)
    regime_score, weak_days = compute_regime_score(target_date, index_df, market_df)

    # Iter 10: Tradeable-day gate — abstain when market is deeply hostile.
    # Two simultaneous signals required: low regime + sustained broad weakness.
    should_abstain = regime_score < 45 and weak_days >= 3
    if should_abstain:
        return []

    scoring = ScoringEngine(market_regime_score=regime_score)

    # Get trading dates up to target_date
    all_dates = market_df.select("date").unique().sort("date")
    dates_before = all_dates.filter(pl.col("date") <= target_date)
    if dates_before.height < 20:
        return []

    # Get lookback start
    lookback_dates = dates_before.tail(lookback_days)
    lookback_start = lookback_dates["date"][0]

    # Filter market data to [lookback_start, target_date]
    df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= target_date))

    if df.is_empty():
        return []

    # Calculate technical indicators
    df = engine.calculate_technical_indicators(df)

    # Filter to target date only
    df = df.filter(pl.col("date") == target_date)
    if df.is_empty():
        return []

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

    # Iter 9: compute and join sector momentum
    sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, target_date)
    if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
        df = df.join(
            sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
            on="sw_industry_l1",
            how="left",
        )
        df = df.with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

    # Hard filter: exclude near_limit_up stocks for trend/panorama/smart (matches screener_service.py)
    if tab in ("trend", "panorama", "smart"):
        if "near_limit_up" in df.columns:
            df = df.filter(pl.col("near_limit_up") == False)  # noqa: E712

    if tab == "smart" and "pct_chg" in df.columns:
        df = df.filter(pl.col("pct_chg").fill_null(0.0).abs() <= 7.0)

    # Calculate scores
    if tab == "panorama":
        df = scoring.calculate_panorama_score(df)
        score_col = "panorama_score"
    elif tab == "smart":
        df = scoring.calculate_smart_accumulation_score(df)
        score_col = "smart_score"
    elif tab == "value":
        df = scoring.calculate_deep_value_score(df)
        score_col = "value_score"
    elif tab == "trend":
        df = scoring.calculate_super_trend_score(df)
        score_col = "trend_score"
    else:
        return []

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
        results.append(
            {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "score": row.get(score_col, 0),
                "close": row.get("close", 0),
                "pct_chg": row.get("pct_chg", 0),
            }
        )
    return results


def evaluate_t_plus_n(
    recommendations: list[dict],
    target_date: datetime.date,
    market_df: pl.DataFrame,
    period: int = 5,
) -> dict:
    """Evaluate T+N performance using pre-loaded market data."""

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

    stock_details = []
    for row in perf.iter_rows(named=True):
        rec = next((r for r in recommendations if r["code"] == row["code"]), {})
        stock_details.append(
            {
                "code": row["code"],
                "name": rec.get("name", ""),
                "score": rec.get("score", 0),
                "return": row["return_pct"],
            }
        )

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
    top_n: int = 5,
    period: int = 5,
    verbose: bool = False,
):
    """Run the full backtest."""

    t_start = time.time()

    # Determine date range: earliest test date minus lookback, latest test date + period buffer
    earliest = min(test_dates)
    latest = max(test_dates)
    # Add buffer for T+period evaluation (30 calendar days should cover ~20 trading days)
    end_date = latest + datetime.timedelta(days=45)

    log.info(f"=== Alpha Radar Backtest ===")
    log.info(f"Test dates: {len(test_dates)} dates ({earliest} to {latest})")
    log.info(f"Tabs: {', '.join(tabs)}")
    log.info(f"Top N: {top_n}, Eval period: T+{period}")
    log.info(f"Loading data...")

    async with async_session_maker() as db:
        market_df, valuation_df, style_df, profile_df, index_df = await load_all_data(
            db, earliest, end_date, lookback_days=60
        )

    log.info(f"\nRunning backtest...")

    all_results = {}
    abstained_dates = set()
    for d in test_dates:
        all_results[d] = {}
        for tab in tabs:
            t0 = time.time()
            recs = compute_scores_for_date(
                d, market_df, valuation_df, style_df, profile_df, index_df, tab, top_n
            )
            if not recs and tab == tabs[0]:
                # Gate triggered — check if this is abstention vs genuinely empty
                regime_score, weak_days = compute_regime_score(d, index_df, market_df)
                if regime_score < 45 and weak_days >= 3:
                    abstained_dates.add(d)
            perf = evaluate_t_plus_n(recs, d, market_df, period)
            elapsed = time.time() - t0
            all_results[d][tab] = {
                "recommendations": recs,
                "performance": perf,
                "elapsed": elapsed,
            }

            if verbose and recs:
                log.info(f"\n  {d} | {tab} | {elapsed:.2f}s")
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
                    log.info(
                        f"    {r['code']} {r['name']:<8} score={r['score']:.1f} close={r['close']:.2f} chg={r['pct_chg']:.2f}% → T+{period}: {ret_val}% {marker}"
                    )
            elif verbose and d in abstained_dates and tab == tabs[0]:
                regime_score, weak_days = compute_regime_score(d, index_df, market_df)
                log.info(f"\n  {d} | ABSTAIN (regime={regime_score:.1f}, weak_days={weak_days})")

    # Print summary table
    total_elapsed = time.time() - t_start
    log.info(f"\n{'=' * 110}")
    log.info(f"{'Date':<14} {'PANORAMA':<26} {'SMART':<26} {'VALUE':<26} {'TREND':<26}")
    log.info(f"{'=' * 110}")

    for d in test_dates:
        row_parts = [f"{d!s:<14}"]
        for tab in tabs:
            if d in abstained_dates:
                row_parts.append("ABSTAIN                   ")
            else:
                perf = all_results[d][tab]["performance"]
                wr = perf["win_rate"]
                ar = perf["avg_return"]
                plr = perf["profit_loss_ratio"]
                if wr is not None:
                    row_parts.append(f"WR={wr}% AR={ar}% PL={plr or 'N/A':<5}")
                else:
                    row_parts.append("NO_DATA                   ")
        log.info(" ".join(row_parts))

    # Averages (excluding abstained dates)
    log.info(f"\n{'--- AVERAGES ---':^110}")
    if abstained_dates:
        log.info(
            f"  (Excluding {len(abstained_dates)} abstained date(s): {', '.join(str(d) for d in sorted(abstained_dates))})"
        )
    log.info(f"{'Tab':<14} {'Avg WR':<12} {'Avg AR':<12} {'Avg P/L':<12} {'# Dates':<10}")
    log.info("-" * 60)

    for tab in tabs:
        wrs, ars, plrs = [], [], []
        for d in test_dates:
            if d in abstained_dates:
                continue
            perf = all_results[d][tab]["performance"]
            if perf["win_rate"] is not None:
                wrs.append(perf["win_rate"])
                ars.append(perf["avg_return"])
                if perf["profit_loss_ratio"] is not None:
                    plrs.append(perf["profit_loss_ratio"])

        if wrs:
            avg_wr = sum(wrs) / len(wrs)
            avg_ar = sum(ars) / len(ars)
            avg_plr = sum(plrs) / len(plrs) if plrs else None
            plr_str = f"{avg_plr:.2f}" if avg_plr is not None else "N/A"
            log.info(
                f"{tab:<14} {avg_wr:.1f}%{'':<7} {avg_ar:.2f}%{'':<7} {plr_str:<12} {len(wrs)}"
            )
        else:
            log.info(f"{tab:<14} NO DATA")

    log.info(f"\nTotal time: {total_elapsed:.1f}s")
    return all_results


def parse_args():
    parser = argparse.ArgumentParser(description="Alpha Radar Backtest")
    parser.add_argument("--top-n", type=int, default=5, help="Top N stocks per tab (default: 5)")
    parser.add_argument("--period", type=int, default=5, help="Evaluation period T+N (default: 5)")
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated dates (YYYY-MM-DD). Default: 8 dates from 2025-12 to 2026-01",
    )
    parser.add_argument(
        "--tabs",
        type=str,
        default="panorama,smart,value,trend",
        help="Comma-separated tabs to test",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-stock details")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.dates:
        test_dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    else:
        test_dates = [
            datetime.date(2025, 12, 1),
            datetime.date(2025, 12, 8),
            datetime.date(2025, 12, 15),
            datetime.date(2025, 12, 22),
            datetime.date(2025, 12, 29),
            datetime.date(2026, 1, 5),
            datetime.date(2026, 1, 12),
            datetime.date(2026, 1, 19),
        ]

    tabs = [t.strip() for t in args.tabs.split(",")]
    asyncio.run(run_backtest(test_dates, tabs, args.top_n, args.period, args.verbose))


if __name__ == "__main__":
    main()
