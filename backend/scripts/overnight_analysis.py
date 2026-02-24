"""Overnight strategy deep analysis.

Runs full-population backtest and outputs structured analysis:
- Per-date WR/AR with regime details
- Score-return correlation
- Regime-based performance segmentation
- Abstention analysis

Usage:
    docker compose exec api python -m scripts.overnight_analysis
"""

import asyncio
import csv
import datetime
import json
import logging
import os
import statistics
import sys
import time

import polars as pl

from scripts.alpha_radar_backtest import (
    SAMPLE_MARGIN_DAYS,
    compute_regime_score,
    compute_scores_for_date,
    evaluate_t_plus_n,
    get_trading_days,
    load_all_data,
)

from app.db.session import async_session_maker
from app.services.alpha_radar.engine.config_loader import load_strategy_config

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


async def run_analysis():
    t_start = time.time()
    tab = "overnight"
    config = load_strategy_config(tab)
    top_n = config.backtest_top_n
    eval_period = config.eval_period

    # Get ALL eligible trading days
    all_days = get_trading_days()
    eligible = all_days[SAMPLE_MARGIN_DAYS : len(all_days) - SAMPLE_MARGIN_DAYS]
    log.info(f"Total eligible dates: {len(eligible)} ({eligible[0]} to {eligible[-1]})")

    # Load data
    earliest = min(eligible)
    latest = max(eligible)
    end_date = latest + datetime.timedelta(days=55)

    async with async_session_maker() as db:
        (
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
        ) = await load_all_data(db, earliest, end_date, lookback_days=60)

    log.info(f"Data loaded in {time.time() - t_start:.1f}s\n")

    # ---- Run per-date analysis ----
    date_results = []
    all_stock_results = []
    abstained_dates = []
    active_dates = []

    for i, d in enumerate(eligible):
        regime_score, weak_days, regime_details = compute_regime_score(
            d, index_df, market_df, moneyflow_df, limit_df
        )

        recs, _confidence = compute_scores_for_date(
            d,
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            tab,
            top_n,
            moneyflow_df=moneyflow_df,
            limit_df=limit_df,
        )

        if not recs:
            # Abstained
            date_results.append(
                {
                    "date": str(d),
                    "status": "abstained",
                    "regime_score": round(regime_score, 1),
                    "weak_days": weak_days,
                    "breadth_today": round(regime_details.get("breadth_today", 0), 1),
                    "mf_pct_inflow": round(regime_details.get("mf_pct_inflow", 0), 1),
                    "mf_avg_net": round(regime_details.get("mf_avg_net", 0), 0),
                    "limit_down": regime_details.get("limit_down", 0),
                    "breadth_5d_avg": round(regime_details.get("breadth_5d_avg", 0), 1),
                    "win_rate": None,
                    "avg_return": None,
                    "n_stocks": 0,
                    "avg_score": None,
                    "max_score": None,
                    "min_score": None,
                }
            )
            abstained_dates.append(d)
            continue

        perf = evaluate_t_plus_n(recs, d, market_df, eval_period, limit_df=limit_df, tab=tab)

        scores = [r["score"] for r in recs if r.get("score") is not None]
        avg_score = round(statistics.mean(scores), 1) if scores else None
        max_score = round(max(scores), 1) if scores else None
        min_score = round(min(scores), 1) if scores else None

        date_results.append(
            {
                "date": str(d),
                "status": "active",
                "regime_score": round(regime_score, 1),
                "weak_days": weak_days,
                "breadth_today": round(regime_details.get("breadth_today", 0), 1),
                "mf_pct_inflow": round(regime_details.get("mf_pct_inflow", 0), 1),
                "mf_avg_net": round(regime_details.get("mf_avg_net", 0), 0),
                "limit_down": regime_details.get("limit_down", 0),
                "breadth_5d_avg": round(regime_details.get("breadth_5d_avg", 0), 1),
                "win_rate": perf["win_rate"],
                "avg_return": perf["avg_return"],
                "n_stocks": len(perf["stocks"]),
                "avg_score": avg_score,
                "max_score": max_score,
                "min_score": min_score,
            }
        )
        active_dates.append(d)

        # Collect per-stock details
        for stock in perf["stocks"]:
            all_stock_results.append(
                {
                    "date": str(d),
                    "code": stock["code"],
                    "name": stock.get("name", ""),
                    "score": stock.get("score", 0),
                    "return_pct": stock.get("return", 0),
                    "regime_score": round(regime_score, 1),
                    "breadth_today": round(regime_details.get("breadth_today", 0), 1),
                }
            )

        if (i + 1) % 50 == 0:
            log.info(f"  Processed {i + 1}/{len(eligible)} dates...")

    # ---- Write raw data ----
    with open("/tmp/overnight_dates.json", "w") as f:
        json.dump(date_results, f, indent=2)
    with open("/tmp/overnight_stocks.json", "w") as f:
        json.dump(all_stock_results, f, indent=2)

    # ---- Print analysis ----
    active_results = [r for r in date_results if r["status"] == "active"]
    wrs = [r["win_rate"] for r in active_results if r["win_rate"] is not None]
    ars = [r["avg_return"] for r in active_results if r["avg_return"] is not None]

    log.info(f"\n{'=' * 80}")
    log.info(f"OVERNIGHT STRATEGY — FULL POPULATION ANALYSIS")
    log.info(f"{'=' * 80}")
    log.info(f"Total dates: {len(eligible)}")
    log.info(
        f"Active dates: {len(active_results)} ({len(active_results) / len(eligible) * 100:.1f}%)"
    )
    log.info(
        f"Abstained dates: {len(abstained_dates)} ({len(abstained_dates) / len(eligible) * 100:.1f}%)"
    )
    log.info(f"")
    log.info(f"Population WR: {statistics.mean(wrs):.1f}%")
    log.info(f"Population AR: {statistics.mean(ars):.2f}%")
    log.info(f"WR Median: {statistics.median(wrs):.1f}%")
    log.info(f"AR Median: {statistics.median(ars):.2f}%")
    log.info(f"WR Std Dev: {statistics.stdev(wrs):.1f}%")
    log.info(f"AR Std Dev: {statistics.stdev(ars):.2f}%")

    # WR distribution
    wr_buckets = {"0%": 0, "20%": 0, "40%": 0, "60%": 0, "80%": 0, "100%": 0}
    for wr in wrs:
        if wr == 0:
            wr_buckets["0%"] += 1
        elif wr <= 20:
            wr_buckets["20%"] += 1
        elif wr <= 40:
            wr_buckets["40%"] += 1
        elif wr <= 60:
            wr_buckets["60%"] += 1
        elif wr <= 80:
            wr_buckets["80%"] += 1
        else:
            wr_buckets["100%"] += 1

    log.info(f"\n--- Win Rate Distribution ---")
    for bucket, count in wr_buckets.items():
        pct = count / len(wrs) * 100
        bar = "█" * int(pct / 2)
        log.info(f"  WR={bucket:<4} : {count:>3} dates ({pct:>5.1f}%) {bar}")

    # AR distribution
    ar_buckets = {"< -5%": 0, "-5 to -2%": 0, "-2 to 0%": 0, "0 to 2%": 0, "2 to 5%": 0, "> 5%": 0}
    for ar in ars:
        if ar < -5:
            ar_buckets["< -5%"] += 1
        elif ar < -2:
            ar_buckets["-5 to -2%"] += 1
        elif ar < 0:
            ar_buckets["-2 to 0%"] += 1
        elif ar < 2:
            ar_buckets["0 to 2%"] += 1
        elif ar < 5:
            ar_buckets["2 to 5%"] += 1
        else:
            ar_buckets["> 5%"] += 1

    log.info(f"\n--- Avg Return Distribution ---")
    for bucket, count in ar_buckets.items():
        pct = count / len(ars) * 100
        bar = "█" * int(pct / 2)
        log.info(f"  AR {bucket:<10} : {count:>3} dates ({pct:>5.1f}%) {bar}")

    # ---- Regime-based segmentation ----
    log.info(f"\n--- Performance by Regime Score ---")
    regime_bins = [
        ("regime < 40", lambda r: r["regime_score"] < 40),
        ("regime 40-55", lambda r: 40 <= r["regime_score"] < 55),
        ("regime 55-70", lambda r: 55 <= r["regime_score"] < 70),
        ("regime >= 70", lambda r: r["regime_score"] >= 70),
    ]
    for label, pred in regime_bins:
        subset = [r for r in active_results if pred(r) and r["win_rate"] is not None]
        if subset:
            s_wr = statistics.mean([r["win_rate"] for r in subset])
            s_ar = statistics.mean([r["avg_return"] for r in subset])
            log.info(f"  {label:<15}: n={len(subset):>3}, WR={s_wr:.1f}%, AR={s_ar:+.2f}%")
        else:
            log.info(f"  {label:<15}: n=  0")

    # ---- Breadth-based segmentation ----
    log.info(f"\n--- Performance by Breadth Today ---")
    breadth_bins = [
        ("breadth < 40%", lambda r: r["breadth_today"] < 40),
        ("breadth 40-50%", lambda r: 40 <= r["breadth_today"] < 50),
        ("breadth 50-60%", lambda r: 50 <= r["breadth_today"] < 60),
        ("breadth >= 60%", lambda r: r["breadth_today"] >= 60),
    ]
    for label, pred in breadth_bins:
        subset = [r for r in active_results if pred(r) and r["win_rate"] is not None]
        if subset:
            s_wr = statistics.mean([r["win_rate"] for r in subset])
            s_ar = statistics.mean([r["avg_return"] for r in subset])
            log.info(f"  {label:<18}: n={len(subset):>3}, WR={s_wr:.1f}%, AR={s_ar:+.2f}%")
        else:
            log.info(f"  {label:<18}: n=  0")

    # ---- Moneyflow-based segmentation ----
    log.info(f"\n--- Performance by Moneyflow Inflow % ---")
    mf_bins = [
        ("mf_inflow < 40%", lambda r: r["mf_pct_inflow"] < 40),
        ("mf_inflow 40-50%", lambda r: 40 <= r["mf_pct_inflow"] < 50),
        ("mf_inflow 50-60%", lambda r: 50 <= r["mf_pct_inflow"] < 60),
        ("mf_inflow >= 60%", lambda r: r["mf_pct_inflow"] >= 60),
    ]
    for label, pred in mf_bins:
        subset = [r for r in active_results if pred(r) and r["win_rate"] is not None]
        if subset:
            s_wr = statistics.mean([r["win_rate"] for r in subset])
            s_ar = statistics.mean([r["avg_return"] for r in subset])
            log.info(f"  {label:<20}: n={len(subset):>3}, WR={s_wr:.1f}%, AR={s_ar:+.2f}%")
        else:
            log.info(f"  {label:<20}: n=  0")

    # ---- Score-return correlation ----
    log.info(f"\n--- Score vs Return Correlation ---")
    scores_list = [s["score"] for s in all_stock_results if s["return_pct"] is not None]
    returns_list = [s["return_pct"] for s in all_stock_results if s["return_pct"] is not None]

    if len(scores_list) > 10:
        # Simple Pearson correlation
        n = len(scores_list)
        mean_s = statistics.mean(scores_list)
        mean_r = statistics.mean(returns_list)
        cov = sum((s - mean_s) * (r - mean_r) for s, r in zip(scores_list, returns_list)) / n
        std_s = statistics.stdev(scores_list)
        std_r = statistics.stdev(returns_list)
        corr = cov / (std_s * std_r) if std_s > 0 and std_r > 0 else 0
        log.info(f"  Pearson(score, return): {corr:.4f}")
        log.info(f"  Mean score: {mean_s:.1f}, Std: {std_s:.1f}")
        log.info(f"  Mean return: {mean_r:.2f}%, Std: {std_r:.2f}%")

    # Score quintile analysis
    log.info(f"\n--- Return by Score Quintile ---")
    sorted_stocks = sorted(all_stock_results, key=lambda s: s["score"])
    q_size = len(sorted_stocks) // 5
    if q_size > 0:
        for qi in range(5):
            start_idx = qi * q_size
            end_idx = (qi + 1) * q_size if qi < 4 else len(sorted_stocks)
            quintile = sorted_stocks[start_idx:end_idx]
            q_scores = [s["score"] for s in quintile]
            q_returns = [s["return_pct"] for s in quintile if s["return_pct"] is not None]
            if q_returns:
                q_wr = len([r for r in q_returns if r > 0]) / len(q_returns) * 100
                q_ar = statistics.mean(q_returns)
                log.info(
                    f"  Q{qi + 1} (score {min(q_scores):.0f}-{max(q_scores):.0f}): n={len(q_returns)}, WR={q_wr:.1f}%, AR={q_ar:+.2f}%"
                )

    # ---- Avg score on winning vs losing dates ----
    log.info(f"\n--- Score on Winning vs Losing Dates ---")
    win_dates = [r for r in active_results if r["win_rate"] is not None and r["win_rate"] >= 60]
    lose_dates = [r for r in active_results if r["win_rate"] is not None and r["win_rate"] <= 20]
    mid_dates = [r for r in active_results if r["win_rate"] is not None and 20 < r["win_rate"] < 60]

    for label, group in [
        ("Win (WR>=60%)", win_dates),
        ("Mid (20<WR<60%)", mid_dates),
        ("Lose (WR<=20%)", lose_dates),
    ]:
        if group:
            g_regime = statistics.mean([r["regime_score"] for r in group])
            g_breadth = statistics.mean([r["breadth_today"] for r in group])
            g_mf = statistics.mean([r["mf_pct_inflow"] for r in group])
            g_score = statistics.mean([r["avg_score"] for r in group if r["avg_score"] is not None])
            log.info(
                f"  {label:<20}: n={len(group):>3}, regime={g_regime:.1f}, breadth={g_breadth:.1f}%, mf_inflow={g_mf:.1f}%, avg_score={g_score:.1f}"
            )

    # ---- Hypothetical: more aggressive abstention ----
    log.info(f"\n--- Hypothetical Abstention Scenarios ---")

    scenarios = [
        ("Current (baseline)", lambda r: True),
        ("regime >= 45 only", lambda r: r["regime_score"] >= 45),
        ("regime >= 50 only", lambda r: r["regime_score"] >= 50),
        ("regime >= 55 only", lambda r: r["regime_score"] >= 55),
        ("regime >= 60 only", lambda r: r["regime_score"] >= 60),
        ("breadth >= 45% only", lambda r: r["breadth_today"] >= 45),
        ("breadth >= 50% only", lambda r: r["breadth_today"] >= 50),
        ("breadth >= 55% only", lambda r: r["breadth_today"] >= 55),
        ("mf_inflow >= 45%", lambda r: r["mf_pct_inflow"] >= 45),
        ("mf_inflow >= 50%", lambda r: r["mf_pct_inflow"] >= 50),
        (
            "regime>=50 & breadth>=50",
            lambda r: r["regime_score"] >= 50 and r["breadth_today"] >= 50,
        ),
        (
            "regime>=55 & breadth>=50",
            lambda r: r["regime_score"] >= 55 and r["breadth_today"] >= 50,
        ),
        (
            "regime>=60 & breadth>=55",
            lambda r: r["regime_score"] >= 60 and r["breadth_today"] >= 55,
        ),
        ("score >= 55 only", lambda r: r["avg_score"] is not None and r["avg_score"] >= 55),
        ("score >= 60 only", lambda r: r["avg_score"] is not None and r["avg_score"] >= 60),
        ("score >= 65 only", lambda r: r["avg_score"] is not None and r["avg_score"] >= 65),
    ]

    for label, pred in scenarios:
        subset = [r for r in active_results if pred(r) and r["win_rate"] is not None]
        if subset:
            s_wr = statistics.mean([r["win_rate"] for r in subset])
            s_ar = statistics.mean([r["avg_return"] for r in subset])
            log.info(f"  {label:<30}: n={len(subset):>3} active, WR={s_wr:.1f}%, AR={s_ar:+.2f}%")
        else:
            log.info(f"  {label:<30}: n=  0 active")

    # ---- Abstained date analysis: what if we hadn't abstained? ----
    log.info(f"\n--- Abstained Date Counterfactual ---")
    abstained_results = [r for r in date_results if r["status"] == "abstained"]
    if abstained_results:
        log.info(f"  {len(abstained_results)} abstained dates — regime stats:")
        abs_regimes = [r["regime_score"] for r in abstained_results]
        abs_breadths = [r["breadth_today"] for r in abstained_results]
        log.info(
            f"  Regime: mean={statistics.mean(abs_regimes):.1f}, min={min(abs_regimes):.1f}, max={max(abs_regimes):.1f}"
        )
        log.info(
            f"  Breadth: mean={statistics.mean(abs_breadths):.1f}%, min={min(abs_breadths):.1f}%, max={max(abs_breadths):.1f}%"
        )

    # ---- Top 10 best and worst dates ----
    log.info(f"\n--- Top 10 BEST Active Dates ---")
    sorted_by_ar = sorted(
        [r for r in active_results if r["avg_return"] is not None],
        key=lambda r: r["avg_return"],
        reverse=True,
    )
    for r in sorted_by_ar[:10]:
        log.info(
            f"  {r['date']} WR={r['win_rate']}% AR={r['avg_return']:+.2f}% regime={r['regime_score']:.0f} breadth={r['breadth_today']:.0f}% mf={r['mf_pct_inflow']:.0f}% score={r['avg_score']}"
        )

    log.info(f"\n--- Top 10 WORST Active Dates ---")
    for r in sorted_by_ar[-10:]:
        log.info(
            f"  {r['date']} WR={r['win_rate']}% AR={r['avg_return']:+.2f}% regime={r['regime_score']:.0f} breadth={r['breadth_today']:.0f}% mf={r['mf_pct_inflow']:.0f}% score={r['avg_score']}"
        )

    total_elapsed = time.time() - t_start
    log.info(f"\nAnalysis complete in {total_elapsed:.1f}s")
    log.info(f"Data saved to /tmp/overnight_dates.json and /tmp/overnight_stocks.json")


def main():
    asyncio.run(run_analysis())


if __name__ == "__main__":
    main()
