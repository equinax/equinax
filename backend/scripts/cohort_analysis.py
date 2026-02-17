"""Cohort analysis: retrospective limit-up factor attribution pipeline.

Given a date range, identifies stocks that hit limit-up, stratifies them by
consecutive limit-up count (1板/2板/3+板), extracts factor snapshots at
various lookback windows (T-5/T-10/T-20), compares against a control group,
and computes Cohen's d effect sizes for each factor.

Includes discovery/validation set split (60/40) to prevent data leakage.
Outputs structured cohort_analysis.yaml for use by the alpha-analyst skill.

Usage:
    docker compose exec api python -m scripts.cohort_analysis
    docker compose exec api python -m scripts.cohort_analysis --months 6
    docker compose exec api python -m scripts.cohort_analysis --output /app/alpha_lab/theories/dragon/cohort_analysis.yaml
    docker compose exec api python -m scripts.cohort_analysis --strategy dragon --months 3
"""

import argparse
import asyncio
import datetime
import logging
import math
import os
import random
import time
from collections import defaultdict
from typing import Optional

import polars as pl
import yaml
from sqlalchemy import text

from app.db.session import async_session_maker
from app.services.alpha_radar.polars_engine import PolarsEngine
from scripts.alpha_radar_backtest import load_all_data

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# Default output path inside Docker container
DEFAULT_OUTPUT_DIR = "/app/alpha_lab/theories/dragon"

FACTOR_COLUMNS = [
    # Core scoring factors
    "main_strength_proxy",
    "accumulation_score",
    "volume_consistency_score",
    "trend_quality_20d",
    "volume_buildup_quality",
    "climax_score",
    "stability_score",
    "ma_alignment_score",
    # Price position
    "price_position_60d",
    "price_range_position_20d",
    # Momentum
    "momentum_quality_ratio",
    "return_5d",
    "pct_chg",
    # Volume dynamics
    "vol_ramp_5v20",
    "turn_ramp_5v20",
    "volume_ratio_5d",
    "vol_cv_10d",
    # Post-spike indicators
    "days_since_vol_peak_20d",
    "recent_vol_spike_max",
    "post_spike_consolidation",
    # Resistance / exhaustion
    "resistance_proximity_penalty",
    "exhaustion_at_ceiling",
    # Sector
    "sector_momentum_5d",
    # Moneyflow
    "mf_net_percentile",
    "elg_net_percentile",
    # Candle structure
    "close_strength",
    "upper_shadow_ratio",
    # Trend internals
    "positive_day_ratio_20d",
    "up_volume_ratio",
    "turnover_change_20d",
]


def get_trading_days(
    start: datetime.date,
    end: datetime.date,
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


def find_limit_up_events(
    limit_df: pl.DataFrame,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pl.DataFrame:
    """Find all limit-up events and compute consecutive limit-up streaks.

    Returns a DataFrame with columns:
        code, first_lu_date, lu_count (total in window), max_consecutive,
        streak_category ('1板', '2板', '3+板')
    """
    lu = limit_df.filter(
        (pl.col("date") >= start_date)
        & (pl.col("date") <= end_date)
        & (pl.col("limit_type") == "U")
    ).sort(["code", "date"])

    if lu.is_empty():
        return pl.DataFrame(
            schema={
                "code": pl.Utf8,
                "first_lu_date": pl.Date,
                "lu_count": pl.UInt32,
                "max_consecutive": pl.Int64,
                "streak_category": pl.Utf8,
            }
        )

    # Consecutive limit-up detection: gap ≤ 4 calendar days = adjacent trading days
    # (accounts for weekends without requiring a full trading calendar)
    records = []
    for code in lu["code"].unique().to_list():
        code_lu = lu.filter(pl.col("code") == code).sort("date")
        dates = code_lu["date"].to_list()
        lu_count = len(dates)
        first_lu_date = dates[0]

        max_consec = 1
        current_consec = 1
        for i in range(1, len(dates)):
            gap = (dates[i] - dates[i - 1]).days
            if gap <= 4:
                current_consec += 1
            else:
                max_consec = max(max_consec, current_consec)
                current_consec = 1
        max_consec = max(max_consec, current_consec)

        if max_consec >= 3:
            streak_cat = "3+板"
        elif max_consec == 2:
            streak_cat = "2板"
        else:
            streak_cat = "1板"

        records.append(
            {
                "code": code,
                "first_lu_date": first_lu_date,
                "lu_count": lu_count,
                "max_consecutive": max_consec,
                "streak_category": streak_cat,
            }
        )

    return pl.DataFrame(records)


def extract_factor_snapshot(
    target_date: datetime.date,
    codes: list[str],
    market_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    lookback_days: int = 60,
) -> pl.DataFrame:
    """Extract factor values for given stocks on a given date.

    Reuses PolarsEngine.calculate_technical_indicators() to ensure
    exact same factor computation as the scoring pipeline.

    Returns a DataFrame with columns: code + FACTOR_COLUMNS (those available).
    """
    engine = PolarsEngine.__new__(PolarsEngine)

    all_dates = market_df.select("date").unique().sort("date")
    dates_before = all_dates.filter(pl.col("date") <= target_date)
    if dates_before.height < 20:
        return pl.DataFrame(schema={"code": pl.Utf8})

    lookback_dates = dates_before.tail(lookback_days)
    lookback_start = lookback_dates["date"][0]

    df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= target_date))
    if df.is_empty():
        return pl.DataFrame(schema={"code": pl.Utf8})

    if codes:
        df = df.filter(pl.col("code").is_in(codes))

    df = engine.calculate_technical_indicators(df)

    df = df.filter(pl.col("date") == target_date)
    if df.is_empty():
        return pl.DataFrame(schema={"code": pl.Utf8})

    if not profile_df.is_empty():
        df = df.join(profile_df, on="code", how="left")

    sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, target_date)
    if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
        df = df.join(
            sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
            on="sw_industry_l1",
            how="left",
        )
        df = df.with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

    if moneyflow_df is not None and not moneyflow_df.is_empty():
        mf_today = moneyflow_df.filter(pl.col("date") == target_date)
        if not mf_today.is_empty():
            mf_features = mf_today.with_columns(
                [
                    (pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw"),
                ]
            ).select(["code", "net_mf_amount", "elg_net_raw"])
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

    available_factors = [c for c in FACTOR_COLUMNS if c in df.columns]
    select_cols = ["code"] + available_factors
    return df.select(select_cols)


def sample_control_group(
    target_date: datetime.date,
    exclude_codes: set[str],
    market_df: pl.DataFrame,
    n_samples: int = 200,
    seed: int = 42,
) -> list[str]:
    """Sample control group stocks: non-limit-up stocks from the same date.

    Excludes index codes and the treatment group codes.
    """
    universe = (
        market_df.filter(
            (pl.col("date") == target_date)
            & (~pl.col("code").str.starts_with("sh.000"))
            & (~pl.col("code").str.starts_with("sz.399"))
            & (~pl.col("code").is_in(list(exclude_codes)))
        )
        .select("code")
        .unique()
    )

    all_codes = universe["code"].to_list()
    if len(all_codes) <= n_samples:
        return all_codes

    rng = random.Random(seed)
    return rng.sample(all_codes, n_samples)


def cohens_d(treatment: list[float], control: list[float]) -> Optional[float]:
    """Compute Cohen's d effect size between two groups.

    Returns None if either group has fewer than 2 observations.
    Uses pooled standard deviation.
    """
    n1, n2 = len(treatment), len(control)
    if n1 < 2 or n2 < 2:
        return None

    mean1 = sum(treatment) / n1
    mean2 = sum(control) / n2

    var1 = sum((x - mean1) ** 2 for x in treatment) / (n1 - 1)
    var2 = sum((x - mean2) ** 2 for x in control) / (n2 - 1)

    pooled_sd = math.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    if pooled_sd < 1e-10:
        return 0.0

    return (mean1 - mean2) / pooled_sd


def interpret_effect_size(d: Optional[float]) -> str:
    """Interpret Cohen's d magnitude."""
    if d is None:
        return "insufficient_data"
    ad = abs(d)
    if ad < 0.2:
        return "negligible"
    elif ad < 0.5:
        return "small"
    elif ad < 0.8:
        return "medium"
    else:
        return "large"


async def run_cohort_analysis(
    months: int = 6,
    strategy: str = "dragon",
    output_dir: str = DEFAULT_OUTPUT_DIR,
    control_n: int = 200,
    seed: int = 42,
    lookback_windows: list[int] | None = None,
) -> dict:
    """Run the full cohort analysis pipeline.

    1. Load data for the specified time window
    2. Find all limit-up events and stratify
    3. For each lookback window, extract factor snapshots
    4. Sample control group and compute effect sizes
    5. Split into discovery/validation sets (60/40)
    6. Output structured YAML

    Args:
        months: How many months of history to analyze
        strategy: Strategy name (used for output path)
        output_dir: Directory for output YAML
        control_n: Number of control group stocks to sample per date
        seed: Random seed for control sampling and discovery/validation split
        lookback_windows: Days before first limit-up to extract factors (default [5, 10, 20])

    Returns:
        The cohort analysis result dict.
    """
    if lookback_windows is None:
        lookback_windows = [5, 10, 20]

    t_start = time.time()

    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=months * 30)

    # Extra lookback for technical indicator computation (60d rolling windows)
    data_start = start_date - datetime.timedelta(days=90)

    log.info("=" * 80)
    log.info("COHORT ANALYSIS: Limit-Up Factor Attribution")
    log.info("=" * 80)
    log.info(f"  Strategy:     {strategy}")
    log.info(f"  Date range:   {start_date} to {end_date}")
    log.info(f"  Lookback windows: T-{', T-'.join(str(w) for w in lookback_windows)}")
    log.info(f"  Control group size: {control_n} per date")
    log.info(f"  Seed: {seed}")
    log.info("")

    log.info("Step 1: Loading data...")
    async with async_session_maker() as db:
        (
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
        ) = await load_all_data(db, data_start, end_date, lookback_days=60)

    log.info(f"  Loaded. Market rows: {market_df.height}, Limit rows: {limit_df.height}")

    log.info("\nStep 2: Finding limit-up events...")
    lu_events = find_limit_up_events(limit_df, start_date, end_date)
    if lu_events.is_empty():
        log.info("  No limit-up events found in date range. Exiting.")
        return {}

    total_lu = lu_events.height
    cat_counts = lu_events.group_by("streak_category").agg(pl.len().alias("count"))
    log.info(f"  Total limit-up stocks: {total_lu}")
    for row in cat_counts.iter_rows(named=True):
        log.info(f"    {row['streak_category']}: {row['count']}")

    log.info("\nStep 3: Splitting discovery/validation sets (60/40 by date)...")
    all_lu_dates = sorted(lu_events["first_lu_date"].unique().to_list())
    split_idx = int(len(all_lu_dates) * 0.6)
    discovery_dates = set(all_lu_dates[:split_idx])
    validation_dates = set(all_lu_dates[split_idx:])

    discovery_events = lu_events.filter(pl.col("first_lu_date").is_in(list(discovery_dates)))
    validation_events = lu_events.filter(pl.col("first_lu_date").is_in(list(validation_dates)))
    log.info(f"  Discovery: {discovery_events.height} stocks ({len(discovery_dates)} dates)")
    log.info(f"  Validation: {validation_events.height} stocks ({len(validation_dates)} dates)")

    log.info("\nStep 4: Extracting factor snapshots...")

    all_trading_dates = sorted(market_df.select("date").unique()["date"].to_list())
    date_to_idx = {d: i for i, d in enumerate(all_trading_dates)}

    def get_lookback_date(from_date: datetime.date, n_days: int) -> Optional[datetime.date]:
        """Get the trading date that is n_days before from_date."""
        idx = date_to_idx.get(from_date)
        if idx is None:
            for i, d in enumerate(all_trading_dates):
                if d >= from_date:
                    idx = max(0, i - 1)
                    break
            if idx is None:
                return None
        target_idx = idx - n_days
        if target_idx < 0:
            return None
        return all_trading_dates[target_idx]

    results_by_window: dict[int, dict] = {}

    for window in lookback_windows:
        log.info(f"\n  Processing T-{window} window...")

        treatment_data: dict[str, list[float]] = defaultdict(list)
        control_data: dict[str, list[float]] = defaultdict(list)
        treatment_by_streak: dict[str, dict[str, list[float]]] = {
            "1板": defaultdict(list),
            "2板": defaultdict(list),
            "3+板": defaultdict(list),
        }
        n_treatment_extracted = 0
        n_control_extracted = 0

        events_by_date: dict[datetime.date, list[dict]] = defaultdict(list)
        for row in lu_events.iter_rows(named=True):
            events_by_date[row["first_lu_date"]].append(row)

        unique_dates = sorted(events_by_date.keys())
        for lu_date in unique_dates:
            snapshot_date = get_lookback_date(lu_date, window)
            if snapshot_date is None:
                continue

            events = events_by_date[lu_date]
            lu_codes = [e["code"] for e in events]

            treatment_snapshot = extract_factor_snapshot(
                snapshot_date,
                lu_codes,
                market_df,
                profile_df,
                moneyflow_df,
            )

            if treatment_snapshot.is_empty() or treatment_snapshot.height == 0:
                continue

            control_codes = sample_control_group(
                snapshot_date,
                set(lu_codes),
                market_df,
                n_samples=control_n,
                seed=seed + hash(lu_date) % 10000,
            )

            control_snapshot = extract_factor_snapshot(
                snapshot_date,
                control_codes,
                market_df,
                profile_df,
                moneyflow_df,
            )

            available_factors = [c for c in FACTOR_COLUMNS if c in treatment_snapshot.columns]
            for factor in available_factors:
                vals = treatment_snapshot[factor].drop_nulls().to_list()
                treatment_data[factor].extend(vals)
                n_treatment_extracted += len(vals)

                for event in events:
                    code = event["code"]
                    cat = event["streak_category"]
                    row = treatment_snapshot.filter(pl.col("code") == code)
                    if not row.is_empty():
                        val = row[factor][0]
                        if val is not None:
                            treatment_by_streak[cat][factor].append(val)

                if not control_snapshot.is_empty() and factor in control_snapshot.columns:
                    c_vals = control_snapshot[factor].drop_nulls().to_list()
                    control_data[factor].extend(c_vals)
                    n_control_extracted += len(c_vals)

        log.info(
            f"    Treatment observations: {n_treatment_extracted // max(len(treatment_data), 1)} "
            f"per factor ({len(treatment_data)} factors)"
        )
        log.info(
            f"    Control observations: {n_control_extracted // max(len(control_data), 1)} "
            f"per factor"
        )

        window_results: dict = {
            "overall": {},
            "by_streak": {"1板": {}, "2板": {}, "3+板": {}},
        }

        for factor in sorted(treatment_data.keys()):
            t_vals = treatment_data[factor]
            c_vals = control_data.get(factor, [])

            d = cohens_d(t_vals, c_vals)
            interpretation = interpret_effect_size(d)
            t_mean = sum(t_vals) / len(t_vals) if t_vals else None
            c_mean = sum(c_vals) / len(c_vals) if c_vals else None

            window_results["overall"][factor] = {
                "cohens_d": round(d, 4) if d is not None else None,
                "effect_size": interpretation,
                "treatment_mean": round(t_mean, 4) if t_mean is not None else None,
                "control_mean": round(c_mean, 4) if c_mean is not None else None,
                "treatment_n": len(t_vals),
                "control_n": len(c_vals),
            }

            for cat in ["1板", "2板", "3+板"]:
                cat_vals = treatment_by_streak[cat].get(factor, [])
                if cat_vals:
                    cat_d = cohens_d(cat_vals, c_vals)
                    cat_mean = sum(cat_vals) / len(cat_vals)
                    window_results["by_streak"][cat][factor] = {
                        "cohens_d": round(cat_d, 4) if cat_d is not None else None,
                        "effect_size": interpret_effect_size(cat_d),
                        "treatment_mean": round(cat_mean, 4),
                        "treatment_n": len(cat_vals),
                    }

        results_by_window[window] = window_results

    log.info("\nStep 5: Computing discovery vs validation comparison...")
    dv_comparison: dict = {}

    primary_window = lookback_windows[0]
    for split_name, split_events in [
        ("discovery", discovery_events),
        ("validation", validation_events),
    ]:
        split_treatment: dict[str, list[float]] = defaultdict(list)
        split_control: dict[str, list[float]] = defaultdict(list)

        split_by_date: dict[datetime.date, list[dict]] = defaultdict(list)
        for row in split_events.iter_rows(named=True):
            split_by_date[row["first_lu_date"]].append(row)

        for lu_date in sorted(split_by_date.keys()):
            snapshot_date = get_lookback_date(lu_date, primary_window)
            if snapshot_date is None:
                continue

            events = split_by_date[lu_date]
            lu_codes = [e["code"] for e in events]

            treatment_snapshot = extract_factor_snapshot(
                snapshot_date,
                lu_codes,
                market_df,
                profile_df,
                moneyflow_df,
            )
            if treatment_snapshot.is_empty():
                continue

            control_codes = sample_control_group(
                snapshot_date,
                set(lu_codes),
                market_df,
                n_samples=control_n,
                seed=seed + hash(lu_date) % 10000,
            )
            control_snapshot = extract_factor_snapshot(
                snapshot_date,
                control_codes,
                market_df,
                profile_df,
                moneyflow_df,
            )

            available_factors = [c for c in FACTOR_COLUMNS if c in treatment_snapshot.columns]
            for factor in available_factors:
                vals = treatment_snapshot[factor].drop_nulls().to_list()
                split_treatment[factor].extend(vals)
                if not control_snapshot.is_empty() and factor in control_snapshot.columns:
                    c_vals = control_snapshot[factor].drop_nulls().to_list()
                    split_control[factor].extend(c_vals)

        split_results = {}
        for factor in sorted(split_treatment.keys()):
            t_vals = split_treatment[factor]
            c_vals = split_control.get(factor, [])
            d = cohens_d(t_vals, c_vals)
            split_results[factor] = {
                "cohens_d": round(d, 4) if d is not None else None,
                "effect_size": interpret_effect_size(d),
                "treatment_n": len(t_vals),
            }
        dv_comparison[split_name] = split_results

    log.info("\nStep 6: Identifying stable signals...")
    stable_signals = []
    discovery_results = dv_comparison.get("discovery", {})
    validation_results = dv_comparison.get("validation", {})

    for factor in sorted(set(discovery_results.keys()) & set(validation_results.keys())):
        disc_d = discovery_results[factor].get("cohens_d")
        val_d = validation_results[factor].get("cohens_d")
        if disc_d is None or val_d is None:
            continue

        # A signal is "stable" if:
        # 1. Same sign in both sets (direction consistent)
        # 2. Both have at least small effect size (|d| >= 0.2)
        same_sign = (disc_d > 0 and val_d > 0) or (disc_d < 0 and val_d < 0)
        both_meaningful = abs(disc_d) >= 0.2 and abs(val_d) >= 0.2

        if same_sign and both_meaningful:
            stable_signals.append(
                {
                    "factor": factor,
                    "discovery_d": round(disc_d, 4),
                    "validation_d": round(val_d, 4),
                    "direction": "positive" if disc_d > 0 else "negative",
                    "avg_effect": round((abs(disc_d) + abs(val_d)) / 2, 4),
                    "stability_ratio": round(
                        min(abs(disc_d), abs(val_d)) / max(abs(disc_d), abs(val_d)), 4
                    ),
                }
            )

    stable_signals.sort(key=lambda x: x["avg_effect"], reverse=True)

    log.info(f"  Found {len(stable_signals)} stable signals:")
    for sig in stable_signals:
        direction_marker = "↑" if sig["direction"] == "positive" else "↓"
        log.info(
            f"    {direction_marker} {sig['factor']:<35} "
            f"disc={sig['discovery_d']:+.3f} val={sig['validation_d']:+.3f} "
            f"stability={sig['stability_ratio']:.2f}"
        )

    elapsed = time.time() - t_start
    log.info(f"\nStep 7: Assembling output... (elapsed: {elapsed:.1f}s)")

    output = {
        "metadata": {
            "generated_at": datetime.datetime.now().isoformat(),
            "strategy": strategy,
            "date_range": {
                "start": str(start_date),
                "end": str(end_date),
            },
            "lookback_windows": lookback_windows,
            "control_group_size": control_n,
            "seed": seed,
            "total_limit_up_stocks": total_lu,
            "elapsed_seconds": round(elapsed, 1),
        },
        "stratification": {
            cat: int(row["count"])
            for row in cat_counts.iter_rows(named=True)
            for cat in [row["streak_category"]]
        },
        "discovery_validation_split": {
            "discovery_dates": len(discovery_dates),
            "validation_dates": len(validation_dates),
            "discovery_stocks": discovery_events.height,
            "validation_stocks": validation_events.height,
            "split_method": "temporal_60_40",
        },
        "factor_attribution": {f"T_minus_{w}": results_by_window[w] for w in lookback_windows},
        "discovery_vs_validation": dv_comparison,
        "stable_signals": stable_signals,
    }

    log.info(f"\n{'=' * 100}")
    log.info("FACTOR ATTRIBUTION SUMMARY (T-5 window)")
    log.info(f"{'=' * 100}")
    log.info(
        f"{'Factor':<35} {'Cohen d':>8} {'Effect':>12} "
        f"{'Treat μ':>9} {'Ctrl μ':>9} {'n_t':>5} {'n_c':>5}"
    )
    log.info("-" * 100)

    if primary_window in results_by_window:
        overall = results_by_window[primary_window]["overall"]
        sorted_factors = sorted(
            overall.items(),
            key=lambda x: abs(x[1].get("cohens_d") or 0),
            reverse=True,
        )
        for factor, stats in sorted_factors:
            d = stats.get("cohens_d")
            eff = stats.get("effect_size", "?")
            t_mean = stats.get("treatment_mean")
            c_mean = stats.get("control_mean")
            t_n = stats.get("treatment_n", 0)
            c_n = stats.get("control_n", 0)

            d_str = f"{d:+.4f}" if d is not None else "N/A"
            t_str = f"{t_mean:.4f}" if t_mean is not None else "N/A"
            c_str = f"{c_mean:.4f}" if c_mean is not None else "N/A"

            log.info(f"  {factor:<33} {d_str:>8} {eff:>12} {t_str:>9} {c_str:>9} {t_n:>5} {c_n:>5}")

    output_path = os.path.join(output_dir, "cohort_analysis.yaml")
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(
            output,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )

    log.info(f"\nOutput written to: {output_path}")
    log.info(f"Total elapsed: {elapsed:.1f}s")

    return output


def parse_args():
    parser = argparse.ArgumentParser(description="Cohort Analysis: Limit-Up Factor Attribution")
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Months of history to analyze (default: 6)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="dragon",
        help="Strategy name for output path (default: dragon)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output YAML path (default: /app/alpha_lab/theories/<strategy>/cohort_analysis.yaml)",
    )
    parser.add_argument(
        "--control-n",
        type=int,
        default=200,
        help="Control group size per date (default: 200)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default: 42)",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="5,10,20",
        help="Comma-separated lookback windows in trading days (default: 5,10,20)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    output_dir = (
        os.path.dirname(args.output) if args.output else f"/app/alpha_lab/theories/{args.strategy}"
    )

    lookback_windows = [int(w.strip()) for w in args.windows.split(",")]

    asyncio.run(
        run_cohort_analysis(
            months=args.months,
            strategy=args.strategy,
            output_dir=output_dir,
            control_n=args.control_n,
            seed=args.seed,
            lookback_windows=lookback_windows,
        )
    )


if __name__ == "__main__":
    main()
