"""Uptrend Cycle Factor — Measures uptrend quality, rally/pullback asymmetry, and phase.

Designed for overnight strategy v7: identifies stocks in "main uptrend" (主升段)
with controlled pullback cycles, where the next day has high probability of continuation.

Output columns:
    return_60d (float): 60-day cumulative return percentage.
    uptrend_score_60d (0-100): Overall 60-day uptrend quality.
        Combines: positive return, rally dominance, smoothness.
    rally_dominance_20d (0-100): Rally days dominate pullback days in 20d.
        High when: more up days, up magnitudes > down magnitudes.
    cycle_smoothness_20d (0-100): Low daily volatility relative to trend.
        High when: return_std is low relative to return_mean (controlled moves).
    max_consecutive_down_20d (float): Longest pullback streak in 20 trading days.
        Low is good (short pullbacks = healthy uptrend, not death spiral).

Dependencies:
    Requires columns: pct_chg, close, _close_5d_ago (from intermediates)
    Phase: Must run AFTER intermediates and streak.

Used by strategies:
    overnight (v7+ uptrend pullback reversal pattern)
"""

import polars as pl
from . import EPS


def compute_uptrend_cycle(df: pl.DataFrame) -> pl.DataFrame:
    """Compute uptrend cycle quality factors."""

    pct = pl.col("pct_chg").fill_null(0.0)

    # --- return_60d: 60-day cumulative return ---
    # Simple close/close_60d_ago - 1, as percentage.
    df = df.with_columns(
        pl.col("close").shift(60).over("code", order_by="date").alias("_close_60d_ago"),
    )
    df = df.with_columns(
        ((pl.col("close") / (pl.col("_close_60d_ago") + EPS) - 1) * 100)
        .fill_null(0.0)
        .alias("return_60d"),
    )

    # --- Rally dominance factors (20d window) ---
    # Count of up days and down days in 20d
    is_up = pl.when(pct > 0).then(1.0).otherwise(0.0)
    is_down = pl.when(pct < 0).then(1.0).otherwise(0.0)
    up_magnitude = pl.when(pct > 0).then(pct).otherwise(0.0)
    down_magnitude = pl.when(pct < 0).then(pct.abs()).otherwise(0.0)

    df = df.with_columns(
        [
            is_up.rolling_sum(window_size=20)
            .over("code", order_by="date")
            .fill_null(10.0)
            .alias("_up_days_20d"),
            is_down.rolling_sum(window_size=20)
            .over("code", order_by="date")
            .fill_null(10.0)
            .alias("_down_days_20d"),
            up_magnitude.rolling_sum(window_size=20)
            .over("code", order_by="date")
            .fill_null(0.0)
            .alias("_up_magnitude_sum_20d"),
            down_magnitude.rolling_sum(window_size=20)
            .over("code", order_by="date")
            .fill_null(0.0)
            .alias("_down_magnitude_sum_20d"),
        ]
    )

    # rally_dominance_20d (0-100):
    # Component 1: Day count ratio — more up days than down days (50% weight)
    #   up_ratio = up_days / 20.  Range 0.3 (bear) to 0.7 (bull). Normalize.
    # Component 2: Magnitude asymmetry — up magnitude > down magnitude (50% weight)
    #   mag_ratio = up_sum / (up_sum + down_sum). Range 0.3 to 0.7.
    up_ratio = (pl.col("_up_days_20d") / 20.0).clip(0.0, 1.0)
    mag_ratio = (
        pl.col("_up_magnitude_sum_20d")
        / (pl.col("_up_magnitude_sum_20d") + pl.col("_down_magnitude_sum_20d") + EPS)
    ).clip(0.0, 1.0)

    # Normalize: 0.5 = neutral, 0.7+ = strong rally dominance
    # Map [0.35, 0.75] → [0, 100]
    day_score = ((up_ratio - 0.35) / 0.40).clip(0.0, 1.0)
    mag_score = ((mag_ratio - 0.35) / 0.40).clip(0.0, 1.0)

    rally_dom = ((day_score * 0.50 + mag_score * 0.50) * 100).clip(0.0, 100.0)

    # --- cycle_smoothness_20d (0-100) ---
    # Stocks with controlled moves (low std) relative to their mean return.
    # Use return_std_20d directly (already computed in intermediates).
    # Low std = smooth. Map std [0.5, 5.0] → [100, 0].
    # Also factor in: if return_mean_20d is positive and std is moderate, that's ideal.
    smooth = ((5.0 - pl.col("return_std_20d").fill_null(2.5).clip(0.5, 5.0)) / 4.5 * 100).clip(
        0.0, 100.0
    )

    # --- max_consecutive_down_20d ---
    # The longest pullback streak within the last 20 days.
    # This requires a rolling window approach. We'll compute it via a UDF-free method:
    # For each row, look at the last 20 values of consecutive_down_days.
    # The max of consecutive_down_days in that window gives us the longest streak.
    # NOTE: consecutive_down_days resets on up days, so rolling_max captures the peak.
    df = df.with_columns(
        pl.col("consecutive_down_days")
        .fill_null(0.0)
        .rolling_max(window_size=20)
        .over("code", order_by="date")
        .fill_null(0.0)
        .alias("max_consecutive_down_20d"),
    )

    # --- uptrend_score_60d (0-100) ---
    # Comprehensive uptrend quality score.
    # Components:
    #   1. return_60d > 0 (stock is going up over 60d) — 40% weight
    #   2. rally_dominance_20d (recent rally > pullback) — 30% weight
    #   3. cycle_smoothness_20d (controlled moves) — 30% weight
    #
    # return_60d component: map [0%, 30%+] → [0, 100]. Negative = 0.
    ret60_score = (pl.col("return_60d").clip(0.0, 30.0) / 30.0 * 100).clip(0.0, 100.0)

    uptrend = (ret60_score * 0.40 + rally_dom * 0.30 + smooth * 0.30).clip(0.0, 100.0)

    df = df.with_columns(
        [
            rally_dom.fill_null(50.0).alias("rally_dominance_20d"),
            smooth.fill_null(50.0).alias("cycle_smoothness_20d"),
            uptrend.fill_null(0.0).alias("uptrend_score_60d"),
        ]
    )

    # Cleanup temp columns
    df = df.drop(
        [
            "_close_60d_ago",
            "_up_days_20d",
            "_down_days_20d",
            "_up_magnitude_sum_20d",
            "_down_magnitude_sum_20d",
        ]
    )

    return df
