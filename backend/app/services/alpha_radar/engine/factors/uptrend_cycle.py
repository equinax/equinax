"""Uptrend Cycle Factor — Measures uptrend quality, rally/pullback asymmetry, and phase.

Designed for overnight strategy v8: identifies stocks in "main uptrend" (主升段)
with controlled pullback cycles, where the next day has high probability of continuation.

Output columns:
    return_30d (float): 30-day cumulative return percentage (single shift(30)).
    uptrend_score_30d (0-100): Overall 30-day uptrend quality.
        Combines: positive return, rally dominance, smoothness.
    rally_dominance_20d (0-100): Rally days dominate pullback days in 20d.
    cycle_smoothness_20d (0-100): Low daily volatility relative to trend.
    max_consecutive_down_20d (float): Longest pullback streak in 20 trading days.

Dependencies:
    Requires columns: pct_chg, close (from intermediates)
    Phase: Must run AFTER intermediates and streak.

Used by strategies:
    overnight (v8+ uptrend pullback reversal pattern)

Constraint:
    All operations must fit within 60 rows of data (db_lookback = lookback_days = 60).
    Max shift depth = 30. No chained shifts.
"""

import polars as pl
from . import EPS


def compute_uptrend_cycle(df: pl.DataFrame) -> pl.DataFrame:
    """Compute uptrend cycle quality factors.

    All shift/rolling operations use ≤30 row depth, fitting within
    the 60-row db_lookback constraint.
    """

    pct = pl.col("pct_chg").fill_null(0.0)

    # --- return_30d via single shift(30) ---
    # Needs only 31 rows of data (current + 30 history).
    df = df.with_columns(
        pl.col("close").shift(30).over("code", order_by="date").alias("_close_30d_ago"),
    )
    df = df.with_columns(
        ((pl.col("close") / (pl.col("_close_30d_ago") + EPS) - 1) * 100)
        .fill_null(0.0)
        .alias("return_30d"),
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
    # Low std = smooth. Map std [0.5, 5.0] → [100, 0].
    smooth = ((5.0 - pl.col("return_std_20d").fill_null(2.5).clip(0.5, 5.0)) / 4.5 * 100).clip(
        0.0, 100.0
    )

    # --- max_consecutive_down_20d ---
    # Longest pullback streak in 20d window via rolling_max.
    df = df.with_columns(
        pl.col("consecutive_down_days")
        .fill_null(0.0)
        .rolling_max(window_size=20)
        .over("code", order_by="date")
        .fill_null(0.0)
        .alias("max_consecutive_down_20d"),
    )

    # --- uptrend_score_30d (0-100) ---
    # return_30d component: map [0%, 20%] → [0, 100]. Negative = 0.
    # 30d returns are naturally smaller than 60d, so ceiling is 20% (vs 30% for 60d).
    ret30_score = (pl.col("return_30d").clip(0.0, 20.0) / 20.0 * 100).clip(0.0, 100.0)

    uptrend = (ret30_score * 0.40 + rally_dom * 0.30 + smooth * 0.30).clip(0.0, 100.0)

    df = df.with_columns(
        [
            rally_dom.fill_null(50.0).alias("rally_dominance_20d"),
            smooth.fill_null(50.0).alias("cycle_smoothness_20d"),
            uptrend.fill_null(0.0).alias("uptrend_score_30d"),
        ]
    )

    # Cleanup temp columns
    df = df.drop(
        [
            "_close_30d_ago",
            "_up_days_20d",
            "_down_days_20d",
            "_up_magnitude_sum_20d",
            "_down_magnitude_sum_20d",
        ]
    )

    return df
