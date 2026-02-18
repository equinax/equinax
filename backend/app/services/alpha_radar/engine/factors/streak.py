"""Streak Factor — Counts consecutive up/down days.

Output columns:
    consecutive_up_days: Consecutive positive pct_chg days ending today.
    consecutive_down_days: Consecutive negative pct_chg days ending today.

Dependencies:
    Requires columns: pct_chg

Used by strategies:
    overnight (inertia momentum pattern, doji reversal pattern)
"""

import polars as pl


def compute_streak(df: pl.DataFrame) -> pl.DataFrame:
    """Compute consecutive_up_days and consecutive_down_days.

    Uses cumsum-of-breaks: non-up days increment a group counter,
    and within each group cumsum gives streak length.
    """
    pct = pl.col("pct_chg").fill_null(0.0)

    df = df.with_columns(
        [
            (pct > 0).cast(pl.Int32).alias("_is_up"),
            (pct < 0).cast(pl.Int32).alias("_is_down"),
        ]
    )

    df = df.with_columns(
        [
            (1 - pl.col("_is_up")).cum_sum().over("code", order_by="date").alias("_up_group"),
            (1 - pl.col("_is_down")).cum_sum().over("code", order_by="date").alias("_down_group"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("_is_up")
            .cum_sum()
            .over(["code", "_up_group"], order_by="date")
            .cast(pl.Float64)
            .alias("consecutive_up_days"),
            pl.col("_is_down")
            .cum_sum()
            .over(["code", "_down_group"], order_by="date")
            .cast(pl.Float64)
            .alias("consecutive_down_days"),
        ]
    )

    df = df.drop(["_is_up", "_is_down", "_up_group", "_down_group"])
    return df
