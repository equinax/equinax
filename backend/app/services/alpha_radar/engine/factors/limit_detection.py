"""Limit Detection Factor — Identifies stocks near limit-up.

Output columns:
    near_limit_up (bool): True if pct_chg is near the limit (10% or 20%).

Dependencies:
    Requires columns: code, pct_chg

Used by strategies:
    (Filter, not scoring)
"""

import polars as pl


def compute_limit_detection(df: pl.DataFrame) -> pl.DataFrame:
    """Compute near_limit_up flag."""
    # Limit-up / near-limit detection
    is_20pct = pl.col("code").str.contains(r"^(sz\.(300|301)|sh\.688)")
    limit_threshold = pl.when(is_20pct).then(pl.lit(19.0)).otherwise(pl.lit(9.5))

    df = df.with_columns(
        [
            (pl.col("pct_chg").fill_null(0.0) >= limit_threshold).alias("near_limit_up"),
        ]
    )
    return df
