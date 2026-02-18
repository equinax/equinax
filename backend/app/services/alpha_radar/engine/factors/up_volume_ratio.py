"""Up Volume Ratio — Measures volume concentration on up days.

Output columns:
    up_volume_ratio (0-1): Ratio of up-day volume to total volume.

Dependencies:
    Requires columns: up_volume_20d, total_volume_20d

Used by strategies:
    dragon
"""

import polars as pl


def compute_up_volume_ratio(df: pl.DataFrame) -> pl.DataFrame:
    """Compute up_volume_ratio and add to DataFrame."""
    df = df.with_columns(
        [
            (pl.col("up_volume_20d") / pl.col("total_volume_20d").clip(lower_bound=1.0))
            .fill_null(0.5)
            .alias("up_volume_ratio"),
        ]
    )
    return df
