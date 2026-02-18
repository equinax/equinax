"""Main Strength Factor — Proxy for main force (institutional) strength.

Output columns:
    main_strength_proxy (0-100): Score based on volume ramp, consistency, etc.

Dependencies:
    Requires columns: vol_ramp_5v20, vol_cv_10d, up_volume_ratio, turnover_change_20d

Used by strategies:
    dragon
"""

import polars as pl


def compute_main_strength(df: pl.DataFrame) -> pl.DataFrame:
    """Compute main_strength_proxy."""
    # main_strength_proxy (0-100): quiet multi-day accumulation signal
    # Iter 3: replaced volume_ratio_5d (rewarded same-day spikes) with
    # gradual ramp + consistency + up-volume concentration + turnover ramp
    df = df.with_columns(
        [
            (
                pl.col("vol_ramp_5v20").clip(0.0, 0.6) / 0.6 * 30
                + (1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 0.8)) / 0.7 * 25
                + pl.col("up_volume_ratio").fill_null(0.5).clip(0.3, 0.8) / 0.8 * 25
                + (pl.col("turnover_change_20d").clip(-0.5, 0.5) + 0.5) * 20
            )
            .clip(0.0, 100.0)
            .alias("main_strength_proxy"),
        ]
    )
    return df
