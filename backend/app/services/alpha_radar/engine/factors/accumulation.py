"""Accumulation Factor — Detects signs of institutional accumulation.

Output columns:
    accumulation_score (0-100): Score based on volume ramp, close strength, etc.

Dependencies:
    Requires columns: vol_ramp_5v20, turn_ramp_5v20, close_strength, up_volume_ratio

Used by strategies:
    overnight, dragon, weekly, rally
"""

import polars as pl


def compute_accumulation(df: pl.DataFrame) -> pl.DataFrame:
    """Compute accumulation_score."""
    # Accumulation score: gradual volume/turnover ramp + strong close + up-volume
    df = df.with_columns(
        [
            (
                pl.col("vol_ramp_5v20").clip(-0.2, 1.0) * 0.30
                + pl.col("turn_ramp_5v20").clip(-0.2, 1.0) * 0.30
                + pl.col("close_strength").clip(0.0, 1.0) * 0.20
                + pl.col("up_volume_ratio").fill_null(0.5).clip(0.0, 1.0) * 0.20
            )
            .fill_null(0.0)
            .alias("accumulation_score"),
        ]
    )
    return df
