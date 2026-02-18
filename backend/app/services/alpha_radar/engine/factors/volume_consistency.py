"""Volume Consistency Factor — Measures the smoothness of volume expansion.

Output columns:
    volume_consistency_score (0-100): Inverse of volume CV. High score = consistent volume (good).

Dependencies:
    Requires columns: vol_cv_10d

Used by strategies:
    overnight, dragon, weekly
"""

import polars as pl


def compute_volume_consistency(df: pl.DataFrame) -> pl.DataFrame:
    """Compute volume_consistency_score and add to DataFrame."""
    df = df.with_columns(
        [
            # volume_consistency_score (0-100): inverse of vol_cv_10d
            # Low CV = gradual ramp (恒邦 pattern, good)
            # High CV = explosive spikes (神宇 pattern, bad)
            # vol_cv_10d typical range: 0.1 (very consistent) to 1.0+ (explosive)
            ((1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 1.0)) / 0.9 * 100)
            .fill_null(50.0)
            .alias("volume_consistency_score"),
        ]
    )
    return df
