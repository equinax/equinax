"""Climax Factor — Detects signs of buying climax (potential reversal).

Output columns:
    climax_score (0-100): High score indicates high risk of climax.

Dependencies:
    Requires columns: pct_chg, volume, vol_ma20, price_position_60d, upper_shadow_ratio, vol_peak_today_20d

Used by strategies:
    overnight, dragon, weekly, rally
"""

import polars as pl
from . import EPS


def compute_climax(df: pl.DataFrame) -> pl.DataFrame:
    """Compute climax_score."""
    # Climax score: big daily gain + volume spike + at 60d high + upper shadow + vol peak
    df = df.with_columns(
        [
            (
                (pl.col("pct_chg").fill_null(0.0).clip(0.0, 20.0) / 10.0) * 0.30
                + (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + EPS))
                .clip(0.0, 5.0)
                .fill_null(1.0)
                / 5.0
                * 0.25
                + pl.col("price_position_60d").fill_null(0.5).clip(0.0, 1.0) * 0.20
                + pl.col("upper_shadow_ratio").fill_null(0.0).clip(0.0, 1.0) * 0.15
                + pl.col("vol_peak_today_20d").fill_null(0.0) * 0.10
            )
            .fill_null(0.0)
            .alias("climax_score"),
        ]
    )
    return df
