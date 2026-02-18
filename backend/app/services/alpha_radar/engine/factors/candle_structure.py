"""Candle Structure Factor — Analyzes daily candle shape.

Output columns:
    close_strength (0-1): Close relative to high/low range.
    upper_shadow_ratio (0-1): Length of upper shadow relative to range.

Dependencies:
    Requires columns: high, low, close

Used by strategies:
    overnight
"""

import polars as pl
from . import EPS


def compute_candle_structure(df: pl.DataFrame) -> pl.DataFrame:
    """Compute close_strength and upper_shadow_ratio."""
    # Candle structure: close strength & upper shadow
    df = df.with_columns(
        [
            # close_strength: close near high = strong (no selling pressure)
            ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + EPS))
            .fill_null(0.5)
            .clip(0.0, 1.0)
            .alias("close_strength"),
            # upper_shadow_ratio: high selling pressure = bad
            ((pl.col("high") - pl.col("close")) / (pl.col("high") - pl.col("low") + EPS))
            .fill_null(0.0)
            .clip(0.0, 1.0)
            .alias("upper_shadow_ratio"),
        ]
    )
    return df
