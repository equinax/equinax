"""Price Position Factor — Measures where price sits relative to recent ranges.

Output columns:
    price_position_60d (0-1): Position in 60d range.
    price_range_position_20d (0-1): Position in 20d range.

Dependencies:
    Requires columns: high_60d, low_60d, high_20d, low_20d

Used by strategies:
    dragon (price_position_60d)
"""

import polars as pl
from . import EPS


def compute_price_position(df: pl.DataFrame) -> pl.DataFrame:
    """Compute price_position_60d and price_range_position_20d."""
    # Calculate derived indicators
    df = df.with_columns(
        [
            # Price position (0-1)
            ((pl.col("close") - pl.col("low_60d")) / (pl.col("high_60d") - pl.col("low_60d")))
            .fill_null(0.5)
            .clip(0.0, 1.0)
            .alias("price_position_60d"),
        ]
    )

    # Factor 1: price_range_position_20d (0-100)
    # Where the close sits in the 20-day high-low range.
    # Failed stocks on 12-22: avg ~77-93% (at ceiling). Success: ~30-73% (room to expand).
    # Unlike price_position_60d which uses 60d range, this captures SHORT-TERM ceiling.
    df = df.with_columns(
        [
            ((pl.col("close") - pl.col("low_20d")) / (pl.col("high_20d") - pl.col("low_20d") + EPS))
            .fill_null(0.5)
            .clip(0.0, 1.0)
            .alias("price_range_position_20d"),
        ]
    )

    return df
