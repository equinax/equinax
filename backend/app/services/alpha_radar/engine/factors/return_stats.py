"""Return Statistics Factor — Basic return statistics and stability.

Output columns:
    return_std_20d, return_mean_20d, max_loss_20d
    stability_score (0-100)

Dependencies:
    Requires columns: return_std_20d (from intermediates)

Used by strategies:
    overnight (stability_score)
"""

import polars as pl


def compute_return_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Compute stability_score (other stats are in intermediates)."""
    # Stability score (0-100): calmer uptrends survive market pullbacks better
    # return_std_20d typical range: 0.5 (very stable) to 5.0+ (volatile)
    # Stocks with std < 1.5% get high stability; std > 4% get near-zero
    df = df.with_columns(
        [
            ((1 - (pl.col("return_std_20d").clip(0.5, 4.5) - 0.5) / 4.0) * 100)
            .fill_null(50.0)
            .alias("stability_score"),
        ]
    )
    return df
