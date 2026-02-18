"""Post Spike Consolidation — Rewards stocks that stabilized after a volume spike.

Output columns:
    post_spike_consolidation (0-100): Score for consolidation quality.
    days_since_vol_peak_20d (0-20): Days since the 20d volume peak.

Dependencies:
    Requires columns: vol_ma20, recent_vol_spike_max

Used by strategies:
    weekly, rally
"""

import polars as pl
from . import EPS


def compute_post_spike_consolidation(df: pl.DataFrame) -> pl.DataFrame:
    """Compute post_spike_consolidation and days_since_vol_peak_20d."""
    # --- Iter 6: Time-since-volume-peak factors ---
    # 12-08 failure: massive vol explosions 2-4 days before → post-spike decay.
    # 01-05 success: vol peak 6-10 days earlier → consolidated at elevated levels.
    # Existing volume_spike_penalty only checks TODAY's vol vs 20d MA, missing
    # the temporal dimension entirely.

    # Factor 1: days_since_vol_peak_20d (0-20)
    # Trading days since 20d volume maximum. Peak day = 0. Penalty when < 5.
    vol_rolling_max_20 = (
        pl.col("volume").cast(pl.Float64).rolling_max(window_size=20).over("code", order_by="date")
    )
    df = df.with_columns(
        [
            (pl.col("volume").cast(pl.Float64) >= vol_rolling_max_20 - EPS)
            .cast(pl.Int32)
            .alias("_is_vol_peak"),
        ]
    )
    # cum_sum of peak markers creates group IDs; counting rows within each group
    # gives days since last peak (peak day itself = row 1 → subtract 1)
    df = df.with_columns(
        [pl.col("_is_vol_peak").cum_sum().over("code", order_by="date").alias("_peak_group")]
    )
    df = df.with_columns(
        [pl.col("date").rank("ordinal").over(["code", "_peak_group"]).alias("_rows_in_group")]
    )
    df = df.with_columns(
        [
            (pl.col("_rows_in_group") - 1)
            .clip(0, 20)
            .cast(pl.Float64)
            .alias("days_since_vol_peak_20d"),
        ]
    )
    df = df.drop(["_is_vol_peak", "_peak_group", "_rows_in_group"])

    # Factor 3: post_spike_consolidation (0-100)
    # Rewards 01-05 pattern: vol subsided + enough time since peak + no recent mega-spike.
    # A: temporal distance (0-40pts), B: vol calm-down (0-30pts), C: no recent spike (0-30pts)
    df = df.with_columns(
        [
            (
                (pl.col("days_since_vol_peak_20d").clip(0.0, 10.0) / 10.0 * 40)
                + (
                    (
                        1.0
                        - (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + EPS)).clip(
                            0.5, 2.0
                        )
                        / 2.0
                    )
                    * 30
                ).fill_null(15.0)
                + ((1.0 - pl.col("recent_vol_spike_max") / 100.0) * 30)
            )
            .clip(0.0, 100.0)
            .fill_null(50.0)
            .alias("post_spike_consolidation"),
        ]
    )

    return df
