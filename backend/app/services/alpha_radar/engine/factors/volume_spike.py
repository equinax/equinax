"""Volume Spike Factor — Penalizes explosive volume spikes.

Output columns:
    volume_spike_penalty (0-100): Penalty for massive volume explosion today.
    recent_vol_spike_max (0-100): Max volume spike ratio in last 5 days.

Dependencies:
    Requires columns: vol_ma20

Used by strategies:
    overnight, dragon, weekly, rally (recent_vol_spike_max)
"""

import polars as pl
from . import EPS


def compute_volume_spike(df: pl.DataFrame) -> pl.DataFrame:
    """Compute volume_spike_penalty and recent_vol_spike_max."""
    # Iter 5: volume_spike_penalty (0-100) — penalizes stocks where today's
    # volume exploded vs 20d average (like 神宇 10x spike on 12-02/03).
    # vol/vol_ma20 > 3.0 → penalty starts; > 5.0 → max penalty
    df = df.with_columns(
        [
            (
                (
                    (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + EPS)).clip(1.0, 5.0)
                    - 1.0
                )
                / 4.0
                * 100
            )
            .fill_null(0.0)
            .alias("volume_spike_penalty"),
        ]
    )

    # Factor 2: recent_vol_spike_max (0-100)
    # Max(vol/vol_ma20) over last 5 days. Captures explosions in IMMEDIATE past
    # even if today's volume subsided. Normalized: ratio 1.0→0, 5.0+→100.
    # 12-08 picks ~8-13x (spike 2-4 days ago); 01-05 picks ~2-4x (spike >5 days ago)
    df = df.with_columns(
        [
            (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + EPS))
            .fill_null(1.0)
            .alias("_vol_ratio_today"),
        ]
    )
    df = df.with_columns(
        [
            pl.col("_vol_ratio_today")
            .rolling_max(window_size=5)
            .over("code", order_by="date")
            .fill_null(1.0)
            .alias("_recent_max_vol_ratio"),
        ]
    )
    df = df.with_columns(
        [
            ((pl.col("_recent_max_vol_ratio").clip(1.0, 5.0) - 1.0) / 4.0 * 100)
            .fill_null(0.0)
            .alias("recent_vol_spike_max"),
        ]
    )
    df = df.drop(["_vol_ratio_today", "_recent_max_vol_ratio"])

    return df
