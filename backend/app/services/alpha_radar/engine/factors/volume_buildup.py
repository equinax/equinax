"""Volume Buildup Quality — Sequential accumulation signal.

Output columns:
    volume_buildup_quality (0-100): Score based on 3d/5d/10d volume alignment.

Dependencies:
    Requires columns: volume_ma5, vol_ma20

Used by strategies:
    dragon, weekly, rally
"""

import polars as pl
from . import EPS, _sig


def compute_volume_buildup(df: pl.DataFrame) -> pl.DataFrame:
    """Compute volume_buildup_quality and add to DataFrame."""
    # Iter 5: volume_buildup_quality (0-100) — sequential accumulation signal.
    # Compares 3d-avg volume vs 10d-avg volume, penalized by today's spike.
    # A stock where 3d > 5d > 10d avg volume AND today isn't a spike = quality buildup.
    # vol_ma5 already computed above.
    df = df.with_columns(
        [
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=3)
            .over("code", order_by="date")
            .alias("vol_ma3"),
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=10)
            .over("code", order_by="date")
            .alias("vol_ma10"),
        ]
    )

    # Continuous volume_buildup_quality (Iter 10 de-ceiling)
    # Log-ratio alignment strength + acceleration + smoothness for within-regime differentiation
    _vol3 = pl.col("vol_ma3").clip(lower_bound=1.0)
    _vol5 = pl.col("volume_ma5").clip(lower_bound=1.0)
    _vol10 = pl.col("vol_ma10").clip(lower_bound=1.0)

    _r35 = (_vol3 / _vol5).log()
    _r510 = (_vol5 / _vol10).log()
    _r310 = (_vol3 / _vol10).log()

    _ratio35 = _sig(_r35 / 0.06)
    _ratio510 = _sig(_r510 / 0.06)
    _ratio310 = _sig(_r310 / 0.08)

    _vol3_lag3 = pl.col("vol_ma3").shift(3).over("code", order_by="date").clip(lower_bound=1.0)
    _accel = _sig((_vol3 / _vol3_lag3).log().fill_null(0.0) / 0.18)

    _vmean10 = (
        pl.col("volume")
        .cast(pl.Float64)
        .rolling_mean(window_size=10)
        .over("code", order_by="date")
        .clip(lower_bound=1.0)
    )
    _vstd10 = (
        pl.col("volume")
        .cast(pl.Float64)
        .rolling_std(window_size=10)
        .over("code", order_by="date")
        .fill_null(0.0)
    )
    _cv10 = _vstd10 / _vmean10
    _smooth = pl.lit(1.0) - _sig((_cv10 - 1.0) / 0.35)

    _vbq_strength80 = 0.55 * (0.5 * _ratio35 + 0.5 * _ratio510) + 0.25 * _accel + 0.20 * _smooth
    _vbq_strength60 = 0.70 * _ratio310 + 0.30 * _smooth
    _vbq_strength50 = 0.70 * _ratio510 + 0.30 * _smooth
    _vbq_strength25 = _smooth

    _vbq_base = (
        pl.when(
            (pl.col("vol_ma3") > pl.col("volume_ma5")) & (pl.col("volume_ma5") > pl.col("vol_ma10"))
        )
        .then(74.0 + 6.0 * _vbq_strength80)
        .when(pl.col("vol_ma3") > pl.col("vol_ma10"))
        .then(56.0 + 4.0 * _vbq_strength60)
        .when(pl.col("volume_ma5") > pl.col("vol_ma10"))
        .then(47.0 + 3.0 * _vbq_strength50)
        .otherwise(22.0 + 3.0 * _vbq_strength25)
    )

    _vbq_spike = (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma3") + EPS) - 1.0).clip(
        0.0, 2.0
    ) * 20.0

    df = df.with_columns(
        [
            (_vbq_base - _vbq_spike)
            .clip(0.0, 100.0)
            .fill_null(40.0)
            .alias("volume_buildup_quality"),
        ]
    )

    df = df.drop(["vol_ma3", "vol_ma10"])
    return df
