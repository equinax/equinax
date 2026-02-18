"""MA Alignment Factor — Scores the alignment of moving averages.

Output columns:
    ma_alignment_score (0-100): Score based on MA spacing, slope, and order.

Dependencies:
    Requires columns: ma_5, ma_10, ma_20, atr_20d

Used by strategies:
    rally
"""

import polars as pl
from . import _sig


def compute_ma_alignment(df: pl.DataFrame) -> pl.DataFrame:
    """Compute ma_alignment_score."""
    # Continuous ma_alignment_score (Iter 10 de-ceiling)
    # ATR-normalized spacing, slope, and overextension for within-regime differentiation
    _atr = pl.col("atr_20d")

    _z_c10 = (pl.col("close") - pl.col("ma_10")) / _atr
    _z_c20 = (pl.col("close") - pl.col("ma_20")) / _atr
    _z_1020 = (pl.col("ma_10") - pl.col("ma_20")) / _atr
    _z_510 = (pl.col("ma_5") - pl.col("ma_10")) / _atr
    _z_c5 = (pl.col("close") - pl.col("ma_5")) / _atr

    _d_ma10 = (pl.col("ma_10") - pl.col("ma_10").shift(3).over("code", order_by="date")) / _atr
    _d_ma20 = (pl.col("ma_20") - pl.col("ma_20").shift(5).over("code", order_by="date")) / _atr

    _price_ok = _sig((_z_c10 - 0.05) / 0.25)
    _spacing_ok = _sig((_z_1020 - 0.05) / 0.25)
    _slope10_ok = _sig(_d_ma10.fill_null(0.0) / 0.20)
    _slope20_ok = _sig(_d_ma20.fill_null(0.0) / 0.15)
    _ma5_ok = _sig(_z_510 / 0.20) * _sig(_z_c5 / 0.20)
    _overext = _sig((_z_c10 - 1.8) / 0.35)

    _strength100 = (
        0.30 * _slope10_ok
        + 0.20 * _slope20_ok
        + 0.25 * _spacing_ok
        + 0.15 * _price_ok
        + 0.10 * _ma5_ok
    ) * (pl.lit(1.0) - 0.6 * _overext)

    _strength65 = (
        0.55 * _sig(_z_c20 / 0.35) + 0.30 * _slope10_ok + 0.15 * _sig(_z_1020 / 0.35)
    ) * (pl.lit(1.0) - 0.4 * _overext)

    _strength45 = (0.60 * _sig(_z_c10 / 0.35) + 0.40 * _slope10_ok) * (pl.lit(1.0) - 0.4 * _overext)

    _strength20 = _sig(_z_c20 / 0.60)

    df = df.with_columns(
        [
            (
                pl.when((pl.col("close") > pl.col("ma_10")) & (pl.col("ma_10") > pl.col("ma_20")))
                .then(93.0 + 7.0 * _strength100)
                .when(pl.col("close") > pl.col("ma_20"))
                .then(60.0 + 5.0 * _strength65)
                .when(pl.col("close") > pl.col("ma_10"))
                .then(40.0 + 5.0 * _strength45)
                .otherwise(17.0 + 3.0 * _strength20)
            )
            .clip(0.0, 100.0)
            .fill_null(50.0)
            .alias("ma_alignment_score"),
        ]
    )
    return df
