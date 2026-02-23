"""Shared intermediate calculations used by multiple factors.

Output columns:
    volume_ma5, turnover_ma20, turnover_ma5
    vol_ma20, vol_cv_10d, vol_jump_1d
    vol_ramp_5v20, turn_ramp_5v20
    vol_peak_today_20d
    up_volume_20d, total_volume_20d
    high_60d, low_60d
    ma_5, ma_10, ma_20, atr_20d
    high_20d, low_20d
    positive_day_ratio_20d
    return_std_20d, return_mean_20d, max_loss_20d
    _close_5d_ago, _close_10d_ago, _ret_5d, _ret_10d
    volume_ratio_5d
"""

import polars as pl
from . import EPS


def compute_intermediates(df: pl.DataFrame) -> pl.DataFrame:
    """Compute all shared intermediate columns."""

    # --- From volume.py ---
    df = df.with_columns(
        [
            # 5-day average volume
            pl.col("volume")
            .rolling_mean(window_size=5)
            .over("code", order_by="date")
            .alias("volume_ma5"),
            # 20-day average turnover
            pl.col("turn")
            .rolling_mean(window_size=20)
            .over("code", order_by="date")
            .alias("turnover_ma20"),
            # 5-day average turnover
            pl.col("turn")
            .rolling_mean(window_size=5)
            .over("code", order_by="date")
            .alias("turnover_ma5"),
        ]
    )

    df = df.with_columns(
        [
            # Volume ratio (current / 5d avg)
            (pl.col("volume") / pl.col("volume_ma5").shift(1).over("code", order_by="date"))
            .fill_null(1.0)
            .alias("volume_ratio_5d"),
            # Turnover change (5d avg / 20d avg - 1)
            ((pl.col("turnover_ma5") / pl.col("turnover_ma20")) - 1)
            .fill_null(0.0)
            .alias("turnover_change_20d"),
        ]
    )

    df = df.with_columns(
        [
            (
                pl.when(pl.col("pct_chg").fill_null(0.0) > 0)
                .then(pl.col("volume").cast(pl.Float64))
                .otherwise(pl.lit(0.0))
                .rolling_sum(window_size=20)
                .over("code", order_by="date")
            ).alias("up_volume_20d"),
            (
                pl.col("volume")
                .cast(pl.Float64)
                .rolling_sum(window_size=20)
                .over("code", order_by="date")
            ).alias("total_volume_20d"),
        ]
    )

    df = df.with_columns(
        [
            # 20d volume moving average (5d already exists as volume_ma5)
            pl.col("volume")
            .cast(pl.Float64)
            .rolling_mean(window_size=20)
            .over("code", order_by="date")
            .alias("vol_ma20"),
            # Volume coefficient of variation over 10d (low = gradual ramp)
            (
                pl.col("volume")
                .cast(pl.Float64)
                .rolling_std(window_size=10)
                .over("code", order_by="date")
                / (
                    pl.col("volume")
                    .cast(pl.Float64)
                    .rolling_mean(window_size=10)
                    .over("code", order_by="date")
                    + EPS
                )
            )
            .fill_null(0.5)
            .alias("vol_cv_10d"),
            # Volume ratio vs 20d average
            (
                pl.col("volume").cast(pl.Float64)
                / (pl.col("volume_ma5").shift(1).over("code", order_by="date") + EPS)
            )
            .fill_null(1.0)
            .alias("vol_jump_1d"),
            # Volume ramp: 5d avg / 20d avg - 1 (gradual increase > 0)
            (
                (
                    pl.col("volume")
                    .cast(pl.Float64)
                    .rolling_mean(window_size=5)
                    .over("code", order_by="date")
                    / (
                        pl.col("volume")
                        .cast(pl.Float64)
                        .rolling_mean(window_size=20)
                        .over("code", order_by="date")
                        + EPS
                    )
                )
                - 1
            )
            .fill_null(0.0)
            .alias("vol_ramp_5v20"),
            # Turnover ramp: 5d avg / 20d avg - 1
            (
                (
                    pl.col("turn")
                    .fill_null(0.0)
                    .rolling_mean(window_size=5)
                    .over("code", order_by="date")
                    / (
                        pl.col("turn")
                        .fill_null(0.0)
                        .rolling_mean(window_size=20)
                        .over("code", order_by="date")
                        + EPS
                    )
                )
                - 1
            )
            .fill_null(0.0)
            .alias("turn_ramp_5v20"),
            # Volume peak today (20d)
            (
                pl.col("volume").cast(pl.Float64)
                >= pl.col("volume")
                .cast(pl.Float64)
                .rolling_max(window_size=20)
                .over("code", order_by="date")
            )
            .cast(pl.Float64)
            .alias("vol_peak_today_20d"),
        ]
    )

    # --- From price.py ---
    df = df.with_columns(
        [
            pl.col("high")
            .rolling_max(window_size=60)
            .over("code", order_by="date")
            .alias("high_60d"),
            pl.col("low")
            .rolling_min(window_size=60)
            .over("code", order_by="date")
            .alias("low_60d"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("close").rolling_mean(window_size=5).over("code", order_by="date").alias("ma_5"),
            pl.col("close")
            .rolling_mean(window_size=10)
            .over("code", order_by="date")
            .alias("ma_10"),
            pl.col("close")
            .rolling_mean(window_size=20)
            .over("code", order_by="date")
            .alias("ma_20"),
            (
                pl.max_horizontal(
                    pl.col("high") - pl.col("low"),
                    (pl.col("high") - pl.col("preclose")).abs(),
                    (pl.col("low") - pl.col("preclose")).abs(),
                )
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .fill_null(1.0)
                .clip(lower_bound=0.01)
            ).alias("atr_20d"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("high")
            .rolling_max(window_size=20)
            .over("code", order_by="date")
            .alias("high_20d"),
            pl.col("low")
            .rolling_min(window_size=20)
            .over("code", order_by="date")
            .alias("low_20d"),
        ]
    )

    # --- From momentum.py ---
    df = df.with_columns(
        [
            (
                pl.when(pl.col("pct_chg").fill_null(0.0) > 0)
                .then(1.0)
                .otherwise(0.0)
                .rolling_mean(window_size=20)
                .over("code", order_by="date")
                .fill_null(0.5)
            ).alias("positive_day_ratio_20d"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("pct_chg")
            .fill_null(0.0)
            .rolling_std(window_size=20)
            .over("code", order_by="date")
            .fill_null(2.0)
            .alias("return_std_20d"),
            pl.col("pct_chg")
            .fill_null(0.0)
            .rolling_mean(window_size=20)
            .over("code", order_by="date")
            .fill_null(0.0)
            .alias("return_mean_20d"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("pct_chg")
            .fill_null(0.0)
            .rolling_min(window_size=20)
            .over("code", order_by="date")
            .fill_null(0.0)
            .alias("max_loss_20d"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("close").shift(5).over("code", order_by="date").alias("_close_5d_ago"),
            pl.col("close").shift(10).over("code", order_by="date").alias("_close_10d_ago"),
        ]
    )
    df = df.with_columns(
        [
            ((pl.col("close") / (pl.col("_close_5d_ago") + EPS) - 1) * 100)
            .fill_null(0.0)
            .alias("_ret_5d"),
            ((pl.col("close") / (pl.col("_close_10d_ago") + EPS) - 1) * 100)
            .fill_null(0.0)
            .alias("_ret_10d"),
        ]
    )

    return df
