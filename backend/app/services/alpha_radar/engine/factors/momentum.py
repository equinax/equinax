import polars as pl
from . import EPS


def compute_momentum_factors(df: pl.DataFrame) -> pl.DataFrame:
    # Positive day ratio: % of days with positive return in last 20d
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

    # Price stability (20d): standard deviation of daily returns / abs(mean return)
    # Lower = more stable. We'll invert for scoring.
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

    # Max single-day loss in 20d (absolute, lower = better)
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

    # Factor 2: momentum_quality_ratio (0-100)
    # Compares 5d return vs 10d return to detect sprint exhaustion.
    # 5d and 10d returns computed from close prices (no look-ahead).
    # When 5d_ret >> 10d_ret: all momentum concentrated in recent days → exhaustion → low score.
    # When 5d_ret ≤ 10d_ret: sustained trend → high score.
    # Formula: ratio = 10d_ret / (5d_ret + eps). Clamped and normalized.
    #
    # 恩捷 (01-19 failure): momentum_acceleration = 12.2 → almost all gains in last 5 days.
    # 恒邦 (01-20 success): gradual climb over 10+ days, 5d_ret ≈ 10d_ret.
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
    # momentum_quality: high when momentum is spread over 10 days (not concentrated in 5).
    # If 5d_ret is positive and > 10d_ret → concentration penalty.
    # If 5d_ret is positive and ≤ 10d_ret → sustained trend, reward.
    # If 5d_ret is negative → no momentum quality issue (different problem).
    df = df.with_columns(
        [
            (
                pl.when(pl.col("_ret_5d") > 1.0)  # Only evaluate when there IS momentum
                .then(
                    pl.when(pl.col("_ret_10d") > pl.col("_ret_5d"))
                    # 10d > 5d: sustained trend. Score scales with how spread out the gains are.
                    # 10d_ret/5d_ret > 2 → very spread → 100. Around 1 → 60.
                    .then(
                        (pl.col("_ret_10d") / (pl.col("_ret_5d") + EPS))
                        .clip(1.0, 3.0)
                        .alias("_ratio")
                        * 100
                        / 3.0
                    )
                    .otherwise(
                        # 5d > 10d: concentrated momentum (sprint).
                        # Score: lower when 5d/10d ratio is higher.
                        # 5d_ret/10d_ret = 1 → 60. = 2 → 30. >= 3 → 0.
                        (
                            1.0
                            - (pl.col("_ret_5d") / (pl.col("_ret_10d").abs() + EPS) - 1.0).clip(
                                0.0, 2.0
                            )
                            / 2.0
                        )
                        * 60.0
                    )
                )
                .otherwise(pl.lit(50.0))  # No upward momentum → neutral
            )
            .fill_null(50.0)
            .clip(0.0, 100.0)
            .alias("momentum_quality_ratio"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("_ret_5d").fill_null(0.0).alias("return_5d"),
        ]
    )

    return df
