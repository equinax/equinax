import polars as pl
from . import EPS


def compute_price_factors(df: pl.DataFrame) -> pl.DataFrame:
    # Calculate rolling indicators per stock
    df = df.with_columns(
        [
            # 60-day high
            pl.col("high")
            .rolling_max(window_size=60)
            .over("code", order_by="date")
            .alias("high_60d"),
            # 60-day low
            pl.col("low")
            .rolling_min(window_size=60)
            .over("code", order_by="date")
            .alias("low_60d"),
        ]
    )

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

    # 5/10/20-day price moving averages + ATR for normalization
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

    # 20d high and low for range position
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

    # Limit-up / near-limit detection
    is_20pct = pl.col("code").str.contains(r"^(sz\.(300|301)|sh\.688)")
    limit_threshold = pl.when(is_20pct).then(pl.lit(19.0)).otherwise(pl.lit(9.5))

    df = df.with_columns(
        [
            (pl.col("pct_chg").fill_null(0.0) >= limit_threshold).alias("near_limit_up"),
        ]
    )

    return df
