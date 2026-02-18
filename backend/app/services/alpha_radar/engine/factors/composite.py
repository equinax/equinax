import polars as pl
from . import EPS


def _sig(x: pl.Expr) -> pl.Expr:
    return pl.lit(1.0) / (pl.lit(1.0) + (-x).exp())


def compute_composite_factors(df: pl.DataFrame) -> pl.DataFrame:
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

    df = df.with_columns(
        [
            # trend_quality_20d (0-100):
            # High when: many positive days + no severe daily losses
            # 恒邦: pos_ratio ~0.65, max_loss ~-2% → quality ~80
            # 神宇: pos_ratio ~0.50, max_loss ~-5% → quality ~30
            (
                pl.col("positive_day_ratio_20d").fill_null(0.5) * 50
                + (1 - pl.col("max_loss_20d").abs().clip(0.0, 10.0) / 10.0) * 50
            )
            .fill_null(50.0)
            .alias("trend_quality_20d"),
        ]
    )

    # main_strength_proxy (0-100): quiet multi-day accumulation signal
    # Iter 3: replaced volume_ratio_5d (rewarded same-day spikes) with
    # gradual ramp + consistency + up-volume concentration + turnover ramp
    df = df.with_columns(
        [
            (
                pl.col("vol_ramp_5v20").clip(0.0, 0.6) / 0.6 * 30
                + (1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 0.8)) / 0.7 * 25
                + pl.col("up_volume_ratio").fill_null(0.5).clip(0.3, 0.8) / 0.8 * 25
                + (pl.col("turnover_change_20d").clip(-0.5, 0.5) + 0.5) * 20
            )
            .clip(0.0, 100.0)
            .alias("main_strength_proxy"),
        ]
    )

    # Factor 3: resistance_proximity_penalty (0-100)
    # Two trap patterns detected:
    # A) Surge into resistance: at 20d ceiling + strong 5d return + expanding volume
    # B) Stall at resistance: at 20d ceiling + strong 5d return + today's gain near zero
    #    12-08 losers: pos=86-97%, ret_5d=2-4%, today_chg=0.0-1.0% → exhaustion
    #    This pattern was missed when volume is declining (post-peak)
    ceiling_proximity = (pl.col("price_range_position_20d") - 0.85).clip(0.0, 0.15) / 0.15
    ret_5d_factor = pl.col("_ret_5d").clip(0.0, 8.0) / 8.0

    vol_expansion = (
        pl.col("vol_ramp_5v20").clip(0.0, 1.0) + (pl.col("vol_jump_1d").clip(1.0, 3.0) - 1.0) / 2.0
    ).clip(0.0, 1.0)

    # B: Stall signal — today's gain is small relative to 5d return
    # When _ret_5d > 2% but today_chg < 1%, momentum has stalled at the ceiling
    stall_signal = (
        pl.when(pl.col("_ret_5d") > 2.0)
        .then((1.0 - pl.col("pct_chg").fill_null(0.0).clip(0.0, 2.0) / 2.0))
        .otherwise(pl.lit(0.0))
    )

    df = df.with_columns(
        [
            (
                ceiling_proximity
                * ret_5d_factor
                * (vol_expansion + stall_signal).clip(0.0, 1.5)
                * 100.0
            )
            .fill_null(0.0)
            .clip(0.0, 100.0)
            .alias("resistance_proximity_penalty"),
        ]
    )

    # Factor 4: exhaustion_at_ceiling (0-100, for panorama tab)
    # More targeted than resistance_proximity_penalty — requires THREE simultaneous signals:
    # 1. Price near 20d ceiling (pos > 0.80)
    # 2. Prior 5d uptrend (ret_5d > 1.5%)
    # 3. Today's momentum has stalled (pct_chg < 1.0%)
    # This catches the 12-08 pattern: stocks that ran up 2-4% over 5d, reached ceiling,
    # but today barely moved — exhaustion/distribution. Unlike resistance_proximity_penalty,
    # this does NOT penalize stocks with strong today's momentum (genuine breakouts).
    ceiling_signal = (pl.col("price_range_position_20d") - 0.80).clip(0.0, 0.20) / 0.20
    prior_uptrend = (pl.col("_ret_5d") - 1.5).clip(0.0, 6.5) / 6.5
    # Stall: today's gain is small. Max penalty when pct_chg <= 0, tapers to 0 at pct_chg >= 1.5
    momentum_stall = (1.5 - pl.col("pct_chg").fill_null(0.0).clip(0.0, 1.5)) / 1.5

    df = df.with_columns(
        [
            (ceiling_signal * prior_uptrend * momentum_stall * 100.0)
            .fill_null(0.0)
            .clip(0.0, 100.0)
            .alias("exhaustion_at_ceiling"),
        ]
    )

    # Composite scores: accumulation (good) and climax (bad)
    df = df.with_columns(
        [
            # Accumulation score: gradual volume/turnover ramp + strong close + up-volume
            (
                pl.col("vol_ramp_5v20").clip(-0.2, 1.0) * 0.30
                + pl.col("turn_ramp_5v20").clip(-0.2, 1.0) * 0.30
                + pl.col("close_strength").clip(0.0, 1.0) * 0.20
                + pl.col("up_volume_ratio").fill_null(0.5).clip(0.0, 1.0) * 0.20
            )
            .fill_null(0.0)
            .alias("accumulation_score"),
            # Climax score: big daily gain + volume spike + at 60d high + upper shadow + vol peak
            (
                (pl.col("pct_chg").fill_null(0.0).clip(0.0, 20.0) / 10.0) * 0.30
                + (pl.col("volume").cast(pl.Float64) / (pl.col("vol_ma20") + EPS))
                .clip(0.0, 5.0)
                .fill_null(1.0)
                / 5.0
                * 0.25
                + pl.col("price_position_60d").fill_null(0.5).clip(0.0, 1.0) * 0.20
                + pl.col("upper_shadow_ratio").fill_null(0.0).clip(0.0, 1.0) * 0.15
                + pl.col("vol_peak_today_20d").fill_null(0.0) * 0.10
            )
            .fill_null(0.0)
            .alias("climax_score"),
        ]
    )

    df = df.drop(["_close_5d_ago", "_close_10d_ago", "_ret_5d", "_ret_10d", "high_20d", "low_20d"])

    return df
