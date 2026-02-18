import polars as pl
from . import EPS


def _sig(x: pl.Expr) -> pl.Expr:
    return pl.lit(1.0) / (pl.lit(1.0) + (-x).exp())


def compute_volume_factors(df: pl.DataFrame) -> pl.DataFrame:
    # Calculate rolling indicators per stock
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

    # Calculate derived indicators
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

    # Up-volume ratio: volume on up days / total volume over 20d
    # This measures whether volume concentrates on up moves (accumulation)
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
            (pl.col("up_volume_20d") / pl.col("total_volume_20d").clip(lower_bound=1.0))
            .fill_null(0.5)
            .alias("up_volume_ratio"),
        ]
    )

    # Volume ramp vs spike indicators
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
            # volume_consistency_score (0-100): inverse of vol_cv_10d
            # Low CV = gradual ramp (恒邦 pattern, good)
            # High CV = explosive spikes (神宇 pattern, bad)
            # vol_cv_10d typical range: 0.1 (very consistent) to 1.0+ (explosive)
            ((1 - pl.col("vol_cv_10d").fill_null(0.5).clip(0.1, 1.0)) / 0.9 * 100)
            .fill_null(50.0)
            .alias("volume_consistency_score"),
        ]
    )

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
