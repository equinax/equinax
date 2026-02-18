import polars as pl

EPS = 1e-9


def _sig(x: pl.Expr) -> pl.Expr:
    """Logistic sigmoid for smooth 0-1 transitions in Polars expressions."""
    return pl.lit(1.0) / (pl.lit(1.0) + (-x).exp())


# Import all factor computation functions
from ._intermediates import compute_intermediates
from .volume_consistency import compute_volume_consistency
from .volume_buildup import compute_volume_buildup
from .volume_spike import compute_volume_spike
from .up_volume_ratio import compute_up_volume_ratio
from .post_spike_consolidation import compute_post_spike_consolidation
from .price_position import compute_price_position
from .candle_structure import compute_candle_structure
from .limit_detection import compute_limit_detection
from .momentum_quality import compute_momentum_quality
from .positive_day_ratio import compute_positive_day_ratio
from .return_stats import compute_return_stats
from .ma_alignment import compute_ma_alignment
from .trend_quality import compute_trend_quality
from .accumulation import compute_accumulation
from .climax import compute_climax
from .main_strength import compute_main_strength
from .resistance import compute_resistance
from .streak import compute_streak
from .doji import compute_doji
from .overnight_patterns import compute_overnight_patterns
from .uptrend_cycle import compute_uptrend_cycle


def compute_all_factors(df: pl.DataFrame) -> pl.DataFrame:
    """Compute all alpha factors for the given DataFrame."""

    # Phase 1: All shared intermediates
    df = compute_intermediates(df)

    # Phase 2: Factors that only depend on intermediates (parallel-safe, order doesn't matter)
    df = compute_volume_consistency(df)
    df = compute_volume_buildup(df)
    df = compute_volume_spike(df)  # produces recent_vol_spike_max
    df = compute_up_volume_ratio(df)
    df = compute_price_position(df)
    df = compute_candle_structure(df)
    df = compute_limit_detection(df)
    df = compute_positive_day_ratio(df)  # produces positive_day_ratio_20d
    df = compute_return_stats(df)  # produces return_std_20d, max_loss_20d, stability_score
    df = compute_momentum_quality(df)  # produces momentum_quality_ratio, return_5d
    df = compute_streak(df)  # produces consecutive_up_days, consecutive_down_days
    df = compute_doji(df)  # produces doji_score, shadow_range_pct

    # Phase 3: Factors that depend on Phase 2 outputs
    df = compute_post_spike_consolidation(df)  # needs recent_vol_spike_max
    df = compute_trend_quality(df)  # needs positive_day_ratio_20d, max_loss_20d
    df = compute_ma_alignment(df)  # needs ma_5, ma_10, ma_20, atr_20d
    df = compute_accumulation(df)  # needs vol_ramp_5v20, close_strength, up_volume_ratio
    df = compute_climax(
        df
    )  # needs vol_ma20, price_position_60d, upper_shadow_ratio, vol_peak_today_20d
    df = compute_main_strength(
        df
    )  # needs vol_ramp_5v20, vol_cv_10d, up_volume_ratio, turnover_change_20d
    df = compute_resistance(
        df
    )  # needs price_range_position_20d, _ret_5d, vol_ramp_5v20, vol_jump_1d
    df = compute_uptrend_cycle(df)  # needs consecutive_down_days, return_std_20d

    # Phase 4: Overnight pattern composites (need Phase 2+3 outputs)
    df = compute_overnight_patterns(df)

    # Phase 5: Cleanup intermediates
    df = df.drop(
        [
            "up_volume_20d",
            "total_volume_20d",
            "ma_10",
            "ma_20",
            "vol_ma20",
            "max_loss_20d",
            "_close_5d_ago",
            "_close_10d_ago",
            "_ret_5d",
            "_ret_10d",
            "high_20d",
            "low_20d",
        ]
    )
    return df
