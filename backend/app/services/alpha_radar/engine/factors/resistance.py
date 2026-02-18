"""Resistance Factor — Detects proximity to resistance and exhaustion.

Output columns:
    resistance_proximity_penalty (0-100): Penalty for stalling at resistance.
    exhaustion_at_ceiling (0-100): Score for exhaustion at ceiling.

Dependencies:
    Requires columns: price_range_position_20d, _ret_5d, vol_ramp_5v20, vol_jump_1d, pct_chg

Used by strategies:
    overnight, dragon, weekly
"""

import polars as pl


def compute_resistance(df: pl.DataFrame) -> pl.DataFrame:
    """Compute resistance_proximity_penalty and exhaustion_at_ceiling."""
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

    return df
