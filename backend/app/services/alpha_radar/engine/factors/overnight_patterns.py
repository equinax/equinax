"""Overnight Pattern Factors — Reversal-focused composite scores.

v5: All reversal patterns. Continuation/momentum patterns removed (proven
negative alpha for T+1 open → T+2 open holding period).

Each pattern computes a product of [0,1] gates → rescale to 0-100.
Gate thresholds calibrated against actual factor distributions (p25-p90).

Output columns (reversal):
    doji_reversal_score (0-100): Doji candle after consecutive decline.
    oversold_bounce_score (0-100): Price near 20d low with decline slowing.
    decline_exhaustion_score (0-100): Extended downtrend losing momentum.
    failed_breakdown_score (0-100): Breaks below prior support but recovers.

Output columns (anti-momentum penalty):
    momentum_continuation_score (0-100): Stocks exhibiting continuation patterns.
        Used as PENALTY in YAML config (continuation = negative alpha overnight).

Dependencies:
    consecutive_down_days, doji_score, shadow_range_pct,
    price_range_position_20d, close_strength, return_std_20d,
    consecutive_up_days, positive_day_ratio_20d, trend_quality_20d
"""

import polars as pl
from . import EPS


def _gate_higher(col_name: str, lo: float, hi: float) -> pl.Expr:
    return ((pl.col(col_name).fill_null(lo) - lo) / (hi - lo + EPS)).clip(0.0, 1.0)


def _gate_lower(col_name: str, lo: float, hi: float) -> pl.Expr:
    return ((hi - pl.col(col_name).fill_null(hi)) / (hi - lo + EPS)).clip(0.0, 1.0)


def _gate_higher_expr(expr: pl.Expr, lo: float, hi: float) -> pl.Expr:
    return ((expr.fill_null(lo) - lo) / (hi - lo + EPS)).clip(0.0, 1.0)


def compute_overnight_patterns(df: pl.DataFrame) -> pl.DataFrame:
    # Compute prior_5d_low: rolling min of low over 5 prior days (exclude today)
    df = df.with_columns(
        pl.col("low")
        .shift(1)
        .rolling_min(window_size=5)
        .over("code", order_by="date")
        .alias("_prior_5d_low"),
    )

    # --- Pattern 1: Doji Reversal (十字星反转) ---
    # Validated: WR=56.2%, lift=+7.0pp at score>50 (n=5,400)
    # v5.1: Added oversold gate — doji at top of range is NOT reversal
    g_doji = _gate_higher("doji_score", 20.0, 70.0)
    g_decline = _gate_higher("consecutive_down_days", 1.0, 3.0)
    g_range = _gate_higher("shadow_range_pct", 1.5, 4.0)
    g_oversold_doji = _gate_lower("price_range_position_20d", 0.10, 0.50)

    doji_rev = (g_doji * g_decline * g_range * g_oversold_doji * 100).clip(0.0, 100.0)

    # --- Pattern 2: Oversold Bounce (超卖反弹) ---
    # Validated: pos<0.15 gives WR=54.5% lift=+5.2pp (n=64,595)
    # Combined with close_strength > 0.5 (rebound day): WR=53.2% lift=+3.3pp
    g_oversold = _gate_lower("price_range_position_20d", 0.05, 0.25)
    g_rebound = _gate_higher("close_strength", 0.30, 0.65)
    g_down_context = _gate_higher("consecutive_down_days", 0.5, 3.0)

    oversold = (g_oversold * g_rebound * g_down_context * 100).clip(0.0, 100.0)

    # --- Pattern 3: Decline Exhaustion (跌势衰竭) ---
    # Validated: down>=4d + oversold(<0.20): WR=57.9% lift=+8.0pp (n=9,721)
    #            down>=3d + deceleration + oversold: WR=55.7% lift=+5.8pp (n=4,538)
    g_long_decline = _gate_higher("consecutive_down_days", 2.0, 5.0)
    g_oversold2 = _gate_lower("price_range_position_20d", 0.05, 0.30)
    g_decel = _gate_lower("return_std_20d", 1.5, 4.0)

    exhaustion = (g_long_decline * g_oversold2 * g_decel * 100).clip(0.0, 100.0)

    # --- Pattern 4: Failed Breakdown (假突破反转) ---
    # Validated: low<5d_low + close>5d_low + close upper half: WR=54.1% lift=+4.2pp (n=39,328)
    # v5.1: Added oversold + decline gates — breakdown at range top is meaningless
    broke_below = (pl.col("low") < pl.col("_prior_5d_low")).cast(pl.Float64)
    recovered = (pl.col("close") > pl.col("_prior_5d_low")).cast(pl.Float64)
    g_close_loc = _gate_higher("close_strength", 0.35, 0.70)
    g_oversold_bd = _gate_lower("price_range_position_20d", 0.10, 0.45)
    g_decline_bd = _gate_higher("consecutive_down_days", 1.0, 3.0)

    failed_bd = (broke_below * recovered * g_close_loc * g_oversold_bd * g_decline_bd * 100).clip(
        0.0, 100.0
    )

    # --- Anti-Pattern: Momentum Continuation (动量延续 — 反向信号) ---
    # All continuation signals showed NEGATIVE alpha for overnight:
    # inertia: -5.8pp, strong_close: -6.4pp, range_squeeze: -4.5pp
    g_consec_up = _gate_higher("consecutive_up_days", 1.0, 4.0)
    g_pos_ratio = _gate_higher("positive_day_ratio_20d", 0.50, 0.65)
    g_trend_up = _gate_higher("trend_quality_20d", 50.0, 68.0)

    momentum_cont = (g_consec_up * g_pos_ratio * g_trend_up * 100).clip(0.0, 100.0)

    df = df.with_columns(
        [
            doji_rev.fill_null(0.0).alias("doji_reversal_score"),
            oversold.fill_null(0.0).alias("oversold_bounce_score"),
            exhaustion.fill_null(0.0).alias("decline_exhaustion_score"),
            failed_bd.fill_null(0.0).alias("failed_breakdown_score"),
            momentum_cont.fill_null(0.0).alias("momentum_continuation_score"),
        ]
    )
    return df
