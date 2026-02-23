"""Pullback Reversal Phase Factor — Detects the first uptick after a pullback.

Core insight: The overnight strategy should NOT pick stocks that are still
declining. Instead, we want the exact transition point: the first up day
after a pullback within an uptrend (止跌回升第一升).

Output columns:
    prev_consecutive_down_days (float): Yesterday's consecutive_down_days.
        Used as pre_filter: must be >= 1 (yesterday was in pullback).
    first_uptick_after_pullback (0-100): Composite score for the quality
        of the reversal signal. High when:
        - Today is an up day (pct_chg > 0)
        - Yesterday ended a pullback of 1-5 days
        - Price is in the lower half of 20d range (near box bottom)
        - Close strength is high (closed near the high = bullish candle)

Dependencies:
    consecutive_down_days, consecutive_up_days (from streak)
    price_range_position_20d (from price_position)
    close_strength (from candle_structure)
    pct_chg (raw)

Phase: Must run AFTER streak and price_position (Phase 3).
"""

import polars as pl
from . import EPS


def _gate_higher(col_name: str, lo: float, hi: float) -> pl.Expr:
    """Score 0→1 as col goes from lo→hi."""
    return ((pl.col(col_name).fill_null(lo) - lo) / (hi - lo + EPS)).clip(0.0, 1.0)


def _gate_lower(col_name: str, lo: float, hi: float) -> pl.Expr:
    """Score 1→0 as col goes from lo→hi (inverted: lower is better)."""
    return ((hi - pl.col(col_name).fill_null(hi)) / (hi - lo + EPS)).clip(0.0, 1.0)


def compute_pullback_reversal_phase(df: pl.DataFrame) -> pl.DataFrame:
    """Compute pullback reversal phase factors."""

    # --- prev_consecutive_down_days ---
    # Shift consecutive_down_days by 1 day to get yesterday's value.
    # If today is the first up day after a pullback:
    #   consecutive_down_days = 0 (today is up)
    #   prev_consecutive_down_days >= 1 (yesterday was down)
    df = df.with_columns(
        pl.col("consecutive_down_days")
        .shift(1)
        .over("code", order_by="date")
        .fill_null(0.0)
        .alias("prev_consecutive_down_days"),
    )

    # --- first_uptick_after_pullback (0-100) ---
    # Gate 1: Today must be an up day (hard gate)
    is_up_today = (pl.col("pct_chg").fill_null(0.0) > 0).cast(pl.Float64)

    # Gate 2: Yesterday was in pullback (prev_consecutive_down_days >= 1)
    # Score ramps from 1d to 4d pullback — longer pullback = more conviction
    # that the reversal is meaningful. Beyond 5d, it's an extended decline
    # which the existing max_consecutive_down filter handles.
    g_pullback_length = _gate_higher("prev_consecutive_down_days", 0.5, 4.0)

    # Gate 3: Price near box bottom (lower 20d range = better entry)
    # position 0.0 = at 20d low (best), 0.5 = mid-range (ok), 0.8+ = near top (bad)
    g_box_bottom = _gate_lower("price_range_position_20d", 0.10, 0.60)

    # Gate 4: Close strength — bullish candle (closed near the high)
    # High close_strength on the first up day = strong reversal conviction
    g_close_strong = _gate_higher("close_strength", 0.30, 0.70)

    # Composite: product of gates, scaled to 0-100
    # is_up_today is a hard 0/1 gate — if today is not up, score is 0.
    first_uptick = (is_up_today * g_pullback_length * g_box_bottom * g_close_strong * 100).clip(
        0.0, 100.0
    )

    df = df.with_columns(
        first_uptick.fill_null(0.0).alias("first_uptick_after_pullback"),
    )

    return df
