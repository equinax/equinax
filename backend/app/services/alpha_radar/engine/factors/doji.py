"""Doji Detection Factor — Identifies doji and reversal candle patterns.

Output columns:
    doji_score (0-100): High when candle body is tiny relative to shadow range.
    shadow_range_pct (float): (high - low) / close as percentage.

Dependencies:
    Requires columns: open, high, low, close

Used by strategies:
    overnight (doji reversal pattern)
"""

import polars as pl
from . import EPS


def compute_doji(df: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close") - pl.col("open")).abs()
    candle_range = pl.col("high") - pl.col("low") + EPS

    # body_ratio: 0 = perfect doji (no body), 1 = full body (no shadows)
    body_ratio = (body / candle_range).clip(0.0, 1.0)

    # doji_score: invert body_ratio — perfect doji = 100, full body = 0
    # Apply nonlinear mapping: body_ratio < 0.15 → strong doji (score > 70)
    # body_ratio > 0.40 → not a doji (score ≈ 0)
    doji = ((0.40 - body_ratio) / 0.40).clip(0.0, 1.0) * 100

    # shadow_range_pct: total range as % of close price
    # High values (>2.5%) indicate strong intraday volatility / divergence
    shadow_pct = (candle_range / (pl.col("close") + EPS) * 100).fill_null(0.0)

    df = df.with_columns(
        [
            doji.fill_null(0.0).alias("doji_score"),
            shadow_pct.alias("shadow_range_pct"),
        ]
    )
    return df
