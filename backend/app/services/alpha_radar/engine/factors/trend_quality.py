"""Trend Quality Factor — Measures the robustness of the trend.

Output columns:
    trend_quality_20d (0-100): Score based on positive days and max loss.

Dependencies:
    Requires columns: positive_day_ratio_20d, max_loss_20d

Used by strategies:
    overnight, dragon, weekly, rally
"""

import polars as pl


def compute_trend_quality(df: pl.DataFrame) -> pl.DataFrame:
    """Compute trend_quality_20d."""
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
    return df
