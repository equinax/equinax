"""Positive Day Ratio Factor — Measures consistency of positive returns.

Output columns:
    positive_day_ratio_20d (0-1): Fraction of positive days in last 20 days.

Dependencies:
    Requires columns: positive_day_ratio_20d (from intermediates)

Used by strategies:
    overnight, dragon
"""

import polars as pl


def compute_positive_day_ratio(df: pl.DataFrame) -> pl.DataFrame:
    """Pass-through for positive_day_ratio_20d (computed in intermediates)."""
    # Note: positive_day_ratio_20d is computed in _intermediates.py because it's used
    # by trend_quality.py. This function exists to maintain the factor file structure
    # and could be used for additional derived logic if needed.
    return df
