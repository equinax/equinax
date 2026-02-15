"""Base classes for strategy scoring engines."""

from dataclasses import dataclass

import polars as pl


@dataclass
class ScoreResult:
    """Result of a strategy scoring operation."""

    df: pl.DataFrame
    score_col: str
