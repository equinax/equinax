import polars as pl

EPS = 1e-9

from .volume import compute_volume_factors
from .price import compute_price_factors
from .momentum import compute_momentum_factors
from .composite import compute_composite_factors


def compute_all_factors(df: pl.DataFrame) -> pl.DataFrame:
    df = compute_volume_factors(df)
    df = compute_price_factors(df)
    df = compute_momentum_factors(df)
    df = compute_composite_factors(df)

    # Drop intermediate columns used across factor modules
    df = df.drop(
        ["up_volume_20d", "total_volume_20d", "ma_10", "ma_20", "vol_ma20", "max_loss_20d"]
    )
    return df
