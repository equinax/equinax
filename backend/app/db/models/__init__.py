"""Database models."""

from app.db.models.user import User

# Asset models
from app.db.models.asset import (
    AssetMeta,
    AssetType,
    ETFType,
    MarketDaily,
    IndicatorValuation,
    AdjustFactor,
)
from app.db.models.profile import StockProfile, ETFProfile, IndexProfile
from app.db.models.sync import SyncHistory

from app.db.models.indicator import (
    FundamentalIndicator,
    MoneyflowDaily,
    LimitListDaily,
)
from app.db.models.strategy import Strategy, StrategyVersion
from app.db.models.backtest import BacktestJob, BacktestResult, BacktestEquity, BacktestTrade
from app.db.models.stock_pool import (
    StockPool,
    StockPoolMember,
    IndexConstituent,
    StockPoolCombination,
)

# Classification models (4+1 system)
from app.db.models.classification import (
    StockStructuralInfo,
    IndustryClassification,
    StockIndustryMapping,
    StockStyleExposure,
    MarketRegime,
    StockClassificationSnapshot,
    BoardType,
    StructuralType,
    SizeCategory,
    VolatilityCategory,
    TurnoverCategory,
    ValueCategory,
    MarketRegimeType,
)

__all__ = [
    "User",
    # Asset models
    "AssetMeta",
    "AssetType",
    "ETFType",
    "MarketDaily",
    "IndicatorValuation",
    "AdjustFactor",
    "StockProfile",
    "ETFProfile",
    "IndexProfile",
    "SyncHistory",
    # Other models
    "FundamentalIndicator",
    "MoneyflowDaily",
    "LimitListDaily",
    "Strategy",
    "StrategyVersion",
    "BacktestJob",
    "BacktestResult",
    "BacktestEquity",
    "BacktestTrade",
    "StockPool",
    "StockPoolMember",
    "IndexConstituent",
    "StockPoolCombination",
    # Classification models (4+1 system)
    "StockStructuralInfo",
    "IndustryClassification",
    "StockIndustryMapping",
    "StockStyleExposure",
    "MarketRegime",
    "StockClassificationSnapshot",
    "BoardType",
    "StructuralType",
    "SizeCategory",
    "VolatilityCategory",
    "TurnoverCategory",
    "ValueCategory",
    "MarketRegimeType",
]
