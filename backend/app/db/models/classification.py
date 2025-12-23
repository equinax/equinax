"""4+1 Classification system models.

This module implements the "4+1" classification framework:
1. Structural Classification (交易制度) - Board type, price limits, ST status
2. Industry Classification (行业分类) - Shenwan industry classification
3. Style Factors (风格因子) - Size, volatility, turnover, valuation, momentum
4. Microstructure (微观结构) - Trading patterns, retail activity, main player control
+1. Market Regime (市场环境) - Bull/Bear/Range market conditions

Plus a unified snapshot table for convenient queries.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Dict, Any
from enum import Enum

from sqlalchemy import String, Integer, Date, DateTime, Numeric, Boolean, Index, func, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


# =============================================================================
# Enums
# =============================================================================

class BoardType(str, Enum):
    """Board type enumeration."""
    MAIN = "MAIN"         # 主板
    GEM = "GEM"           # 创业板 (Growth Enterprise Market)
    STAR = "STAR"         # 科创板 (STAR Market)
    BSE = "BSE"           # 北交所 (Beijing Stock Exchange)


class StructuralType(str, Enum):
    """Structural classification type."""
    NORMAL = "NORMAL"     # 普通股票
    ST = "ST"             # ST股票
    ST_STAR = "ST_STAR"   # *ST股票
    NEW = "NEW"           # 新股 (上市60日内)


class SizeCategory(str, Enum):
    """Market cap size category."""
    MEGA = "MEGA"         # 超大盘 (>1000亿)
    LARGE = "LARGE"       # 大盘 (200-1000亿)
    MID = "MID"           # 中盘 (50-200亿)
    SMALL = "SMALL"       # 小盘 (10-50亿)
    MICRO = "MICRO"       # 微盘 (<10亿)


class VolatilityCategory(str, Enum):
    """Volatility category."""
    HIGH = "HIGH"         # 高波动
    NORMAL = "NORMAL"     # 正常波动
    LOW = "LOW"           # 低波动


class TurnoverCategory(str, Enum):
    """Turnover activity category."""
    HOT = "HOT"           # 热门 (高换手)
    NORMAL = "NORMAL"     # 正常
    DEAD = "DEAD"         # 冷门 (低换手)


class ValueCategory(str, Enum):
    """Value/Growth style category."""
    VALUE = "VALUE"       # 价值型
    NEUTRAL = "NEUTRAL"   # 中性
    GROWTH = "GROWTH"     # 成长型


class MarketRegimeType(str, Enum):
    """Market regime type."""
    BULL = "BULL"         # 牛市
    BEAR = "BEAR"         # 熊市
    RANGE = "RANGE"       # 震荡市


# =============================================================================
# Dimension 1: Structural Classification (交易制度)
# =============================================================================

class StockStructuralInfo(Base):
    """
    第一维度：交易制度分类

    包含股票的板块、涨跌停限制、ST状态等交易制度相关信息。
    低频更新（仅在状态变化时更新）。
    """

    __tablename__ = "stock_structural_info"

    code: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("asset_meta.code", ondelete="CASCADE"),
        primary_key=True
    )

    # Board and structural type
    board: Mapped[BoardType] = mapped_column(String(20), nullable=False)  # MAIN/GEM/STAR/BSE
    structural_type: Mapped[StructuralType] = mapped_column(String(20), nullable=False)  # NORMAL/ST/ST_STAR/NEW

    # Price limits
    price_limit_up: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False, default=10)   # 涨停幅度 10/20/30/5
    price_limit_down: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False, default=10) # 跌停幅度

    # Status flags
    is_st: Mapped[bool] = mapped_column(Boolean, default=False)
    is_new: Mapped[bool] = mapped_column(Boolean, default=False)  # 上市60日内
    is_suspended: Mapped[bool] = mapped_column(Boolean, default=False)  # 停牌

    # Dates
    list_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    st_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # ST起始日

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        Index("idx_structural_board", "board"),
        Index("idx_structural_type", "structural_type"),
        Index("idx_structural_is_st", "is_st"),
    )

    def __repr__(self) -> str:
        return f"<StockStructuralInfo(code={self.code}, board={self.board}, type={self.structural_type})>"


# =============================================================================
# Dimension 2: Industry Classification (行业分类)
# =============================================================================

class IndustryClassification(Base):
    """
    申万行业分类表

    存储完整的申万行业分类体系（一级、二级、三级）
    """

    __tablename__ = "industry_classification"

    industry_code: Mapped[str] = mapped_column(String(20), primary_key=True)
    industry_name: Mapped[str] = mapped_column(String(100), nullable=False)
    industry_level: Mapped[int] = mapped_column(Integer, nullable=False)  # 1/2/3
    parent_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 父级行业代码

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_industry_level", "industry_level"),
        Index("idx_industry_parent", "parent_code"),
    )

    def __repr__(self) -> str:
        return f"<IndustryClassification(code={self.industry_code}, name={self.industry_name}, level={self.industry_level})>"


class StockIndustryMapping(Base):
    """
    股票-行业映射表

    记录股票的行业分类，支持历史追溯（通过 effective_date）
    支持多种分类体系（sw=申万, em=东方财富, csrc=证监会）
    """

    __tablename__ = "stock_industry_mapping"

    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)
    industry_code: Mapped[str] = mapped_column(String(20), nullable=False)
    classification_system: Mapped[str] = mapped_column(String(20), nullable=False, default="sw")  # sw/em/csrc
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expire_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # NULL = current

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("stock_code", "industry_code", "classification_system", "effective_date"),
        Index("idx_industry_mapping_code", "stock_code"),
        Index("idx_industry_mapping_industry", "industry_code"),
        Index("idx_industry_mapping_system", "classification_system"),
        Index("idx_industry_mapping_effective", "effective_date"),
    )

    def __repr__(self) -> str:
        return f"<StockIndustryMapping(stock={self.stock_code}, industry={self.industry_code})>"


# =============================================================================
# Dimension 3: Style Factors (风格因子)
# =============================================================================

class StockStyleExposure(Base):
    """
    第三维度：风格因子暴露

    This table is a TimescaleDB hypertable partitioned by date.
    每日更新，记录股票在各风格因子上的暴露度。
    """

    __tablename__ = "stock_style_exposure"

    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # Size factor (市值因子)
    market_cap: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 总市值
    size_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # 市值排名 (1=最大)
    size_percentile: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)  # 市值分位数
    size_category: Mapped[Optional[SizeCategory]] = mapped_column(String(20), nullable=True)

    # Volatility factor (波动率因子)
    volatility_20d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 20日波动率
    vol_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    vol_percentile: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    vol_category: Mapped[Optional[VolatilityCategory]] = mapped_column(String(20), nullable=True)

    # Turnover factor (换手率因子)
    avg_turnover_20d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 20日平均换手率
    turnover_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    turnover_percentile: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    turnover_category: Mapped[Optional[TurnoverCategory]] = mapped_column(String(20), nullable=True)

    # Value factor (估值因子)
    ep_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # E/P (PE的倒数)
    bp_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # B/P (PB的倒数)
    value_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    value_percentile: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    value_category: Mapped[Optional[ValueCategory]] = mapped_column(String(20), nullable=True)

    # Momentum factor (动量因子)
    momentum_20d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 20日收益率
    momentum_60d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 60日收益率
    momentum_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    momentum_percentile: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
        Index("idx_style_exposure_date", "date"),
        Index("idx_style_exposure_code", "code"),
        Index("idx_style_exposure_size_cat", "size_category"),
        Index("idx_style_exposure_vol_cat", "vol_category"),
    )

    def __repr__(self) -> str:
        return f"<StockStyleExposure(code={self.code}, date={self.date})>"


# =============================================================================
# Dimension 4: Microstructure (微观结构)
# =============================================================================

class StockMicrostructure(Base):
    """
    第四维度：微观结构

    This table is a TimescaleDB hypertable partitioned by date.
    记录股票的交易微观结构信息（换手率特征、主力控盘、龙虎榜等）。
    更新频率：每日
    """

    __tablename__ = "stock_microstructure"

    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # Classification flags (基于量价关系推断)
    is_retail_hot: Mapped[bool] = mapped_column(Boolean, default=False)  # 散户热门股 (高换手)
    is_main_controlled: Mapped[bool] = mapped_column(Boolean, default=False)  # 庄股嫌疑 (量价异常)

    # Dragon & Tiger (龙虎榜)
    dragon_tiger_count_20d: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # 20日龙虎榜次数

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
        Index("idx_microstructure_date", "date"),
        Index("idx_microstructure_code", "code"),
    )

    def __repr__(self) -> str:
        return f"<StockMicrostructure(code={self.code}, date={self.date})>"


# =============================================================================
# +1 Dimension: Market Regime (市场环境)
# =============================================================================

class MarketRegime(Base):
    """
    +1维度：市场环境

    This table is a TimescaleDB hypertable partitioned by date.
    记录整体市场状态，用于判断牛熊市环境。
    每日更新。
    """

    __tablename__ = "market_regime"

    date: Mapped[date] = mapped_column(Date, primary_key=True)

    # Market regime
    regime: Mapped[MarketRegimeType] = mapped_column(String(20), nullable=False)
    regime_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), nullable=True)  # -100 to +100

    # Market breadth (市场宽度)
    total_stocks: Mapped[int] = mapped_column(Integer, nullable=False)
    up_count: Mapped[int] = mapped_column(Integer, nullable=False)
    down_count: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_up_count: Mapped[int] = mapped_column(Integer, default=0)  # 涨停数
    limit_down_count: Mapped[int] = mapped_column(Integer, default=0)  # 跌停数

    # Market turnover (市场成交)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)  # 总成交额
    avg_turnover: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 平均换手率

    # Index levels
    sh_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 上证指数
    sz_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 深证成指
    hs300_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 沪深300

    # Technical indicators
    sh_above_ma20: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)  # 上证在20日线上
    sh_above_ma60: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)  # 上证在60日线上

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_market_regime_regime", "regime"),
    )

    def __repr__(self) -> str:
        return f"<MarketRegime(date={self.date}, regime={self.regime})>"


# =============================================================================
# Unified Snapshot (综合快照)
# =============================================================================

class StockClassificationSnapshot(Base):
    """
    综合分类快照表

    This table is a TimescaleDB hypertable partitioned by date.
    将所有维度的分类信息汇总到一张表，方便查询和分析。
    每日更新。
    """

    __tablename__ = "stock_classification_snapshot"

    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # Dimension 1: Structural
    board: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    structural_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_st: Mapped[bool] = mapped_column(Boolean, default=False)
    is_new: Mapped[bool] = mapped_column(Boolean, default=False)

    # Dimension 2: Industry
    industry_l1: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    industry_l2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    industry_l3: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Dimension 3: Style
    size_category: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    vol_category: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    turnover_category: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    value_category: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Dimension 4: Microstructure
    is_retail_hot: Mapped[bool] = mapped_column(Boolean, default=False)
    is_main_controlled: Mapped[bool] = mapped_column(Boolean, default=False)

    # +1: Market Regime
    market_regime: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Full classification tags as JSONB for flexible querying
    classification_tags: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
        Index("idx_snapshot_date", "date"),
        Index("idx_snapshot_code", "code"),
        Index("idx_snapshot_board", "board"),
        Index("idx_snapshot_industry_l1", "industry_l1"),
        Index("idx_snapshot_size", "size_category"),
        Index("idx_snapshot_regime", "market_regime"),
    )

    def __repr__(self) -> str:
        return f"<StockClassificationSnapshot(code={self.code}, date={self.date})>"
