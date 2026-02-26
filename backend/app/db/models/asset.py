"""Asset data models - unified asset management for stocks, ETFs, indices."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from enum import Enum

from sqlalchemy import (
    String,
    Integer,
    Date,
    DateTime,
    Numeric,
    BigInteger,
    Index,
    func,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class AssetType(str, Enum):
    """Asset type enumeration."""

    STOCK = "STOCK"
    ETF = "ETF"
    INDEX = "INDEX"
    CB = "CB"  # Convertible Bond


class ETFType(str, Enum):
    """ETF type enumeration."""

    BROAD_BASED = "BROAD_BASED"  # 宽基ETF
    SECTOR = "SECTOR"  # 行业ETF
    THEME = "THEME"  # 主题ETF
    CROSS_BORDER = "CROSS_BORDER"  # 跨境ETF
    COMMODITY = "COMMODITY"  # 商品ETF
    BOND = "BOND"  # 债券ETF
    CURRENCY = "CURRENCY"  # 货币ETF


class AssetMeta(Base):
    """
    资产元数据主表 - 所有资产类型通用

    替代旧的 StockBasic 表，支持股票、ETF、指数、可转债等多种资产类型
    """

    __tablename__ = "asset_meta"

    code: Mapped[str] = mapped_column(String(20), primary_key=True)  # "sh.600000", "sh.510050"
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    asset_type: Mapped[AssetType] = mapped_column(String(20), nullable=False)
    exchange: Mapped[str] = mapped_column(String(10), nullable=False)  # sh/sz/bj
    list_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    delist_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    status: Mapped[int] = mapped_column(Integer, default=1)  # 1=上市, 0=退市
    category: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )  # 主板/创业板/科创板/ETF

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
        Index("idx_asset_meta_type", "asset_type"),
        Index("idx_asset_meta_exchange", "exchange"),
        Index("idx_asset_meta_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<AssetMeta(code={self.code}, name={self.name}, type={self.asset_type})>"


class MarketDaily(Base):
    """
    统一行情表 - 纯OHLCV数据，所有资产通用

    This table is a TimescaleDB hypertable partitioned by date.
    Primary key is (code, date) for optimal time-series queries.

    注：估值指标已移至 IndicatorValuation 表，ETF特有指标在 IndicatorETF 表
    """

    __tablename__ = "market_daily"

    # Composite primary key for TimescaleDB hypertable
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # OHLCV data
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    preclose: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)

    # Trading metrics
    pct_chg: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 涨跌幅
    change: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 涨跌额

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
        Index("idx_market_daily_date", "date"),
        Index("idx_market_daily_code", "code"),
    )

    def __repr__(self) -> str:
        return f"<MarketDaily(code={self.code}, date={self.date}, close={self.close})>"


class IndicatorValuation(Base):
    """
    股票估值指标表

    This table is a TimescaleDB hypertable partitioned by date.
    仅适用于股票，ETF无估值数据
    """

    __tablename__ = "indicator_valuation"

    # Composite primary key for TimescaleDB hypertable
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # Price reference (from daily_basic, avoids JOIN with market_daily)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # Turnover metrics
    turnover_rate: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )  # 换手率(%)
    turnover_rate_f: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )  # 自由流通换手率(%)
    volume_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 量比

    # Valuation metrics
    pe: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 市盈率(静态)
    pe_ttm: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 市盈率(TTM)
    pb_mrq: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 市净率(MRQ)
    ps: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 市销率(静态)
    ps_ttm: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)  # 市销率(TTM)
    dv_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # 股息率(%)
    dv_ttm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )  # 股息率(TTM)(%)

    # Market cap
    total_mv: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )  # 总市值(亿元)
    circ_mv: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )  # 流通市值(亿元)

    # Share structure (万股)
    total_share: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )  # 总股本(万股)
    float_share: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )  # 流通股本(万股)
    free_share: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 2), nullable=True
    )  # 自由流通股本(万股)

    # ST status
    is_st: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
        Index("idx_indicator_valuation_date", "date"),
        Index("idx_indicator_valuation_code", "code"),
    )

    def __repr__(self) -> str:
        return f"<IndicatorValuation(code={self.code}, date={self.date})>"


class AdjustFactor(Base):
    """
    复权因子表 - 所有资产通用

    This table is a TimescaleDB hypertable partitioned by divid_operate_date.
    Primary key is (code, divid_operate_date) for optimal time-series queries.
    """

    __tablename__ = "adjust_factor"

    # Composite primary key for TimescaleDB hypertable
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    divid_operate_date: Mapped[date] = mapped_column(Date, nullable=False)

    adjust_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        PrimaryKeyConstraint("code", "divid_operate_date"),
        Index("idx_adjust_factor_code", "code"),
        Index("idx_adjust_factor_date", "divid_operate_date"),
    )

    def __repr__(self) -> str:
        return f"<AdjustFactor(code={self.code}, date={self.divid_operate_date})>"
