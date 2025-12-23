"""Asset profile models - extended information for stocks and ETFs."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import String, Date, DateTime, Numeric, BigInteger, Index, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.asset import ETFType


class StockProfile(Base):
    """
    股票扩展信息表

    存储股票特有的静态信息，如行业分类、概念板块、省份等
    与 AssetMeta 通过 code 关联
    """

    __tablename__ = "stock_profile"

    code: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("asset_meta.code", ondelete="CASCADE"),
        primary_key=True
    )

    # 东方财富行业分类 (扁平结构，只有一级)
    em_industry: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # 东方财富行业

    # 申万行业分类 (层级结构)
    sw_industry_l1: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # 一级行业
    sw_industry_l2: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # 二级行业
    sw_industry_l3: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # 三级行业

    # 概念板块 (JSONB array)
    concepts: Mapped[Optional[List[str]]] = mapped_column(JSONB, nullable=True)

    # 地域信息
    province: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # 股本信息
    total_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)  # 总股本
    float_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)  # 流通股本

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
        Index("idx_stock_profile_em_industry", "em_industry"),
        Index("idx_stock_profile_sw_l1", "sw_industry_l1"),
        Index("idx_stock_profile_sw_l2", "sw_industry_l2"),
        Index("idx_stock_profile_province", "province"),
    )

    def __repr__(self) -> str:
        return f"<StockProfile(code={self.code}, industry={self.sw_industry_l1})>"


class ETFProfile(Base):
    """
    ETF扩展信息表

    存储ETF特有的静态信息，如跟踪指数、基金公司、管理费率等
    与 AssetMeta 通过 code 关联
    """

    __tablename__ = "etf_profile"

    code: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("asset_meta.code", ondelete="CASCADE"),
        primary_key=True
    )

    # ETF类型
    etf_type: Mapped[Optional[ETFType]] = mapped_column(String(20), nullable=True)

    # 跟踪指数
    underlying_index_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    underlying_index_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # 基金公司
    fund_company: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # 费率信息
    management_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)  # 管理费率
    custody_fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)     # 托管费率

    # 规模信息
    fund_size: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 基金规模(亿元)

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
        Index("idx_etf_profile_type", "etf_type"),
        Index("idx_etf_profile_index", "underlying_index_code"),
        Index("idx_etf_profile_company", "fund_company"),
    )

    def __repr__(self) -> str:
        return f"<ETFProfile(code={self.code}, type={self.etf_type})>"
