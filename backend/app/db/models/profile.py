"""Asset profile models - extended information for stocks and ETFs."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List

from sqlalchemy import String, Date, DateTime, Numeric, BigInteger, Index, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.asset import ETFType


class ClassificationSource(str, Enum):
    """ETF分类来源枚举"""
    AUTO = "AUTO"      # 算法自动分类
    MANUAL = "MANUAL"  # 人工手动设置


class ClassificationConfidence(str, Enum):
    """ETF分类置信度枚举"""
    HIGH = "HIGH"      # 高置信度（精确匹配关键词）
    MEDIUM = "MEDIUM"  # 中置信度（模糊匹配）
    LOW = "LOW"        # 低置信度（默认分类或推断）


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

    # 分类扩展字段
    etf_sub_category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # 子品类：银行, 医药, 新能源 等
    classification_source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # AUTO/MANUAL
    classification_confidence: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # HIGH/MEDIUM/LOW
    classified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)  # 分类时间

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
        Index("idx_etf_profile_sub_category", "etf_sub_category"),
        Index("idx_etf_profile_source", "classification_source"),
    )

    def __repr__(self) -> str:
        return f"<ETFProfile(code={self.code}, type={self.etf_type}, sub={self.etf_sub_category})>"


class IndexProfile(Base):
    """
    指数扩展信息表

    存储指数特有的静态信息，包括行业分布（通过成分股聚合计算）
    与 AssetMeta 通过 code 关联
    """

    __tablename__ = "index_profile"

    code: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("asset_meta.code", ondelete="CASCADE"),
        primary_key=True
    )

    # 指数元数据
    short_name: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # hs300, zz500
    index_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # broad_based, sector, theme
    constituent_count: Mapped[int] = mapped_column(BigInteger, default=0)

    # 行业分布 (JSONB)
    # 结构: {
    #   "sw_l1": {"银行": 0.15, "非银金融": 0.12, ...},
    #   "sw_l2": {"股份制银行": 0.08, ...},
    #   "em": {"银行": 0.15, ...},
    #   "computation_date": "2024-12-24",
    #   "total_weight_covered": 0.95
    # }
    industry_composition: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # 行业集中度指标
    top_industry_l1: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    top_industry_weight: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    herfindahl_index: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)  # 行业集中度

    composition_updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

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
        Index("idx_index_profile_type", "index_type"),
        Index("idx_index_profile_top_industry", "top_industry_l1"),
    )

    def __repr__(self) -> str:
        return f"<IndexProfile(code={self.code}, type={self.index_type})>"
