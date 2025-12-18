"""Stock pool models for managing stock universes."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from sqlalchemy import (
    String, Text, Integer, Date, DateTime, Numeric, Boolean,
    ForeignKey, Index, PrimaryKeyConstraint, func, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PoolType(str, Enum):
    """Stock pool type enum."""
    PREDEFINED = "predefined"
    CUSTOM = "custom"
    DYNAMIC = "dynamic"


class PredefinedPoolKey(str, Enum):
    """Predefined pool identifiers."""
    # Exchange pools - Stocks
    SH_ALL = "sh_all"           # 全部沪市股票
    SZ_ALL = "sz_all"           # 全部深市股票

    # Special pools - Stocks
    NON_ST = "non_st"           # 非ST股票
    MAIN_BOARD = "main_board"   # 主板股票
    GEM = "gem"                 # 创业板
    STAR = "star"               # 科创板

    # ETF pools
    ETF_ALL = "etf_all"         # 全部ETF
    ETF_SH = "etf_sh"           # 沪市ETF
    ETF_SZ = "etf_sz"           # 深市ETF
    ETF_BROAD = "etf_broad"     # 宽基ETF
    ETF_SECTOR = "etf_sector"   # 行业ETF
    ETF_THEME = "etf_theme"     # 主题ETF
    ETF_CROSS_BORDER = "etf_cross_border"  # 跨境ETF
    ETF_COMMODITY = "etf_commodity"  # 商品ETF

    # Index constituents (future - need external data)
    # HS300 = "hs300"           # 沪深300
    # ZZ500 = "zz500"           # 中证500
    # ZZ1000 = "zz1000"         # 中证1000


class StockPool(Base):
    """
    股票池主表

    支持三种类型：
    - predefined: 系统预定义池（如全部沪市、非ST股票）
    - custom: 用户自定义池（手动添加股票）
    - dynamic: 动态筛选池（基于条件实时筛选）
    """

    __tablename__ = "stock_pools"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )

    # Pool metadata
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pool_type: Mapped[PoolType] = mapped_column(String(20), nullable=False)

    # For predefined pools
    predefined_key: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # For dynamic pools - filter expression stored as JSONB
    # Format: {"version": "1.0", "conditions": [...], "logic": "AND"}
    filter_expression: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Sharing settings
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)

    # Cached statistics (updated periodically)
    stock_count: Mapped[int] = mapped_column(Integer, default=0)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    members: Mapped[List["StockPoolMember"]] = relationship(
        "StockPoolMember", back_populates="pool", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_stock_pools_user", "user_id"),
        Index("idx_stock_pools_type", "pool_type"),
        Index("idx_stock_pools_predefined", "predefined_key"),
        Index("idx_stock_pools_public", "is_public"),
        CheckConstraint(
            "pool_type IN ('predefined', 'custom', 'dynamic')",
            name="valid_pool_type"
        ),
    )

    def __repr__(self) -> str:
        return f"<StockPool(id={self.id}, name={self.name}, type={self.pool_type})>"


class StockPoolMember(Base):
    """
    自定义股票池成员表

    记录 custom 类型池中的股票列表
    """

    __tablename__ = "stock_pool_members"

    pool_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("stock_pools.id", ondelete="CASCADE"), nullable=False
    )
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    added_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )

    pool: Mapped["StockPool"] = relationship("StockPool", back_populates="members")

    __table_args__ = (
        PrimaryKeyConstraint("pool_id", "stock_code"),
        Index("idx_pool_members_stock", "stock_code"),
    )

    def __repr__(self) -> str:
        return f"<StockPoolMember(pool_id={self.pool_id}, stock_code={self.stock_code})>"


class IndexConstituent(Base):
    """
    指数成分股数据表

    存储指数成分股及其权重，用于预定义指数池
    支持历史成分股查询（通过 effective_date 和 expire_date）

    注：MVP 阶段暂不填充数据，后续通过 AKShare 导入
    """

    __tablename__ = "index_constituents"

    index_code: Mapped[str] = mapped_column(String(20), nullable=False)  # 'hs300', 'zz500', etc.
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)
    weight: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    expire_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # NULL = current

    __table_args__ = (
        PrimaryKeyConstraint("index_code", "stock_code", "effective_date"),
        Index("idx_constituents_index", "index_code"),
        Index("idx_constituents_stock", "stock_code"),
        Index("idx_constituents_effective", "effective_date"),
    )

    def __repr__(self) -> str:
        return f"<IndexConstituent(index={self.index_code}, stock={self.stock_code})>"


class StockPoolCombination(Base):
    """
    股票池组合表

    支持多个池的并集、交集、差集操作
    """

    __tablename__ = "stock_pool_combinations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Combination expression: {"operation": "union|intersection|difference", "pool_ids": [...]}
    combination_expr: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)

    stock_count: Mapped[int] = mapped_column(Integer, default=0)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_pool_combinations_user", "user_id"),
    )

    def __repr__(self) -> str:
        return f"<StockPoolCombination(id={self.id}, name={self.name})>"
