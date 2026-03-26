"""Stock tracker models for individual stock tracking and analysis."""

import uuid
from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from sqlalchemy import (
    String,
    Text,
    Integer,
    SmallInteger,
    Date,
    DateTime,
    Time,
    Numeric,
    Boolean,
    Float,
    ForeignKey,
    Index,
    UniqueConstraint,
    func,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PatternType(str, Enum):
    """天干 pattern types for daily candlestick analysis."""

    JIA = "甲"  # 甲 - Simple ascending
    YI = "乙"  # 乙 - Simple descending
    BING = "丙"  # 丙 - H in middle
    DING = "丁"  # 丁 - L in middle
    WU = "戊"  # 戊 - O.time == L.time (low at open)
    JI = "己"  # 己 - O.time == H.time, C.time == L.time
    GENG = "庚"  # 庚 - O,L,H,C sequence
    XIN = "辛"  # 辛 - O,H,L,C sequence
    REN = "壬"  # 壬 - L at close, H before
    GUI = "癸"  # 癸 - H at close, L before


class OpType(str, Enum):
    """Trade operation types."""

    BUY = "buy"
    SELL = "sell"
    ADD = "add"  # 加仓
    REDUCE = "reduce"  # 减仓


class StockTrack(Base):
    """
    股票追踪主表

    每只股票只能有一个 track（唯一约束 on ts_code）。
    记录用户追踪的个股及其元信息。
    """

    __tablename__ = "stock_tracks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ts_code: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    stock_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Tracking metadata
    watch_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    tags: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)  # ["tag1", "tag2"]

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    daily_entries: Mapped[List["TrackDailyEntry"]] = relationship(
        "TrackDailyEntry", back_populates="track", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_stock_tracks_ts_code", "ts_code"),
        Index("idx_stock_tracks_active", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<StockTrack(ts_code={self.ts_code}, name={self.stock_name})>"


class TrackDailyEntry(Base):
    """
    每日追踪条目

    每只股票每个交易日一条记录，关联 OHLCV 数据和用户标注。
    通过 sync 从 market_daily 表同步基础数据。
    """

    __tablename__ = "track_daily_entries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    track_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("stock_tracks.id", ondelete="CASCADE"), nullable=False
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    ts_code: Mapped[str] = mapped_column(String(20), nullable=False)

    # OHLCV from market_daily (synced)
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    pre_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    pct_chg: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # User annotations
    pattern: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # 天干 pattern
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mood: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # e.g. "confident", "cautious"

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    track: Mapped["StockTrack"] = relationship("StockTrack", back_populates="daily_entries")
    sketch: Mapped[Optional["DailySketchPoints"]] = relationship(
        "DailySketchPoints", back_populates="entry", uselist=False, cascade="all, delete-orphan"
    )
    scores: Mapped[Optional["DailySituationScore"]] = relationship(
        "DailySituationScore", back_populates="entry", uselist=False, cascade="all, delete-orphan"
    )
    operations: Mapped[List["TradeOperation"]] = relationship(
        "TradeOperation", back_populates="entry", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("track_id", "trade_date", name="uq_track_daily_entry"),
        Index("idx_track_daily_entries_track", "track_id"),
        Index("idx_track_daily_entries_date", "trade_date"),
        Index("idx_track_daily_entries_ts_code", "ts_code"),
    )

    def __repr__(self) -> str:
        return f"<TrackDailyEntry(ts_code={self.ts_code}, date={self.trade_date})>"


class DailySketchPoints(Base):
    """
    每日草图关键点

    存储用户在 Konva 画布上标记的 OHLC 关键时间点。
    key_points 是 JSONB，结构:
    {
        "open":  {"time": "09:30", "price_pct": 0.0},
        "high":  {"time": "10:30", "price_pct": 3.5},
        "low":   {"time": "14:00", "price_pct": -1.2},
        "close": {"time": "15:00", "price_pct": 2.1}
    }
    price_pct 相对于前收盘价的百分比偏移。
    """

    __tablename__ = "daily_sketch_points"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("track_daily_entries.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    key_points: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    auto_pattern: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )  # Auto-recognized pattern

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    entry: Mapped["TrackDailyEntry"] = relationship("TrackDailyEntry", back_populates="sketch")

    __table_args__ = (Index("idx_daily_sketch_entry", "entry_id"),)

    def __repr__(self) -> str:
        return f"<DailySketchPoints(entry_id={self.entry_id}, pattern={self.auto_pattern})>"


class DailySituationScore(Base):
    """
    每日形势评分

    三个维度: 大盘(30分) / 板块(30分) / 个股(40分) = 满分100分
    scores 是 JSONB，结构:
    {
        "market": {"trend": 10, "volume": 5, "sentiment": 5},
        "sector": {"trend": 10, "relative": 10, "flow": 5},
        "stock":  {"pattern": 15, "volume": 10, "position": 10, "catalyst": 5}
    }
    """

    __tablename__ = "daily_situation_scores"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("track_daily_entries.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    scores: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    total_score: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)

    # Auto-derived conclusion
    market_label: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 偏多/震荡/偏空
    sector_label: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 强/一般/弱
    stock_label: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # 强/震荡/弱
    decision: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # 做多日/震荡日/回撤日

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    entry: Mapped["TrackDailyEntry"] = relationship("TrackDailyEntry", back_populates="scores")

    __table_args__ = (Index("idx_daily_situation_entry", "entry_id"),)

    def __repr__(self) -> str:
        return f"<DailySituationScore(entry_id={self.entry_id}, total={self.total_score})>"


class TradeOperation(Base):
    """
    交易操作记录

    记录每日的买卖操作，包含时间、价格、数量、情绪等。
    """

    __tablename__ = "trade_operations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("track_daily_entries.id", ondelete="CASCADE"), nullable=False
    )

    op_type: Mapped[str] = mapped_column(String(20), nullable=False)  # buy/sell/add/reduce
    op_time: Mapped[Optional[time]] = mapped_column(Time, nullable=True)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    emotion: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )  # e.g. "FOMO", "rational"
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    entry: Mapped["TrackDailyEntry"] = relationship("TrackDailyEntry", back_populates="operations")

    __table_args__ = (
        Index("idx_trade_operations_entry", "entry_id"),
        CheckConstraint("op_type IN ('buy', 'sell', 'add', 'reduce')", name="valid_op_type"),
    )

    def __repr__(self) -> str:
        return f"<TradeOperation(entry_id={self.entry_id}, type={self.op_type})>"


class BaostockMinuteCache(Base):
    """
    BaoStock 分钟级数据缓存

    缓存从 BaoStock 获取的分钟级数据，避免重复请求。
    每条记录是一根分钟K线。
    """

    __tablename__ = "baostock_minute_cache"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ts_code: Mapped[str] = mapped_column(String(20), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    frequency: Mapped[str] = mapped_column(
        String(10), nullable=False, default="5"
    )  # "5", "15", "30", "60"

    # Minute OHLCV data stored as JSONB array for efficient storage
    # Each element: {"time": "09:35", "open": 10.5, "high": 10.6, "low": 10.4, "close": 10.55, "volume": 1234}
    candles: Mapped[List[Dict[str, Any]]] = mapped_column(JSONB, nullable=False)

    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("ts_code", "trade_date", "frequency", name="uq_baostock_minute_cache"),
        Index("idx_baostock_cache_code_date", "ts_code", "trade_date"),
    )

    def __repr__(self) -> str:
        return f"<BaostockMinuteCache(ts_code={self.ts_code}, date={self.trade_date}, freq={self.frequency})>"
