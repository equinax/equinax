"""Backtest job and result models."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from sqlalchemy import (
    String, Text, Integer, Date, DateTime, Numeric,
    ForeignKey, Index, PrimaryKeyConstraint, func, Enum as SQLEnum
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship, WriteOnlyMapped

from app.db.base import Base


class BacktestStatus(str, Enum):
    """Backtest job status enum."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestJob(Base):
    """Backtest job configuration and status."""

    __tablename__ = "backtest_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Job configuration
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Strategies to run (array of UUIDs)
    strategy_ids: Mapped[List[str]] = mapped_column(ARRAY(String), nullable=False)

    # Stock universe
    stock_codes: Mapped[List[str]] = mapped_column(ARRAY(String(20)), nullable=False)
    stock_filter: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Time range
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Backtest parameters
    initial_capital: Mapped[Decimal] = mapped_column(
        Numeric(18, 2), default=1000000.00
    )
    commission_rate: Mapped[Decimal] = mapped_column(
        Numeric(8, 6), default=0.0003  # 0.03%
    )
    slippage: Mapped[Decimal] = mapped_column(
        Numeric(8, 6), default=0.001  # 0.1%
    )
    position_sizing: Mapped[Dict[str, Any]] = mapped_column(
        JSONB, default={"type": "percent", "value": 10}
    )

    # Execution settings
    priority: Mapped[int] = mapped_column(Integer, default=5)  # 1-10

    # Status
    status: Mapped[BacktestStatus] = mapped_column(
        SQLEnum(BacktestStatus),
        default=BacktestStatus.PENDING,
    )
    progress: Mapped[Decimal] = mapped_column(Numeric(5, 2), default=0)  # 0-100
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Task tracking
    arq_job_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Results summary
    total_backtests: Mapped[int] = mapped_column(Integer, default=0)
    successful_backtests: Mapped[int] = mapped_column(Integer, default=0)
    failed_backtests: Mapped[int] = mapped_column(Integer, default=0)

    # Strategy code snapshot at execution time
    strategy_snapshots: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB, nullable=True, comment="策略执行时的完整快照 {strategy_id: {name, code, ...}}"
    )

    # Relationships
    results: Mapped[List["BacktestResult"]] = relationship(
        "BacktestResult",
        back_populates="job",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_backtest_jobs_user", "user_id"),
        Index("idx_backtest_jobs_status", "status"),
        Index("idx_backtest_jobs_created", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<BacktestJob(id={self.id}, status={self.status})>"


class BacktestResult(Base):
    """Individual backtest result for a strategy-stock combination."""

    __tablename__ = "backtest_results"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtest_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategies.id"),
        nullable=False,
    )
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)

    # Parameters used
    parameters: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    # Performance Metrics
    total_return: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    annual_return: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)  # CAGR
    sharpe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    sortino_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    calmar_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    max_drawdown: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    max_drawdown_duration: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Days

    # Risk Metrics
    volatility: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    var_95: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    cvar_95: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    beta: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    alpha: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Trade Statistics
    total_trades: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    winning_trades: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    losing_trades: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    win_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    avg_win: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    avg_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    profit_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    avg_trade_duration: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2), nullable=True)

    # Portfolio Metrics
    final_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    peak_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)

    # Monthly returns (kept as JSONB - small data)
    monthly_returns: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Execution info
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[BacktestStatus] = mapped_column(
        SQLEnum(BacktestStatus),
        default=BacktestStatus.COMPLETED,
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    # Relationships
    job: Mapped["BacktestJob"] = relationship("BacktestJob", back_populates="results")

    # Relationships to new tables (lazy load for performance)
    equity_points: WriteOnlyMapped[List["BacktestEquity"]] = relationship(
        "BacktestEquity",
        back_populates="result",
        cascade="all, delete-orphan",
    )
    trade_records: WriteOnlyMapped[List["BacktestTrade"]] = relationship(
        "BacktestTrade",
        back_populates="result",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_backtest_results_job", "job_id"),
        Index("idx_backtest_results_strategy", "strategy_id"),
        Index("idx_backtest_results_stock", "stock_code"),
        Index("idx_backtest_results_sharpe", "sharpe_ratio"),
        Index(
            "idx_backtest_results_job_strategy_stock",
            "job_id", "strategy_id", "stock_code",
            unique=True
        ),
    )

    def __repr__(self) -> str:
        return f"<BacktestResult(job_id={self.job_id}, strategy_id={self.strategy_id}, stock={self.stock_code})>"


class BacktestEquity(Base):
    """
    资金曲线时序数据表

    存储每个交易日的账户价值，用于绘制权益曲线图表。
    使用 TimescaleDB hypertable 进行时序优化。
    """

    __tablename__ = "backtest_equity"

    result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtest_results.id", ondelete="CASCADE"),
        nullable=False,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # 核心数据
    value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    cash: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    position_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)

    # 风险指标
    drawdown: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)
    daily_return: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)

    # Relationship
    result: Mapped["BacktestResult"] = relationship("BacktestResult", back_populates="equity_points")

    __table_args__ = (
        PrimaryKeyConstraint("result_id", "date"),
        Index("idx_backtest_equity_result", "result_id"),
        Index("idx_backtest_equity_date", "date"),
    )

    def __repr__(self) -> str:
        return f"<BacktestEquity(result_id={self.result_id}, date={self.date}, value={self.value})>"


class BacktestTrade(Base):
    """
    回测交易记录详情表

    记录每笔完整的买卖操作，包括入场、出场、盈亏等信息。
    """

    __tablename__ = "backtest_trades"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtest_results.id", ondelete="CASCADE"),
        nullable=False,
    )

    # 交易标的
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)

    # 交易方向
    direction: Mapped[str] = mapped_column(String(10), nullable=False)  # 'long' or 'short'

    # 开仓信息
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)

    # 平仓信息 (可能尚未平仓)
    exit_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    exit_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # 交易规模
    size: Mapped[int] = mapped_column(Integer, nullable=False)  # 股数

    # 盈亏
    pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 毛利
    commission: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    net_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)  # 净利
    pnl_percent: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6), nullable=True)

    # 持仓统计
    bars_held: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # 时间戳
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    # Relationship
    result: Mapped["BacktestResult"] = relationship("BacktestResult", back_populates="trade_records")

    __table_args__ = (
        Index("idx_backtest_trades_result", "result_id"),
        Index("idx_backtest_trades_stock", "stock_code"),
        Index("idx_backtest_trades_entry_date", "entry_date"),
    )

    def __repr__(self) -> str:
        return f"<BacktestTrade(id={self.id}, stock={self.stock_code}, direction={self.direction})>"
