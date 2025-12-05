"""Backtest job and result models."""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from sqlalchemy import (
    String, Text, Integer, Date, DateTime, Numeric,
    ForeignKey, Index, func, Enum as SQLEnum
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

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

    # Detailed data (stored as JSONB)
    equity_curve: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB, nullable=True)
    trades: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB, nullable=True)
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
