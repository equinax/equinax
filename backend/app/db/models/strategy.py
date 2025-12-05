"""Strategy models for storing trading strategies."""

import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import String, Text, Integer, Boolean, DateTime, ForeignKey, Index, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Strategy(Base):
    """Strategy model for storing trading strategy code and metadata."""

    __tablename__ = "strategies"

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
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)

    # Strategy code stored as text
    code: Mapped[str] = mapped_column(Text, nullable=False)
    code_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # SHA256 hash

    # Strategy metadata
    strategy_type: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )  # momentum, mean_reversion, trend_following, etc.
    indicators_used: Mapped[List[str]] = mapped_column(JSONB, default=list)
    parameters: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)

    # Validation & Status
    is_validated: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)

    # Future: Live trading support
    execution_mode: Mapped[str] = mapped_column(
        String(20), default="backtest"
    )  # backtest, paper, live
    risk_params: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    broker_config: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relationships
    versions: Mapped[List["StrategyVersion"]] = relationship(
        "StrategyVersion",
        back_populates="strategy",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_strategies_user", "user_id"),
        Index("idx_strategies_type", "strategy_type"),
        Index("idx_strategies_active", "is_active"),
        Index("idx_strategies_user_name_version", "user_id", "name", "version", unique=True),
    )

    def __repr__(self) -> str:
        return f"<Strategy(id={self.id}, name={self.name}, version={self.version})>"


class StrategyVersion(Base):
    """Strategy version history for tracking changes."""

    __tablename__ = "strategy_versions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategies.id", ondelete="CASCADE"),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    code_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    parameters: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    change_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id"),
        nullable=True,
    )

    # Relationships
    strategy: Mapped["Strategy"] = relationship("Strategy", back_populates="versions")

    __table_args__ = (
        Index("idx_strategy_versions_strategy_version", "strategy_id", "version", unique=True),
    )

    def __repr__(self) -> str:
        return f"<StrategyVersion(strategy_id={self.strategy_id}, version={self.version})>"
