"""Data sync tracking models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from sqlalchemy import String, DateTime, Numeric, Integer, Text, Index, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SyncHistory(Base):
    """
    数据同步历史记录表

    记录每次数据同步的执行状态、持续时间、处理记录数等信息
    用于监控和问题排查
    """

    __tablename__ = "sync_history"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4())
    )

    # 同步类型: daily, full, manual, fix
    sync_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # 时间戳
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )

    # 状态: running, success, failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")

    # 触发来源: cron, cli, api
    triggered_by: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # 指标
    duration_seconds: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    records_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    records_imported: Mapped[int] = mapped_column(Integer, default=0)
    records_classified: Mapped[int] = mapped_column(Integer, default=0)

    # 错误信息
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # 详细信息 (各步骤耗时、处理的数据类型等)
    details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_sync_history_started_at", "started_at", postgresql_ops={"started_at": "DESC"}),
        Index("idx_sync_history_status", "status"),
        Index("idx_sync_history_type", "sync_type"),
    )

    def __repr__(self) -> str:
        return f"<SyncHistory(id={self.id}, type={self.sync_type}, status={self.status})>"

    def mark_completed(self, status: str = "success", error_message: str = None):
        """标记同步完成"""
        self.completed_at = datetime.now()
        self.status = status
        if self.started_at:
            delta = self.completed_at - self.started_at
            self.duration_seconds = Decimal(str(delta.total_seconds()))
        if error_message:
            self.error_message = error_message
