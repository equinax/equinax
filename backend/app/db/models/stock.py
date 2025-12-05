"""Stock data models."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import String, Integer, Date, DateTime, Numeric, BigInteger, Index, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class StockBasic(Base):
    """Stock basic information."""

    __tablename__ = "stock_basic"

    code: Mapped[str] = mapped_column(String(20), primary_key=True)
    code_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    ipo_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    out_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    stock_type: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # 'sh' or 'sz'
    sector: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    industry: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

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
        Index("idx_stock_basic_exchange", "exchange"),
        Index("idx_stock_basic_sector", "sector"),
    )

    def __repr__(self) -> str:
        return f"<StockBasic(code={self.code}, name={self.code_name})>"


class DailyKData(Base):
    """Daily K-line (OHLCV) data."""

    __tablename__ = "daily_k_data"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # OHLCV data
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    preclose: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)

    # Trading metrics
    turn: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # Turnover rate
    trade_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    pct_chg: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)  # Percent change

    # Fundamental indicators (from baostock)
    pe_ttm: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    pb_mrq: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ps_ttm: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    pcf_ncf_ttm: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    is_st: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_daily_k_date", "date"),
        Index("idx_daily_k_code_date", "code", "date", unique=True),
    )

    def __repr__(self) -> str:
        return f"<DailyKData(code={self.code}, date={self.date}, close={self.close})>"


class AdjustFactor(Base):
    """Stock adjustment factors for splits and dividends."""

    __tablename__ = "adjust_factor"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    divid_operate_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    fore_adjust_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    back_adjust_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    adjust_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_adjust_factor_code_date", "code", "divid_operate_date", unique=True),
    )

    def __repr__(self) -> str:
        return f"<AdjustFactor(code={self.code}, date={self.divid_operate_date})>"
