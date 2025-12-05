"""Technical and fundamental indicator models."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import String, Date, DateTime, Numeric, BigInteger, Index, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TechnicalIndicator(Base):
    """Pre-computed technical indicators."""

    __tablename__ = "technical_indicators"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    # Moving Averages
    ma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ma_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ma_120: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ma_250: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # EMA
    ema_12: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ema_26: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # MACD
    macd_dif: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    macd_dea: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    macd_hist: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)

    # RSI
    rsi_6: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_12: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_24: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # KDJ
    kdj_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    kdj_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    kdj_j: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Bollinger Bands
    boll_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    boll_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    boll_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # Volume indicators
    vol_ma_5: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    vol_ma_10: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    # Additional
    atr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    obv: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_tech_ind_code_date", "code", "date", unique=True),
    )

    def __repr__(self) -> str:
        return f"<TechnicalIndicator(code={self.code}, date={self.date})>"


class FundamentalIndicator(Base):
    """Quarterly/annual fundamental indicators."""

    __tablename__ = "fundamental_indicators"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    report_date: Mapped[date] = mapped_column(Date, nullable=False)
    report_type: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # Q1, Q2, Q3, Q4, annual

    # Valuation
    pe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    pb_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ps_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    ev_ebitda: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # Profitability
    roe: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    roa: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    gross_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    net_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    operating_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Growth
    revenue_growth_yoy: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    profit_growth_yoy: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    eps_growth_yoy: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Cash Flow
    operating_cash_flow: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    free_cash_flow: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), nullable=True)
    cash_flow_per_share: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)

    # Debt & Liquidity
    debt_to_equity: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    current_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    quick_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Per Share
    eps: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    bvps: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    dividend_yield: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("idx_fund_ind_code", "code"),
        Index("idx_fund_ind_code_date", "code", "report_date", unique=True),
    )

    def __repr__(self) -> str:
        return f"<FundamentalIndicator(code={self.code}, date={self.report_date})>"
