"""Stock data API endpoints."""

from datetime import date
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.stock import StockBasic, DailyKData
from app.db.models.indicator import TechnicalIndicator

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class StockBasicResponse(BaseModel):
    """Schema for stock basic info response."""
    code: str
    code_name: Optional[str]
    ipo_date: Optional[date]
    out_date: Optional[date]
    stock_type: Optional[int]
    status: Optional[int]
    exchange: Optional[str]
    sector: Optional[str]
    industry: Optional[str]

    class Config:
        from_attributes = True


class StockListResponse(BaseModel):
    """Schema for paginated stock list."""
    items: List[StockBasicResponse]
    total: int
    page: int
    page_size: int
    pages: int


class KLineData(BaseModel):
    """Schema for K-line data point."""
    date: date
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    volume: Optional[int]
    amount: Optional[Decimal]
    pct_chg: Optional[Decimal]
    turn: Optional[Decimal]

    class Config:
        from_attributes = True


class KLineResponse(BaseModel):
    """Schema for K-line data response."""
    code: str
    code_name: Optional[str]
    data: List[KLineData]
    total: int


class TechnicalIndicatorResponse(BaseModel):
    """Schema for technical indicator response."""
    date: date
    ma_5: Optional[Decimal]
    ma_10: Optional[Decimal]
    ma_20: Optional[Decimal]
    ma_60: Optional[Decimal]
    ema_12: Optional[Decimal]
    ema_26: Optional[Decimal]
    macd_dif: Optional[Decimal]
    macd_dea: Optional[Decimal]
    macd_hist: Optional[Decimal]
    rsi_6: Optional[Decimal]
    rsi_12: Optional[Decimal]
    rsi_24: Optional[Decimal]
    kdj_k: Optional[Decimal]
    kdj_d: Optional[Decimal]
    kdj_j: Optional[Decimal]
    boll_upper: Optional[Decimal]
    boll_middle: Optional[Decimal]
    boll_lower: Optional[Decimal]

    class Config:
        from_attributes = True


class StockSearchResult(BaseModel):
    """Schema for stock search result."""
    code: str
    code_name: Optional[str]
    exchange: Optional[str]


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=StockListResponse)
async def list_stocks(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    exchange: Optional[str] = Query(default=None, description="Filter by exchange: sh or sz"),
    sector: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None, description="Search by code or name"),
    db: AsyncSession = Depends(get_db),
):
    """List all stocks with pagination and filtering."""
    query = select(StockBasic)

    # Apply filters
    if exchange:
        query = query.where(StockBasic.exchange == exchange)
    if sector:
        query = query.where(StockBasic.sector == sector)
    if search:
        query = query.where(
            StockBasic.code.ilike(f"%{search}%") | StockBasic.code_name.ilike(f"%{search}%")
        )

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    query = query.order_by(StockBasic.code)

    result = await db.execute(query)
    stocks = result.scalars().all()

    return StockListResponse(
        items=[StockBasicResponse.model_validate(s) for s in stocks],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/search", response_model=List[StockSearchResult])
async def search_stocks(
    q: str = Query(min_length=1, description="Search query"),
    limit: int = Query(default=20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Search stocks by code or name."""
    query = (
        select(StockBasic)
        .where(
            StockBasic.code.ilike(f"%{q}%") | StockBasic.code_name.ilike(f"%{q}%")
        )
        .limit(limit)
    )

    result = await db.execute(query)
    stocks = result.scalars().all()

    return [
        StockSearchResult(
            code=s.code,
            code_name=s.code_name,
            exchange=s.exchange,
        )
        for s in stocks
    ]


@router.get("/{code}", response_model=StockBasicResponse)
async def get_stock(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get stock basic information by code."""
    result = await db.execute(
        select(StockBasic).where(StockBasic.code == code)
    )
    stock = result.scalar_one_or_none()

    if not stock:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stock not found",
        )

    return StockBasicResponse.model_validate(stock)


@router.get("/{code}/kline", response_model=KLineResponse)
async def get_kline(
    code: str,
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get K-line (OHLCV) data for a stock."""
    # Get stock info
    stock_result = await db.execute(
        select(StockBasic).where(StockBasic.code == code)
    )
    stock = stock_result.scalar_one_or_none()

    if not stock:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stock not found",
        )

    # Build K-line query
    query = select(DailyKData).where(DailyKData.code == code)

    if start_date:
        query = query.where(DailyKData.date >= start_date)
    if end_date:
        query = query.where(DailyKData.date <= end_date)

    query = query.order_by(DailyKData.date.desc()).limit(limit)

    result = await db.execute(query)
    kline_data = result.scalars().all()

    # Reverse to get chronological order
    kline_data = list(reversed(kline_data))

    return KLineResponse(
        code=code,
        code_name=stock.code_name,
        data=[KLineData.model_validate(k) for k in kline_data],
        total=len(kline_data),
    )


@router.get("/{code}/indicators", response_model=List[TechnicalIndicatorResponse])
async def get_indicators(
    code: str,
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get technical indicators for a stock."""
    # Check stock exists
    stock_result = await db.execute(
        select(StockBasic).where(StockBasic.code == code)
    )
    if not stock_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Stock not found",
        )

    # Build query
    query = select(TechnicalIndicator).where(TechnicalIndicator.code == code)

    if start_date:
        query = query.where(TechnicalIndicator.date >= start_date)
    if end_date:
        query = query.where(TechnicalIndicator.date <= end_date)

    query = query.order_by(TechnicalIndicator.date.desc()).limit(limit)

    result = await db.execute(query)
    indicators = result.scalars().all()

    # Reverse to get chronological order
    indicators = list(reversed(indicators))

    return [TechnicalIndicatorResponse.model_validate(i) for i in indicators]


@router.get("/{code}/fundamentals")
async def get_fundamentals(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get fundamental data for a stock (from daily_k_data)."""
    # Get latest fundamental data from daily_k_data
    result = await db.execute(
        select(DailyKData)
        .where(
            and_(
                DailyKData.code == code,
                DailyKData.pe_ttm.isnot(None),
            )
        )
        .order_by(DailyKData.date.desc())
        .limit(1)
    )
    latest = result.scalar_one_or_none()

    if not latest:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Fundamental data not found",
        )

    return {
        "code": code,
        "date": latest.date,
        "pe_ttm": latest.pe_ttm,
        "pb_mrq": latest.pb_mrq,
        "ps_ttm": latest.ps_ttm,
        "pcf_ncf_ttm": latest.pcf_ncf_ttm,
        "is_st": latest.is_st,
    }
