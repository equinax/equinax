"""Asset data API endpoints.

Provides unified access to stocks and ETFs through the new data model.
"""

from datetime import date
from typing import List, Optional
from decimal import Decimal
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType, MarketDaily, IndicatorValuation, IndicatorETF, AdjustFactor
from app.db.models.profile import StockProfile, ETFProfile
from app.db.models.indicator import TechnicalIndicator

router = APIRouter()


# ============================================
# Enums for API
# ============================================

class AssetTypeFilter(str, Enum):
    """Asset type filter for API."""
    ALL = "all"
    STOCK = "stock"
    ETF = "etf"


# ============================================
# Pydantic Schemas
# ============================================

class AssetBasicResponse(BaseModel):
    """Schema for asset basic info response."""
    code: str
    name: str
    asset_type: str
    exchange: str
    list_date: Optional[date]
    delist_date: Optional[date]
    status: int
    category: Optional[str]

    class Config:
        from_attributes = True


class StockBasicResponse(BaseModel):
    """Schema for stock basic info response (backward compatible)."""
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


class AssetListResponse(BaseModel):
    """Schema for paginated asset list."""
    items: List[AssetBasicResponse]
    total: int
    page: int
    page_size: int
    pages: int


class StockListResponse(BaseModel):
    """Schema for paginated stock list (backward compatible)."""
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


class AssetSearchResult(BaseModel):
    """Schema for asset search result."""
    code: str
    name: str
    asset_type: str
    exchange: str


class StockSearchResult(BaseModel):
    """Schema for stock search result (backward compatible)."""
    code: str
    code_name: Optional[str]
    exchange: Optional[str]


class AdjustFactorResponse(BaseModel):
    """Schema for adjust factor response."""
    divid_operate_date: date
    fore_adjust_factor: Optional[Decimal]
    back_adjust_factor: Optional[Decimal]
    adjust_factor: Optional[Decimal]

    class Config:
        from_attributes = True


class ValuationResponse(BaseModel):
    """Schema for valuation data response."""
    code: str
    date: date
    pe_ttm: Optional[Decimal]
    pb_mrq: Optional[Decimal]
    ps_ttm: Optional[Decimal]
    pcf_ncf_ttm: Optional[Decimal]
    total_mv: Optional[Decimal]
    circ_mv: Optional[Decimal]
    is_st: Optional[int]


class ETFIndicatorResponse(BaseModel):
    """Schema for ETF indicator response."""
    code: str
    date: date
    iopv: Optional[Decimal]
    discount_rate: Optional[Decimal]
    unit_total: Optional[Decimal]
    tracking_error: Optional[Decimal]


# ============================================
# Helper Functions
# ============================================

def convert_to_stock_response(asset: AssetMeta, profile: Optional[StockProfile] = None) -> StockBasicResponse:
    """Convert AssetMeta to backward-compatible StockBasicResponse."""
    return StockBasicResponse(
        code=asset.code,
        code_name=asset.name,
        ipo_date=asset.list_date,
        out_date=asset.delist_date,
        stock_type=1 if asset.asset_type == AssetType.STOCK else 2,
        status=asset.status,
        exchange=asset.exchange,
        sector=asset.category,
        industry=profile.sw_industry_l1 if profile else None,
    )


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
    asset_type: AssetTypeFilter = Query(default=AssetTypeFilter.STOCK, description="Asset type filter"),
    include_etf: bool = Query(default=False, description="Include ETF (deprecated, use asset_type)"),
    db: AsyncSession = Depends(get_db),
):
    """List assets with pagination and filtering."""
    query = select(AssetMeta)

    # Apply asset type filter
    if asset_type == AssetTypeFilter.STOCK:
        query = query.where(AssetMeta.asset_type == AssetType.STOCK)
    elif asset_type == AssetTypeFilter.ETF:
        query = query.where(AssetMeta.asset_type == AssetType.ETF)
    elif include_etf:
        # Backward compatibility
        query = query.where(AssetMeta.asset_type.in_([AssetType.STOCK, AssetType.ETF]))
    else:
        query = query.where(AssetMeta.asset_type == AssetType.STOCK)

    # Apply filters
    if exchange:
        query = query.where(AssetMeta.exchange == exchange)
    if sector:
        query = query.where(AssetMeta.category == sector)
    if search:
        query = query.where(
            or_(
                AssetMeta.code.ilike(f"%{search}%"),
                AssetMeta.name.ilike(f"%{search}%")
            )
        )

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    query = query.order_by(AssetMeta.code)

    result = await db.execute(query)
    assets = result.scalars().all()

    # Get stock profiles for industry info
    codes = [a.code for a in assets]
    profile_result = await db.execute(
        select(StockProfile).where(StockProfile.code.in_(codes))
    )
    profiles = {p.code: p for p in profile_result.scalars().all()}

    return StockListResponse(
        items=[convert_to_stock_response(a, profiles.get(a.code)) for a in assets],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/assets", response_model=AssetListResponse)
async def list_assets(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    exchange: Optional[str] = Query(default=None),
    asset_type: AssetTypeFilter = Query(default=AssetTypeFilter.ALL),
    search: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """List all assets with pagination (new API)."""
    query = select(AssetMeta)

    # Apply filters
    if asset_type == AssetTypeFilter.STOCK:
        query = query.where(AssetMeta.asset_type == AssetType.STOCK)
    elif asset_type == AssetTypeFilter.ETF:
        query = query.where(AssetMeta.asset_type == AssetType.ETF)

    if exchange:
        query = query.where(AssetMeta.exchange == exchange)
    if search:
        query = query.where(
            or_(
                AssetMeta.code.ilike(f"%{search}%"),
                AssetMeta.name.ilike(f"%{search}%")
            )
        )

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    query = query.order_by(AssetMeta.code)

    result = await db.execute(query)
    assets = result.scalars().all()

    return AssetListResponse(
        items=[AssetBasicResponse(
            code=a.code,
            name=a.name,
            asset_type=a.asset_type.value if isinstance(a.asset_type, AssetType) else a.asset_type,
            exchange=a.exchange,
            list_date=a.list_date,
            delist_date=a.delist_date,
            status=a.status,
            category=a.category,
        ) for a in assets],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/search", response_model=List[AssetSearchResult])
async def search_assets(
    q: str = Query(min_length=1, description="Search query"),
    limit: int = Query(default=20, ge=1, le=50),
    asset_type: AssetTypeFilter = Query(default=AssetTypeFilter.ALL),
    db: AsyncSession = Depends(get_db),
):
    """Search assets by code or name."""
    query = select(AssetMeta).where(
        or_(
            AssetMeta.code.ilike(f"%{q}%"),
            AssetMeta.name.ilike(f"%{q}%")
        )
    )

    if asset_type == AssetTypeFilter.STOCK:
        query = query.where(AssetMeta.asset_type == AssetType.STOCK)
    elif asset_type == AssetTypeFilter.ETF:
        query = query.where(AssetMeta.asset_type == AssetType.ETF)

    query = query.limit(limit)

    result = await db.execute(query)
    assets = result.scalars().all()

    return [
        AssetSearchResult(
            code=a.code,
            name=a.name,
            asset_type=a.asset_type.value if isinstance(a.asset_type, AssetType) else a.asset_type,
            exchange=a.exchange,
        )
        for a in assets
    ]


@router.get("/{code}", response_model=StockBasicResponse)
async def get_stock(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get asset basic information by code."""
    result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    asset = result.scalar_one_or_none()

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    # Get profile for industry info
    profile_result = await db.execute(
        select(StockProfile).where(StockProfile.code == code)
    )
    profile = profile_result.scalar_one_or_none()

    return convert_to_stock_response(asset, profile)


@router.get("/{code}/kline", response_model=KLineResponse)
async def get_kline(
    code: str,
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get K-line (OHLCV) data for an asset."""
    # Get asset info
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    asset = asset_result.scalar_one_or_none()

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    # Build K-line query from market_daily
    query = select(MarketDaily).where(MarketDaily.code == code)

    if start_date:
        query = query.where(MarketDaily.date >= start_date)
    if end_date:
        query = query.where(MarketDaily.date <= end_date)

    query = query.order_by(MarketDaily.date.desc()).limit(limit)

    result = await db.execute(query)
    kline_data = result.scalars().all()

    # Reverse to get chronological order
    kline_data = list(reversed(kline_data))

    return KLineResponse(
        code=code,
        code_name=asset.name,
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
    """Get technical indicators for an asset."""
    # Check asset exists
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    if not asset_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
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
    """Get fundamental/valuation data for an asset."""
    # Check asset exists and get type
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    asset = asset_result.scalar_one_or_none()

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    # For stocks, get valuation data
    if asset.asset_type == AssetType.STOCK or asset.asset_type == "STOCK":
        result = await db.execute(
            select(IndicatorValuation)
            .where(IndicatorValuation.code == code)
            .order_by(IndicatorValuation.date.desc())
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
            "asset_type": "STOCK",
            "date": latest.date,
            "pe_ttm": latest.pe_ttm,
            "pb_mrq": latest.pb_mrq,
            "ps_ttm": latest.ps_ttm,
            "pcf_ncf_ttm": latest.pcf_ncf_ttm,
            "total_mv": latest.total_mv,
            "circ_mv": latest.circ_mv,
            "is_st": latest.is_st,
        }

    # For ETFs, get ETF-specific indicators
    elif asset.asset_type == AssetType.ETF or asset.asset_type == "ETF":
        result = await db.execute(
            select(IndicatorETF)
            .where(IndicatorETF.code == code)
            .order_by(IndicatorETF.date.desc())
            .limit(1)
        )
        latest = result.scalar_one_or_none()

        # ETFs may not have indicator data
        if latest:
            return {
                "code": code,
                "asset_type": "ETF",
                "date": latest.date,
                "iopv": latest.iopv,
                "discount_rate": latest.discount_rate,
                "unit_total": latest.unit_total,
                "tracking_error": latest.tracking_error,
            }
        else:
            # Return empty response for ETF without indicator data
            return {
                "code": code,
                "asset_type": "ETF",
                "date": None,
                "iopv": None,
                "discount_rate": None,
                "unit_total": None,
                "tracking_error": None,
            }

    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported asset type",
        )


@router.get("/{code}/adjust-factors", response_model=List[AdjustFactorResponse])
async def get_adjust_factors(
    code: str,
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Get adjustment factors for an asset."""
    # Check asset exists
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    if not asset_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    # Build query
    query = select(AdjustFactor).where(AdjustFactor.code == code)

    if start_date:
        query = query.where(AdjustFactor.divid_operate_date >= start_date)
    if end_date:
        query = query.where(AdjustFactor.divid_operate_date <= end_date)

    query = query.order_by(AdjustFactor.divid_operate_date.desc()).limit(limit)

    result = await db.execute(query)
    factors = result.scalars().all()

    # Reverse to get chronological order
    factors = list(reversed(factors))

    return [AdjustFactorResponse.model_validate(f) for f in factors]


@router.get("/{code}/profile")
async def get_asset_profile(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get detailed profile for an asset (stock or ETF)."""
    # Get asset
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    asset = asset_result.scalar_one_or_none()

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    base_info = {
        "code": asset.code,
        "name": asset.name,
        "asset_type": asset.asset_type.value if isinstance(asset.asset_type, AssetType) else asset.asset_type,
        "exchange": asset.exchange,
        "list_date": asset.list_date,
        "delist_date": asset.delist_date,
        "status": asset.status,
        "category": asset.category,
    }

    # Get type-specific profile
    if asset.asset_type == AssetType.STOCK or asset.asset_type == "STOCK":
        profile_result = await db.execute(
            select(StockProfile).where(StockProfile.code == code)
        )
        profile = profile_result.scalar_one_or_none()

        if profile:
            base_info.update({
                "sw_industry_l1": profile.sw_industry_l1,
                "sw_industry_l2": profile.sw_industry_l2,
                "sw_industry_l3": profile.sw_industry_l3,
                "concepts": profile.concepts,
                "province": profile.province,
                "total_shares": profile.total_shares,
                "float_shares": profile.float_shares,
            })

    elif asset.asset_type == AssetType.ETF or asset.asset_type == "ETF":
        profile_result = await db.execute(
            select(ETFProfile).where(ETFProfile.code == code)
        )
        profile = profile_result.scalar_one_or_none()

        if profile:
            base_info.update({
                "etf_type": profile.etf_type.value if profile.etf_type else None,
                "underlying_index_code": profile.underlying_index_code,
                "underlying_index_name": profile.underlying_index_name,
                "fund_company": profile.fund_company,
                "management_fee": profile.management_fee,
                "custody_fee": profile.custody_fee,
                "fund_size": profile.fund_size,
            })

    return base_info
