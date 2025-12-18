"""Universe Cockpit API endpoints.

Provides comprehensive asset classification, filtering, and statistics for
the stock universe management interface.
"""

import datetime
from typing import List, Optional
from decimal import Decimal
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, or_, desc, case, literal
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType, MarketDaily, IndicatorValuation
from app.db.models.profile import StockProfile, ETFProfile
from app.db.models.classification import (
    StockClassificationSnapshot,
    StockStyleExposure,
    StockMicrostructure,
    MarketRegime,
    StockStructuralInfo,
    BoardType,
    SizeCategory,
    VolatilityCategory,
    TurnoverCategory,
    ValueCategory,
    MarketRegimeType,
)

router = APIRouter()


# ============================================
# Enums for API
# ============================================

class AssetTypeFilter(str, Enum):
    """Asset type filter for API."""
    ALL = "all"
    STOCK = "stock"
    ETF = "etf"


class SortField(str, Enum):
    """Sort field options."""
    CODE = "code"
    NAME = "name"
    MARKET_CAP = "market_cap"
    PRICE = "price"
    CHANGE = "change"
    PE = "pe"
    PB = "pb"
    TURNOVER = "turnover"


class SortOrder(str, Enum):
    """Sort order options."""
    ASC = "asc"
    DESC = "desc"


# ============================================
# Pydantic Schemas
# ============================================

class UniverseAssetItem(BaseModel):
    """Schema for a single asset in the universe table."""
    code: str
    name: str
    asset_type: str
    exchange: str

    # Price data
    price: Optional[Decimal] = None
    change_pct: Optional[Decimal] = None
    volume: Optional[int] = None
    amount: Optional[Decimal] = None
    turnover: Optional[Decimal] = None

    # Valuation
    market_cap: Optional[Decimal] = None
    circ_mv: Optional[Decimal] = None
    pe_ttm: Optional[Decimal] = None
    pb_mrq: Optional[Decimal] = None

    # Classification
    board: Optional[str] = None
    industry_l1: Optional[str] = None
    industry_l2: Optional[str] = None

    # Style factors
    size_category: Optional[str] = None
    vol_category: Optional[str] = None
    value_category: Optional[str] = None
    turnover_category: Optional[str] = None

    # Flags
    is_st: bool = False
    is_institutional: bool = False
    is_northbound_heavy: bool = False

    class Config:
        from_attributes = True


class UniverseSnapshotResponse(BaseModel):
    """Schema for paginated universe snapshot."""
    items: List[UniverseAssetItem]
    total: int
    page: int
    page_size: int
    pages: int
    date: datetime.date | None = None


class IndustryStats(BaseModel):
    """Industry statistics."""
    count: int
    total_mcap: Optional[Decimal] = None


class MarketRegimeInfo(BaseModel):
    """Market regime information."""
    regime: str
    score: Optional[Decimal] = None
    up_count: Optional[int] = None
    down_count: Optional[int] = None


class UniverseStatsResponse(BaseModel):
    """Schema for universe statistics."""
    total_stocks: int
    total_etfs: int
    trading_date: Optional[datetime.date] = None
    by_size_category: dict[str, int] = Field(default_factory=dict)
    by_board: dict[str, int] = Field(default_factory=dict)
    by_industry_l1: dict[str, IndustryStats] = Field(default_factory=dict)
    market_regime: Optional[MarketRegimeInfo] = None


class StyleFactorDetail(BaseModel):
    """Style factor detail."""
    value: Optional[Decimal] = None
    rank: Optional[int] = None
    percentile: Optional[Decimal] = None
    category: Optional[str] = None


class UniverseAssetDetail(BaseModel):
    """Schema for detailed asset information."""
    # Basic info
    code: str
    name: str
    asset_type: str
    exchange: str
    list_date: Optional[datetime.date] = None

    # Profile
    industry_l1: Optional[str] = None
    industry_l2: Optional[str] = None
    industry_l3: Optional[str] = None
    board: Optional[str] = None
    province: Optional[str] = None
    concepts: Optional[list] = None

    # Latest price
    price: Optional[Decimal] = None
    change_pct: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    volume: Optional[int] = None
    amount: Optional[Decimal] = None
    turnover: Optional[Decimal] = None
    price_date: Optional[datetime.date] = None

    # Valuation
    market_cap: Optional[Decimal] = None
    circ_mv: Optional[Decimal] = None
    pe_ttm: Optional[Decimal] = None
    pb_mrq: Optional[Decimal] = None
    ps_ttm: Optional[Decimal] = None

    # Style factors
    size_factor: Optional[StyleFactorDetail] = None
    vol_factor: Optional[StyleFactorDetail] = None
    value_factor: Optional[StyleFactorDetail] = None
    turnover_factor: Optional[StyleFactorDetail] = None
    momentum_20d: Optional[Decimal] = None
    momentum_60d: Optional[Decimal] = None

    # Microstructure
    fund_holding_ratio: Optional[Decimal] = None
    northbound_holding_ratio: Optional[Decimal] = None
    is_institutional: bool = False
    is_northbound_heavy: bool = False
    is_retail_hot: bool = False

    # Flags
    is_st: bool = False
    is_new: bool = False

    # Recent klines for mini chart (last 60 days)
    recent_klines: Optional[List[dict]] = None


# ============================================
# Helper Functions
# ============================================

def parse_list_param(value: Optional[str]) -> List[str]:
    """Parse comma-separated list parameter."""
    if not value or not isinstance(value, str):
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


# ============================================
# API Endpoints
# ============================================

@router.get("/snapshot", response_model=UniverseSnapshotResponse)
async def get_universe_snapshot(
    # Pagination
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),

    # Filters
    asset_type: AssetTypeFilter = Query(default=AssetTypeFilter.STOCK),
    exchange: Optional[str] = Query(default=None, description="Filter by exchange: sh, sz"),
    search: Optional[str] = Query(default=None, description="Search by code or name"),

    # Classification filters (comma-separated for multi-select)
    board: Optional[str] = Query(default=None, description="Board filter: MAIN,GEM,STAR,BSE"),
    size_category: Optional[str] = Query(default=None, description="Size: MEGA,LARGE,MID,SMALL,MICRO"),
    vol_category: Optional[str] = Query(default=None, description="Volatility: HIGH,NORMAL,LOW"),
    value_category: Optional[str] = Query(default=None, description="Value style: VALUE,NEUTRAL,GROWTH"),
    turnover_category: Optional[str] = Query(default=None, description="Turnover: HOT,NORMAL,DEAD"),
    industry_l1: Optional[str] = Query(default=None, description="Industry L1 filter"),
    is_st: Optional[bool] = Query(default=None, description="ST status filter"),

    # Sorting
    sort_by: SortField = Query(default=SortField.CODE),
    sort_order: SortOrder = Query(default=SortOrder.ASC),

    db: AsyncSession = Depends(get_db),
):
    """
    Get paginated universe snapshot with filtering and sorting.

    This endpoint returns a list of assets with their latest market data,
    valuation metrics, and classification information.
    """
    # Parse multi-select filters
    board_list = parse_list_param(board)
    size_list = parse_list_param(size_category)
    vol_list = parse_list_param(vol_category)
    value_list = parse_list_param(value_category)
    turnover_list = parse_list_param(turnover_category)

    # Get latest trading date from market_daily
    latest_date_result = await db.execute(
        select(func.max(MarketDaily.date))
    )
    latest_date = latest_date_result.scalar()

    if not latest_date:
        return UniverseSnapshotResponse(
            items=[],
            total=0,
            page=page,
            page_size=page_size,
            pages=0,
            date=None,
        )

    # Build base query joining AssetMeta with latest MarketDaily and Valuation
    # Using subquery to get latest date data
    market_subq = (
        select(MarketDaily)
        .where(MarketDaily.date == latest_date)
        .subquery()
    )

    valuation_subq = (
        select(IndicatorValuation)
        .where(IndicatorValuation.date == latest_date)
        .subquery()
    )

    # Main query
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            AssetMeta.asset_type,
            AssetMeta.exchange,
            market_subq.c.close.label("price"),
            market_subq.c.pct_chg.label("change_pct"),
            market_subq.c.volume,
            market_subq.c.amount,
            market_subq.c.turn.label("turnover"),
            valuation_subq.c.total_mv.label("market_cap"),
            valuation_subq.c.circ_mv,
            valuation_subq.c.pe_ttm,
            valuation_subq.c.pb_mrq,
            valuation_subq.c.is_st,
            StockProfile.sw_industry_l1.label("industry_l1"),
            StockProfile.sw_industry_l2.label("industry_l2"),
        )
        .outerjoin(market_subq, AssetMeta.code == market_subq.c.code)
        .outerjoin(valuation_subq, AssetMeta.code == valuation_subq.c.code)
        .outerjoin(StockProfile, AssetMeta.code == StockProfile.code)
    )

    # Apply asset type filter
    if asset_type == AssetTypeFilter.STOCK:
        query = query.where(AssetMeta.asset_type == AssetType.STOCK)
    elif asset_type == AssetTypeFilter.ETF:
        query = query.where(AssetMeta.asset_type == AssetType.ETF)

    # Apply exchange filter
    if exchange:
        query = query.where(AssetMeta.exchange == exchange)

    # Apply search filter
    if search:
        query = query.where(
            or_(
                AssetMeta.code.ilike(f"%{search}%"),
                AssetMeta.name.ilike(f"%{search}%")
            )
        )

    # Apply industry filter
    if industry_l1:
        query = query.where(StockProfile.sw_industry_l1 == industry_l1)

    # Apply ST filter
    if is_st is not None:
        if is_st:
            query = query.where(valuation_subq.c.is_st == 1)
        else:
            query = query.where(or_(valuation_subq.c.is_st == 0, valuation_subq.c.is_st.is_(None)))

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply sorting
    sort_column_map = {
        SortField.CODE: AssetMeta.code,
        SortField.NAME: AssetMeta.name,
        SortField.MARKET_CAP: valuation_subq.c.total_mv,
        SortField.PRICE: market_subq.c.close,
        SortField.CHANGE: market_subq.c.pct_chg,
        SortField.PE: valuation_subq.c.pe_ttm,
        SortField.PB: valuation_subq.c.pb_mrq,
        SortField.TURNOVER: market_subq.c.turn,
    }
    sort_col = sort_column_map.get(sort_by, AssetMeta.code)

    if sort_order == SortOrder.DESC:
        query = query.order_by(desc(sort_col).nulls_last())
    else:
        query = query.order_by(sort_col.nulls_last())

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    rows = result.all()

    # Convert to response
    items = []
    for row in rows:
        items.append(UniverseAssetItem(
            code=row.code,
            name=row.name,
            asset_type=row.asset_type.value if isinstance(row.asset_type, AssetType) else row.asset_type,
            exchange=row.exchange,
            price=row.price,
            change_pct=row.change_pct,
            volume=row.volume,
            amount=row.amount,
            turnover=row.turnover,
            market_cap=row.market_cap,
            circ_mv=row.circ_mv,
            pe_ttm=row.pe_ttm,
            pb_mrq=row.pb_mrq,
            industry_l1=row.industry_l1,
            industry_l2=row.industry_l2,
            is_st=bool(row.is_st) if row.is_st is not None else False,
        ))

    return UniverseSnapshotResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
        date=latest_date,
    )


@router.get("/stats", response_model=UniverseStatsResponse)
async def get_universe_stats(
    asset_type: AssetTypeFilter = Query(default=AssetTypeFilter.ALL),
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregate statistics for the universe dashboard.

    Returns counts by category, industry distribution, and market regime.
    """
    # Get latest trading date
    latest_date_result = await db.execute(
        select(func.max(MarketDaily.date))
    )
    latest_date = latest_date_result.scalar()

    # Count stocks
    stock_count_result = await db.execute(
        select(func.count()).where(AssetMeta.asset_type == AssetType.STOCK)
    )
    total_stocks = stock_count_result.scalar() or 0

    # Count ETFs
    etf_count_result = await db.execute(
        select(func.count()).where(AssetMeta.asset_type == AssetType.ETF)
    )
    total_etfs = etf_count_result.scalar() or 0

    # Count by board (using category field in AssetMeta)
    board_result = await db.execute(
        select(AssetMeta.category, func.count())
        .where(AssetMeta.asset_type == AssetType.STOCK)
        .where(AssetMeta.category.isnot(None))
        .group_by(AssetMeta.category)
    )
    by_board = {row[0]: row[1] for row in board_result.all()}

    # Count by industry L1
    industry_result = await db.execute(
        select(
            StockProfile.sw_industry_l1,
            func.count(),
        )
        .where(StockProfile.sw_industry_l1.isnot(None))
        .group_by(StockProfile.sw_industry_l1)
    )
    by_industry_l1 = {
        row[0]: IndustryStats(count=row[1])
        for row in industry_result.all()
    }

    # Try to get market regime
    market_regime = None
    if latest_date:
        regime_result = await db.execute(
            select(MarketRegime).where(MarketRegime.date == latest_date)
        )
        regime = regime_result.scalar_one_or_none()
        if regime:
            market_regime = MarketRegimeInfo(
                regime=regime.regime.value if isinstance(regime.regime, MarketRegimeType) else regime.regime,
                score=regime.regime_score,
                up_count=regime.up_count,
                down_count=regime.down_count,
            )

    return UniverseStatsResponse(
        total_stocks=total_stocks,
        total_etfs=total_etfs,
        trading_date=latest_date,
        by_size_category={},  # Will be populated when classification data exists
        by_board=by_board,
        by_industry_l1=by_industry_l1,
        market_regime=market_regime,
    )


@router.get("/industries", response_model=List[str])
async def get_industry_list(
    db: AsyncSession = Depends(get_db),
):
    """Get list of unique industry L1 values for filter dropdown."""
    result = await db.execute(
        select(StockProfile.sw_industry_l1)
        .where(StockProfile.sw_industry_l1.isnot(None))
        .distinct()
        .order_by(StockProfile.sw_industry_l1)
    )
    return [row[0] for row in result.all()]


@router.get("/{code}", response_model=UniverseAssetDetail)
async def get_asset_detail(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information for a specific asset.

    Includes profile, latest price, valuation, style factors, and microstructure data.
    """
    # Get basic asset info
    asset_result = await db.execute(
        select(AssetMeta).where(AssetMeta.code == code)
    )
    asset = asset_result.scalar_one_or_none()

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Asset not found",
        )

    # Get stock profile
    profile_result = await db.execute(
        select(StockProfile).where(StockProfile.code == code)
    )
    profile = profile_result.scalar_one_or_none()

    # Get latest market data
    market_result = await db.execute(
        select(MarketDaily)
        .where(MarketDaily.code == code)
        .order_by(desc(MarketDaily.date))
        .limit(1)
    )
    market = market_result.scalar_one_or_none()

    # Get latest valuation
    valuation_result = await db.execute(
        select(IndicatorValuation)
        .where(IndicatorValuation.code == code)
        .order_by(desc(IndicatorValuation.date))
        .limit(1)
    )
    valuation = valuation_result.scalar_one_or_none()

    # Get style factors
    style_result = await db.execute(
        select(StockStyleExposure)
        .where(StockStyleExposure.code == code)
        .order_by(desc(StockStyleExposure.date))
        .limit(1)
    )
    style = style_result.scalar_one_or_none()

    # Get microstructure
    micro_result = await db.execute(
        select(StockMicrostructure)
        .where(StockMicrostructure.code == code)
        .order_by(desc(StockMicrostructure.date))
        .limit(1)
    )
    micro = micro_result.scalar_one_or_none()

    # Get structural info
    structural_result = await db.execute(
        select(StockStructuralInfo).where(StockStructuralInfo.code == code)
    )
    structural = structural_result.scalar_one_or_none()

    # Get recent klines for mini chart (60 days)
    kline_result = await db.execute(
        select(MarketDaily)
        .where(MarketDaily.code == code)
        .order_by(desc(MarketDaily.date))
        .limit(60)
    )
    klines = kline_result.scalars().all()
    recent_klines = [
        {
            "date": str(k.date),
            "open": float(k.open) if k.open else None,
            "high": float(k.high) if k.high else None,
            "low": float(k.low) if k.low else None,
            "close": float(k.close) if k.close else None,
            "volume": k.volume,
        }
        for k in reversed(klines)
    ]

    # Build response
    response = UniverseAssetDetail(
        code=asset.code,
        name=asset.name,
        asset_type=asset.asset_type.value if isinstance(asset.asset_type, AssetType) else asset.asset_type,
        exchange=asset.exchange,
        list_date=asset.list_date,

        # Profile
        industry_l1=profile.sw_industry_l1 if profile else None,
        industry_l2=profile.sw_industry_l2 if profile else None,
        industry_l3=profile.sw_industry_l3 if profile else None,
        board=structural.board.value if structural and structural.board else None,
        province=profile.province if profile else None,
        concepts=profile.concepts if profile else None,

        # Price data
        price=market.close if market else None,
        change_pct=market.pct_chg if market else None,
        high=market.high if market else None,
        low=market.low if market else None,
        volume=market.volume if market else None,
        amount=market.amount if market else None,
        turnover=market.turn if market else None,
        price_date=market.date if market else None,

        # Valuation
        market_cap=valuation.total_mv if valuation else None,
        circ_mv=valuation.circ_mv if valuation else None,
        pe_ttm=valuation.pe_ttm if valuation else None,
        pb_mrq=valuation.pb_mrq if valuation else None,
        ps_ttm=valuation.ps_ttm if valuation else None,

        # Style factors
        size_factor=StyleFactorDetail(
            value=style.market_cap,
            rank=style.size_rank,
            percentile=style.size_percentile,
            category=style.size_category.value if style and style.size_category else None,
        ) if style else None,
        vol_factor=StyleFactorDetail(
            value=style.volatility_20d,
            rank=style.vol_rank,
            percentile=style.vol_percentile,
            category=style.vol_category.value if style and style.vol_category else None,
        ) if style else None,
        value_factor=StyleFactorDetail(
            value=style.ep_ratio,
            rank=style.value_rank,
            percentile=style.value_percentile,
            category=style.value_category.value if style and style.value_category else None,
        ) if style else None,
        turnover_factor=StyleFactorDetail(
            value=style.avg_turnover_20d,
            rank=style.turnover_rank,
            percentile=style.turnover_percentile,
            category=style.turnover_category.value if style and style.turnover_category else None,
        ) if style else None,
        momentum_20d=style.momentum_20d if style else None,
        momentum_60d=style.momentum_60d if style else None,

        # Microstructure
        fund_holding_ratio=micro.fund_holding_ratio if micro else None,
        northbound_holding_ratio=micro.northbound_holding_ratio if micro else None,
        is_institutional=micro.is_institutional if micro else False,
        is_northbound_heavy=micro.is_northbound_heavy if micro else False,
        is_retail_hot=micro.is_retail_hot if micro else False,

        # Flags
        is_st=structural.is_st if structural else False,
        is_new=structural.is_new if structural else False,

        recent_klines=recent_klines,
    )

    return response
