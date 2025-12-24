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
    industry_l3: Optional[str] = None
    em_industry: Optional[str] = None

    # Style factors
    size_category: Optional[str] = None
    vol_category: Optional[str] = None
    value_category: Optional[str] = None
    turnover_category: Optional[str] = None

    # Flags
    is_st: bool = False
    is_retail_hot: bool = False
    is_main_controlled: bool = False

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
    industry_l1: Optional[str] = Query(default=None, description="Industry L1 filter (SW)"),
    industry_l2: Optional[str] = Query(default=None, description="Industry L2 filter (SW)"),
    industry_l3: Optional[str] = Query(default=None, description="Industry L3 filter (SW)"),
    em_industry: Optional[str] = Query(default=None, description="Industry filter (EM/EastMoney)"),
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

    # Get latest trading date from market_daily for the requested asset_type
    # This ensures stocks and ETFs don't interfere with each other's date calculation
    # Note: asset_type.value is lowercase ('stock'/'etf') but DB stores uppercase ('STOCK'/'ETF')
    latest_date_result = await db.execute(
        select(func.max(MarketDaily.date))
        .select_from(MarketDaily)
        .join(AssetMeta, MarketDaily.code == AssetMeta.code)
        .where(AssetMeta.asset_type == asset_type.value.upper())
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

    # Style factors subquery (for size, volatility, value, turnover categories)
    style_subq = (
        select(
            StockStyleExposure.code,
            StockStyleExposure.size_category,
            StockStyleExposure.vol_category,
            StockStyleExposure.value_category,
            StockStyleExposure.turnover_category,
        )
        .where(StockStyleExposure.date == latest_date)
        .subquery()
    )

    # Microstructure subquery (for retail/main-controlled flags)
    micro_subq = (
        select(
            StockMicrostructure.code,
            StockMicrostructure.is_retail_hot,
            StockMicrostructure.is_main_controlled,
        )
        .where(StockMicrostructure.date == latest_date)
        .subquery()
    )

    # Main query with all classification data
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
            StockProfile.sw_industry_l3.label("industry_l3"),
            StockProfile.em_industry,
            # Classification fields
            StockStructuralInfo.board,
            style_subq.c.size_category,
            style_subq.c.vol_category,
            style_subq.c.value_category,
            style_subq.c.turnover_category,
            micro_subq.c.is_retail_hot,
            micro_subq.c.is_main_controlled,
        )
        .outerjoin(market_subq, AssetMeta.code == market_subq.c.code)
        .outerjoin(valuation_subq, AssetMeta.code == valuation_subq.c.code)
        .outerjoin(StockProfile, AssetMeta.code == StockProfile.code)
        .outerjoin(StockStructuralInfo, AssetMeta.code == StockStructuralInfo.code)
        .outerjoin(style_subq, AssetMeta.code == style_subq.c.code)
        .outerjoin(micro_subq, AssetMeta.code == micro_subq.c.code)
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

    # Apply industry filters (SW)
    if industry_l1:
        query = query.where(StockProfile.sw_industry_l1 == industry_l1)
    if industry_l2:
        query = query.where(StockProfile.sw_industry_l2 == industry_l2)
    if industry_l3:
        query = query.where(StockProfile.sw_industry_l3 == industry_l3)

    # Apply EM industry filter
    if em_industry:
        query = query.where(StockProfile.em_industry == em_industry)

    # Apply ST filter
    if is_st is not None:
        if is_st:
            query = query.where(valuation_subq.c.is_st == 1)
        else:
            query = query.where(or_(valuation_subq.c.is_st == 0, valuation_subq.c.is_st.is_(None)))

    # Apply classification filters
    if board_list:
        query = query.where(StockStructuralInfo.board.in_(board_list))
    if size_list:
        query = query.where(style_subq.c.size_category.in_(size_list))
    if vol_list:
        query = query.where(style_subq.c.vol_category.in_(vol_list))
    if value_list:
        query = query.where(style_subq.c.value_category.in_(value_list))
    if turnover_list:
        query = query.where(style_subq.c.turnover_category.in_(turnover_list))

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
        # Handle enum values for board and style categories
        board_value = None
        if row.board:
            board_value = row.board.value if isinstance(row.board, BoardType) else row.board

        size_cat = None
        if row.size_category:
            size_cat = row.size_category.value if isinstance(row.size_category, SizeCategory) else row.size_category

        vol_cat = None
        if row.vol_category:
            vol_cat = row.vol_category.value if isinstance(row.vol_category, VolatilityCategory) else row.vol_category

        value_cat = None
        if row.value_category:
            value_cat = row.value_category.value if isinstance(row.value_category, ValueCategory) else row.value_category

        turnover_cat = None
        if row.turnover_category:
            turnover_cat = row.turnover_category.value if isinstance(row.turnover_category, TurnoverCategory) else row.turnover_category

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
            industry_l3=row.industry_l3,
            em_industry=row.em_industry,
            is_st=bool(row.is_st) if row.is_st is not None else False,
            # Classification fields
            board=board_value,
            size_category=size_cat,
            vol_category=vol_cat,
            value_category=value_cat,
            turnover_category=turnover_cat,
            is_retail_hot=bool(row.is_retail_hot) if row.is_retail_hot else False,
            is_main_controlled=bool(row.is_main_controlled) if row.is_main_controlled else False,
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

    # Count by board (from StockStructuralInfo)
    board_result = await db.execute(
        select(StockStructuralInfo.board, func.count())
        .where(StockStructuralInfo.board.isnot(None))
        .group_by(StockStructuralInfo.board)
    )
    by_board = {}
    for row in board_result.all():
        board_val = row[0].value if isinstance(row[0], BoardType) else row[0]
        by_board[board_val] = row[1]

    # Count by size category (from StockStyleExposure for latest date)
    by_size_category = {}
    if latest_date:
        size_result = await db.execute(
            select(StockStyleExposure.size_category, func.count())
            .where(StockStyleExposure.date == latest_date)
            .where(StockStyleExposure.size_category.isnot(None))
            .group_by(StockStyleExposure.size_category)
        )
        for row in size_result.all():
            size_val = row[0].value if isinstance(row[0], SizeCategory) else row[0]
            by_size_category[size_val] = row[1]

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
        by_size_category=by_size_category,
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


class IndustryTreeItem(BaseModel):
    """Industry tree item for cascading selection."""
    name: str
    level: int
    stock_count: int
    has_children: bool = False


class IndustryTreeResponse(BaseModel):
    """Industry tree response."""
    system: str
    parent: Optional[str] = None
    items: List[IndustryTreeItem]


@router.get("/industries/tree", response_model=IndustryTreeResponse)
async def get_industry_tree(
    system: str = Query(default="sw", description="Classification system: sw, em"),
    parent: Optional[str] = Query(default=None, description="Parent industry name for SW L2/L3"),
    level: int = Query(default=1, description="Industry level (1, 2, 3 for SW; always 1 for EM)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get industry tree for cascading selection.

    For SW (申万):
    - level=1: Returns all L1 industries
    - level=2 with parent: Returns L2 industries under the given L1
    - level=3 with parent: Returns L3 industries under the given L2

    For EM (东方财富):
    - Returns flat list of all 86 industries (L1 only)
    """
    items = []

    if system == "em":
        # EM is flat structure, only L1
        result = await db.execute(
            select(
                StockProfile.em_industry,
                func.count().label("count")
            )
            .where(StockProfile.em_industry.isnot(None))
            .group_by(StockProfile.em_industry)
            .order_by(StockProfile.em_industry)
        )
        for row in result.all():
            items.append(IndustryTreeItem(
                name=row[0],
                level=1,
                stock_count=row[1],
                has_children=False,
            ))
    else:
        # SW hierarchical structure
        if level == 1:
            # Get all L1 industries
            result = await db.execute(
                select(
                    StockProfile.sw_industry_l1,
                    func.count().label("count")
                )
                .where(StockProfile.sw_industry_l1.isnot(None))
                .group_by(StockProfile.sw_industry_l1)
                .order_by(StockProfile.sw_industry_l1)
            )
            for row in result.all():
                # Check if this L1 has L2 children
                has_l2 = await db.execute(
                    select(func.count())
                    .where(StockProfile.sw_industry_l1 == row[0])
                    .where(StockProfile.sw_industry_l2.isnot(None))
                )
                has_children = (has_l2.scalar() or 0) > 0
                items.append(IndustryTreeItem(
                    name=row[0],
                    level=1,
                    stock_count=row[1],
                    has_children=has_children,
                ))
        elif level == 2 and parent:
            # Get L2 industries under the given L1
            result = await db.execute(
                select(
                    StockProfile.sw_industry_l2,
                    func.count().label("count")
                )
                .where(StockProfile.sw_industry_l1 == parent)
                .where(StockProfile.sw_industry_l2.isnot(None))
                .group_by(StockProfile.sw_industry_l2)
                .order_by(StockProfile.sw_industry_l2)
            )
            for row in result.all():
                # Check if this L2 has L3 children
                has_l3 = await db.execute(
                    select(func.count())
                    .where(StockProfile.sw_industry_l2 == row[0])
                    .where(StockProfile.sw_industry_l3.isnot(None))
                )
                has_children = (has_l3.scalar() or 0) > 0
                items.append(IndustryTreeItem(
                    name=row[0],
                    level=2,
                    stock_count=row[1],
                    has_children=has_children,
                ))
        elif level == 3 and parent:
            # Get L3 industries under the given L2
            result = await db.execute(
                select(
                    StockProfile.sw_industry_l3,
                    func.count().label("count")
                )
                .where(StockProfile.sw_industry_l2 == parent)
                .where(StockProfile.sw_industry_l3.isnot(None))
                .group_by(StockProfile.sw_industry_l3)
                .order_by(StockProfile.sw_industry_l3)
            )
            for row in result.all():
                items.append(IndustryTreeItem(
                    name=row[0],
                    level=3,
                    stock_count=row[1],
                    has_children=False,
                ))

    return IndustryTreeResponse(
        system=system,
        parent=parent,
        items=items,
    )


def _get_enum_value(val):
    """Safely get enum value or return string directly."""
    if val is None:
        return None
    return val.value if hasattr(val, 'value') else val


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
        board=_get_enum_value(structural.board) if structural else None,
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

        # Style factors (with defensive None checks)
        size_factor=StyleFactorDetail(
            value=style.market_cap if style else None,
            rank=style.size_rank if style else None,
            percentile=style.size_percentile if style else None,
            category=_get_enum_value(style.size_category) if style else None,
        ) if style else None,
        vol_factor=StyleFactorDetail(
            value=style.volatility_20d if style else None,
            rank=style.vol_rank if style else None,
            percentile=style.vol_percentile if style else None,
            category=_get_enum_value(style.vol_category) if style else None,
        ) if style else None,
        value_factor=StyleFactorDetail(
            value=style.ep_ratio if style else None,
            rank=style.value_rank if style else None,
            percentile=style.value_percentile if style else None,
            category=_get_enum_value(style.value_category) if style else None,
        ) if style else None,
        turnover_factor=StyleFactorDetail(
            value=style.avg_turnover_20d if style else None,
            rank=style.turnover_rank if style else None,
            percentile=style.turnover_percentile if style else None,
            category=_get_enum_value(style.turnover_category) if style else None,
        ) if style else None,
        momentum_20d=style.momentum_20d if style else None,
        momentum_60d=style.momentum_60d if style else None,

        # Microstructure (with defensive None checks)
        fund_holding_ratio=micro.fund_holding_ratio if micro else None,
        northbound_holding_ratio=micro.northbound_holding_ratio if micro else None,
        is_institutional=bool(micro.is_institutional) if micro else False,
        is_northbound_heavy=bool(micro.is_northbound_heavy) if micro else False,
        is_retail_hot=bool(micro.is_retail_hot) if micro else False,

        # Flags (with defensive None checks)
        is_st=bool(structural.is_st) if structural else False,
        is_new=bool(structural.is_new) if structural else False,

        recent_klines=recent_klines,
    )

    return response
