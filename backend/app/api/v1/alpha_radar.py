"""Alpha Radar API endpoints.

Market discovery and intelligent screening system with real-time scoring
using Polars for high-performance calculations.
"""

import datetime
from typing import List, Optional
from decimal import Decimal
from enum import Enum

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db


router = APIRouter()


# ============================================
# Enums
# ============================================

class TimeMode(str, Enum):
    """Time mode for data analysis."""
    SNAPSHOT = "snapshot"  # Single date (default: latest trading day)
    PERIOD = "period"      # Date range for period analysis


class ScreenerTab(str, Enum):
    """Screener tab types with different scoring strategies."""
    PANORAMA = "panorama"           # 全景综合
    SMART_ACCUMULATION = "smart"    # 聪明钱吸筹
    DEEP_VALUE = "value"            # 深度价值
    SUPER_TREND = "trend"           # 趋势共振


class MarketRegimeType(str, Enum):
    """Market regime classification."""
    BULL = "BULL"
    BEAR = "BEAR"
    RANGE = "RANGE"


class SectorMetric(str, Enum):
    """Metric type for sector heatmap coloring."""
    CHANGE = "change"           # 涨跌幅
    AMOUNT = "amount"           # 成交额
    MAIN_STRENGTH = "main_strength"  # 主力强度
    SCORE = "score"             # 综合评分


class ValuationLevel(str, Enum):
    """Valuation level based on historical percentile."""
    LOW = "LOW"           # 0-25%
    MEDIUM = "MEDIUM"     # 25-75%
    HIGH = "HIGH"         # 75-90%
    EXTREME = "EXTREME"   # 90-100%


# ============================================
# Time Controller Schemas
# ============================================

class CalendarDayInfo(BaseModel):
    """Calendar day information for heatmap display."""
    date: datetime.date
    is_trading_day: bool
    market_change: Optional[float] = Field(default=None, description="上证指数涨跌幅 (%)")


class TimeControllerRequest(BaseModel):
    """Request schema for time controller."""
    mode: TimeMode = TimeMode.SNAPSHOT
    date: Optional[datetime.date] = Field(default=None, description="Target date for snapshot mode")
    start_date: Optional[datetime.date] = Field(default=None, description="Start date for period mode")
    end_date: Optional[datetime.date] = Field(default=None, description="End date for period mode")


class TimeControllerResponse(BaseModel):
    """Response schema for time controller with resolved dates."""
    mode: TimeMode
    resolved_date: Optional[datetime.date] = None        # For snapshot mode
    resolved_start_date: Optional[datetime.date] = None  # For period mode
    resolved_end_date: Optional[datetime.date] = None    # For period mode
    trading_days_count: int = 0
    earliest_available_date: datetime.date
    latest_available_date: datetime.date


# ============================================
# Dashboard Schemas
# ============================================

class MarketStateInfo(BaseModel):
    """Market state (regime) information."""
    regime: MarketRegimeType
    regime_score: Decimal = Field(description="Score from -100 (extreme bear) to +100 (extreme bull)")
    regime_description: str = Field(description="Human-readable description like '强势上涨'")


class MarketBreadthInfo(BaseModel):
    """Market breadth metrics."""
    up_count: int = Field(description="Number of stocks up today")
    down_count: int = Field(description="Number of stocks down today")
    flat_count: int = Field(description="Number of stocks unchanged")
    up_down_ratio: Decimal = Field(description="Ratio of up to down stocks")
    above_ma20_ratio: Decimal = Field(description="Percentage of stocks above 20-day MA")
    limit_up_count: int = Field(default=0, description="Number of stocks at limit up")
    limit_down_count: int = Field(default=0, description="Number of stocks at limit down")


class StyleRotationInfo(BaseModel):
    """Style rotation metrics (large value vs small growth)."""
    large_value_strength: Decimal = Field(description="Large value relative strength (0-100)")
    small_growth_strength: Decimal = Field(description="Small growth relative strength (0-100)")
    dominant_style: str = Field(description="'large_value', 'small_growth', or 'balanced'")
    rotation_signal: str = Field(description="'switching_to_value', 'switching_to_growth', or 'stable'")


class SmartMoneyInfo(BaseModel):
    """Smart money proxy metrics (based on turnover and volume patterns)."""
    market_avg_volume_ratio: Decimal = Field(description="Market average 5-day volume ratio")
    high_turnover_count: int = Field(description="Number of stocks with high turnover")
    accumulation_signal_count: int = Field(description="Stocks showing accumulation signals")
    distribution_signal_count: int = Field(description="Stocks showing distribution signals")
    money_flow_proxy: str = Field(description="'inflow', 'outflow', or 'neutral'")


class DashboardResponse(BaseModel):
    """Dashboard response with all 4 regime cards."""
    time_mode: TimeMode
    date: Optional[datetime.date] = None
    start_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None

    market_state: MarketStateInfo
    market_breadth: MarketBreadthInfo
    style_rotation: StyleRotationInfo
    smart_money: SmartMoneyInfo


# ============================================
# Screener Schemas
# ============================================

class QuantLabel(str, Enum):
    """Quantitative labels for stocks."""
    MAIN_ACCUMULATION = "main_accumulation"  # 主力吸筹
    UNDERVALUED = "undervalued"              # 低估
    OVERSOLD = "oversold"                    # 超跌
    HIGH_VOLATILITY = "high_volatility"      # 高波
    BREAKOUT = "breakout"                    # 突破
    VOLUME_SURGE = "volume_surge"            # 放量


class ScreenerItem(BaseModel):
    """Single item in screener results."""
    code: str
    name: str
    asset_type: str = Field(description="'stock', 'etf', or 'index'")

    # Price data
    price: Optional[Decimal] = None
    change_pct: Optional[Decimal] = Field(default=None, description="Daily change % (snapshot mode)")

    # Scores (0-100)
    composite_score: Decimal = Field(description="Tab-specific composite score (0-100)")

    # Quantitative labels
    quant_labels: List[str] = Field(default_factory=list, description="Active quant labels")

    # Main strength proxy (替代主力强度)
    main_strength_proxy: Optional[Decimal] = Field(default=None, description="Main force strength proxy (0-100)")

    # Valuation
    valuation_level: Optional[str] = Field(default=None, description="LOW/MEDIUM/HIGH/EXTREME")
    valuation_percentile: Optional[Decimal] = Field(default=None, description="Current PE percentile (0-100)")

    # Classification
    size_category: Optional[str] = None
    industry_l1: Optional[str] = None

    # Period mode metrics (only populated in period mode)
    period_return: Optional[Decimal] = Field(default=None, description="Period return % (period mode)")
    max_drawdown: Optional[Decimal] = Field(default=None, description="Max drawdown % (period mode)")
    avg_turnover: Optional[Decimal] = Field(default=None, description="Average turnover (period mode)")

    class Config:
        from_attributes = True


class ScreenerResponse(BaseModel):
    """Paginated screener response."""
    items: List[ScreenerItem]
    total: int
    page: int
    page_size: int
    pages: int
    tab: ScreenerTab
    time_mode: TimeMode
    date: Optional[datetime.date] = None
    start_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None


# ============================================
# Sector Heatmap Schemas
# ============================================

class SectorL2Item(BaseModel):
    """L2 (二级行业) item in sector heatmap."""
    name: str = Field(description="L2 industry name")
    stock_count: int = Field(description="Number of stocks in this sector")
    value: Optional[Decimal] = Field(default=None, description="Value for color mapping")
    size_value: Optional[Decimal] = Field(default=None, description="Value for area sizing (typically amount)")
    avg_change_pct: Optional[Decimal] = Field(default=None, description="Average change percentage")
    total_amount: Optional[Decimal] = Field(default=None, description="Total trading amount")
    avg_main_strength: Optional[Decimal] = Field(default=None, description="Average main strength proxy")
    avg_score: Optional[Decimal] = Field(default=None, description="Average panorama score")
    up_count: int = Field(default=0, description="Number of stocks up")
    down_count: int = Field(default=0, description="Number of stocks down")
    flat_count: int = Field(default=0, description="Number of stocks flat")


class SectorL1Item(BaseModel):
    """L1 (一级行业) item in sector heatmap with nested L2 children."""
    name: str = Field(description="L1 industry name")
    stock_count: int = Field(description="Total stocks in this sector")
    value: Optional[Decimal] = Field(default=None, description="Value for color mapping")
    size_value: Optional[Decimal] = Field(default=None, description="Value for area sizing")
    avg_change_pct: Optional[Decimal] = Field(default=None, description="Average change percentage")
    total_amount: Optional[Decimal] = Field(default=None, description="Total trading amount")
    avg_main_strength: Optional[Decimal] = Field(default=None, description="Average main strength proxy")
    avg_score: Optional[Decimal] = Field(default=None, description="Average panorama score")
    up_count: int = Field(default=0, description="Number of stocks up")
    down_count: int = Field(default=0, description="Number of stocks down")
    flat_count: int = Field(default=0, description="Number of stocks flat")
    children: List[SectorL2Item] = Field(default_factory=list, description="L2 sub-sectors")


class SectorHeatmapResponse(BaseModel):
    """Response for sector heatmap visualization."""
    time_mode: TimeMode
    date: Optional[datetime.date] = None
    start_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None
    metric: SectorMetric
    sectors: List[SectorL1Item] = Field(description="L1 sectors with nested L2 children")
    min_value: Decimal = Field(description="Minimum value across all sectors")
    max_value: Decimal = Field(description="Maximum value across all sectors")
    market_avg: Decimal = Field(description="Market average value")


# ============================================
# Sort Options
# ============================================

class ScreenerSortField(str, Enum):
    """Sort field options for screener."""
    SCORE = "score"
    CHANGE = "change"
    VOLUME = "volume"
    VALUATION = "valuation"
    MAIN_STRENGTH = "main_strength"
    CODE = "code"


class SortOrder(str, Enum):
    """Sort order options."""
    ASC = "asc"
    DESC = "desc"


# ============================================
# API Endpoints
# ============================================

@router.post("/time-controller", response_model=TimeControllerResponse)
async def resolve_time_controller(
    request: TimeControllerRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Resolve and validate time parameters.

    Returns available date range and resolves requested dates to actual trading days.
    """
    from sqlalchemy import text

    # Get earliest and latest available dates
    result = await db.execute(
        text("SELECT MIN(date), MAX(date) FROM market_daily")
    )
    row = result.fetchone()
    earliest_date = row[0] if row and row[0] else datetime.date.today()
    latest_date = row[1] if row and row[1] else datetime.date.today()

    if request.mode == TimeMode.SNAPSHOT:
        # Resolve snapshot date
        resolved_date = request.date or latest_date
        trading_days = 1

        return TimeControllerResponse(
            mode=request.mode,
            resolved_date=resolved_date,
            trading_days_count=trading_days,
            earliest_available_date=earliest_date,
            latest_available_date=latest_date,
        )
    else:
        # Period mode
        resolved_start = request.start_date or earliest_date
        resolved_end = request.end_date or latest_date

        # Count trading days in period
        count_result = await db.execute(
            text("""
                SELECT COUNT(DISTINCT date) FROM market_daily
                WHERE date >= :start_date AND date <= :end_date
            """),
            {"start_date": resolved_start, "end_date": resolved_end}
        )
        trading_days = count_result.scalar() or 0

        return TimeControllerResponse(
            mode=request.mode,
            resolved_start_date=resolved_start,
            resolved_end_date=resolved_end,
            trading_days_count=trading_days,
            earliest_available_date=earliest_date,
            latest_available_date=latest_date,
        )


@router.get("/calendar", response_model=List[CalendarDayInfo])
async def get_calendar(
    start_date: datetime.date = Query(..., description="Start date for calendar range"),
    end_date: datetime.date = Query(..., description="End date for calendar range"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get calendar data for date heatmap display.

    Returns each day in the range with:
    - is_trading_day: Whether the date is a trading day
    - market_change: 上证指数涨跌幅 (%) for trading days, None for non-trading days
    """
    from sqlalchemy import text
    from datetime import timedelta

    # Query 上证指数 (sh.000001) data for the date range
    result = await db.execute(
        text("""
            SELECT date, pct_chg
            FROM market_daily
            WHERE code = 'sh.000001'
              AND date >= :start_date
              AND date <= :end_date
            ORDER BY date
        """),
        {"start_date": start_date, "end_date": end_date}
    )
    trading_data = {row[0]: row[1] for row in result.fetchall()}

    # Generate all dates in range and mark trading days
    calendar_days = []
    current_date = start_date
    while current_date <= end_date:
        if current_date in trading_data:
            calendar_days.append(CalendarDayInfo(
                date=current_date,
                is_trading_day=True,
                market_change=float(trading_data[current_date]) if trading_data[current_date] is not None else None,
            ))
        else:
            calendar_days.append(CalendarDayInfo(
                date=current_date,
                is_trading_day=False,
                market_change=None,
            ))
        current_date += timedelta(days=1)

    return calendar_days


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    mode: TimeMode = Query(default=TimeMode.SNAPSHOT),
    date: Optional[datetime.date] = Query(default=None, description="Target date for snapshot"),
    start_date: Optional[datetime.date] = Query(default=None, description="Start date for period"),
    end_date: Optional[datetime.date] = Query(default=None, description="End date for period"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get market regime dashboard with 4 key metrics.

    - Market State: Bull/Bear/Range with regime score
    - Market Breadth: Up/down ratio, MA20 breadth
    - Style Rotation: Large value vs small growth strength
    - Smart Money: Proxy indicators based on turnover/volume patterns
    """
    from app.services.alpha_radar.dashboard_service import DashboardService

    service = DashboardService(db)
    data = await service.get_full_dashboard(
        target_date=date,
        start_date=start_date,
        end_date=end_date,
    )

    return DashboardResponse(
        time_mode=mode,
        date=data["date"] if mode == TimeMode.SNAPSHOT else None,
        start_date=start_date if mode == TimeMode.PERIOD else None,
        end_date=end_date if mode == TimeMode.PERIOD else None,
        market_state=MarketStateInfo(
            regime=MarketRegimeType(data["market_state"]["regime"]),
            regime_score=data["market_state"]["regime_score"],
            regime_description=data["market_state"]["regime_description"],
        ),
        market_breadth=MarketBreadthInfo(
            up_count=data["market_breadth"]["up_count"],
            down_count=data["market_breadth"]["down_count"],
            flat_count=data["market_breadth"]["flat_count"],
            up_down_ratio=data["market_breadth"]["up_down_ratio"],
            above_ma20_ratio=data["market_breadth"]["above_ma20_ratio"],
            limit_up_count=data["market_breadth"]["limit_up_count"],
            limit_down_count=data["market_breadth"]["limit_down_count"],
        ),
        style_rotation=StyleRotationInfo(
            large_value_strength=data["style_rotation"]["large_value_strength"],
            small_growth_strength=data["style_rotation"]["small_growth_strength"],
            dominant_style=data["style_rotation"]["dominant_style"],
            rotation_signal=data["style_rotation"]["rotation_signal"],
        ),
        smart_money=SmartMoneyInfo(
            market_avg_volume_ratio=data["smart_money"]["market_avg_volume_ratio"],
            high_turnover_count=data["smart_money"]["high_turnover_count"],
            accumulation_signal_count=data["smart_money"]["accumulation_signal_count"],
            distribution_signal_count=data["smart_money"]["distribution_signal_count"],
            money_flow_proxy=data["smart_money"]["money_flow_proxy"],
        ),
    )


@router.get("/screener", response_model=ScreenerResponse)
async def get_screener(
    # Tab selection
    tab: ScreenerTab = Query(default=ScreenerTab.PANORAMA),

    # Time parameters
    mode: TimeMode = Query(default=TimeMode.SNAPSHOT),
    date: Optional[datetime.date] = Query(default=None, description="Target date for snapshot"),
    start_date: Optional[datetime.date] = Query(default=None, description="Start date for period"),
    end_date: Optional[datetime.date] = Query(default=None, description="End date for period"),

    # Filters (reuse from universe.py pattern)
    board: Optional[str] = Query(default=None, description="Board filter: MAIN,GEM,STAR,BSE"),
    size_category: Optional[str] = Query(default=None, description="Size: MEGA,LARGE,MID,SMALL,MICRO"),
    industry_l1: Optional[str] = Query(default=None, description="Industry L1 filter (SW)"),
    min_score: Optional[float] = Query(default=None, ge=0, le=100, description="Minimum score filter"),

    # Pagination
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),

    # Sorting
    sort_by: ScreenerSortField = Query(default=ScreenerSortField.SCORE),
    sort_order: SortOrder = Query(default=SortOrder.DESC),

    db: AsyncSession = Depends(get_db),
):
    """
    Get intelligent screener results with composite scoring.

    Tabs:
    - panorama: Comprehensive score (momentum + value + quality + smart money + technical)
    - smart: Smart accumulation (main strength + volume pattern + price position)
    - value: Deep value (valuation rank + quality + stability + dividend)
    - trend: Super trend (momentum + breakout + volume confirm + trend strength)

    In period mode, returns period_return, max_drawdown, avg_turnover instead of daily metrics.
    """
    from app.services.alpha_radar.screener_service import ScreenerService

    service = ScreenerService(db)

    # Build filters dict
    filters = {}
    if board:
        filters["board"] = board
    if size_category:
        filters["size_category"] = size_category
    if industry_l1:
        filters["industry_l1"] = industry_l1
    if min_score is not None:
        filters["min_score"] = min_score

    result = await service.get_screener_results(
        tab=tab.value,
        mode=mode.value,
        target_date=date,
        start_date=start_date,
        end_date=end_date,
        filters=filters,
        page=page,
        page_size=page_size,
        sort_by=sort_by.value,
        sort_order=sort_order.value,
    )

    # Convert items to ScreenerItem models
    items = [
        ScreenerItem(
            code=item["code"],
            name=item["name"],
            asset_type=item["asset_type"],
            price=item.get("price"),
            change_pct=item.get("change_pct"),
            composite_score=item["composite_score"] or Decimal("50"),
            quant_labels=item.get("quant_labels", []),
            main_strength_proxy=item.get("main_strength_proxy"),
            valuation_level=item.get("valuation_level"),
            valuation_percentile=item.get("valuation_percentile"),
            size_category=item.get("size_category"),
            industry_l1=item.get("industry_l1"),
            period_return=item.get("period_return"),
            max_drawdown=item.get("max_drawdown"),
            avg_turnover=item.get("avg_turnover"),
        )
        for item in result["items"]
    ]

    return ScreenerResponse(
        items=items,
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
        pages=result["pages"],
        tab=ScreenerTab(result["tab"]),
        time_mode=TimeMode(result["time_mode"]),
        date=result.get("date"),
        start_date=result.get("start_date"),
        end_date=result.get("end_date"),
    )


@router.get("/sector-heatmap", response_model=SectorHeatmapResponse)
async def get_sector_heatmap(
    # Metric selection
    metric: SectorMetric = Query(default=SectorMetric.CHANGE, description="Metric for color mapping"),

    # Time parameters
    mode: TimeMode = Query(default=TimeMode.SNAPSHOT),
    date: Optional[datetime.date] = Query(default=None, description="Target date for snapshot"),
    start_date: Optional[datetime.date] = Query(default=None, description="Start date for period"),
    end_date: Optional[datetime.date] = Query(default=None, description="End date for period"),

    db: AsyncSession = Depends(get_db),
):
    """
    Get sector heatmap data aggregated by industry (SW申万 L1/L2).

    Metrics:
    - change: Average price change percentage (涨跌幅)
    - amount: Total trading amount (成交额)
    - main_strength: Average main strength proxy (主力强度)
    - score: Average panorama score (综合评分)

    Returns nested structure: L1 sectors containing L2 sub-sectors.
    Each sector includes value for color mapping and size_value for area sizing.
    """
    from app.services.alpha_radar.sector_heatmap_service import SectorHeatmapService

    service = SectorHeatmapService(db)
    result = await service.get_sector_heatmap(
        metric=metric.value,
        mode=mode.value,
        target_date=date,
        start_date=start_date,
        end_date=end_date,
    )

    # Convert sectors to response models
    sectors = [
        SectorL1Item(
            name=s["name"],
            stock_count=s["stock_count"],
            value=s.get("value"),
            size_value=s.get("size_value"),
            avg_change_pct=s.get("avg_change_pct"),
            total_amount=s.get("total_amount"),
            avg_main_strength=s.get("avg_main_strength"),
            avg_score=s.get("avg_score"),
            up_count=s.get("up_count", 0),
            down_count=s.get("down_count", 0),
            flat_count=s.get("flat_count", 0),
            children=[
                SectorL2Item(
                    name=c["name"],
                    stock_count=c["stock_count"],
                    value=c.get("value"),
                    size_value=c.get("size_value"),
                    avg_change_pct=c.get("avg_change_pct"),
                    total_amount=c.get("total_amount"),
                    avg_main_strength=c.get("avg_main_strength"),
                    avg_score=c.get("avg_score"),
                    up_count=c.get("up_count", 0),
                    down_count=c.get("down_count", 0),
                    flat_count=c.get("flat_count", 0),
                )
                for c in s.get("children", [])
            ],
        )
        for s in result.get("sectors", [])
    ]

    return SectorHeatmapResponse(
        time_mode=TimeMode(result["time_mode"]),
        date=result.get("date"),
        start_date=result.get("start_date"),
        end_date=result.get("end_date"),
        metric=SectorMetric(result["metric"]),
        sectors=sectors,
        min_value=result["min_value"] or Decimal("0"),
        max_value=result["max_value"] or Decimal("0"),
        market_avg=result["market_avg"] or Decimal("0"),
    )
