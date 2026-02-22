import datetime
import math
from typing import List, Optional, Dict, Any
from decimal import Decimal

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType, MarketDaily
from app.db.models.profile import StockProfile

router = APIRouter()

# ============================================
# Constants & Mappings
# ============================================

SW_INDUSTRY_NAME_TO_INDEX = {
    "农林牧渔": "sw.801010",
    "基础化工": "sw.801030",
    "钢铁": "sw.801040",
    "有色金属": "sw.801050",
    "电子": "sw.801080",
    "家用电器": "sw.801110",
    "食品饮料": "sw.801120",
    "纺织服饰": "sw.801130",
    "轻工制造": "sw.801140",
    "医药生物": "sw.801150",
    "公用事业": "sw.801160",
    "交通运输": "sw.801170",
    "房地产": "sw.801180",
    "商贸零售": "sw.801200",
    "社会服务": "sw.801210",
    "综合": "sw.801230",
    "建筑材料": "sw.801710",
    "建筑装饰": "sw.801720",
    "电力设备": "sw.801730",
    "国防军工": "sw.801740",
    "计算机": "sw.801750",
    "传媒": "sw.801760",
    "通信": "sw.801770",
    "银行": "sw.801780",
    "非银金融": "sw.801790",
    "汽车": "sw.801880",
    "机械设备": "sw.801890",
    "煤炭": "sw.801950",
    "石油石化": "sw.801960",
    "环保": "sw.801970",
    "美容护理": "sw.801980",
}

SW_INDEX_TO_NAME = {v: k for k, v in SW_INDUSTRY_NAME_TO_INDEX.items()}

BASE_INDEX_CODE = "sh.000001"  # 上证综指 - always loaded
BASE_INDEX_NAME = "上证综指"
MIN_OBSERVATIONS = 30  # minimum trading days for any calculation

# ============================================
# Pydantic Models
# ============================================


class StockSearchItem(BaseModel):
    code: str
    name: str
    asset_type: str
    exchange: str
    sw_industry_l1: Optional[str] = None


class StockSearchResponse(BaseModel):
    items: List[StockSearchItem]


class IndustryIndexInfo(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    is_available: bool = False


class StockContextResponse(BaseModel):
    stock_code: str
    stock_name: str
    sw_industry_l1: Optional[str] = None
    industry_index: IndustryIndexInfo


class SeriesData(BaseModel):
    dates: List[str]  # ISO date strings
    close: List[Optional[float]]


class OHLCData(BaseModel):
    dates: List[str]
    open: List[Optional[float]]
    high: List[Optional[float]]
    low: List[Optional[float]]
    close: List[Optional[float]]


class PearsonMetrics(BaseModel):
    stock_vs_base: Optional[float] = None
    stock_vs_industry: Optional[float] = None
    industry_vs_base: Optional[float] = None
    n: int = 0


class RollingCorrData(BaseModel):
    window: int
    dates: List[str]
    stock_vs_base: List[Optional[float]]
    stock_vs_industry: List[Optional[float]]
    industry_vs_base: List[Optional[float]]


class RegressionResult(BaseModel):
    alpha: float
    beta: float
    r_squared: float
    residual_std: float
    n: int


class RegressionMetrics(BaseModel):
    stock_on_base: Optional[RegressionResult] = None
    stock_on_industry: Optional[RegressionResult] = None


class ResidualData(BaseModel):
    dates: List[str]
    stock_on_base: List[Optional[float]]
    stock_on_industry: List[Optional[float]]


class ResidualVolatility(BaseModel):
    daily: float
    annualized: float


class ResidualVolMetrics(BaseModel):
    stock_on_base: Optional[ResidualVolatility] = None
    stock_on_industry: Optional[ResidualVolatility] = None


class ReturnSeries(BaseModel):
    dates: List[str]
    base: List[Optional[float]]
    industry: List[Optional[float]]
    stock: List[Optional[float]]


class AnalysisBundleItem(BaseModel):
    stock_code: str
    stock_name: str
    industry_l1_name: Optional[str] = None
    industry_index_code: Optional[str] = None
    industry_index_available: bool = False

    series_base: SeriesData
    series_industry: Optional[SeriesData] = None
    series_stock: SeriesData
    ohlc_stock: Optional[OHLCData] = None

    returns: ReturnSeries
    pearson: PearsonMetrics
    rolling_corr: RollingCorrData
    regression: RegressionMetrics
    residuals: ResidualData
    residual_vol: ResidualVolMetrics

    warnings: List[str] = []


class AnalysisBundleRequest(BaseModel):
    stock_codes: List[str]
    start_date: str
    end_date: str
    rolling_window: int = 60
    include_ohlc: bool = True


class AnalysisBundleResponse(BaseModel):
    base_index_code: str
    base_index_name: str
    items: List[AnalysisBundleItem]


# ============================================
# Helper Functions
# ============================================


async def _fetch_series(
    db: AsyncSession, code: str, start_date: datetime.date, end_date: datetime.date
):
    """Fetch price series. Returns (dates, opens, highs, lows, closes) all as lists."""
    result = await db.execute(
        select(
            MarketDaily.date, MarketDaily.open, MarketDaily.high, MarketDaily.low, MarketDaily.close
        )
        .where(MarketDaily.code == code)
        .where(MarketDaily.date >= start_date)
        .where(MarketDaily.date <= end_date)
        .where(MarketDaily.close.isnot(None))
        .order_by(MarketDaily.date)
    )
    rows = result.all()
    if not rows:
        return [], [], [], [], []
    dates = [r.date for r in rows]
    opens = [float(r.open) if r.open is not None else None for r in rows]
    highs = [float(r.high) if r.high is not None else None for r in rows]
    lows = [float(r.low) if r.low is not None else None for r in rows]
    closes = [float(r.close) for r in rows]
    return dates, opens, highs, lows, closes


def _rolling_corr(x, y, window):
    result = [None] * (window - 1)
    for i in range(window, len(x) + 1):
        c = np.corrcoef(x[i - window : i], y[i - window : i])[0, 1]
        result.append(round(float(c), 6) if not np.isnan(c) else None)
    return result


def _ols(x, y):
    # x = independent (e.g. base returns), y = dependent (stock returns)
    n = len(x)
    if n < 2:
        return None

    x_mean, y_mean = np.mean(x), np.mean(y)
    var_x = np.var(x, ddof=1)
    if var_x == 0:
        return None
    cov_xy = np.cov(x, y, ddof=1)[0, 1]
    beta = cov_xy / var_x
    alpha = y_mean - beta * x_mean
    residuals = y - (alpha + beta * x)
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    resid_std = float(np.std(residuals, ddof=2))
    return alpha, beta, r_squared, resid_std, residuals, n


# ============================================
# Endpoints
# ============================================


@router.get("/search", response_model=StockSearchResponse)
async def search_stocks(
    q: str = Query("", min_length=0),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Search stocks by code or name. Returns popular stocks when q is empty."""
    base_query = (
        select(AssetMeta, StockProfile.sw_industry_l1)
        .outerjoin(StockProfile, AssetMeta.code == StockProfile.code)
        .where(AssetMeta.asset_type == AssetType.STOCK)
    )

    if q.strip():
        query = base_query.where(
            or_(
                AssetMeta.code.ilike(f"%{q}%"),
                AssetMeta.name.ilike(f"%{q}%"),
            )
        ).limit(limit)
    else:
        # Return well-known stocks across diverse industries
        popular_codes = [
            "sh.600879",
            "sh.600519",
            "sz.000858",  # 白酒: 茅台, 五粮液
            "sz.000001",
            "sh.601318",  # 金融: 平安银行, 中国平安
            "sz.300750",
            "sh.601012",  # 新能源: 宁德时代, 隆基绿能
            "sh.600036",
            "sh.601166",  # 银行: 招商银行, 兴业银行
            "sz.000333",
            "sh.600887",  # 消费: 美的集团, 伊利股份
            "sh.601899",
            "sh.600900",  # 资源: 紫金矿业, 长江电力
            "sz.002415",
            "sh.603259",  # 科技: 海康威视, 药明康德
            "sz.300059",
            "sz.002475",  # 传媒/电子: 东方财富, 立讯精密
        ]
        query = base_query.where(AssetMeta.code.in_(popular_codes)).limit(limit)

    result = await db.execute(query)
    rows = result.all()

    items = []
    for asset, industry in rows:
        items.append(
            StockSearchItem(
                code=asset.code,
                name=asset.name,
                asset_type=asset.asset_type
                if isinstance(asset.asset_type, str)
                else asset.asset_type.value,
                exchange=asset.exchange,
                sw_industry_l1=industry,
            )
        )

    return StockSearchResponse(items=items)


@router.get("/context/{stock_code}", response_model=StockContextResponse)
async def get_stock_context(
    stock_code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get stock context including industry index info."""
    query = (
        select(AssetMeta, StockProfile.sw_industry_l1)
        .outerjoin(StockProfile, AssetMeta.code == StockProfile.code)
        .where(AssetMeta.code == stock_code)
    )
    result = await db.execute(query)
    row = result.first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stock {stock_code} not found",
        )

    asset, industry = row

    industry_index_code = None
    industry_index_name = None
    is_available = False

    if industry and industry in SW_INDUSTRY_NAME_TO_INDEX:
        industry_index_code = SW_INDUSTRY_NAME_TO_INDEX[industry]
        industry_index_name = industry

        # Check if index data exists
        idx_check = await db.execute(
            select(func.count())
            .select_from(MarketDaily)
            .where(MarketDaily.code == industry_index_code)
        )
        count = idx_check.scalar()
        is_available = count > 0

    return StockContextResponse(
        stock_code=asset.code,
        stock_name=asset.name,
        sw_industry_l1=industry,
        industry_index=IndustryIndexInfo(
            code=industry_index_code,
            name=industry_index_name,
            is_available=is_available,
        ),
    )


@router.post("/analysis/bundle", response_model=AnalysisBundleResponse)
async def analyze_bundle(
    request: AnalysisBundleRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Perform comprehensive market research analysis on a bundle of stocks.
    Calculates correlations, beta, residuals against base index and industry index.
    """
    try:
        start_date = datetime.date.fromisoformat(request.start_date)
        end_date = datetime.date.fromisoformat(request.end_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use ISO format (YYYY-MM-DD)",
        )

    # 1. Fetch Base Index Data
    base_dates, _, _, _, base_closes = await _fetch_series(
        db, BASE_INDEX_CODE, start_date, end_date
    )
    if len(base_closes) < MIN_OBSERVATIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Insufficient data for base index {BASE_INDEX_CODE}",
        )

    # Create date map for base index
    base_map = {d: c for d, c in zip(base_dates, base_closes)}

    items = []

    for stock_code in request.stock_codes:
        warnings = []

        # 2. Get Stock Info & Industry
        stock_ctx = await get_stock_context(stock_code, db)
        industry_code = stock_ctx.industry_index.code
        industry_avail = stock_ctx.industry_index.is_available

        # 3. Fetch Stock Data
        s_dates, s_opens, s_highs, s_lows, s_closes = await _fetch_series(
            db, stock_code, start_date, end_date
        )
        if len(s_closes) < MIN_OBSERVATIONS:
            warnings.append(f"Insufficient data for stock {stock_code} (n={len(s_closes)})")
            # Create empty item with warnings
            items.append(
                AnalysisBundleItem(
                    stock_code=stock_code,
                    stock_name=stock_ctx.stock_name,
                    industry_l1_name=stock_ctx.sw_industry_l1,
                    industry_index_code=industry_code,
                    industry_index_available=industry_avail,
                    series_base=SeriesData(
                        dates=[d.isoformat() for d in base_dates], close=base_closes
                    ),
                    series_stock=SeriesData(dates=[], close=[]),
                    returns=ReturnSeries(dates=[], base=[], industry=[], stock=[]),
                    pearson=PearsonMetrics(),
                    rolling_corr=RollingCorrData(
                        window=request.rolling_window,
                        dates=[],
                        stock_vs_base=[],
                        stock_vs_industry=[],
                        industry_vs_base=[],
                    ),
                    regression=RegressionMetrics(),
                    residuals=ResidualData(dates=[], stock_on_base=[], stock_on_industry=[]),
                    residual_vol=ResidualVolMetrics(),
                    warnings=warnings,
                )
            )
            continue

        s_map = {d: c for d, c in zip(s_dates, s_closes)}

        # 4. Fetch Industry Data (if available)
        i_map = {}
        i_dates, i_closes = [], []
        if industry_avail and industry_code:
            i_dates, _, _, _, i_closes = await _fetch_series(
                db, industry_code, start_date, end_date
            )
            i_map = {d: c for d, c in zip(i_dates, i_closes)}

        # 5. Align Data (Intersection)
        common_dates = set(base_map.keys()) & set(s_map.keys())
        if industry_avail and i_map:
            common_dates &= set(i_map.keys())

        sorted_dates = sorted(list(common_dates))

        if len(sorted_dates) < MIN_OBSERVATIONS:
            warnings.append(f"Insufficient overlapping data (n={len(sorted_dates)})")
            # Return partial data
            items.append(
                AnalysisBundleItem(
                    stock_code=stock_code,
                    stock_name=stock_ctx.stock_name,
                    industry_l1_name=stock_ctx.sw_industry_l1,
                    industry_index_code=industry_code,
                    industry_index_available=industry_avail,
                    series_base=SeriesData(
                        dates=[d.isoformat() for d in base_dates], close=base_closes
                    ),
                    series_stock=SeriesData(dates=[d.isoformat() for d in s_dates], close=s_closes),
                    returns=ReturnSeries(dates=[], base=[], industry=[], stock=[]),
                    pearson=PearsonMetrics(),
                    rolling_corr=RollingCorrData(
                        window=request.rolling_window,
                        dates=[],
                        stock_vs_base=[],
                        stock_vs_industry=[],
                        industry_vs_base=[],
                    ),
                    regression=RegressionMetrics(),
                    residuals=ResidualData(dates=[], stock_on_base=[], stock_on_industry=[]),
                    residual_vol=ResidualVolMetrics(),
                    warnings=warnings,
                )
            )
            continue

        # Aligned arrays
        aligned_base = np.array([base_map[d] for d in sorted_dates])
        aligned_stock = np.array([s_map[d] for d in sorted_dates])
        aligned_industry = (
            np.array([i_map[d] for d in sorted_dates]) if industry_avail and i_map else None
        )

        # 6. Compute Returns
        # Log returns: ln(P_t) - ln(P_{t-1})
        ret_base = np.diff(np.log(aligned_base))
        ret_stock = np.diff(np.log(aligned_stock))
        ret_industry = np.diff(np.log(aligned_industry)) if aligned_industry is not None else None

        ret_dates = sorted_dates[1:]
        ret_dates_iso = [d.isoformat() for d in ret_dates]

        # 7. Pearson Correlation
        pearson = PearsonMetrics(n=len(ret_dates))

        # Stock vs Base
        c_sb = np.corrcoef(ret_stock, ret_base)[0, 1]
        if not np.isnan(c_sb):
            pearson.stock_vs_base = round(float(c_sb), 6)

        # Stock vs Industry
        if ret_industry is not None:
            c_si = np.corrcoef(ret_stock, ret_industry)[0, 1]
            if not np.isnan(c_si):
                pearson.stock_vs_industry = round(float(c_si), 6)

            # Industry vs Base
            c_ib = np.corrcoef(ret_industry, ret_base)[0, 1]
            if not np.isnan(c_ib):
                pearson.industry_vs_base = round(float(c_ib), 6)

        # 8. Rolling Correlation
        roll_sb = _rolling_corr(ret_stock, ret_base, request.rolling_window)
        roll_si = (
            _rolling_corr(ret_stock, ret_industry, request.rolling_window)
            if ret_industry is not None
            else [None] * len(ret_dates)
        )
        roll_ib = (
            _rolling_corr(ret_industry, ret_base, request.rolling_window)
            if ret_industry is not None
            else [None] * len(ret_dates)
        )

        # 9. Regression (OLS)
        reg_metrics = RegressionMetrics()
        res_data = ResidualData(dates=ret_dates_iso, stock_on_base=[], stock_on_industry=[])
        res_vol = ResidualVolMetrics()

        # Stock on Base
        ols_sb = _ols(ret_base, ret_stock)
        if ols_sb:
            alpha, beta, r2, resid_std, residuals, n = ols_sb
            reg_metrics.stock_on_base = RegressionResult(
                alpha=round(alpha, 6),
                beta=round(beta, 6),
                r_squared=round(r2, 6),
                residual_std=round(resid_std, 6),
                n=n,
            )
            res_data.stock_on_base = [round(float(r), 6) for r in residuals]
            res_vol.stock_on_base = ResidualVolatility(
                daily=round(resid_std, 6), annualized=round(resid_std * math.sqrt(252), 6)
            )
        else:
            res_data.stock_on_base = [None] * len(ret_dates)

        # Stock on Industry
        if ret_industry is not None:
            ols_si = _ols(ret_industry, ret_stock)
            if ols_si:
                alpha, beta, r2, resid_std, residuals, n = ols_si
                reg_metrics.stock_on_industry = RegressionResult(
                    alpha=round(alpha, 6),
                    beta=round(beta, 6),
                    r_squared=round(r2, 6),
                    residual_std=round(resid_std, 6),
                    n=n,
                )
                res_data.stock_on_industry = [round(float(r), 6) for r in residuals]
                res_vol.stock_on_industry = ResidualVolatility(
                    daily=round(resid_std, 6), annualized=round(resid_std * math.sqrt(252), 6)
                )
            else:
                res_data.stock_on_industry = [None] * len(ret_dates)
        else:
            res_data.stock_on_industry = [None] * len(ret_dates)

        # 10. Assemble Item
        item = AnalysisBundleItem(
            stock_code=stock_code,
            stock_name=stock_ctx.stock_name,
            industry_l1_name=stock_ctx.sw_industry_l1,
            industry_index_code=industry_code,
            industry_index_available=industry_avail,
            series_base=SeriesData(dates=[d.isoformat() for d in base_dates], close=base_closes),
            series_stock=SeriesData(dates=[d.isoformat() for d in s_dates], close=s_closes),
            series_industry=SeriesData(dates=[d.isoformat() for d in i_dates], close=i_closes)
            if industry_avail
            else None,
            ohlc_stock=OHLCData(
                dates=[d.isoformat() for d in s_dates],
                open=s_opens,
                high=s_highs,
                low=s_lows,
                close=s_closes,
            )
            if request.include_ohlc
            else None,
            returns=ReturnSeries(
                dates=ret_dates_iso,
                base=[round(float(r), 6) for r in ret_base],
                stock=[round(float(r), 6) for r in ret_stock],
                industry=[round(float(r), 6) for r in ret_industry]
                if ret_industry is not None
                else [None] * len(ret_dates),
            ),
            pearson=pearson,
            rolling_corr=RollingCorrData(
                window=request.rolling_window,
                dates=ret_dates_iso,
                stock_vs_base=roll_sb,
                stock_vs_industry=roll_si,
                industry_vs_base=roll_ib,
            ),
            regression=reg_metrics,
            residuals=res_data,
            residual_vol=res_vol,
            warnings=warnings,
        )
        items.append(item)

    return AnalysisBundleResponse(
        base_index_code=BASE_INDEX_CODE, base_index_name=BASE_INDEX_NAME, items=items
    )
