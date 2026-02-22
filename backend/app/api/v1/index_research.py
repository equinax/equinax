import datetime
from typing import List, Optional, Dict, Any
import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
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
BASE_INDEX_CODE = "sh.000001"  # 上证综指

# ============================================
# Pydantic Models
# ============================================


class IndexResearchParams(BaseModel):
    start_date: str
    end_date: str
    window: int = 20


class MarketCoherenceSeries(BaseModel):
    dates: List[str]
    index_returns: List[Optional[float]]
    dispersion_std: List[Optional[float]]
    dispersion_mad: List[Optional[float]]
    ici: List[Optional[float]]
    stock_count: List[int]


class IndustryInfo(BaseModel):
    code: str
    name: str


class SCIData(BaseModel):
    industries: List[IndustryInfo]
    dates: List[str]
    matrix: List[List[Optional[float]]]  # [industry_idx][date_idx]
    dispersion_matrix: List[List[Optional[float]]]  # [industry_idx][date_idx]


class CoherenceSummary(BaseModel):
    latest_dispersion_std: float
    latest_dispersion_mad: float
    latest_ici: Optional[float]
    avg_dispersion_std: float
    market_regime: str


class IndexResearchResponse(BaseModel):
    params: IndexResearchParams
    market: MarketCoherenceSeries
    sci: SCIData
    summary: CoherenceSummary


# ============================================
# Helper Functions
# ============================================


def _determine_regime(ici: Optional[float], index_return: Optional[float]) -> str:
    if ici is None or index_return is None:
        return "未知"

    # index_return is percentage (e.g. 1.5 for 1.5%)
    if ici > 0.5:
        if index_return >= 0:
            return "系统性主导"
        else:
            return "系统性风险"
    elif 0.25 < ici <= 0.5:
        return "行业轮动"
    else:
        return "个股分化"


# ============================================
# Endpoints
# ============================================


@router.post("/analyze", response_model=IndexResearchResponse)
async def analyze_index_coherence(
    params: IndexResearchParams,
    db: AsyncSession = Depends(get_db),
):
    """
    Analyze market-wide coherence (ICI) and sector coherence (SCI).
    """
    try:
        start_date = datetime.date.fromisoformat(params.start_date)
        end_date = datetime.date.fromisoformat(params.end_date)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use ISO format (YYYY-MM-DD)",
        )

    # 1. Fetch Stock Profile (Code -> Industry)
    # We only care about stocks that have an industry classification
    profile_query = select(StockProfile.code, StockProfile.sw_industry_l1).where(
        StockProfile.sw_industry_l1.isnot(None)
    )
    profile_result = await db.execute(profile_query)
    stock_industry_map = {row.code: row.sw_industry_l1 for row in profile_result.all()}

    if not stock_industry_map:
        raise HTTPException(status_code=404, detail="No stock profiles found")

    valid_stocks = list(stock_industry_map.keys())

    # 2. Fetch Market Data (Stock Returns)
    # Optimization: Use JOIN to filter by valid stocks instead of passing large IN list
    stock_query = (
        select(MarketDaily.code, MarketDaily.date, MarketDaily.pct_chg)
        .join(StockProfile, MarketDaily.code == StockProfile.code)
        .where(MarketDaily.date >= start_date)
        .where(MarketDaily.date <= end_date)
        .where(StockProfile.sw_industry_l1.isnot(None))
        .where(MarketDaily.pct_chg.isnot(None))
    )
    stock_result = await db.execute(stock_query)
    stock_rows = stock_result.all()

    if not stock_rows:
        raise HTTPException(status_code=404, detail="No market data found for the given period")

    # 3. Fetch Base Index Data
    index_query = (
        select(MarketDaily.date, MarketDaily.pct_chg)
        .where(MarketDaily.code == BASE_INDEX_CODE)
        .where(MarketDaily.date >= start_date)
        .where(MarketDaily.date <= end_date)
        .order_by(MarketDaily.date)
    )
    index_result = await db.execute(index_query)
    index_rows = index_result.all()

    if not index_rows:
        raise HTTPException(status_code=404, detail=f"Base index {BASE_INDEX_CODE} data not found")

    # 4. Fetch Industry Index Data
    industry_index_codes = list(SW_INDUSTRY_NAME_TO_INDEX.values())
    ind_idx_query = (
        select(MarketDaily.code, MarketDaily.date, MarketDaily.pct_chg)
        .where(MarketDaily.code.in_(industry_index_codes))
        .where(MarketDaily.date >= start_date)
        .where(MarketDaily.date <= end_date)
    )
    ind_idx_result = await db.execute(ind_idx_query)
    ind_idx_rows = ind_idx_result.all()

    # ============================================
    # Data Processing (Pandas/Numpy)
    # ============================================

    # Convert to DataFrame: Index=Date, Columns=StockCode, Values=pct_chg
    df_stocks = pd.DataFrame(stock_rows, columns=["code", "date", "pct_chg"])
    # Ensure date is datetime for proper sorting/indexing
    df_stocks["date"] = pd.to_datetime(df_stocks["date"])

    # Pivot
    # This creates a matrix of returns. Missing values (suspended stocks) will be NaN.
    pivot_stocks = df_stocks.pivot(index="date", columns="code", values="pct_chg")

    # Base Index Series
    df_index = pd.DataFrame(index_rows, columns=["date", "pct_chg"])
    df_index["date"] = pd.to_datetime(df_index["date"])
    df_index.set_index("date", inplace=True)
    series_index = df_index["pct_chg"]

    # Industry Indices DataFrame
    df_ind_idx = pd.DataFrame(ind_idx_rows, columns=["code", "date", "pct_chg"])
    df_ind_idx["date"] = pd.to_datetime(df_ind_idx["date"])
    pivot_ind_idx = df_ind_idx.pivot(index="date", columns="code", values="pct_chg")

    # Align all data to common dates (intersection of stocks and base index)
    common_dates = pivot_stocks.index.intersection(series_index.index)
    pivot_stocks = pivot_stocks.loc[common_dates]
    series_index = series_index.loc[common_dates]
    # Reindex industry indices to match common dates (fill missing with NaN)
    pivot_ind_idx = pivot_ind_idx.reindex(common_dates)

    # Convert to float for numpy operations
    pivot_stocks = pivot_stocks.astype(float)
    series_index = series_index.astype(float)
    pivot_ind_idx = pivot_ind_idx.astype(float)

    # --------------------------------------------
    # Market Metrics (Dispersion & ICI)
    # --------------------------------------------

    # Dispersion: Cross-sectional std/mad of (Stock Return - Index Return)
    # Broadcast subtraction
    excess_returns = pivot_stocks.sub(series_index, axis=0)

    # Cross-sectional stats (per day)
    dispersion_std = excess_returns.std(axis=1)
    dispersion_mad = excess_returns.abs().mean(axis=1)
    stock_counts = excess_returns.count(axis=1)

    # ICI: Rolling correlation of stocks vs index
    # We compute rolling correlation for EACH stock against the index, then average across stocks
    # rolling_corr returns a DataFrame of same shape as pivot_stocks
    stock_corrs = pivot_stocks.rolling(window=params.window).corr(series_index)
    ici_series = stock_corrs.mean(axis=1)

    # --------------------------------------------
    # Industry Metrics (SCI & Sector Dispersion)
    # --------------------------------------------

    sci_matrix = []
    sector_disp_matrix = []
    industry_list = []

    # Pre-group stocks by industry to avoid looping through map repeatedly
    industry_to_stocks = {}
    for code in pivot_stocks.columns:
        ind = stock_industry_map.get(code)
        if ind:
            if ind not in industry_to_stocks:
                industry_to_stocks[ind] = []
            industry_to_stocks[ind].append(code)

    # Process each industry defined in constants
    # Sort by industry code for consistent order
    sorted_industries = sorted(SW_INDUSTRY_NAME_TO_INDEX.items(), key=lambda x: x[1])

    for ind_name, ind_code in sorted_industries:
        industry_list.append(IndustryInfo(code=ind_code, name=ind_name))

        # If we have data for this industry index and stocks
        if ind_code in pivot_ind_idx.columns and ind_name in industry_to_stocks:
            ind_idx_series = pivot_ind_idx[ind_code]
            stocks_in_sector = industry_to_stocks[ind_name]

            # Subset of stock returns for this sector
            sector_stock_df = pivot_stocks[stocks_in_sector]

            # SCI: Rolling corr of sector stocks vs sector index
            sector_corrs = sector_stock_df.rolling(window=params.window).corr(ind_idx_series)
            sci_val = sector_corrs.mean(axis=1)

            # Sector Dispersion: Std of (Stock - Sector Index)
            sector_excess = sector_stock_df.sub(ind_idx_series, axis=0)
            sector_disp = sector_excess.std(axis=1)

            # Replace NaN with None for JSON serialization
            sci_matrix.append([None if np.isnan(x) else round(x, 4) for x in sci_val.values])
            sector_disp_matrix.append(
                [None if np.isnan(x) else round(x, 4) for x in sector_disp.values]
            )
        else:
            # No data
            empty_row = [None] * len(common_dates)
            sci_matrix.append(empty_row)
            sector_disp_matrix.append(empty_row)

    # --------------------------------------------
    # Summary & Response Construction
    # --------------------------------------------

    # Prepare lists for response (handle NaNs)
    dates_iso = [d.strftime("%Y-%m-%d") for d in common_dates]

    # Latest values
    if len(dates_iso) > 0:
        last_idx = -1
        latest_disp_std = (
            float(dispersion_std.iloc[last_idx])
            if not np.isnan(dispersion_std.iloc[last_idx])
            else 0.0
        )
        latest_disp_mad = (
            float(dispersion_mad.iloc[last_idx])
            if not np.isnan(dispersion_mad.iloc[last_idx])
            else 0.0
        )
        latest_ici = (
            float(ici_series.iloc[last_idx]) if not np.isnan(ici_series.iloc[last_idx]) else None
        )
        latest_idx_ret = (
            float(series_index.iloc[last_idx])
            if not np.isnan(series_index.iloc[last_idx])
            else None
        )
        avg_disp_std = float(dispersion_std.mean()) if not np.isnan(dispersion_std.mean()) else 0.0

        regime = _determine_regime(latest_ici, latest_idx_ret)
    else:
        latest_disp_std = 0.0
        latest_disp_mad = 0.0
        latest_ici = None
        avg_disp_std = 0.0
        regime = "无数据"

    return IndexResearchResponse(
        params=params,
        market=MarketCoherenceSeries(
            dates=dates_iso,
            index_returns=[None if np.isnan(x) else round(x, 4) for x in series_index.values],
            dispersion_std=[None if np.isnan(x) else round(x, 4) for x in dispersion_std.values],
            dispersion_mad=[None if np.isnan(x) else round(x, 4) for x in dispersion_mad.values],
            ici=[None if np.isnan(x) else round(x, 4) for x in ici_series.values],
            stock_count=stock_counts.values.tolist(),
        ),
        sci=SCIData(
            industries=industry_list,
            dates=dates_iso,
            matrix=sci_matrix,
            dispersion_matrix=sector_disp_matrix,
        ),
        summary=CoherenceSummary(
            latest_dispersion_std=round(latest_disp_std, 4),
            latest_dispersion_mad=round(latest_disp_mad, 4),
            latest_ici=round(latest_ici, 4) if latest_ici is not None else None,
            avg_dispersion_std=round(avg_disp_std, 4),
            market_regime=regime,
        ),
    )
