import polars as pl
import pandas as pd
import numpy as np
from datetime import date

SW_INDUSTRY_NAME_TO_INDEX = {
    "农林牧渔": "sh.801010",
    "基础化工": "sh.801030",
    "钢铁": "sh.801040",
    "有色金属": "sh.801050",
    "电子": "sh.801080",
    "汽车": "sh.801880",
    "家用电器": "sh.801110",
    "食品饮料": "sh.801120",
    "纺织服饰": "sh.801130",
    "轻工制造": "sh.801140",
    "医药生物": "sh.801150",
    "公用事业": "sh.801160",
    "交通运输": "sh.801170",
    "房地产": "sh.801180",
    "商贸零售": "sh.801200",
    "社会服务": "sh.801210",
    "综合": "sh.801230",
    "建筑材料": "sh.801710",
    "建筑装饰": "sh.801720",
    "电力设备": "sh.801730",
    "国防军工": "sh.801740",
    "计算机": "sh.801750",
    "传媒": "sh.801760",
    "通信": "sh.801770",
    "银行": "sh.801780",
    "非银金融": "sh.801790",
    "煤炭": "sh.801950",
    "石油石化": "sh.801960",
    "环保": "sh.801970",
    "美容护理": "sh.801980",
    "机械设备": "sh.801890",
}
SW_INDEX_TO_NAME = {v: k for k, v in SW_INDUSTRY_NAME_TO_INDEX.items()}


def compute_ici(
    stock_pct_chg_df: pl.DataFrame,  # columns: date, code, pct_chg (ALL stocks)
    index_pct_chg_df: pl.DataFrame,  # columns: date, pct_chg (Shanghai Composite sh.000001)
    target_date: date,
    window: int = 20,
) -> float:
    """Compute ICI (Index Coherence Index) for a single date.

    Steps:
    1. Filter to last `window` trading days ending on target_date
    2. Pivot stock_pct_chg to wide format: rows=dates, cols=stock codes, values=pct_chg
    3. For each stock, compute correlation with index pct_chg over these `window` days
    4. Take the cross-sectional mean of all stock correlations
    5. Return single float (0.0 to 1.0), or 0.0 if insufficient data
    """
    # 1. Filter data
    # We need to find the dates first to ensure we have enough data
    # Assuming index_df is sorted or we sort it
    index_subset = (
        index_pct_chg_df.filter(pl.col("date") <= target_date)
        .sort("date", descending=True)
        .head(window)
    )

    if index_subset.height < window:
        return 0.0

    valid_dates = index_subset["date"].to_list()
    min_date = min(valid_dates)

    # Filter stocks to these dates
    stocks_subset = stock_pct_chg_df.filter(
        (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
    )

    if stocks_subset.is_empty():
        return 0.0

    # 2. Convert to pandas for pivot and correlation
    # We need to align dates.
    # Pivot: index=date, columns=code, values=pct_chg
    # Use to_dict to avoid pyarrow dependency
    pdf_stocks = pd.DataFrame(stocks_subset.to_dict(as_series=False))
    pdf_stocks = pdf_stocks.pivot(index="date", columns="code", values="pct_chg")

    pdf_index = pd.DataFrame(index_subset.to_dict(as_series=False))
    pdf_index = pdf_index.set_index("date")["pct_chg"]

    # Ensure alignment
    common_dates = pdf_stocks.index.intersection(pdf_index.index)
    if len(common_dates) < window * 0.8:  # Allow some missing data but not too much
        return 0.0

    pdf_stocks = pdf_stocks.loc[common_dates]
    pdf_index = pdf_index.loc[common_dates]

    # 3. Compute correlation
    # corrwith computes pairwise correlation between rows or columns of DataFrame with rows or columns of Series or DataFrame.
    # Default is axis=0 (compute for each column)
    corrs = pdf_stocks.corrwith(pdf_index, axis=0)

    # 4. Mean correlation
    ici = corrs.mean()

    if np.isnan(ici):
        return 0.0

    return float(ici)


def compute_sci(
    stock_pct_chg_df: pl.DataFrame,  # columns: date, code, pct_chg (ALL stocks)
    industry_index_pct_chg_df: pl.DataFrame,  # columns: date, code, pct_chg (31 SW L1 industry indices)
    stock_industry_map: dict[str, str],  # stock_code -> industry_index_code mapping
    target_date: date,
    window: int = 20,
) -> dict[str, float]:
    """Compute SCI for each SW L1 industry on a single date.

    Returns dict mapping sw_industry_l1_name -> SCI value.
    """
    # 1. Filter data (similar to ICI)
    # We need the dates from industry indices. Since there are multiple, let's pick one or just use the range.
    # Ideally we use the same dates as we would for ICI, but here we are comparing stocks to their industry index.
    # Let's find the dates present in industry_index_pct_chg_df

    dates_df = (
        industry_index_pct_chg_df.select("date")
        .unique()
        .filter(pl.col("date") <= target_date)
        .sort("date", descending=True)
        .head(window)
    )

    if dates_df.height < window:
        return {}

    valid_dates = dates_df["date"].to_list()
    min_date = min(valid_dates)

    # Filter dataframes
    ind_subset = industry_index_pct_chg_df.filter(
        (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
    )
    stocks_subset = stock_pct_chg_df.filter(
        (pl.col("date") >= min_date) & (pl.col("date") <= target_date)
    )

    if ind_subset.is_empty() or stocks_subset.is_empty():
        return {}

    # Convert to pandas
    # Stocks: index=date, columns=code, values=pct_chg
    pdf_stocks = pd.DataFrame(stocks_subset.to_dict(as_series=False))
    pdf_stocks = pdf_stocks.pivot(index="date", columns="code", values="pct_chg")

    # Industries: index=date, columns=code, values=pct_chg
    pdf_inds = pd.DataFrame(ind_subset.to_dict(as_series=False))
    pdf_inds = pdf_inds.pivot(index="date", columns="code", values="pct_chg")

    # Align dates
    common_dates = pdf_stocks.index.intersection(pdf_inds.index)
    if len(common_dates) < window * 0.8:
        return {}

    pdf_stocks = pdf_stocks.loc[common_dates]
    pdf_inds = pdf_inds.loc[common_dates]

    # Group stocks by industry
    industry_to_stocks = {}
    for stock_code in pdf_stocks.columns:
        ind_code = stock_industry_map.get(stock_code)
        if ind_code:
            if ind_code not in industry_to_stocks:
                industry_to_stocks[ind_code] = []
            industry_to_stocks[ind_code].append(stock_code)

    results = {}

    # Compute SCI for each industry
    for ind_code, stock_codes in industry_to_stocks.items():
        if ind_code not in pdf_inds.columns:
            continue

        # Get industry index series
        ind_series = pdf_inds[ind_code]

        # Get stocks data
        # Filter to stocks that are in our pdf_stocks columns (already checked by loop)
        sector_stocks = pdf_stocks[stock_codes]

        # Compute correlation
        corrs = sector_stocks.corrwith(ind_series, axis=0)

        # Mean correlation
        sci = corrs.mean()

        if not np.isnan(sci):
            # Map back to industry name
            ind_name = SW_INDEX_TO_NAME.get(ind_code)
            if ind_name:
                results[ind_name] = float(sci)

    return results


def compute_dispersion(
    stock_pct_chg_df: pl.DataFrame,  # columns: date, code, pct_chg (ALL stocks)
    index_pct_chg_df: pl.DataFrame,  # columns: date, pct_chg (Shanghai Composite)
    target_date: date,
) -> float:
    """Compute cross-sectional dispersion for a single date.

    Steps:
    1. Filter to target_date only
    2. Compute excess returns: stock_pct_chg - index_pct_chg for each stock
    3. Return std(excess_returns) as float
    """
    # 1. Filter
    stocks_day = stock_pct_chg_df.filter(pl.col("date") == target_date)
    index_day = index_pct_chg_df.filter(pl.col("date") == target_date)

    if stocks_day.is_empty() or index_day.is_empty():
        return 0.0

    # 2. Compute excess returns
    index_ret = index_day["pct_chg"][0]

    # We can do this in polars directly since it's just one day
    excess_returns = stocks_day["pct_chg"] - index_ret

    # 3. Return std
    dispersion = excess_returns.std()

    if dispersion is None:  # Polars returns None for std of empty or single value sometimes?
        return 0.0

    return float(dispersion)
