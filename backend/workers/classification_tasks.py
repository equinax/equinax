"""Classification calculation tasks for ARQ.

Implements the 4+1 classification system:
1. Structural Classification - Board type, price limits, ST status
2. Industry Classification - Shenwan industry (via external import)
3. Style Factors - Size, volatility, turnover, valuation, momentum
4. Microstructure - Institutional holdings (stub for future data)
+1. Market Regime - Bull/Bear/Range market conditions
"""

from typing import Dict, Any, Optional, List
from datetime import date, timedelta
from decimal import Decimal
import logging
import math

import pandas as pd
import numpy as np
from sqlalchemy import select, func, text, and_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.db.models.asset import AssetMeta, MarketDaily, IndicatorValuation, AssetType
from app.db.models.classification import (
    StockStructuralInfo,
    StockStyleExposure,
    MarketRegime,
    StockClassificationSnapshot,
    BoardType,
    StructuralType,
    SizeCategory,
    VolatilityCategory,
    TurnoverCategory,
    ValueCategory,
    MarketRegimeType,
)

logger = logging.getLogger(__name__)


def get_session_maker(database_url: str = None):
    """Get async session maker for the given database URL.

    If database_url is not provided, uses settings.database_url.
    """
    url = database_url or settings.database_url
    engine = create_async_engine(
        url,
        pool_size=5,
        max_overflow=5,
    )
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


# Default session maker using settings (for worker/ARQ usage)
worker_session_maker = get_session_maker()


# =============================================================================
# Helper Functions
# =============================================================================


def determine_board_type(code: str) -> BoardType:
    """Determine board type from stock code."""
    # Extract numeric part: sh.600000 -> 600000
    if "." in code:
        code_num = code.split(".")[1]
    else:
        code_num = code

    if code_num.startswith("688"):
        return BoardType.STAR  # 科创板
    elif code_num.startswith("30"):
        return BoardType.GEM  # 创业板
    elif code_num.startswith("8") or code_num.startswith("4"):
        return BoardType.BSE  # 北交所
    else:
        return BoardType.MAIN  # 主板 (60, 00)


def get_price_limit(board: BoardType, is_st: bool) -> tuple[Decimal, Decimal]:
    """Get price limit up/down based on board and ST status."""
    if is_st:
        return Decimal("5"), Decimal("5")
    elif board == BoardType.GEM or board == BoardType.STAR:
        return Decimal("20"), Decimal("20")
    elif board == BoardType.BSE:
        return Decimal("30"), Decimal("30")
    else:
        return Decimal("10"), Decimal("10")


def categorize_size(
    market_cap: Optional[float], percentile: Optional[float]
) -> Optional[SizeCategory]:
    """Categorize stock by market cap.

    Note: market_cap is in 亿元 units (from indicator_valuation.total_mv).
    """
    if market_cap is None:
        return None
    # market_cap is in 亿元 units
    if market_cap >= 1000:  # >= 1000亿
        return SizeCategory.MEGA
    elif market_cap >= 200:  # >= 200亿
        return SizeCategory.LARGE
    elif market_cap >= 50:  # >= 50亿
        return SizeCategory.MID
    elif market_cap >= 10:  # >= 10亿
        return SizeCategory.SMALL
    else:
        return SizeCategory.MICRO


def categorize_volatility(percentile: Optional[float]) -> Optional[VolatilityCategory]:
    """Categorize stock by volatility percentile."""
    if percentile is None:
        return None
    if percentile >= 0.7:
        return VolatilityCategory.HIGH
    elif percentile <= 0.3:
        return VolatilityCategory.LOW
    else:
        return VolatilityCategory.NORMAL


def categorize_turnover(percentile: Optional[float]) -> Optional[TurnoverCategory]:
    """Categorize stock by turnover percentile."""
    if percentile is None:
        return None
    if percentile >= 0.7:
        return TurnoverCategory.HOT
    elif percentile <= 0.3:
        return TurnoverCategory.DEAD
    else:
        return TurnoverCategory.NORMAL


def categorize_value(percentile: Optional[float]) -> Optional[ValueCategory]:
    """Categorize stock by value factor percentile."""
    if percentile is None:
        return None
    if percentile >= 0.7:  # High E/P = Value
        return ValueCategory.VALUE
    elif percentile <= 0.3:  # Low E/P = Growth
        return ValueCategory.GROWTH
    else:
        return ValueCategory.NEUTRAL


def determine_market_regime(
    up_ratio: float,
    avg_pct_chg: float,
    total_amount: float,
    sh_above_ma20: bool,
    sh_above_ma60: bool,
) -> tuple[MarketRegimeType, Decimal]:
    """Determine market regime based on multiple signals."""
    score = 0

    # Up ratio signal
    if up_ratio > 0.6:
        score += 30
    elif up_ratio < 0.4:
        score -= 30

    # Average change signal
    if avg_pct_chg > 1:
        score += 20
    elif avg_pct_chg < -1:
        score -= 20

    # MA signals
    if sh_above_ma20:
        score += 15
    else:
        score -= 15

    if sh_above_ma60:
        score += 15
    else:
        score -= 15

    # Determine regime
    if score >= 30:
        regime = MarketRegimeType.BULL
    elif score <= -30:
        regime = MarketRegimeType.BEAR
    else:
        regime = MarketRegimeType.RANGE

    return regime, Decimal(str(score))


# =============================================================================
# Task 1: Structural Classification
# =============================================================================


async def calculate_structural_classification(
    ctx: dict,
    ref_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate structural classification for all stocks.

    This includes board type, price limits, ST status, and new stock status.
    Low frequency update - only when status changes.
    """
    calc_date = date.fromisoformat(ref_date) if ref_date else date.today()
    new_threshold = calc_date - timedelta(days=60)  # 60 days for new stock

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        # Get all stocks
        query = select(AssetMeta).where(AssetMeta.asset_type == AssetType.STOCK)
        result = await db.execute(query)
        stocks = result.scalars().all()

        # Get latest ST status from indicator_valuation
        st_query = text("""
            SELECT DISTINCT ON (code) code, is_st
            FROM indicator_valuation
            WHERE date <= :calc_date
            ORDER BY code, date DESC
        """)
        st_result = await db.execute(st_query, {"calc_date": calc_date})
        st_map = {row.code: bool(row.is_st) for row in st_result}

        # Create/update structural info
        records_updated = 0
        for stock in stocks:
            board = determine_board_type(stock.code)
            is_st = st_map.get(stock.code, False)
            is_new = stock.list_date and stock.list_date > new_threshold

            # Determine structural type
            if is_st:
                structural_type = StructuralType.ST
            elif is_new:
                structural_type = StructuralType.NEW
            else:
                structural_type = StructuralType.NORMAL

            price_up, price_down = get_price_limit(board, is_st)

            # Upsert
            stmt = (
                insert(StockStructuralInfo)
                .values(
                    code=stock.code,
                    board=board.value,
                    structural_type=structural_type.value,
                    price_limit_up=price_up,
                    price_limit_down=price_down,
                    is_st=is_st,
                    is_new=is_new,
                    is_suspended=False,
                    list_date=stock.list_date,
                )
                .on_conflict_do_update(
                    index_elements=["code"],
                    set_={
                        "board": board.value,
                        "structural_type": structural_type.value,
                        "price_limit_up": price_up,
                        "price_limit_down": price_down,
                        "is_st": is_st,
                        "is_new": is_new,
                    },
                )
            )
            await db.execute(stmt)
            records_updated += 1

        await db.commit()

        return {
            "task": "calculate_structural_classification",
            "ref_date": str(calc_date),
            "records_updated": records_updated,
        }


# =============================================================================
# Task 2: Style Factors
# =============================================================================


async def calculate_style_factors(
    ctx: dict,
    calc_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate style factor exposures for all stocks.

    Includes: size, volatility, turnover, value, momentum factors.
    Daily update.
    """
    import time as _time

    t0 = _time.monotonic()
    target_date = date.fromisoformat(calc_date) if calc_date else date.today()

    if target_date.weekday() >= 5:
        return {
            "task": "calculate_style_factors",
            "skipped": True,
            "reason": f"{target_date} is a weekend",
        }

    lookback_60d = target_date - timedelta(days=90)  # ~60 trading days

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        has_market_data = await db.scalar(
            select(func.count())
            .select_from(MarketDaily)
            .where(MarketDaily.date == target_date)
            .limit(1)
        )
        if not has_market_data:
            return {
                "task": "calculate_style_factors",
                "skipped": True,
                "reason": f"No market data for {target_date} (likely a holiday)",
            }

        stocks_query = select(AssetMeta.code).where(
            AssetMeta.asset_type == AssetType.STOCK, AssetMeta.status == 1
        )
        stocks_result = await db.execute(stocks_query)
        stock_codes = [row[0] for row in stocks_result]

        if not stock_codes:
            return {"task": "calculate_style_factors", "error": "No stocks found"}

        market_query = (
            select(
                MarketDaily.code,
                MarketDaily.date,
                MarketDaily.close,
                MarketDaily.pct_chg,
            )
            .where(
                MarketDaily.code.in_(stock_codes),
                MarketDaily.date >= lookback_60d,
                MarketDaily.date <= target_date,
            )
            .order_by(MarketDaily.code, MarketDaily.date)
        )

        turnover_query = select(
            IndicatorValuation.code,
            IndicatorValuation.date,
            IndicatorValuation.turnover_rate,
        ).where(
            IndicatorValuation.code.in_(stock_codes),
            IndicatorValuation.date >= lookback_60d,
            IndicatorValuation.date <= target_date,
        )

        valuation_query = select(
            IndicatorValuation.code,
            IndicatorValuation.total_mv,
            IndicatorValuation.pe_ttm,
            IndicatorValuation.pb_mrq,
        ).where(
            IndicatorValuation.code.in_(stock_codes),
            IndicatorValuation.date == target_date,
        )

        market_result = await db.execute(market_query)
        turnover_result = await db.execute(turnover_query)
        valuation_result = await db.execute(valuation_query)
        market_records = market_result.fetchall()
        turnover_records = turnover_result.fetchall()
        val_records = valuation_result.fetchall()

        t_query = _time.monotonic()

        df = pd.DataFrame(market_records, columns=["code", "date", "close", "pct_chg"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")

        if df.empty:
            return {"task": "calculate_style_factors", "error": "No market data"}

        turn_df = pd.DataFrame(turnover_records, columns=["code", "date", "turn"])
        turn_df["turn"] = pd.to_numeric(turn_df["turn"], errors="coerce")
        if not turn_df.empty:
            df = df.merge(turn_df, on=["code", "date"], how="left")
        else:
            df["turn"] = np.nan

        val_df = pd.DataFrame(val_records, columns=["code", "market_cap", "pe_ttm", "pb_mrq"])
        for c in ["market_cap", "pe_ttm", "pb_mrq"]:
            val_df[c] = pd.to_numeric(val_df[c], errors="coerce")

        group_sizes = df.groupby("code", sort=False).size()
        valid_codes = group_sizes[group_sizes >= 10].index
        df = df[df["code"].isin(valid_codes)]

        if df.empty:
            return {"task": "calculate_style_factors", "error": "No style data calculated"}

        grouped = df.groupby("code", sort=False)

        df["vol_20d"] = grouped["pct_chg"].transform(lambda x: x.rolling(20, min_periods=10).std())

        df["avg_turn_20d"] = grouped["turn"].transform(
            lambda x: x.rolling(20, min_periods=10).mean()
        )

        close_20ago = grouped["close"].shift(19)  # shift(19) matches iloc[-20] relative to iloc[-1]
        close_60ago = grouped["close"].shift(59)
        df["momentum_20d"] = (df["close"] / close_20ago - 1.0) * 100.0
        df["momentum_60d"] = (df["close"] / close_60ago - 1.0) * 100.0

        day_df = df[df["date"] == target_date].copy()

        if day_df.empty:
            return {"task": "calculate_style_factors", "error": "No style data for target date"}

        if not val_df.empty:
            day_df = day_df.merge(val_df, on="code", how="left")
        else:
            day_df["market_cap"] = np.nan
            day_df["pe_ttm"] = np.nan
            day_df["pb_mrq"] = np.nan

        day_df["ep_ratio"] = np.where(
            (day_df["pe_ttm"].notna()) & (day_df["pe_ttm"] > 0),
            1.0 / day_df["pe_ttm"],
            np.nan,
        )
        day_df["bp_ratio"] = np.where(
            (day_df["pb_mrq"].notna()) & (day_df["pb_mrq"] > 0),
            1.0 / day_df["pb_mrq"],
            np.nan,
        )

        style_df = day_df[
            [
                "code",
                "market_cap",
                "vol_20d",
                "avg_turn_20d",
                "ep_ratio",
                "bp_ratio",
                "momentum_20d",
                "momentum_60d",
            ]
        ].copy()
        style_df.rename(
            columns={
                "vol_20d": "volatility_20d",
                "avg_turn_20d": "avg_turnover_20d",
            },
            inplace=True,
        )
        style_df["date"] = target_date

        # --- Ranking (already vectorized) ---
        for col, rank_col, pct_col in [
            ("market_cap", "size_rank", "size_percentile"),
            ("volatility_20d", "vol_rank", "vol_percentile"),
            ("avg_turnover_20d", "turnover_rank", "turnover_percentile"),
            ("ep_ratio", "value_rank", "value_percentile"),
            ("momentum_20d", "momentum_rank", "momentum_percentile"),
        ]:
            style_df[rank_col] = style_df[col].rank(ascending=False, method="min")
            style_df[pct_col] = style_df[col].rank(pct=True)

        # --- Categorization (vectorized via np.select) ---
        # Size category (based on market_cap thresholds)
        mc = style_df["market_cap"]
        style_df["size_category"] = np.select(
            [mc >= 1000, mc >= 200, mc >= 50, mc >= 10, mc > 0],
            [
                SizeCategory.MEGA.value,
                SizeCategory.LARGE.value,
                SizeCategory.MID.value,
                SizeCategory.SMALL.value,
                SizeCategory.MICRO.value,
            ],
            default=None,
        )
        style_df.loc[style_df["market_cap"].isna(), "size_category"] = None

        # Vol/Turnover/Value categories (based on percentile thresholds)
        for pct_col, cat_col, high_val, mid_val, low_val in [
            (
                "vol_percentile",
                "vol_category",
                VolatilityCategory.HIGH.value,
                VolatilityCategory.NORMAL.value,
                VolatilityCategory.LOW.value,
            ),
            (
                "turnover_percentile",
                "turnover_category",
                TurnoverCategory.HOT.value,
                TurnoverCategory.NORMAL.value,
                TurnoverCategory.DEAD.value,
            ),
            (
                "value_percentile",
                "value_category",
                ValueCategory.VALUE.value,
                ValueCategory.NEUTRAL.value,
                ValueCategory.GROWTH.value,
            ),
        ]:
            pct = style_df[pct_col]
            style_df[cat_col] = np.select(
                [pct >= 0.7, pct <= 0.3],
                [high_val, low_val],
                default=mid_val,
            )
            style_df.loc[style_df[pct_col].isna(), cat_col] = None

        t_compute = _time.monotonic()

        # --- Bulk upsert (chunked at 500 rows to stay within PG param limits) ---
        CHUNK_SIZE = 500
        # Prepare records: convert NaN to None, floats to Decimal where needed
        decimal_cols = [
            "market_cap",
            "size_percentile",
            "volatility_20d",
            "vol_percentile",
            "avg_turnover_20d",
            "turnover_percentile",
            "ep_ratio",
            "bp_ratio",
            "value_percentile",
            "momentum_20d",
            "momentum_60d",
            "momentum_percentile",
        ]
        int_cols = ["size_rank", "vol_rank", "turnover_rank", "value_rank", "momentum_rank"]

        all_cols = (
            ["code", "date"]
            + decimal_cols
            + int_cols
            + ["size_category", "vol_category", "turnover_category", "value_category"]
        )
        update_cols = [c for c in all_cols if c not in ("code", "date")]

        for c in decimal_cols:
            style_df[c] = style_df[c].apply(
                lambda v: Decimal(str(round(float(v), 4))) if pd.notna(v) else None
            )
        for c in int_cols:
            style_df[c] = style_df[c].apply(lambda v: int(v) if pd.notna(v) else None)
        for c in ["size_category", "vol_category", "turnover_category", "value_category"]:
            style_df[c] = style_df[c].where(style_df[c].notna(), None)

        records = style_df[all_cols].to_dict("records")

        # to_dict("records") leaks NaN for nullable columns (numeric and category);
        # asyncpg rejects NaN for both integer and string parameters.
        _nan_cols = (
            int_cols
            + decimal_cols
            + ["size_category", "vol_category", "turnover_category", "value_category"]
        )
        for rec in records:
            for c in _nan_cols:
                v = rec[c]
                if v is not None and isinstance(v, float) and math.isnan(v):
                    rec[c] = None

        records_inserted = 0
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i : i + CHUNK_SIZE]
            stmt = insert(StockStyleExposure).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["code", "date"],
                set_={col: stmt.excluded[col] for col in update_cols},
            )
            await db.execute(stmt)
            records_inserted += len(chunk)

        await db.commit()

        t_end = _time.monotonic()
        logger.info(
            f"calculate_style_factors({target_date}): "
            f"{records_inserted} records, "
            f"query={t_query - t0:.1f}s, "
            f"compute={t_compute - t_query:.1f}s, "
            f"upsert={t_end - t_compute:.1f}s, "
            f"total={t_end - t0:.1f}s"
        )

        return {
            "task": "calculate_style_factors",
            "calc_date": str(target_date),
            "records_inserted": records_inserted,
            "records_count": records_inserted,
            "duration_seconds": round(t_end - t0, 1),
        }


# =============================================================================
# Task 3: Market Regime
# =============================================================================


async def calculate_market_regime(
    ctx: dict,
    calc_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate market regime (bull/bear/range) for a given date.

    Uses market breadth, average returns, and index MA signals.
    """
    target_date = date.fromisoformat(calc_date) if calc_date else date.today()

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        # Get market statistics for the day
        stats_query = text("""
            SELECT
                COUNT(*) as total_stocks,
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as up_count,
                SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) as down_count,
                SUM(CASE WHEN pct_chg >= 9.9 THEN 1 ELSE 0 END) as limit_up_count,
                SUM(CASE WHEN pct_chg <= -9.9 THEN 1 ELSE 0 END) as limit_down_count,
                SUM(amount) as total_amount,
                AVG(iv.turnover_rate) as avg_turnover,
                AVG(pct_chg) as avg_pct_chg
            FROM market_daily md
            JOIN asset_meta am ON md.code = am.code
            LEFT JOIN indicator_valuation iv ON md.code = iv.code AND md.date = iv.date
            WHERE md.date = :calc_date
            AND am.asset_type = 'STOCK'
        """)

        stats_result = await db.execute(stats_query, {"calc_date": target_date})
        stats = stats_result.fetchone()

        if not stats or not stats.total_stocks:
            return {"task": "calculate_market_regime", "error": "No market data for date"}

        total_stocks = stats.total_stocks
        up_count = stats.up_count or 0
        down_count = stats.down_count or 0
        limit_up_count = stats.limit_up_count or 0
        limit_down_count = stats.limit_down_count or 0
        total_amount = stats.total_amount or Decimal("0")
        avg_turnover = stats.avg_turnover
        avg_pct_chg = float(stats.avg_pct_chg) if stats.avg_pct_chg else 0

        up_ratio = up_count / total_stocks if total_stocks > 0 else 0.5

        # Get index data (stub - would need actual index data)
        # For now, use simple heuristics
        sh_above_ma20 = avg_pct_chg > 0  # Simplified
        sh_above_ma60 = up_ratio > 0.5  # Simplified

        regime, score = determine_market_regime(
            up_ratio, avg_pct_chg, float(total_amount), sh_above_ma20, sh_above_ma60
        )

        # Insert into database
        stmt = (
            insert(MarketRegime)
            .values(
                date=target_date,
                regime=regime.value,
                regime_score=score,
                total_stocks=total_stocks,
                up_count=up_count,
                down_count=down_count,
                limit_up_count=limit_up_count,
                limit_down_count=limit_down_count,
                total_amount=total_amount,
                avg_turnover=Decimal(str(avg_turnover)) if avg_turnover else None,
                sh_above_ma20=sh_above_ma20,
                sh_above_ma60=sh_above_ma60,
            )
            .on_conflict_do_update(
                index_elements=["date"],
                set_={
                    "regime": regime.value,
                    "regime_score": score,
                    "total_stocks": total_stocks,
                    "up_count": up_count,
                    "down_count": down_count,
                },
            )
        )
        await db.execute(stmt)
        await db.commit()

        return {
            "task": "calculate_market_regime",
            "calc_date": str(target_date),
            "regime": regime.value,
            "score": float(score),
            "up_ratio": round(up_ratio, 4),
        }


# =============================================================================
# Task 4: Classification Snapshot
# =============================================================================


async def generate_classification_snapshot(
    ctx: dict,
    calc_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate unified classification snapshot by joining all dimension tables.
    """
    target_date = date.fromisoformat(calc_date) if calc_date else date.today()

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        # Join all classification data
        # Note: is_retail_hot and is_main_controlled are placeholders for future microstructure analysis
        # Currently we just set them to false since that analysis is not yet implemented
        snapshot_query = text("""
            INSERT INTO stock_classification_snapshot (
                code, date,
                board, structural_type, is_st, is_new,
                industry_l1, industry_l2, industry_l3,
                size_category, vol_category, turnover_category, value_category,
                is_retail_hot, is_main_controlled,
                market_regime,
                classification_tags
            )
            SELECT
                am.code,
                :calc_date as date,
                ssi.board,
                ssi.structural_type,
                COALESCE(ssi.is_st, false),
                COALESCE(ssi.is_new, false),
                sp.sw_industry_l1,
                sp.sw_industry_l2,
                sp.sw_industry_l3,
                sse.size_category,
                sse.vol_category,
                sse.turnover_category,
                sse.value_category,
                false,  -- is_retail_hot: placeholder, not yet implemented
                false,  -- is_main_controlled: placeholder, not yet implemented
                mr.regime,
                jsonb_build_object(
                    'board', ssi.board,
                    'structural_type', ssi.structural_type,
                    'industry_l1', sp.sw_industry_l1,
                    'size_category', sse.size_category,
                    'vol_category', sse.vol_category,
                    'market_regime', mr.regime
                )
            FROM asset_meta am
            LEFT JOIN stock_structural_info ssi ON am.code = ssi.code
            LEFT JOIN stock_profile sp ON am.code = sp.code
            LEFT JOIN stock_style_exposure sse ON am.code = sse.code AND sse.date = :calc_date
            LEFT JOIN market_regime mr ON mr.date = :calc_date
            WHERE am.asset_type = 'STOCK' AND am.status = 1
            ON CONFLICT (code, date) DO UPDATE SET
                board = EXCLUDED.board,
                structural_type = EXCLUDED.structural_type,
                is_st = EXCLUDED.is_st,
                is_new = EXCLUDED.is_new,
                industry_l1 = EXCLUDED.industry_l1,
                industry_l2 = EXCLUDED.industry_l2,
                industry_l3 = EXCLUDED.industry_l3,
                size_category = EXCLUDED.size_category,
                vol_category = EXCLUDED.vol_category,
                turnover_category = EXCLUDED.turnover_category,
                value_category = EXCLUDED.value_category,
                is_retail_hot = EXCLUDED.is_retail_hot,
                is_main_controlled = EXCLUDED.is_main_controlled,
                market_regime = EXCLUDED.market_regime,
                classification_tags = EXCLUDED.classification_tags
        """)

        result = await db.execute(snapshot_query, {"calc_date": target_date})
        await db.commit()

        # Count records
        count_query = (
            select(func.count())
            .select_from(StockClassificationSnapshot)
            .where(StockClassificationSnapshot.date == target_date)
        )
        count_result = await db.execute(count_query)
        record_count = count_result.scalar()

        return {
            "task": "generate_classification_snapshot",
            "calc_date": str(target_date),
            "records_generated": record_count,
        }


# =============================================================================
# Main Daily Update Task
# =============================================================================


async def daily_classification_update(
    ctx: dict,
    calc_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run all classification updates for a given date.

    This is the main task to run daily after market data is updated.
    """
    target_date = calc_date or str(date.today())

    results = {}

    # 1. Structural classification (low frequency, can skip if recent)
    try:
        results["structural"] = await calculate_structural_classification(ctx, target_date)
    except Exception as e:
        logger.error(f"Structural classification failed: {e}")
        results["structural"] = {"error": str(e)}

    # 2. Style factors (daily)
    try:
        results["style_factors"] = await calculate_style_factors(ctx, target_date)
    except Exception as e:
        logger.error(f"Style factors calculation failed: {e}")
        results["style_factors"] = {"error": str(e)}

    # 3. Market regime (daily)
    try:
        results["market_regime"] = await calculate_market_regime(ctx, target_date)
    except Exception as e:
        logger.error(f"Market regime calculation failed: {e}")
        results["market_regime"] = {"error": str(e)}

    # 4. Generate snapshot (daily, after all other calculations)
    try:
        results["snapshot"] = await generate_classification_snapshot(ctx, target_date)
    except Exception as e:
        logger.error(f"Snapshot generation failed: {e}")
        results["snapshot"] = {"error": str(e)}

    return {
        "task": "daily_classification_update",
        "calc_date": target_date,
        "results": results,
    }
