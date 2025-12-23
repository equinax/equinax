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

import pandas as pd
import numpy as np
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.db.models.asset import AssetMeta, MarketDaily, IndicatorValuation, AssetType
from app.db.models.classification import (
    StockStructuralInfo,
    StockStyleExposure,
    StockMicrostructure,
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
    if '.' in code:
        code_num = code.split('.')[1]
    else:
        code_num = code

    if code_num.startswith('688'):
        return BoardType.STAR  # 科创板
    elif code_num.startswith('30'):
        return BoardType.GEM  # 创业板
    elif code_num.startswith('8') or code_num.startswith('4'):
        return BoardType.BSE  # 北交所
    else:
        return BoardType.MAIN  # 主板 (60, 00)


def get_price_limit(board: BoardType, is_st: bool) -> tuple[Decimal, Decimal]:
    """Get price limit up/down based on board and ST status."""
    if is_st:
        return Decimal('5'), Decimal('5')
    elif board == BoardType.GEM or board == BoardType.STAR:
        return Decimal('20'), Decimal('20')
    elif board == BoardType.BSE:
        return Decimal('30'), Decimal('30')
    else:
        return Decimal('10'), Decimal('10')


def categorize_size(market_cap: Optional[float], percentile: Optional[float]) -> Optional[SizeCategory]:
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
    sh_above_ma60: bool
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
            stmt = insert(StockStructuralInfo).values(
                code=stock.code,
                board=board.value,
                structural_type=structural_type.value,
                price_limit_up=price_up,
                price_limit_down=price_down,
                is_st=is_st,
                is_new=is_new,
                is_suspended=False,
                list_date=stock.list_date,
            ).on_conflict_do_update(
                index_elements=['code'],
                set_={
                    'board': board.value,
                    'structural_type': structural_type.value,
                    'price_limit_up': price_up,
                    'price_limit_down': price_down,
                    'is_st': is_st,
                    'is_new': is_new,
                }
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
    target_date = date.fromisoformat(calc_date) if calc_date else date.today()
    lookback_20d = target_date - timedelta(days=30)  # ~20 trading days
    lookback_60d = target_date - timedelta(days=90)  # ~60 trading days

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        # Get stocks with data on target date
        stocks_query = select(AssetMeta.code).where(
            AssetMeta.asset_type == AssetType.STOCK,
            AssetMeta.status == 1
        )
        stocks_result = await db.execute(stocks_query)
        stock_codes = [row[0] for row in stocks_result]

        if not stock_codes:
            return {"task": "calculate_style_factors", "error": "No stocks found"}

        # Load market data for calculation
        market_query = select(
            MarketDaily.code,
            MarketDaily.date,
            MarketDaily.close,
            MarketDaily.turn,
            MarketDaily.pct_chg,
        ).where(
            MarketDaily.code.in_(stock_codes),
            MarketDaily.date >= lookback_60d,
            MarketDaily.date <= target_date,
        ).order_by(MarketDaily.code, MarketDaily.date)

        market_result = await db.execute(market_query)
        market_records = market_result.fetchall()

        # Load valuation data
        valuation_query = select(
            IndicatorValuation.code,
            IndicatorValuation.total_mv,
            IndicatorValuation.pe_ttm,
            IndicatorValuation.pb_mrq,
        ).where(
            IndicatorValuation.code.in_(stock_codes),
            IndicatorValuation.date == target_date,
        )
        valuation_result = await db.execute(valuation_query)
        valuation_map = {
            row.code: {
                'total_mv': float(row.total_mv) if row.total_mv else None,
                'pe_ttm': float(row.pe_ttm) if row.pe_ttm else None,
                'pb_mrq': float(row.pb_mrq) if row.pb_mrq else None,
            }
            for row in valuation_result
        }

        # Convert to DataFrame for calculation
        df = pd.DataFrame([
            {
                'code': r.code,
                'date': r.date,
                'close': float(r.close) if r.close else None,
                'turn': float(r.turn) if r.turn else None,
                'pct_chg': float(r.pct_chg) if r.pct_chg else None,
            }
            for r in market_records
        ])

        if df.empty:
            return {"task": "calculate_style_factors", "error": "No market data"}

        # Calculate factors per stock
        style_records = []
        for code in stock_codes:
            stock_df = df[df['code'] == code].copy()
            if len(stock_df) < 10:
                continue

            stock_df = stock_df.sort_values('date')
            latest = stock_df.iloc[-1]

            # Volatility: 20-day std of returns
            if len(stock_df) >= 20:
                vol_20d = stock_df['pct_chg'].tail(20).std()
            else:
                vol_20d = stock_df['pct_chg'].std()

            # Turnover: 20-day average
            if len(stock_df) >= 20:
                avg_turn_20d = stock_df['turn'].tail(20).mean()
            else:
                avg_turn_20d = stock_df['turn'].mean()

            # Momentum
            if len(stock_df) >= 20:
                momentum_20d = (stock_df['close'].iloc[-1] / stock_df['close'].iloc[-20] - 1) * 100
            else:
                momentum_20d = None

            if len(stock_df) >= 60:
                momentum_60d = (stock_df['close'].iloc[-1] / stock_df['close'].iloc[-60] - 1) * 100
            else:
                momentum_60d = None

            # Get valuation
            val = valuation_map.get(code, {})
            market_cap = val.get('total_mv')
            pe_ttm = val.get('pe_ttm')
            pb_mrq = val.get('pb_mrq')

            # EP ratio (inverse of PE)
            ep_ratio = 1 / pe_ttm if pe_ttm and pe_ttm > 0 else None
            bp_ratio = 1 / pb_mrq if pb_mrq and pb_mrq > 0 else None

            style_records.append({
                'code': code,
                'date': target_date,
                'market_cap': market_cap,
                'volatility_20d': vol_20d,
                'avg_turnover_20d': avg_turn_20d,
                'ep_ratio': ep_ratio,
                'bp_ratio': bp_ratio,
                'momentum_20d': momentum_20d,
                'momentum_60d': momentum_60d,
            })

        if not style_records:
            return {"task": "calculate_style_factors", "error": "No style data calculated"}

        # Create DataFrame for ranking
        style_df = pd.DataFrame(style_records)

        # Calculate ranks and percentiles
        for col, rank_col, pct_col in [
            ('market_cap', 'size_rank', 'size_percentile'),
            ('volatility_20d', 'vol_rank', 'vol_percentile'),
            ('avg_turnover_20d', 'turnover_rank', 'turnover_percentile'),
            ('ep_ratio', 'value_rank', 'value_percentile'),
            ('momentum_20d', 'momentum_rank', 'momentum_percentile'),
        ]:
            if col in style_df.columns:
                style_df[rank_col] = style_df[col].rank(ascending=False, method='min')
                style_df[pct_col] = style_df[col].rank(pct=True)

        # Add categories
        style_df['size_category'] = style_df.apply(
            lambda r: categorize_size(r.get('market_cap'), r.get('size_percentile')),
            axis=1
        )
        style_df['vol_category'] = style_df['vol_percentile'].apply(categorize_volatility)
        style_df['turnover_category'] = style_df['turnover_percentile'].apply(categorize_turnover)
        style_df['value_category'] = style_df['value_percentile'].apply(categorize_value)

        # Insert into database
        records_inserted = 0
        for _, row in style_df.iterrows():
            stmt = insert(StockStyleExposure).values(
                code=row['code'],
                date=row['date'],
                market_cap=Decimal(str(row['market_cap'])) if pd.notna(row.get('market_cap')) else None,
                size_rank=int(row['size_rank']) if pd.notna(row.get('size_rank')) else None,
                size_percentile=Decimal(str(row['size_percentile'])) if pd.notna(row.get('size_percentile')) else None,
                size_category=row['size_category'].value if row.get('size_category') else None,
                volatility_20d=Decimal(str(row['volatility_20d'])) if pd.notna(row.get('volatility_20d')) else None,
                vol_rank=int(row['vol_rank']) if pd.notna(row.get('vol_rank')) else None,
                vol_percentile=Decimal(str(row['vol_percentile'])) if pd.notna(row.get('vol_percentile')) else None,
                vol_category=row['vol_category'].value if row.get('vol_category') else None,
                avg_turnover_20d=Decimal(str(row['avg_turnover_20d'])) if pd.notna(row.get('avg_turnover_20d')) else None,
                turnover_rank=int(row['turnover_rank']) if pd.notna(row.get('turnover_rank')) else None,
                turnover_percentile=Decimal(str(row['turnover_percentile'])) if pd.notna(row.get('turnover_percentile')) else None,
                turnover_category=row['turnover_category'].value if row.get('turnover_category') else None,
                ep_ratio=Decimal(str(row['ep_ratio'])) if pd.notna(row.get('ep_ratio')) else None,
                bp_ratio=Decimal(str(row['bp_ratio'])) if pd.notna(row.get('bp_ratio')) else None,
                value_rank=int(row['value_rank']) if pd.notna(row.get('value_rank')) else None,
                value_percentile=Decimal(str(row['value_percentile'])) if pd.notna(row.get('value_percentile')) else None,
                value_category=row['value_category'].value if row.get('value_category') else None,
                momentum_20d=Decimal(str(row['momentum_20d'])) if pd.notna(row.get('momentum_20d')) else None,
                momentum_60d=Decimal(str(row['momentum_60d'])) if pd.notna(row.get('momentum_60d')) else None,
                momentum_rank=int(row['momentum_rank']) if pd.notna(row.get('momentum_rank')) else None,
                momentum_percentile=Decimal(str(row['momentum_percentile'])) if pd.notna(row.get('momentum_percentile')) else None,
            ).on_conflict_do_update(
                index_elements=['code', 'date'],
                set_={
                    'market_cap': Decimal(str(row['market_cap'])) if pd.notna(row.get('market_cap')) else None,
                    'size_category': row['size_category'].value if row.get('size_category') else None,
                    'volatility_20d': Decimal(str(row['volatility_20d'])) if pd.notna(row.get('volatility_20d')) else None,
                    'vol_category': row['vol_category'].value if row.get('vol_category') else None,
                }
            )
            await db.execute(stmt)
            records_inserted += 1

        await db.commit()

        return {
            "task": "calculate_style_factors",
            "calc_date": str(target_date),
            "records_inserted": records_inserted,
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
                AVG(turn) as avg_turnover,
                AVG(pct_chg) as avg_pct_chg
            FROM market_daily md
            JOIN asset_meta am ON md.code = am.code
            WHERE md.date = :calc_date
            AND am.asset_type = 'STOCK'
            AND md.trade_status = 1
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
        total_amount = stats.total_amount or Decimal('0')
        avg_turnover = stats.avg_turnover
        avg_pct_chg = float(stats.avg_pct_chg) if stats.avg_pct_chg else 0

        up_ratio = up_count / total_stocks if total_stocks > 0 else 0.5

        # Get index data (stub - would need actual index data)
        # For now, use simple heuristics
        sh_above_ma20 = avg_pct_chg > 0  # Simplified
        sh_above_ma60 = up_ratio > 0.5   # Simplified

        regime, score = determine_market_regime(
            up_ratio, avg_pct_chg, float(total_amount), sh_above_ma20, sh_above_ma60
        )

        # Insert into database
        stmt = insert(MarketRegime).values(
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
        ).on_conflict_do_update(
            index_elements=['date'],
            set_={
                'regime': regime.value,
                'regime_score': score,
                'total_stocks': total_stocks,
                'up_count': up_count,
                'down_count': down_count,
            }
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
        snapshot_query = text("""
            INSERT INTO stock_classification_snapshot (
                code, date,
                board, structural_type, is_st, is_new,
                industry_l1, industry_l2, industry_l3,
                size_category, vol_category, turnover_category, value_category,
                is_institutional, is_northbound_heavy,
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
                COALESCE(sm.is_institutional, false),
                COALESCE(sm.is_northbound_heavy, false),
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
            LEFT JOIN stock_microstructure sm ON am.code = sm.code AND sm.date = :calc_date
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
                is_institutional = EXCLUDED.is_institutional,
                is_northbound_heavy = EXCLUDED.is_northbound_heavy,
                market_regime = EXCLUDED.market_regime,
                classification_tags = EXCLUDED.classification_tags
        """)

        result = await db.execute(snapshot_query, {"calc_date": target_date})
        await db.commit()

        # Count records
        count_query = select(func.count()).select_from(StockClassificationSnapshot).where(
            StockClassificationSnapshot.date == target_date
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
        results['structural'] = await calculate_structural_classification(ctx, target_date)
    except Exception as e:
        logger.error(f"Structural classification failed: {e}")
        results['structural'] = {"error": str(e)}

    # 2. Style factors (daily)
    try:
        results['style_factors'] = await calculate_style_factors(ctx, target_date)
    except Exception as e:
        logger.error(f"Style factors calculation failed: {e}")
        results['style_factors'] = {"error": str(e)}

    # 3. Market regime (daily)
    try:
        results['market_regime'] = await calculate_market_regime(ctx, target_date)
    except Exception as e:
        logger.error(f"Market regime calculation failed: {e}")
        results['market_regime'] = {"error": str(e)}

    # 4. Generate snapshot (daily, after all other calculations)
    try:
        results['snapshot'] = await generate_classification_snapshot(ctx, target_date)
    except Exception as e:
        logger.error(f"Snapshot generation failed: {e}")
        results['snapshot'] = {"error": str(e)}

    return {
        "task": "daily_classification_update",
        "calc_date": target_date,
        "results": results,
    }
