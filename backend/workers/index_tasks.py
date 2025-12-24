"""Index calculation tasks for ARQ.

Calculates index industry composition by aggregating constituent stock industries.
"""

from typing import Dict, Any, Optional
from datetime import date, datetime
from decimal import Decimal
from collections import defaultdict
import logging

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.db.models.stock_pool import IndexConstituent
from app.db.models.profile import StockProfile, IndexProfile

logger = logging.getLogger(__name__)


def get_session_maker(database_url: str = None):
    """Get async session maker for the given database URL."""
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


# Default session maker using settings
worker_session_maker = get_session_maker()


# =============================================================================
# Index Industry Composition Calculation
# =============================================================================

async def calculate_index_industry_composition(
    ctx: dict,
    index_code: Optional[str] = None,
    calc_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calculate industry composition for indices by aggregating constituent stock industries.

    This task:
    1. Gets current constituents for each index (where expire_date IS NULL)
    2. JOINs with stock_profile to get industry classifications
    3. Aggregates weights by industry
    4. Stores results in index_profile.industry_composition

    Args:
        ctx: ARQ context
        index_code: Optional specific index code to calculate (None = all indices)
        calc_date: Optional date for calculation (default: today)
        database_url: Optional database URL (default: settings.database_url)

    Returns:
        Dict with task results
    """
    target_date = date.fromisoformat(calc_date) if calc_date else date.today()
    logger.info(f"Calculating index industry composition for date={target_date}, index={index_code or 'all'}")

    session_maker = get_session_maker(database_url) if database_url else worker_session_maker
    async with session_maker() as db:
        # Get indices to process
        if index_code:
            indices = [index_code]
        else:
            # Get all unique index codes from constituents
            result = await db.execute(
                select(IndexConstituent.index_code).distinct()
            )
            indices = [row[0] for row in result.all()]

        if not indices:
            logger.warning("No indices found in index_constituents table")
            return {
                "task": "calculate_index_industry_composition",
                "calc_date": str(target_date),
                "indices_processed": 0,
                "status": "no_indices_found",
            }

        logger.info(f"Processing {len(indices)} indices: {indices}")

        results = {}
        for idx_code in indices:
            composition = await _compute_single_index_composition(db, idx_code, target_date)
            results[idx_code] = composition

            # Upsert into index_profile
            await _upsert_index_profile(db, idx_code, composition, target_date)

        await db.commit()

        return {
            "task": "calculate_index_industry_composition",
            "calc_date": str(target_date),
            "indices_processed": len(results),
            "results": {k: {"constituent_count": v.get("constituent_count", 0)} for k, v in results.items()},
            "status": "success",
        }


async def _compute_single_index_composition(
    db: AsyncSession,
    index_code: str,
    calc_date: date,
) -> Dict[str, Any]:
    """Compute industry composition for a single index."""

    # Query: Join constituents with stock profiles
    query = text("""
        SELECT
            ic.stock_code,
            COALESCE(ic.weight, 1.0 / NULLIF(COUNT(*) OVER(), 0)) as weight,
            sp.sw_industry_l1,
            sp.sw_industry_l2,
            sp.sw_industry_l3,
            sp.em_industry
        FROM index_constituents ic
        LEFT JOIN stock_profile sp ON ic.stock_code = sp.code
        WHERE ic.index_code = :index_code
          AND (ic.expire_date IS NULL OR ic.expire_date > :calc_date)
    """)

    result = await db.execute(query, {"index_code": index_code, "calc_date": calc_date})
    rows = result.fetchall()

    if not rows:
        logger.warning(f"No constituents found for index {index_code}")
        return {"error": "No constituents found", "constituent_count": 0}

    # Aggregate by industry
    sw_l1_weights: Dict[str, Decimal] = defaultdict(Decimal)
    sw_l2_weights: Dict[str, Decimal] = defaultdict(Decimal)
    sw_l3_weights: Dict[str, Decimal] = defaultdict(Decimal)
    em_weights: Dict[str, Decimal] = defaultdict(Decimal)

    total_weight = Decimal("0")
    covered_weight = Decimal("0")

    for row in rows:
        weight = Decimal(str(row.weight)) if row.weight else Decimal("0")
        total_weight += weight

        if row.sw_industry_l1:
            sw_l1_weights[row.sw_industry_l1] += weight
            covered_weight += weight
        if row.sw_industry_l2:
            sw_l2_weights[row.sw_industry_l2] += weight
        if row.sw_industry_l3:
            sw_l3_weights[row.sw_industry_l3] += weight
        if row.em_industry:
            em_weights[row.em_industry] += weight

    def normalize(d: Dict[str, Decimal]) -> Dict[str, float]:
        """Normalize weights to sum to 1.0 and sort by weight descending."""
        if not d or total_weight == 0:
            return {}
        return {
            k: round(float(v / total_weight), 6)
            for k, v in sorted(d.items(), key=lambda x: -x[1])
        }

    composition = {
        "sw_l1": normalize(sw_l1_weights),
        "sw_l2": normalize(sw_l2_weights),
        "sw_l3": normalize(sw_l3_weights),
        "em": normalize(em_weights),
        "computation_date": str(calc_date),
        "constituent_count": len(rows),
        "total_weight_covered": round(float(covered_weight / total_weight), 4) if total_weight > 0 else 0,
    }

    # Calculate concentration metrics
    if sw_l1_weights:
        sorted_industries = sorted(sw_l1_weights.items(), key=lambda x: -x[1])
        composition["top_industry"] = sorted_industries[0][0]
        composition["top_industry_weight"] = round(float(sorted_industries[0][1] / total_weight), 4)

        # Herfindahl-Hirschman Index (sum of squared market shares)
        hhi = sum((float(v / total_weight) ** 2) for v in sw_l1_weights.values())
        composition["herfindahl_index"] = round(hhi, 6)

    logger.info(
        f"Index {index_code}: {len(rows)} constituents, "
        f"top industry={composition.get('top_industry')} ({composition.get('top_industry_weight', 0)*100:.1f}%)"
    )

    return composition


async def _upsert_index_profile(
    db: AsyncSession,
    index_code: str,
    composition: Dict[str, Any],
    calc_date: date,
) -> None:
    """Upsert index profile with composition data."""

    # Determine index type based on code
    index_type = _get_index_type(index_code)

    stmt = insert(IndexProfile).values(
        code=index_code,
        short_name=index_code,
        index_type=index_type,
        constituent_count=composition.get("constituent_count", 0),
        industry_composition=composition,
        top_industry_l1=composition.get("top_industry"),
        top_industry_weight=composition.get("top_industry_weight"),
        herfindahl_index=composition.get("herfindahl_index"),
        composition_updated_at=datetime.now(),
    )

    stmt = stmt.on_conflict_do_update(
        index_elements=["code"],
        set_={
            "constituent_count": stmt.excluded.constituent_count,
            "industry_composition": stmt.excluded.industry_composition,
            "top_industry_l1": stmt.excluded.top_industry_l1,
            "top_industry_weight": stmt.excluded.top_industry_weight,
            "herfindahl_index": stmt.excluded.herfindahl_index,
            "composition_updated_at": stmt.excluded.composition_updated_at,
            "updated_at": func.now(),
        },
    )

    await db.execute(stmt)


def _get_index_type(index_code: str) -> str:
    """Determine index type from code."""
    broad_based = {"hs300", "zz500", "zz1000", "sz50", "sz100", "zz800", "csi1000"}
    sector = {"cyb", "kc50"}

    if index_code.lower() in broad_based:
        return "broad_based"
    elif index_code.lower() in sector:
        return "sector"
    else:
        return "other"


# =============================================================================
# Daily Update Task
# =============================================================================

async def daily_index_update(
    ctx: dict,
    calc_date: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run all index-related updates.

    Called weekly (or on demand) after constituent data is refreshed.
    """
    target_date = calc_date or str(date.today())
    logger.info(f"Running daily index update for {target_date}")

    results = {}

    # 1. Calculate industry composition for all indices
    try:
        results["composition"] = await calculate_index_industry_composition(
            ctx, index_code=None, calc_date=target_date, database_url=database_url
        )
    except Exception as e:
        logger.error(f"Index composition calculation failed: {e}")
        results["composition"] = {"error": str(e), "status": "failed"}

    return {
        "task": "daily_index_update",
        "calc_date": target_date,
        "results": results,
        "status": "success" if "error" not in results.get("composition", {}) else "partial_failure",
    }
