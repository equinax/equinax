"""Index API endpoints.

Provides access to index information and industry composition.
"""

from datetime import date, datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.asset import AssetMeta, AssetType
from app.db.models.profile import IndexProfile
from app.db.models.stock_pool import IndexConstituent

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class IndexBasicResponse(BaseModel):
    """Basic index information."""
    code: str
    name: Optional[str] = None
    short_name: Optional[str] = None
    index_type: Optional[str] = None
    constituent_count: int = 0
    top_industry_l1: Optional[str] = None
    top_industry_weight: Optional[float] = None
    herfindahl_index: Optional[float] = None
    composition_updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class IndexListResponse(BaseModel):
    """Paginated index list."""
    items: List[IndexBasicResponse]
    total: int
    page: int
    page_size: int


class IndustryWeight(BaseModel):
    """Industry weight entry."""
    industry: str
    weight: float


class IndustryCompositionResponse(BaseModel):
    """Full industry composition for an index."""
    code: str
    name: Optional[str] = None
    constituent_count: int = 0
    computation_date: Optional[str] = None
    total_weight_covered: Optional[float] = None
    sw_l1: List[IndustryWeight] = []
    sw_l2: List[IndustryWeight] = []
    sw_l3: List[IndustryWeight] = []
    em: List[IndustryWeight] = []
    top_industry: Optional[str] = None
    top_industry_weight: Optional[float] = None
    herfindahl_index: Optional[float] = None


class ConstituentResponse(BaseModel):
    """Index constituent info."""
    stock_code: str
    stock_name: Optional[str] = None
    weight: Optional[float] = None
    entry_date: Optional[date] = None


class IndexConcentrationResponse(BaseModel):
    """Index with concentration metrics."""
    code: str
    name: Optional[str] = None
    index_type: Optional[str] = None
    constituent_count: int = 0
    top_industry: Optional[str] = None
    top_industry_weight: Optional[float] = None
    herfindahl_index: Optional[float] = None


class IndexByIndustryResponse(BaseModel):
    """Index exposure to a specific industry."""
    code: str
    name: Optional[str] = None
    index_type: Optional[str] = None
    industry_weight: float


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=IndexListResponse)
async def list_indices(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    index_type: Optional[str] = Query(None, description="Filter by index type (broad_based, sector, theme)"),
    db: AsyncSession = Depends(get_db),
):
    """
    List all indices with basic info and industry composition summary.

    Returns paginated list of indices with their top industry and concentration metrics.
    """
    # Build query
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            IndexProfile.short_name,
            IndexProfile.index_type,
            IndexProfile.constituent_count,
            IndexProfile.top_industry_l1,
            IndexProfile.top_industry_weight,
            IndexProfile.herfindahl_index,
            IndexProfile.composition_updated_at,
        )
        .select_from(AssetMeta)
        .outerjoin(IndexProfile, AssetMeta.code == IndexProfile.code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
    )

    if index_type:
        query = query.where(IndexProfile.index_type == index_type)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await db.execute(query)
    rows = result.all()

    items = [
        IndexBasicResponse(
            code=row.code,
            name=row.name,
            short_name=row.short_name,
            index_type=row.index_type,
            constituent_count=row.constituent_count or 0,
            top_industry_l1=row.top_industry_l1,
            top_industry_weight=float(row.top_industry_weight) if row.top_industry_weight else None,
            herfindahl_index=float(row.herfindahl_index) if row.herfindahl_index else None,
            composition_updated_at=row.composition_updated_at,
        )
        for row in rows
    ]

    return IndexListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/industry-concentration", response_model=List[IndexConcentrationResponse])
async def get_industry_concentration(
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    order: str = Query("desc", description="Sort order (asc/desc)"),
    db: AsyncSession = Depends(get_db),
):
    """
    List indices by industry concentration (Herfindahl Index).

    Higher HHI indicates more concentrated industry exposure.
    """
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            IndexProfile.index_type,
            IndexProfile.constituent_count,
            IndexProfile.top_industry_l1,
            IndexProfile.top_industry_weight,
            IndexProfile.herfindahl_index,
        )
        .select_from(AssetMeta)
        .join(IndexProfile, AssetMeta.code == IndexProfile.code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
        .where(IndexProfile.herfindahl_index.isnot(None))
    )

    if order == "desc":
        query = query.order_by(desc(IndexProfile.herfindahl_index))
    else:
        query = query.order_by(IndexProfile.herfindahl_index)

    query = query.limit(limit)

    result = await db.execute(query)
    rows = result.all()

    return [
        IndexConcentrationResponse(
            code=row.code,
            name=row.name,
            index_type=row.index_type,
            constituent_count=row.constituent_count or 0,
            top_industry=row.top_industry_l1,
            top_industry_weight=float(row.top_industry_weight) if row.top_industry_weight else None,
            herfindahl_index=float(row.herfindahl_index) if row.herfindahl_index else None,
        )
        for row in rows
    ]


@router.get("/by-industry", response_model=List[IndexByIndustryResponse])
async def get_indices_by_industry(
    industry: str = Query(..., description="Industry name (SW L1)"),
    min_weight: float = Query(0.0, ge=0, le=1, description="Minimum weight threshold"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    db: AsyncSession = Depends(get_db),
):
    """
    Find indices with high exposure to a specific industry.

    Returns indices sorted by weight in the specified industry.
    """
    # Get all index profiles with compositions
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            IndexProfile.index_type,
            IndexProfile.industry_composition,
        )
        .select_from(AssetMeta)
        .join(IndexProfile, AssetMeta.code == IndexProfile.code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
        .where(IndexProfile.industry_composition.isnot(None))
    )

    result = await db.execute(query)
    rows = result.all()

    # Filter and sort by industry weight
    matching_indices = []
    for row in rows:
        composition = row.industry_composition or {}
        sw_l1 = composition.get("sw_l1", {})
        weight = sw_l1.get(industry, 0)

        if weight >= min_weight:
            matching_indices.append({
                "code": row.code,
                "name": row.name,
                "index_type": row.index_type,
                "industry_weight": weight,
            })

    # Sort by weight descending
    matching_indices.sort(key=lambda x: x["industry_weight"], reverse=True)

    return [
        IndexByIndustryResponse(**idx)
        for idx in matching_indices[:limit]
    ]


@router.get("/{code}", response_model=IndexBasicResponse)
async def get_index(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """Get basic info for a specific index."""
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            IndexProfile.short_name,
            IndexProfile.index_type,
            IndexProfile.constituent_count,
            IndexProfile.top_industry_l1,
            IndexProfile.top_industry_weight,
            IndexProfile.herfindahl_index,
            IndexProfile.composition_updated_at,
        )
        .select_from(AssetMeta)
        .outerjoin(IndexProfile, AssetMeta.code == IndexProfile.code)
        .where(AssetMeta.code == code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
    )

    result = await db.execute(query)
    row = result.first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index {code} not found",
        )

    return IndexBasicResponse(
        code=row.code,
        name=row.name,
        short_name=row.short_name,
        index_type=row.index_type,
        constituent_count=row.constituent_count or 0,
        top_industry_l1=row.top_industry_l1,
        top_industry_weight=float(row.top_industry_weight) if row.top_industry_weight else None,
        herfindahl_index=float(row.herfindahl_index) if row.herfindahl_index else None,
        composition_updated_at=row.composition_updated_at,
    )


@router.get("/{code}/industry-composition", response_model=IndustryCompositionResponse)
async def get_index_industry_composition(
    code: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed industry composition for an index.

    Returns weights for all industry levels (SW L1/L2/L3, EM).
    """
    query = (
        select(
            AssetMeta.code,
            AssetMeta.name,
            IndexProfile.constituent_count,
            IndexProfile.industry_composition,
            IndexProfile.top_industry_l1,
            IndexProfile.top_industry_weight,
            IndexProfile.herfindahl_index,
        )
        .select_from(AssetMeta)
        .outerjoin(IndexProfile, AssetMeta.code == IndexProfile.code)
        .where(AssetMeta.code == code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
    )

    result = await db.execute(query)
    row = result.first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index {code} not found",
        )

    composition = row.industry_composition or {}

    def to_weights(d: Dict[str, float]) -> List[IndustryWeight]:
        return [IndustryWeight(industry=k, weight=v) for k, v in d.items()]

    return IndustryCompositionResponse(
        code=row.code,
        name=row.name,
        constituent_count=row.constituent_count or 0,
        computation_date=composition.get("computation_date"),
        total_weight_covered=composition.get("total_weight_covered"),
        sw_l1=to_weights(composition.get("sw_l1", {})),
        sw_l2=to_weights(composition.get("sw_l2", {})),
        sw_l3=to_weights(composition.get("sw_l3", {})),
        em=to_weights(composition.get("em", {})),
        top_industry=row.top_industry_l1,
        top_industry_weight=float(row.top_industry_weight) if row.top_industry_weight else None,
        herfindahl_index=float(row.herfindahl_index) if row.herfindahl_index else None,
    )


@router.get("/{code}/constituents", response_model=List[ConstituentResponse])
async def get_index_constituents(
    code: str,
    include_expired: bool = Query(False, description="Include expired constituents"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get constituents for an index.

    Returns current constituents with weights.
    """
    # First check if index exists
    index_query = (
        select(AssetMeta.code)
        .where(AssetMeta.code == code)
        .where(AssetMeta.asset_type == AssetType.INDEX)
    )
    index_result = await db.execute(index_query)
    if not index_result.first():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index {code} not found",
        )

    # Get constituents
    query = (
        select(
            IndexConstituent.stock_code,
            IndexConstituent.weight,
            IndexConstituent.entry_date,
            AssetMeta.name.label("stock_name"),
        )
        .select_from(IndexConstituent)
        .outerjoin(AssetMeta, IndexConstituent.stock_code == AssetMeta.code)
        .where(IndexConstituent.index_code == code)
    )

    if not include_expired:
        query = query.where(IndexConstituent.expire_date.is_(None))

    query = query.order_by(desc(IndexConstituent.weight))

    result = await db.execute(query)
    rows = result.all()

    return [
        ConstituentResponse(
            stock_code=row.stock_code,
            stock_name=row.stock_name,
            weight=float(row.weight) if row.weight else None,
            entry_date=row.entry_date,
        )
        for row in rows
    ]
