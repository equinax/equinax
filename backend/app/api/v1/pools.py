"""Stock pool API endpoints."""

import uuid
from datetime import date, datetime
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.session import get_db
from app.db.models.stock_pool import (
    StockPool, StockPoolMember, IndexConstituent, StockPoolCombination,
    PoolType, PredefinedPoolKey
)
from app.db.models.asset import AssetMeta, MarketDaily
from app.db.models.classification import StockStructuralInfo

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class PoolCreate(BaseModel):
    """Schema for creating a stock pool."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    pool_type: str = Field(..., description="predefined, custom, or dynamic")
    predefined_key: Optional[str] = None
    stock_codes: Optional[List[str]] = None  # For custom pools
    filter_expression: Optional[Dict[str, Any]] = None  # For dynamic pools
    is_public: bool = False


class PoolUpdate(BaseModel):
    """Schema for updating a stock pool."""
    name: Optional[str] = None
    description: Optional[str] = None
    filter_expression: Optional[Dict[str, Any]] = None
    is_public: Optional[bool] = None


class PoolMemberUpdate(BaseModel):
    """Schema for updating custom pool members."""
    add: Optional[List[str]] = None  # Stock codes to add
    remove: Optional[List[str]] = None  # Stock codes to remove


class PoolResponse(BaseModel):
    """Schema for pool response."""
    id: str
    name: str
    description: Optional[str]
    pool_type: str
    predefined_key: Optional[str]
    filter_expression: Optional[Dict[str, Any]]
    is_public: bool
    is_system: bool
    stock_count: int
    last_evaluated_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PoolListResponse(BaseModel):
    """Schema for paginated pool list."""
    items: List[PoolResponse]
    total: int
    page: int
    page_size: int


class StockPreview(BaseModel):
    """Schema for stock preview in pool."""
    code: str
    code_name: Optional[str]
    exchange: Optional[str]
    sector: Optional[str]
    industry: Optional[str]


class PoolPreviewResponse(BaseModel):
    """Schema for pool preview response."""
    pool_id: str
    pool_name: str
    stock_codes: List[str]
    stocks: List[StockPreview]
    total_count: int
    exchange_distribution: Dict[str, int]
    sector_distribution: Dict[str, int]


class PoolCombinationCreate(BaseModel):
    """Schema for creating pool combination."""
    name: str
    description: Optional[str] = None
    operation: str = Field(..., description="union, intersection, or difference")
    pool_ids: List[str]


# ============================================
# Pool Evaluator Service (inline for MVP)
# ============================================

class PoolEvaluator:
    """Evaluates stock pools to get constituent stock codes."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def evaluate(
        self,
        pool: StockPool,
        evaluation_date: Optional[date] = None
    ) -> List[str]:
        """Evaluate a pool and return stock codes."""
        if pool.pool_type == PoolType.PREDEFINED.value:
            return await self._evaluate_predefined(pool.predefined_key, evaluation_date)
        elif pool.pool_type == PoolType.CUSTOM.value:
            return await self._evaluate_custom(pool.id)
        elif pool.pool_type == PoolType.DYNAMIC.value:
            return await self._evaluate_dynamic(pool.filter_expression, evaluation_date)
        else:
            raise ValueError(f"Unknown pool type: {pool.pool_type}")

    async def _evaluate_predefined(
        self,
        key: str,
        evaluation_date: Optional[date] = None
    ) -> List[str]:
        """Evaluate predefined pool."""

        if key == PredefinedPoolKey.SH_ALL.value:
            query = select(AssetMeta.code).where(AssetMeta.exchange == "sh")
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif key == PredefinedPoolKey.SZ_ALL.value:
            query = select(AssetMeta.code).where(AssetMeta.exchange == "sz")
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif key == PredefinedPoolKey.MAIN_BOARD.value:
            # Main board: use classification data
            query = select(StockStructuralInfo.code).where(
                StockStructuralInfo.board == "main"
            )
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif key == PredefinedPoolKey.GEM.value:
            # ChiNext (创业板): use classification data
            query = select(StockStructuralInfo.code).where(
                StockStructuralInfo.board == "gem"
            )
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif key == PredefinedPoolKey.STAR.value:
            # STAR Market (科创板): use classification data
            query = select(StockStructuralInfo.code).where(
                StockStructuralInfo.board == "star"
            )
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif key == PredefinedPoolKey.NON_ST.value:
            # Non-ST stocks: use classification data
            query = select(StockStructuralInfo.code).where(
                StockStructuralInfo.is_st == False
            )
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        # ETF pools
        elif key.startswith("etf_"):
            query = select(AssetMeta.code).where(
                AssetMeta.asset_type == "ETF"
            )
            if key == "etf_sh":
                query = query.where(AssetMeta.exchange == "sh")
            elif key == "etf_sz":
                query = query.where(AssetMeta.exchange == "sz")
            # For etf_broad, etf_sector, etc. - filter by ETFProfile.fund_type
            # TODO: implement when ETF classification is in place
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        raise ValueError(f"Unknown predefined key: {key}")

    async def _evaluate_custom(self, pool_id: uuid.UUID) -> List[str]:
        """Get custom pool members."""
        query = select(StockPoolMember.stock_code).where(
            StockPoolMember.pool_id == pool_id
        )
        result = await self.db.execute(query)
        return [r[0] for r in result.fetchall()]

    async def _evaluate_dynamic(
        self,
        filter_expr: Dict[str, Any],
        evaluation_date: Optional[date] = None
    ) -> List[str]:
        """Evaluate dynamic filter expression."""
        if not filter_expr:
            # Return all stocks if no filter
            query = select(AssetMeta.code)
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        conditions = filter_expr.get("conditions", [])
        logic = filter_expr.get("logic", "AND")

        if not conditions:
            query = select(AssetMeta.code)
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        # Build query dynamically
        # Start with all stock codes
        base_codes: Set[str] = set()

        # Process each condition
        condition_results = []
        for cond in conditions:
            field = cond.get("field")
            operator = cond.get("operator")
            value = cond.get("value")
            table = cond.get("table", "daily_k_data")

            codes = await self._evaluate_single_condition(field, operator, value, table)
            condition_results.append(set(codes))

        if not condition_results:
            return []

        # Combine results based on logic
        if logic == "AND":
            result_set = condition_results[0]
            for s in condition_results[1:]:
                result_set = result_set.intersection(s)
        else:  # OR
            result_set = condition_results[0]
            for s in condition_results[1:]:
                result_set = result_set.union(s)

        return list(result_set)

    async def _evaluate_single_condition(
        self,
        field: str,
        operator: str,
        value: Any,
        table: str
    ) -> List[str]:
        """Evaluate a single filter condition."""

        if table == "asset_meta" or table == "stock_basic":
            # Filter on asset_meta fields
            column = getattr(AssetMeta, field, None)
            if column is None:
                raise ValueError(f"Unknown field: {field}")

            query = select(AssetMeta.code)
            query = self._apply_operator(query, column, operator, value)
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        elif table == "market_daily" or table == "daily_k_data":
            # Filter on latest market_daily
            # Get latest date for each stock, then apply filter
            column = getattr(MarketDaily, field, None)
            if column is None:
                raise ValueError(f"Unknown field: {field}")

            # Subquery to get latest date per stock
            latest_date_subq = (
                select(
                    MarketDaily.code,
                    func.max(MarketDaily.date).label("max_date")
                )
                .group_by(MarketDaily.code)
                .subquery()
            )

            # Main query joining with latest date
            query = (
                select(MarketDaily.code)
                .join(
                    latest_date_subq,
                    and_(
                        MarketDaily.code == latest_date_subq.c.code,
                        MarketDaily.date == latest_date_subq.c.max_date
                    )
                )
            )
            query = self._apply_operator(query, column, operator, value)
            result = await self.db.execute(query)
            return [r[0] for r in result.fetchall()]

        raise ValueError(f"Unknown table: {table}")

    def _apply_operator(self, query, column, operator: str, value: Any):
        """Apply comparison operator to query."""
        if operator == "lt":
            return query.where(column < value)
        elif operator == "lte":
            return query.where(column <= value)
        elif operator == "gt":
            return query.where(column > value)
        elif operator == "gte":
            return query.where(column >= value)
        elif operator == "eq":
            return query.where(column == value)
        elif operator == "neq":
            return query.where(column != value)
        elif operator == "in":
            return query.where(column.in_(value))
        elif operator == "not_in":
            return query.where(~column.in_(value))
        elif operator == "like":
            return query.where(column.like(f"%{value}%"))
        elif operator == "is_null":
            return query.where(column.is_(None))
        elif operator == "is_not_null":
            return query.where(column.isnot(None))
        else:
            raise ValueError(f"Unknown operator: {operator}")

    async def combine_pools(
        self,
        operation: str,
        pool_ids: List[uuid.UUID],
        evaluation_date: Optional[date] = None
    ) -> List[str]:
        """Combine multiple pools using set operations."""

        all_codes: List[Set[str]] = []

        for pool_id in pool_ids:
            result = await self.db.execute(
                select(StockPool).where(StockPool.id == pool_id)
            )
            pool = result.scalar_one_or_none()
            if pool:
                codes = await self.evaluate(pool, evaluation_date)
                all_codes.append(set(codes))

        if not all_codes:
            return []

        if operation == "union":
            result_set = all_codes[0].union(*all_codes[1:])
        elif operation == "intersection":
            result_set = all_codes[0].intersection(*all_codes[1:])
        elif operation == "difference":
            result_set = all_codes[0].difference(*all_codes[1:])
        else:
            raise ValueError(f"Unknown operation: {operation}")

        return list(result_set)


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=PoolListResponse)
async def list_pools(
    pool_type: Optional[str] = Query(default=None, description="Filter by pool type"),
    include_system: bool = Query(default=True, description="Include system pools"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """List all stock pools."""
    query = select(StockPool)

    if pool_type:
        query = query.where(StockPool.pool_type == pool_type)

    if not include_system:
        query = query.where(StockPool.is_system == False)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginate
    query = (
        query
        .order_by(StockPool.is_system.desc(), StockPool.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    pools = result.scalars().all()

    return PoolListResponse(
        items=[
            PoolResponse(
                id=str(p.id),
                name=p.name,
                description=p.description,
                pool_type=p.pool_type,
                predefined_key=p.predefined_key,
                filter_expression=p.filter_expression,
                is_public=p.is_public,
                is_system=p.is_system,
                stock_count=p.stock_count,
                last_evaluated_at=p.last_evaluated_at,
                created_at=p.created_at,
                updated_at=p.updated_at,
            )
            for p in pools
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/predefined", response_model=List[PoolResponse])
async def list_predefined_pools(
    db: AsyncSession = Depends(get_db),
):
    """List all predefined (system) stock pools."""
    query = select(StockPool).where(
        StockPool.is_system == True,
        StockPool.pool_type == PoolType.PREDEFINED.value
    ).order_by(StockPool.name)

    result = await db.execute(query)
    pools = result.scalars().all()

    return [
        PoolResponse(
            id=str(p.id),
            name=p.name,
            description=p.description,
            pool_type=p.pool_type,
            predefined_key=p.predefined_key,
            filter_expression=p.filter_expression,
            is_public=p.is_public,
            is_system=p.is_system,
            stock_count=p.stock_count,
            last_evaluated_at=p.last_evaluated_at,
            created_at=p.created_at,
            updated_at=p.updated_at,
        )
        for p in pools
    ]


@router.post("", response_model=PoolResponse, status_code=status.HTTP_201_CREATED)
async def create_pool(
    pool_in: PoolCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new stock pool."""
    # Validate pool type
    if pool_in.pool_type not in [t.value for t in PoolType]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pool_type. Must be one of: {[t.value for t in PoolType]}"
        )

    # Create pool
    pool = StockPool(
        name=pool_in.name,
        description=pool_in.description,
        pool_type=pool_in.pool_type,
        predefined_key=pool_in.predefined_key,
        filter_expression=pool_in.filter_expression,
        is_public=pool_in.is_public,
        is_system=False,
    )
    db.add(pool)
    await db.flush()

    # If custom pool with initial stocks, add members
    if pool_in.pool_type == PoolType.CUSTOM.value and pool_in.stock_codes:
        for code in pool_in.stock_codes:
            member = StockPoolMember(pool_id=pool.id, stock_code=code)
            db.add(member)
        pool.stock_count = len(pool_in.stock_codes)

    await db.commit()
    await db.refresh(pool)

    return PoolResponse(
        id=str(pool.id),
        name=pool.name,
        description=pool.description,
        pool_type=pool.pool_type,
        predefined_key=pool.predefined_key,
        filter_expression=pool.filter_expression,
        is_public=pool.is_public,
        is_system=pool.is_system,
        stock_count=pool.stock_count,
        last_evaluated_at=pool.last_evaluated_at,
        created_at=pool.created_at,
        updated_at=pool.updated_at,
    )


@router.get("/{pool_id}", response_model=PoolResponse)
async def get_pool(
    pool_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific stock pool by ID."""
    try:
        pool_uuid = uuid.UUID(pool_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid pool ID format")

    result = await db.execute(
        select(StockPool).where(StockPool.id == pool_uuid)
    )
    pool = result.scalar_one_or_none()

    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")

    return PoolResponse(
        id=str(pool.id),
        name=pool.name,
        description=pool.description,
        pool_type=pool.pool_type,
        predefined_key=pool.predefined_key,
        filter_expression=pool.filter_expression,
        is_public=pool.is_public,
        is_system=pool.is_system,
        stock_count=pool.stock_count,
        last_evaluated_at=pool.last_evaluated_at,
        created_at=pool.created_at,
        updated_at=pool.updated_at,
    )


@router.get("/{pool_id}/preview", response_model=PoolPreviewResponse)
async def preview_pool(
    pool_id: str,
    limit: int = Query(default=100, ge=1, le=1000, description="Max stocks to return"),
    db: AsyncSession = Depends(get_db),
):
    """Preview pool contents (evaluate and return stocks)."""
    try:
        pool_uuid = uuid.UUID(pool_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid pool ID format")

    result = await db.execute(
        select(StockPool).where(StockPool.id == pool_uuid)
    )
    pool = result.scalar_one_or_none()

    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")

    # Evaluate pool
    evaluator = PoolEvaluator(db)
    stock_codes = await evaluator.evaluate(pool)

    # Update pool stats
    pool.stock_count = len(stock_codes)
    pool.last_evaluated_at = datetime.utcnow()
    await db.commit()

    # Get stock details for preview
    limited_codes = stock_codes[:limit]
    if limited_codes:
        stocks_query = select(AssetMeta).where(AssetMeta.code.in_(limited_codes))
        stocks_result = await db.execute(stocks_query)
        stocks = stocks_result.scalars().all()
    else:
        stocks = []

    # Calculate distributions
    exchange_dist: Dict[str, int] = {}
    asset_type_dist: Dict[str, int] = {}

    for s in stocks:
        ex = s.exchange or "unknown"
        exchange_dist[ex] = exchange_dist.get(ex, 0) + 1

        at = s.asset_type or "unknown"
        asset_type_dist[at] = asset_type_dist.get(at, 0) + 1

    return PoolPreviewResponse(
        pool_id=str(pool.id),
        pool_name=pool.name,
        stock_codes=stock_codes,
        stocks=[
            StockPreview(
                code=s.code,
                code_name=s.name,
                exchange=s.exchange,
                sector=None,
                industry=None,
            )
            for s in stocks
        ],
        total_count=len(stock_codes),
        exchange_distribution=exchange_dist,
        sector_distribution=asset_type_dist,
    )


@router.patch("/{pool_id}/members", response_model=PoolResponse)
async def update_pool_members(
    pool_id: str,
    members: PoolMemberUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update custom pool members (add/remove stocks)."""
    try:
        pool_uuid = uuid.UUID(pool_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid pool ID format")

    result = await db.execute(
        select(StockPool).where(StockPool.id == pool_uuid)
    )
    pool = result.scalar_one_or_none()

    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")

    if pool.pool_type != PoolType.CUSTOM.value:
        raise HTTPException(
            status_code=400,
            detail="Can only modify members of custom pools"
        )

    # Add new members
    if members.add:
        for code in members.add:
            # Check if already exists
            exists = await db.execute(
                select(StockPoolMember).where(
                    StockPoolMember.pool_id == pool_uuid,
                    StockPoolMember.stock_code == code
                )
            )
            if not exists.scalar_one_or_none():
                db.add(StockPoolMember(pool_id=pool_uuid, stock_code=code))

    # Remove members
    if members.remove:
        await db.execute(
            StockPoolMember.__table__.delete().where(
                StockPoolMember.pool_id == pool_uuid,
                StockPoolMember.stock_code.in_(members.remove)
            )
        )

    # Update count
    count_result = await db.execute(
        select(func.count()).where(StockPoolMember.pool_id == pool_uuid)
    )
    pool.stock_count = count_result.scalar() or 0

    await db.commit()
    await db.refresh(pool)

    return PoolResponse(
        id=str(pool.id),
        name=pool.name,
        description=pool.description,
        pool_type=pool.pool_type,
        predefined_key=pool.predefined_key,
        filter_expression=pool.filter_expression,
        is_public=pool.is_public,
        is_system=pool.is_system,
        stock_count=pool.stock_count,
        last_evaluated_at=pool.last_evaluated_at,
        created_at=pool.created_at,
        updated_at=pool.updated_at,
    )


@router.delete("/{pool_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pool(
    pool_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Delete a stock pool."""
    try:
        pool_uuid = uuid.UUID(pool_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid pool ID format")

    result = await db.execute(
        select(StockPool).where(StockPool.id == pool_uuid)
    )
    pool = result.scalar_one_or_none()

    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")

    if pool.is_system:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete system pools"
        )

    await db.delete(pool)
    await db.commit()


@router.post("/validate-filter")
async def validate_filter(
    filter_expression: Dict[str, Any],
    db: AsyncSession = Depends(get_db),
):
    """Validate a filter expression without saving."""
    try:
        evaluator = PoolEvaluator(db)
        # Create a temporary pool-like object
        class TempPool:
            pool_type = PoolType.DYNAMIC.value
            filter_expression = filter_expression

        codes = await evaluator.evaluate(TempPool())
        return {
            "valid": True,
            "stock_count": len(codes),
            "sample_codes": codes[:10]
        }
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "stock_count": 0,
            "sample_codes": []
        }
