"""Strategy management API endpoints."""

import hashlib
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.db.models.strategy import Strategy
from app.domain.engine import StrategyLoader, STRATEGY_TEMPLATES

router = APIRouter()


# ============================================
# Pydantic Schemas
# ============================================

class StrategyParameter(BaseModel):
    """Strategy parameter definition."""
    name: str
    type: str = Field(description="Parameter type: number, string, boolean, select")
    default: str | int | float | bool
    min: Optional[float] = None
    max: Optional[float] = None
    options: Optional[List[str]] = None


class StrategyCreate(BaseModel):
    """Schema for creating a new strategy."""
    name: str = Field(min_length=3, max_length=255)
    description: Optional[str] = Field(default=None, max_length=2000)
    strategy_type: Optional[str] = Field(
        default=None,
        description="Strategy type: momentum, mean_reversion, trend_following, arbitrage, other"
    )
    code: str = Field(min_length=50, description="Python strategy code")
    parameters: dict = Field(default_factory=dict)
    indicators_used: List[str] = Field(default_factory=list)


class StrategyUpdate(BaseModel):
    """Schema for updating a strategy."""
    name: Optional[str] = Field(default=None, min_length=3, max_length=255)
    description: Optional[str] = Field(default=None, max_length=2000)
    strategy_type: Optional[str] = None
    code: Optional[str] = Field(default=None, min_length=50)
    parameters: Optional[dict] = None
    indicators_used: Optional[List[str]] = None
    is_active: Optional[bool] = None
    is_public: Optional[bool] = None


class StrategyResponse(BaseModel):
    """Schema for strategy response."""
    id: UUID
    name: str
    description: Optional[str]
    version: int
    strategy_type: Optional[str]
    code: str
    code_hash: str
    parameters: dict
    indicators_used: List[str]
    is_validated: bool
    validation_error: Optional[str]
    is_active: bool
    is_public: bool
    execution_mode: str
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True


class StrategyListResponse(BaseModel):
    """Schema for paginated strategy list response."""
    items: List[StrategyResponse]
    total: int
    page: int
    page_size: int
    pages: int


class ValidationResult(BaseModel):
    """Schema for strategy validation result."""
    is_valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ============================================
# Helper Functions
# ============================================

def compute_code_hash(code: str) -> str:
    """Compute SHA256 hash of strategy code."""
    return hashlib.sha256(code.encode()).hexdigest()


# Temporary: Mock user ID until auth is implemented
MOCK_USER_ID = "00000000-0000-0000-0000-000000000001"


# ============================================
# API Endpoints
# ============================================

@router.get("", response_model=StrategyListResponse)
async def list_strategies(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    strategy_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """List all strategies for the current user with pagination and filtering."""
    # Build query
    query = select(Strategy).where(Strategy.user_id == MOCK_USER_ID)

    # Apply filters
    if strategy_type:
        query = query.where(Strategy.strategy_type == strategy_type)
    if is_active is not None:
        query = query.where(Strategy.is_active == is_active)
    if search:
        query = query.where(
            Strategy.name.ilike(f"%{search}%") | Strategy.description.ilike(f"%{search}%")
        )

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Apply pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    query = query.order_by(Strategy.updated_at.desc())

    # Execute query
    result = await db.execute(query)
    strategies = result.scalars().all()

    return StrategyListResponse(
        items=[StrategyResponse.model_validate(s) for s in strategies],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_strategy(
    strategy_in: StrategyCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new strategy."""
    # Compute code hash
    code_hash = compute_code_hash(strategy_in.code)

    # Create strategy
    strategy = Strategy(
        user_id=MOCK_USER_ID,
        name=strategy_in.name,
        description=strategy_in.description,
        strategy_type=strategy_in.strategy_type,
        code=strategy_in.code,
        code_hash=code_hash,
        parameters=strategy_in.parameters,
        indicators_used=strategy_in.indicators_used,
    )

    db.add(strategy)
    await db.commit()
    await db.refresh(strategy)

    return StrategyResponse.model_validate(strategy)


@router.get("/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    strategy_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific strategy by ID."""
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == MOCK_USER_ID,
        )
    )
    strategy = result.scalar_one_or_none()

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Strategy not found",
        )

    return StrategyResponse.model_validate(strategy)


@router.put("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    strategy_id: UUID,
    strategy_in: StrategyUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an existing strategy."""
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == MOCK_USER_ID,
        )
    )
    strategy = result.scalar_one_or_none()

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Strategy not found",
        )

    # Update fields
    update_data = strategy_in.model_dump(exclude_unset=True)

    # If code is updated, recompute hash and mark as not validated
    if "code" in update_data:
        update_data["code_hash"] = compute_code_hash(update_data["code"])
        update_data["is_validated"] = False
        update_data["validation_error"] = None
        strategy.version += 1

    for field, value in update_data.items():
        setattr(strategy, field, value)

    await db.commit()
    await db.refresh(strategy)

    return StrategyResponse.model_validate(strategy)


@router.delete("/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    strategy_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Delete a strategy."""
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == MOCK_USER_ID,
        )
    )
    strategy = result.scalar_one_or_none()

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Strategy not found",
        )

    await db.delete(strategy)
    await db.commit()


@router.post("/{strategy_id}/validate", response_model=ValidationResult)
async def validate_strategy(
    strategy_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    """Validate a strategy's code using the Backtrader strategy loader."""
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == MOCK_USER_ID,
        )
    )
    strategy = result.scalar_one_or_none()

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Strategy not found",
        )

    # Use StrategyLoader for validation
    validation = StrategyLoader.validate_code(strategy.code)

    # Update validation status
    strategy.is_validated = validation['valid']
    strategy.validation_error = "; ".join(validation['errors']) if validation['errors'] else None

    await db.commit()

    return ValidationResult(
        is_valid=validation['valid'],
        errors=validation['errors'],
        warnings=validation['warnings'],
    )


@router.post("/{strategy_id}/clone", response_model=StrategyResponse)
async def clone_strategy(
    strategy_id: UUID,
    new_name: str = Query(description="Name for the cloned strategy"),
    db: AsyncSession = Depends(get_db),
):
    """Clone an existing strategy."""
    result = await db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    )
    source = result.scalar_one_or_none()

    if not source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Strategy not found",
        )

    # Create clone
    clone = Strategy(
        user_id=MOCK_USER_ID,
        name=new_name,
        description=f"Cloned from: {source.name}",
        strategy_type=source.strategy_type,
        code=source.code,
        code_hash=source.code_hash,
        parameters=source.parameters,
        indicators_used=source.indicators_used,
    )

    db.add(clone)
    await db.commit()
    await db.refresh(clone)

    return StrategyResponse.model_validate(clone)


class StrategyTemplateResponse(BaseModel):
    """Schema for strategy template response."""
    name: str
    description: str
    code: str


@router.get("/templates/list", response_model=List[StrategyTemplateResponse])
async def list_strategy_templates():
    """Get list of available strategy templates."""
    templates = []
    for name, code in STRATEGY_TEMPLATES.items():
        # Extract description from docstring
        desc = ""
        lines = code.strip().split('\n')
        for line in lines:
            if '"""' in line:
                desc = line.replace('"""', '').strip()
                break

        templates.append(StrategyTemplateResponse(
            name=name,
            description=desc,
            code=code.strip(),
        ))

    return templates


class ValidateCodeRequest(BaseModel):
    """Schema for validating code inline."""
    code: str


@router.post("/validate-code", response_model=ValidationResult)
async def validate_code_inline(request: ValidateCodeRequest):
    """Validate strategy code without saving it."""
    validation = StrategyLoader.validate_code(request.code)

    return ValidationResult(
        is_valid=validation['valid'],
        errors=validation['errors'],
        warnings=validation['warnings'],
    )
