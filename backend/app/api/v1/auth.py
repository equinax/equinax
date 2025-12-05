"""Authentication API endpoints."""

from fastapi import APIRouter

router = APIRouter()


@router.post("/login")
async def login():
    """User login endpoint (placeholder)."""
    return {"message": "Login endpoint - to be implemented"}


@router.post("/register")
async def register():
    """User registration endpoint (placeholder)."""
    return {"message": "Register endpoint - to be implemented"}


@router.post("/refresh")
async def refresh_token():
    """Token refresh endpoint (placeholder)."""
    return {"message": "Refresh token endpoint - to be implemented"}
