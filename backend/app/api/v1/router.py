"""API v1 router aggregation."""

from fastapi import APIRouter

from app.api.v1 import strategies, backtests, stocks, auth, stats

api_router = APIRouter()

# Include all route modules
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(strategies.router, prefix="/strategies", tags=["Strategies"])
api_router.include_router(backtests.router, prefix="/backtests", tags=["Backtests"])
api_router.include_router(stocks.router, prefix="/stocks", tags=["Stocks"])
api_router.include_router(stats.router, prefix="/stats", tags=["Statistics"])
