"""API v1 router aggregation."""

from fastapi import APIRouter

from app.api.v1 import strategies, backtests, stocks, auth, stats, pools, analytics, universe, indices, data_sync

api_router = APIRouter()

# Include all route modules
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(strategies.router, prefix="/strategies", tags=["Strategies"])
api_router.include_router(backtests.router, prefix="/backtests", tags=["Backtests"])
api_router.include_router(stocks.router, prefix="/stocks", tags=["Stocks"])
api_router.include_router(stats.router, prefix="/stats", tags=["Statistics"])
api_router.include_router(pools.router, prefix="/pools", tags=["Stock Pools"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])
api_router.include_router(universe.router, prefix="/universe", tags=["Universe Cockpit"])
api_router.include_router(indices.router, prefix="/indices", tags=["Indices"])
api_router.include_router(data_sync.router, prefix="/data-sync", tags=["Data Sync"])
