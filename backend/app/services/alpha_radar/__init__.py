"""Alpha Radar service module.

Provides real-time market scanning and scoring using Polars for high-performance calculations.
"""

from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine
from app.services.alpha_radar.dashboard_service import DashboardService
from app.services.alpha_radar.screener_service import ScreenerService
from app.services.alpha_radar.sector_heatmap_service import SectorHeatmapService

__all__ = [
    "PolarsEngine",
    "ScoringEngine",
    "DashboardService",
    "ScreenerService",
    "SectorHeatmapService",
]
