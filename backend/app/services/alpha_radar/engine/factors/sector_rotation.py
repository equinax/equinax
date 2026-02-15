"""Sector Rotation Factor — industry momentum and crowding knowledge.

Used actively by Dragon Leader (sector_overheat_penalty, sector_weakness_penalty)
and previously by Smart Accumulation (sector_weakness_penalty).

Key Insights (from Iter 9 and Iter 15):

  1. sector_momentum_5d is a double-edged signal:
     - Negative sector momentum (< -5%) → penalize (sector_weakness_penalty)
       Stocks in weak sectors face headwinds regardless of individual quality.
     - Positive sector momentum (> 5%) → penalize (sector_overheat_penalty)
       Overheated sectors (>5% 5-day mom) tend to mean-revert. (Iter 15)

  2. Sector diversification at output level:
     Max 2 stocks per sw_industry_l1 in top-N recommendations (screener_service.py)
     prevents concentration risk from a single hot sector dominating the list.

  3. sector_momentum_5d computation:
     Computed in PolarsEngine.compute_sector_momentum() as the average
     5-day return across all stocks in each sw_industry_l1 group.
     Joined to individual stock rows by sw_industry_l1.

  4. For the Panorama tab (still used in sector_heatmap_service.py):
     Sector momentum is not a scoring factor — the panorama score
     is sector-agnostic by design, measuring individual stock quality.

Columns Used (computed in polars_engine / screener_service):
  - sector_momentum_5d: 5-day average return of the stock's L1 industry
  - sw_industry_l1: Stock's SW L1 industry classification
"""
