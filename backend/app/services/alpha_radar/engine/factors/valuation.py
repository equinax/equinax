"""Valuation Factor — deep value and mean-reversion knowledge.

Extracted from the removed Deep Value (深度价值) tab.

Key Insights:

  1. Pure valuation (PE/PB percentile) is a valid signal but has weak
     short-term predictive power. It works best as a safety filter
     rather than a primary scoring factor.

  2. The "trigger" concept matters: cheap stocks need a catalyst.
     accumulation_score combined with low valuation was the value tab's
     edge — "cheap + someone buying" beats "cheap alone".

  3. Resistance penalty should be low for value stocks (0.02 vs 0.10
     for momentum stocks) because value re-ratings naturally push
     prices toward historical highs.

  4. The Panorama tab's low-VBQ penalty (Iter 11) applies universally:
     volume_buildup_quality < 65 indicates sloppy volume = distribution
     rather than accumulation, regardless of valuation level.

Columns Used (computed in polars_engine):
  - value_percentile: Combined PE/PB ranking (0-1)
  - pe_percentile: PE TTM percentile
  - price_position_60d: Current price vs 60-day range (0-1)
  - stability_score: Price stability metric
"""
