"""Fundamentals Factor — PE/PB valuation knowledge.

Extracted from the removed Deep Value (深度价值) tab scoring logic.

Key Insights (from Iter 9 and cross-date analysis):
  - value_percentile (PE分位) was the dominant signal in deep value scoring (weight 0.35)
  - ep_ratio and bp_ratio provide raw valuation signals
  - Resistance penalty was reduced for value stocks (0.05→0.02) because value stocks
    naturally trade near highs after fundamental re-rating

Columns Used (computed in polars_engine):
  - value_percentile: Cross-sectional PE/PB ranking (0-1, lower = cheaper)
  - pe_percentile: PE TTM historical percentile
  - ep_ratio: Earnings-to-price ratio (inverse PE)
  - bp_ratio: Book-to-price ratio (inverse PB)

Note: These columns are still available in polars_engine output and can be used
by any strategy that wants to incorporate fundamental valuation signals.
"""
