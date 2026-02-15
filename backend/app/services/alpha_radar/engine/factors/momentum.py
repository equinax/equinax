"""Momentum Factor — trend quality and breakout knowledge.

Extracted from the removed Super Trend (趋势共振) tab scoring logic.

Key Insights (from Iter 14):

  1. close_strength removed (harmful, see smart_money.py)
  2. volume_buildup_quality removed from trend scoring (redundant with VCS)
  3. elg_net_percentile and mf_net_percentile added as supplementary signals
     but with low weights (0.08 and 0.05 respectively)

  The trend tab taught us that pure momentum-chasing is dangerous.
  The best momentum signals are:
  - trend_quality_20d: Directional consistency over 20 days
  - ma_alignment_score: MA5/10/20/60 alignment (multi-timeframe confirmation)
  - momentum_quality_ratio: Quality of momentum (steady vs spike-driven)

  Anti-momentum signals that protect against reversals:
  - anti_climax_component: Distance from climax/distribution zone
  - post_spike_consolidation: Whether post-spike digestion has occurred
  - resistance_proximity_penalty: Near-term supply zone pressure

Columns Used (computed in polars_engine):
  - trend_quality_20d: 20-day trend direction and magnitude score
  - ma_alignment_score: Multi-MA alignment degree (0-100)
  - momentum_quality_ratio: Momentum health metric
  - momentum_20d, momentum_60d: Raw momentum values
  - momentum_percentile: Cross-sectional momentum ranking
"""
