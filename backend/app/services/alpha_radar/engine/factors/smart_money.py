"""Smart Money Factor — institutional flow and accumulation knowledge.

Extracted from the removed Smart Accumulation (聪明钱吸筹) tab scoring logic.

Key Insights (from Iter 12-14 cross-date qualitative analysis):

  1. close_strength (CS) is HARMFUL — a REVERSED signal:
     - Top-5 losers avg CS=0.90 vs #6-10 winners avg CS=0.71 (delta -0.19)
     - High CS = "closed near daily high" = chasing late momentum
     - Removed from Smart (Iter 12), Trend and Dragon (Iter 14)

  2. volume_consistency_score (VCS) is the STRONGEST cross-date discriminator:
     - Losers avg VCS=84.5 vs winners avg VCS=91.6 (delta +7.12)
     - High VCS = steady, reliable volume accumulation without erratic spikes
     - Weight increased: 0.05 → 0.12

  3. main_strength_proxy (MSP) is the second-strongest positive signal:
     - Losers avg MSP=51.6 vs winners avg MSP=56.4 (delta +4.78)
     - Weight increased: 0.10 → 0.13

  4. Same-day moneyflow (elg_net, mf_net) is a LAG indicator:
     - High institutional inflow TODAY = peak of move, not beginning
     - Weight should never exceed 0.05

Columns Used (computed in polars_engine):
  - main_strength_proxy: Composite proxy for institutional participation
  - volume_consistency_score: Stability of volume over lookback period
  - accumulation_score: Sustained inflow signal
  - close_strength: DO NOT USE — reversed signal (追高)

Note: These insights inform the weight allocation in all active strategy tabs.
"""
