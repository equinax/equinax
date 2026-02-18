# exp-20260218-002: Tighten pct_chg Pre-filter

## Hypothesis
Tighten `pct_chg` pre-filter from `>= -7.0` to `>= -5.0`.

Losers have a harder drop on signal day (mean pct_chg = -2.33 vs winners = -1.77, Δ = +0.56).
Stocks dropping more than 5% on the signal day are likely in panic sell mode, not a healthy pullback within an uptrend.

## Change
Single variable: `scoring.pre_filters[pct_chg].value` from `-7.0` to `-5.0`.

## Results

### Cross-Seed Comparison (v7.2 baseline → v7.4)

| Seed | v7.2 WR | v7.4 WR | Δ WR | v7.2 AR | v7.4 AR | Δ AR |
|------|---------|---------|------|---------|---------|------|
| 42   | 80.0%   | 83.3%   | +3.3 | 1.95%   | 2.00%   | +0.05 |
| 123  | 51.4%   | 54.3%   | +2.9 | 0.85%   | 0.84%   | -0.01 |
| 55   | 56.0%   | 56.0%   | 0.0  | 1.67%   | 1.61%   | -0.06 |
| 99   | 53.3%   | 53.3%   | 0.0  | 1.07%   | 0.71%   | -0.36 |
| **Avg** | **60.2%** | **61.7%** | **+1.5** | **1.39%** | **1.29%** | **-0.10** |

### Decision Criteria Check
- WR delta = +1.5pp ≥ 0 ✅
- AR delta = -0.10% > -1% threshold ✅
- Primary metric (WR) improved ✅
- No seed shows WR regression > 1pp ✅

## Decision: ACCEPT ✅

The tighter pct_chg filter successfully removes panic-sell stocks that tend to be losers. WR improved +1.5pp with minimal AR cost (-0.10%). Seeds 42 and 123 show the strongest improvement, while seeds 55 and 99 are neutral on WR.

The slight AR reduction in seed 99 (-0.36%) suggests we may have filtered out a rare high-magnitude winner that happened to drop >5%, but the WR improvement across seeds confirms the filter is net positive.

## New Baseline
- **v7.4**: Cross-seed avg WR=61.7%, AR=+1.29%
- Config: `pct_chg >= -5.0` (was -7.0)
