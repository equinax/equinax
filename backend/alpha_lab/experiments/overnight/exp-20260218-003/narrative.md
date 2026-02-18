# exp-20260218-003: Boost Reversal Candle Signal Weight

## Hypothesis
Increase `reversal_candle_signal` (shadow_range_pct) weight from 0.15 → 0.20,
decrease `smoothness_signal` (cycle_smoothness_20d) weight from 0.10 → 0.05.

Factor discrimination analysis showed shadow_range_pct has the strongest W-L differential
among positive factors (Winners=7.16, Losers=5.70, Δ=+1.46). cycle_smoothness_20d is
nearly neutral (Δ=-0.24), making it ideal weight donor.

## Change
Two-variable weight reallocation (total unchanged at 1.00):
- `reversal_candle_signal.weight`: 0.15 → 0.20
- `smoothness_signal.weight`: 0.10 → 0.05

## Results

### Cross-Seed Comparison (v7.4 → v7.5)

| Seed | v7.4 WR | v7.5 WR | Δ WR | v7.4 AR | v7.5 AR | Δ AR |
|------|---------|---------|------|---------|---------|------|
| 42   | 83.3%   | 83.3%   | 0.0  | 2.00%   | 2.62%   | +0.62 |
| 123  | 54.3%   | 60.0%   | +5.7 | 0.84%   | 1.12%   | +0.28 |
| 55   | 56.0%   | 60.0%   | +4.0 | 1.61%   | 1.66%   | +0.05 |
| 99   | 53.3%   | 53.3%   | 0.0  | 0.71%   | 0.67%   | -0.04 |
| **Avg** | **61.7%** | **64.2%** | **+2.5** | **1.29%** | **1.52%** | **+0.23** |

### Cumulative Progress (v7.2 baseline → v7.5)

| Metric | v7.2 | v7.4 | v7.5 | Total Δ |
|--------|------|------|------|---------|
| Avg WR | 60.2% | 61.7% | 64.2% | **+4.0pp** |
| Avg AR | 1.39% | 1.29% | 1.52% | **+0.13%** |

### Decision Criteria Check
- WR delta = +2.5pp ≥ 0 ✅
- AR delta = +0.23% ≥ 0 ✅
- Primary metric (WR) improved ✅
- No seed shows WR regression ✅
- Secondary metric (AR) also improved ✅

## Decision: ACCEPT ✅

Strongest iteration yet. Both WR and AR improved simultaneously. The shadow_range_pct
factor (doji/reversal candle presence) is confirmed as a key discriminator for overnight
reversal quality. Stocks with wider shadows on the signal day are more likely to reverse
successfully the next morning.

## New Baseline
- **v7.5**: Cross-seed avg WR=64.2%, AR=+1.52%
- Weights: reversal_candle=0.20, smoothness=0.05
