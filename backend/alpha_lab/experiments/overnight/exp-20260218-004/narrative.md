# exp-20260218-004: Boost Volume Interest Weight

## Hypothesis
Increase `volume_interest_signal` (volume_ratio_5d) weight from 0.05 → 0.10,
decrease `rally_dominance_signal` (rally_dominance_20d) weight from 0.15 → 0.10.

Factor discrimination showed volume_ratio_5d has W-L differential (W=1.64, L=1.23, Δ=+0.41).
Higher volume on pullback day suggests buying interest confirming reversal.

## Change
- `volume_interest_signal.weight`: 0.05 → 0.10
- `rally_dominance_signal.weight`: 0.15 → 0.10

## Results

| Seed | v7.5 WR | v7.6 WR | Δ WR | v7.5 AR | v7.6 AR | Δ AR |
|------|---------|---------|------|---------|---------|------|
| 42   | 83.3%   | 76.7%   | -6.6 | 2.62%   | 1.61%   | -1.01 |
| 123  | 60.0%   | 51.4%   | -8.6 | 1.12%   | 0.33%   | -0.79 |
| 55   | 60.0%   | 52.0%   | -8.0 | 1.66%   | 0.82%   | -0.84 |
| 99   | 53.3%   | 40.0%   | -13.3| 0.67%   | 0.29%   | -0.38 |
| **Avg** | **64.2%** | **55.0%** | **-9.2** | **1.52%** | **0.76%** | **-0.76** |

## Decision: REJECT ❌

Catastrophic WR regression across ALL seeds (-9.2pp avg). Seed 99 collapsed to 40%.
The volume_ratio_5d Δ of +0.41 was too small to be reliable. Boosting its weight
introduced noise that disrupted stock ranking quality. rally_dominance was actually
contributing more than the discrimination analysis suggested.

**Lesson**: Small W-L differentials (<0.5) in scoring factors are unreliable for weight
reallocation. The discrimination analysis measures means, but ranking quality depends on
the distribution shape, not just means.

Config rolled back to v7.5.
