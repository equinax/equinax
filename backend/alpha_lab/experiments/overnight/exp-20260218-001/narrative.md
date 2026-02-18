# exp-20260218-001: Penalize Overextended MA Alignment

**Date**: 2026-02-18
**Strategy**: overnight
**Parent**: v7.2 (cross-seed avg WR=60.2%, AR=+1.39%)
**Decision**: REJECT

## Hypothesis

Losers have mean `ma_alignment_score=98.72` vs winners `93.18` (Δ=-5.54 in seed 42, n=24W/6L).
Near-perfect MA alignment signals overextension, not ideal pullback entry.

## Changes

1. Reduced `ma_trend_signal` clip ceiling from 95→85
2. Reduced `ma_trend_signal` weight from 0.10→0.05
3. Added `ma_overextension_penalty` for `ma_alignment_score > 90` (weight 0.05)

## Results

| Seed | v7.2 WR | v7.3 WR | Δ WR | v7.2 AR | v7.3 AR | Δ AR |
|------|---------|---------|------|---------|---------|------|
| 42   | 80.0%   | 63.3%   | -16.7pp | 1.95% | 1.62% | -0.33% |
| 123  | 51.4%   | 48.6%   | -2.8pp  | 0.85% | 0.41% | -0.44% |
| 55   | 56.0%   | 56.0%   | 0.0pp   | 1.67% | 1.46% | -0.21% |
| 99   | 53.3%   | 46.7%   | -6.6pp  | 1.07% | 1.19% | +0.12% |
| **Avg** | **60.2%** | **53.7%** | **-6.5pp** | **1.39%** | **1.17%** | **-0.22%** |

## Reject Reason

WR dropped -6.5pp, far exceeding -2pp reject threshold. The penalty disrupted stock ranking significantly — many previously-selected winners (圣农发展, 永安药业, 绿色动力) were demoted and replaced by worse picks (江苏博云, 志特新材, 常友科技).

## Key Lesson

The 5.54pt MA alignment mean difference between 24 winners and 6 losers is statistical noise, not a reliable signal. With such a small loser sample, the "inverse correlation" was an artifact. MA alignment should remain a positive signal — it correctly identifies uptrending stocks. The problem is elsewhere.

## Next Direction

Look at other factors with clearer discrimination:
- `shadow_range_pct`: W=7.16 vs L=5.70 (W>L by 1.46) — winners have more doji/reversal candles. Consider increasing weight.
- `volume_ratio_5d`: W=1.64 vs L=1.23 (W>L by 0.41) — winners have higher volume interest. Consider increasing weight.
- `pct_chg`: W=-1.77 vs L=-2.33 — losers drop harder on signal day. Consider tightening the pct_chg floor from -7.0 to -5.0.
