# exp-20260218-008: uptrend_score_60d upper-bound cap (v8.0)

## Hypothesis

Oracle hypothesis #1: Add `uptrend_score_60d <= 85` pre-filter to cap overextended uptrends.

Discrimination analysis (24W vs 6L, seed 42) showed losers have significantly higher
uptrend scores (80.95 vs 73.89, Δ=-7.06). The reasoning: stocks deep into a strong
uptrend are at mean-reversion risk for overnight entries — buying at pullback in a
late-stage trend catches falling knives rather than healthy dips.

## Change

Added one pre-filter to overnight.yaml:
```yaml
- column: uptrend_score_60d
  op: lte
  value: 85.0
  fill_null: 100.0
```

## Results

| Seed | WR (v7.5) | WR (v8.0) | Δ WR | AR (v7.5) | AR (v8.0) | Δ AR |
|------|-----------|-----------|------|-----------|-----------|------|
| 42   | 83.3%     | 83.3%     | 0.0  | +2.62%    | +2.62%    | 0.00 |
| 123  | 60.0%     | 60.0%     | 0.0  | +1.12%    | +1.12%    | 0.00 |
| 55   | 60.0%     | 60.0%     | 0.0  | +1.66%    | +1.66%    | 0.00 |
| 99   | 53.3%     | 50.0%     | -3.3 | +0.67%    | -0.66%    | -1.33|
| **Avg** | **64.2%** | **63.3%** | **-0.9** | **+1.52%** | **+1.40%** | **-0.12** |

## Decision: REJECT

Seed 99 regressed -3.3pp WR and -1.33% AR, dropping below 50% WR. The cap at 85 is
too lenient — most stocks have uptrend_score_60d well below 85, so the filter only
removes a handful of borderline candidates. In seed 99, some of those removed stocks
happened to be winners, causing net harm.

## Lessons

1. **Uptrend cap at 85 is too lenient**: The filter barely activates because few stocks
   reach 85. A tighter cap (e.g., 75) might work but risks over-filtering.
2. **Pre-filter on uptrend_score is risky**: The factor has high variance across seeds.
   What's overextended in one seed's date sample is a winner in another's.
3. **Alternative approach needed**: Instead of hard-capping uptrend_score, consider
   removing ma_trend weight entirely (Oracle hypothesis #3) since ma_alignment also
   discriminates in the wrong direction (L>W) and removing its weight is cleaner than
   adding fragile pre-filters.

## Rollback

Reverted overnight.yaml to v7.5 — removed the uptrend_score_60d pre-filter.
