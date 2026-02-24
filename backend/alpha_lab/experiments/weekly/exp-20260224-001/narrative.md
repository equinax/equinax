experiment_id: exp-20260224-001
date: 2026-02-24
strategy: weekly
parent_version: v1
test_version: v2
decision: accept
iteration: 17

## 假设

Weekly 策略核心理念是"低波动+高一致性"选股，但未直接使用 `stability_score`（基于
`return_std_20d` 的波动率稳定性评分，0-100）。该因子直接度量收益波动率稳定性：
return_std_20d < 1.5% → 高分，> 4% → 接近零分。

变更：新增 `stability_score` 作为正向因子 (weight=0.05)，从 `anti_climax` 中调出
0.05 权重（0.20→0.15），保持 total_positive_weight=0.85 不变。

## 变更明细

| 参数 | v1 | v2 |
|------|----|----|
| anti_climax_component.weight | 0.20 | 0.15 |
| stability_component (新增) | — | 0.05 |
| 正向因子数量 | 7 | 8 |
| total_positive_weight | 0.85 | 0.85 |

## 回测结果 (seed=42, 25 dates)

| 指标 | v1 (baseline) | v2 (experiment) | Delta |
|------|--------------|----------------|-------|
| WR | 47.0% | 47.3% | +0.3pp |
| AR | 0.48% | 0.83% | +0.35pp |
| P/L | 7.28 | 7.82 | +0.54 |

## 决策

**Accept** — 所有指标正向改善，无回退。v2 晋升为 head。

## 关键观察

1. stability_score 补充了"低波动"维度的直接度量，与现有因子（anti_climax 度量远离
   高潮、consistency 度量成交量稳定性）形成互补
2. anti_climax 权重减少 0.05 后仍保持有效（0.15 仍为最高单因子权重之一）
3. AR 从 0.48% 提升至 0.83%，改善幅度 +73%，但仍距目标 1.5% 有较大差距
4. WR 仅微升 0.3pp，说明 stability_score 主要通过提高选中股的收益质量而非数量来改善

## 后续方向

AR 仍距目标 1.5% 较远。可继续探索：
- 调整 stability_component 权重（当前 0.05，bounds 允许到 0.15）
- 引入 `uptrend_score_30d` 或 `rally_dominance_20d` 作为趋势质量补充
- 调整 `backtest_top_n`（当前 5，可尝试 3-4 以提高选股精度）
