---
name: alpha-coder
description: "Alpha Radar 配置编码与引擎适配。用于将理论/假设转化为可执行配置，在约束范围内调整 alpha_lab/configs/*.yaml 参数。触发: 理论假设确定后需要应用配置变更、新增因子代码、或更新评分引擎时。"
---

# Alpha Coder — 配置编码与引擎适配

## 能力范围

1. 读写 `backend/alpha_lab/configs/*.yaml` 策略配置
2. 在 `parameter_bounds` 约束范围内调整参数
3. 当需要新因子时，在 `backend/app/services/alpha_radar/engine/factors/` 下新增
4. 更新 `engine/strategies/*/scoring.py` 以支持新因子
5. 运行 `lsp_diagnostics` 确保无类型错误

## 禁止操作

- 不修改超出 `parameter_bounds` 的参数
- 不删除已有因子代码
- 不修改回测/诊断脚本
- 每次只做一个变更（单一变量原则）

## 工作流: APPLY — 应用变更

1. 读取 `manifest.yaml` 中的变更定义
2. 备份当前 config（git 即可追踪）
3. 应用变更到 `backend/alpha_lab/configs/<strategy>.yaml`
4. 验证配置合法性:
   - 权重在 `parameter_bounds.weights.min` ~ `max` 范围内
   - 正向因子权重总和在 `total_positive_weight.min` ~ `max` 范围内
   - 惩罚因子权重总和在 `total_penalty_weight.min` ~ `max` 范围内
   - `backtest_top_n` 在 `min` ~ `max` 范围内
5. 验证 YAML 可被 `config_loader.py` 正确解析

## 验证方法

```bash
docker compose exec api python3 -c "
from app.services.alpha_radar.engine.config_loader import load_strategy_config
cfg = load_strategy_config('<strategy>')
print(f'Loaded: {cfg[\"strategy\"]} v{cfg[\"version\"]}')
print(f'Positive factors: {len(cfg[\"scoring\"][\"positive_factors\"])}')
print(f'Penalties: {len(cfg[\"scoring\"][\"penalties\"])}')
"
```

## 配置文件结构

```yaml
strategy: dragon
version: "22"
score_column: dragon_score
eval_period: 20
requires_moneyflow: true
backtest_top_n: 4

scoring:
  positive_factors:
    - signal: <component_name>
      source: <column_name>
      transform: "<DSL expression>"  # optional
      fill_null: <default_value>
      weight: <0.02-0.30>
  penalties:
    - signal: <component_name>
      source: <column_name>
      transform: "<DSL expression>"  # optional
      fill_null: <default_value>
      weight: <0.02-0.30>

parameter_bounds:
  weights: { min: 0.02, max: 0.30, step: 0.01 }
  total_positive_weight: { target: X, min: Y, max: Z }
  total_penalty_weight: { target: X, min: Y, max: Z }
  backtest_top_n: { min: 3, max: 8 }
```

## Transform DSL 语法

支持的操作（`col` 是 `pl.col(source).fill_null(fill_null)` 的占位符）:
- 算术: `col + 5.0`, `col * 100`, `(1 - col) * 100`
- `.clip(lo, hi)`: `col.clip(0.0, 1.0)`
- `.abs()`: `col.abs()`
- 组合: `((col + 5.0) / 15.0).clip(0.0, 1.0) * 100`

## 文件路径参考

| 用途 | 路径 |
|------|------|
| 策略配置 | `backend/alpha_lab/configs/<strategy>.yaml` |
| 配置schema | `backend/alpha_lab/configs/_schema.yaml` |
| Config loader | `backend/app/services/alpha_radar/engine/config_loader.py` |
| 因子代码 | `backend/app/services/alpha_radar/engine/factors/` |
| 评分引擎 | `backend/app/services/alpha_radar/engine/strategies/*/scoring.py` |
