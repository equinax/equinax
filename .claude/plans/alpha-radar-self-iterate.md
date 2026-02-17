# Alpha Radar 策略自迭代

## 概述

完整的策略迭代循环：COLLECT → DIAGNOSE → PROPOSE → APPLY → BACKTEST → EVALUATE → DECIDE → DOCUMENT。
适用于 dragon / rally / weekly 三个策略。每次迭代只做**一个变更**（单一变量原则）。

## 前置条件

在开始之前，验证以下条件：

```bash
# 1. Docker 服务运行
docker compose ps  # 确认 quant_api, quant_db, quant_redis 状态 Up

# 2. 数据库有数据
docker compose exec api python3 -c "
from app.db.session import SessionLocal
from sqlalchemy import text
import asyncio
async def check():
    async with SessionLocal() as s:
        r = await s.execute(text('SELECT COUNT(*) FROM market_daily'))
        print(f'market_daily rows: {r.scalar()}')
        r = await s.execute(text('SELECT MAX(trade_date) FROM market_daily'))
        print(f'latest date: {r.scalar()}')
asyncio.run(check())
"

# 3. 基线回测正常
docker compose exec api python -m scripts.alpha_radar_backtest --seed 42 --tabs <STRATEGY>
```

## 变量

| 变量 | 说明 | 示例 |
|------|------|------|
| `<STRATEGY>` | 策略名 | dragon, rally, weekly |
| `<EXP_ID>` | 实验编号 | exp-20260217-001 |
| `<ITERATION>` | 当前迭代轮次 (1-5) | 1 |

---

## Step 1: COLLECT — 收集后验样本

**Skill**: alpha-analyst

### 目标
收集涨停/连板数据的因子快照，建立赢家/输家分组，生成归因分析。

### 执行

1. 读取 `backend/alpha_lab/theories/<STRATEGY>/theory.yaml`，了解当前版本、基线性能、已验证发现
2. 运行归因分析脚本（生成 cohort_analysis.yaml）：
   ```bash
   docker compose exec api python -m scripts.cohort_analysis --months 6 --strategy <STRATEGY>
   ```
3. 运行基线回测并记录当前指标：
   ```bash
   docker compose exec api python -m scripts.alpha_radar_backtest --seed 42 --tabs <STRATEGY>
   ```
4. 记录基线 metrics（WR, AR, P/L），与 theory.yaml 中的 `current_performance.metrics` 交叉验证

### 产出
- `backend/alpha_lab/theories/<STRATEGY>/cohort_analysis.yaml` — 因子归因数据
- 基线 metrics 备查

### 验证
- cohort_analysis.yaml 存在且 `yaml.safe_load()` 通过
- 基线回测数据与 theory.yaml 记录一致（允许 ±0.5pp 浮动，因数据更新）

---

## Step 2: DIAGNOSE — 归因分析

**Skill**: alpha-analyst

### 目标
分析因子区分度，找出最有潜力的改进方向，排除已证伪方向。

### 执行

1. 加载 `backend/alpha_lab/theories/<STRATEGY>/cohort_analysis.yaml`
2. 加载 `backend/alpha_lab/theories/<STRATEGY>/theory.yaml`
3. 分析因子区分度：
   - 从 cohort_analysis.yaml 中提取 `stable_signals`（Cohen's d ≥ 0.2, stability ≥ 0.8）
   - 与 theory.yaml 的 `factor_discrimination` 对比，发现新信号或确认已知信号
4. 排除约束检查：
   - 检查 `validated_findings.ineffective` 中所有 `never_retry: true` 的条目
   - 这些方向**绝对不能**再次尝试
5. 分析各 signal YAML（`backend/alpha_lab/theories/<STRATEGY>/signals/*.yaml`）的 `bounds` 和 `evidence`
6. 输出诊断报告：哪些因子有信号、哪些方向可探索、哪些必须回避

### 产出
- 诊断报告（内容可以直接在对话中输出，不需要独立文件）
- 候选变更方向列表（按潜力排序）

### 关键约束
- 已标记 `never_retry: true` 的方向 → **跳过**，不讨论
- 输出必须引用具体数据点（Cohen's d 值、均值差异），不允许主观臆断
- 如果没有有潜力的方向 → 提前终止，记录 "无可行改进方向"

---

## Step 3: PROPOSE — 提出假设

**Skill**: alpha-analyst

### 目标
选择单一变更，生成实验 manifest。

### 执行

1. 从 DIAGNOSE 结果中选择**最有潜力的单一变更**
2. 检查对应 signal YAML 的 `parameter_bounds`：
   - `weight_min` / `weight_max` 范围
   - `total_positive_weight` / `total_penalty_weight` 约束
3. 创建实验目录：
   ```bash
   mkdir -p backend/alpha_lab/experiments/<STRATEGY>/<EXP_ID>/
   ```
4. 复制模板并填写 manifest：
   ```bash
   cp backend/alpha_lab/experiments/_template/manifest.yaml \
      backend/alpha_lab/experiments/<STRATEGY>/<EXP_ID>/manifest.yaml
   ```
5. 填入：
   - `experiment_id`: `<EXP_ID>`
   - `strategy`: `<STRATEGY>`
   - `parent_version`: 从 theory.yaml 获取
   - `hypothesis`: 描述假设和预期效果（引用 DIAGNOSE 数据）
   - `changes`: 具体参数变更路径和值
   - `backtest_params`: seed=42, tabs=[strategy]

### ⚠️ 人类确认点

展示以下信息并**等待用户确认**后才继续：
```
假设: [hypothesis 描述]
变更: [从X改为Y]
依据: [Cohen's d = ?, stability = ?]
预期: [WR +?pp, AR +?%]
风险: [可能的负面影响]

是否继续？
```

如果用户拒绝 → 回到 Step 2 选择其他方向，或终止流程。

### 产出
- `backend/alpha_lab/experiments/<STRATEGY>/<EXP_ID>/manifest.yaml`

---

## Step 4: APPLY — 应用变更

**Skill**: alpha-coder

### 目标
将 manifest 中的变更应用到策略配置。

### 执行

1. 读取 `backend/alpha_lab/experiments/<STRATEGY>/<EXP_ID>/manifest.yaml`
2. 记录当前配置快照（git 追踪即可）：
   ```bash
   git diff backend/alpha_lab/configs/<STRATEGY>.yaml  # 确认无未提交变更
   ```
3. 应用变更到 `backend/alpha_lab/configs/<STRATEGY>.yaml`
4. 验证配置合法性：
   - 每个权重在 `parameter_bounds.weights.min` ~ `max` 范围内
   - 正向因子权重总和在 `total_positive_weight.min` ~ `max` 范围内
   - 惩罚因子权重总和在 `total_penalty_weight.min` ~ `max` 范围内
   - `backtest_top_n` 在 `min` ~ `max` 范围内
5. 验证配置可解析：
   ```bash
   docker compose exec api python3 -c "
   from app.services.alpha_radar.engine.config_loader import load_strategy_config
   cfg = load_strategy_config('<STRATEGY>')
   print(f'Loaded: {cfg[\"strategy\"]} v{cfg[\"version\"]}')
   print(f'Positive factors: {len(cfg[\"scoring\"][\"positive_factors\"])}')
   print(f'Penalties: {len(cfg[\"scoring\"][\"penalties\"])}')
   "
   ```

### 产出
- 修改后的 `backend/alpha_lab/configs/<STRATEGY>.yaml`

### 验证
- config_loader 解析成功
- 无 YAML 语法错误
- 所有参数在 bounds 范围内

### 回滚
如果验证失败：
```bash
git checkout backend/alpha_lab/configs/<STRATEGY>.yaml
```

---

## Step 5: BACKTEST — 运行回测

**Skill**: alpha-evaluator

### 目标
使用修改后的配置运行回测，获取性能指标。

### 执行

1. 运行主回测（与基线参数一致）：
   ```bash
   docker compose exec api python -m scripts.alpha_radar_backtest \
     --seed 42 --tabs <STRATEGY>
   ```
2. 从输出中解析：
   - `win_rate`: 胜率百分比
   - `avg_return`: 平均收益百分比
   - `profit_loss_ratio`: 盈亏比（如有）
3. 将结果写入 manifest.yaml 的 `results` 部分

### 可选：多 Seed 验证
如果主回测结果接近边界（accept/reject 阈值附近），额外运行：
```bash
for seed in 123 7 99; do
  docker compose exec api python -m scripts.alpha_radar_backtest --seed $seed --tabs <STRATEGY>
done
```

### 产出
- manifest.yaml 的 `results` 部分填充完毕

---

## Step 6: EVALUATE — 评估结果

**Skill**: alpha-evaluator

### 目标
对比实验结果与基线，按决策规则给出建议。

### 执行

1. 获取基线指标：从 `theory.yaml` 的 `current_performance.metrics`
2. 计算 delta：
   - `win_rate_delta = results.win_rate - baseline.win_rate`
   - `avg_return_delta = results.avg_return - baseline.avg_return`
3. 按决策规则判断：

   **accept**（必须同时满足）：
   - `win_rate_delta >= 0`
   - `avg_return_delta >= 0`
   - 主目标有改善（`primary_metric_improvement = true`）
   - 其他指标回退 ≤ 1pp（`no_regression_tolerance = -1.0`）

   **reject**（满足任一即可）：
   - `win_rate_delta < -2.0`（WR 下降超过 2pp）
   - `avg_return_delta < -1.0`（AR 下降超过 1%）

   **iterate**：以上都不满足时，继续下一轮

4. 填入 manifest.yaml：
   ```yaml
   results:
     win_rate: <实测值>
     avg_return: <实测值>
     profit_loss_ratio: <实测值>
     vs_baseline:
       win_rate_delta: <差值>
       avg_return_delta: <差值>
   decision: accept | reject | iterate
   decision_reason: "<具体原因，引用数值>"
   ```

### 产出
- manifest.yaml 完整填充（含 decision 和 decision_reason）
- 对比报告（在对话中输出）

---

## Step 7: DECIDE — 执行决策

基于 EVALUATE 的 decision 值，执行对应操作：

### accept → 进入 Step 8
- 保留配置变更
- 准备更新 theory.yaml

### reject → 进入 Step 8
- **立即回滚**配置：
  ```bash
  git checkout backend/alpha_lab/configs/<STRATEGY>.yaml
  ```
- 记录失败原因

### iterate → 回到 Step 2 或 Step 3
- 迭代计数器 +1
- 如果 `<ITERATION> < 5`：
  - 如果需要全新方向 → 回到 Step 2 (DIAGNOSE)
  - 如果在同一方向上微调 → 回到 Step 3 (PROPOSE)
- 如果 `<ITERATION> >= 5`：
  - **强制终止**：`decision = reject_and_document`
  - 回滚配置
  - 进入 Step 8

### 决策摘要输出
```
┌──────────────────────────────────────┐
│ 实验: <EXP_ID>                       │
│ 迭代: <ITERATION>/5                  │
│ 决策: accept / reject / iterate      │
│ WR: baseline% → new% (Δ+/-pp)       │
│ AR: baseline% → new% (Δ+/-%)        │
│ 原因: <decision_reason>              │
└──────────────────────────────────────┘
```

---

## Step 8: DOCUMENT — 归档记录

**Skill**: alpha-analyst

### 目标
记录实验过程，更新知识库，确保迭代记忆持久化。

### 执行

#### 所有结果（accept + reject）

1. 在实验目录生成 `narrative.md`：
   - 实验 ID、日期、策略、父版本
   - 假设描述
   - 变更内容
   - 回测结果（WR, AR, P/L, delta）
   - 决策和原因
   - 关键观察和教训

#### 仅 accept

2. 更新 `theory.yaml`：
   - `version` +1
   - `current_performance.metrics` 更新为新值
   - 追加到 `validated_findings.effective`
   - 追加到 `iteration_milestones`
3. 更新对应 signal YAML 的 `evidence` 列表
4. 验证更新后的配置仍然通过基线测试：
   ```bash
   docker compose exec api python -m scripts.alpha_radar_backtest --seed 42 --tabs <STRATEGY>
   ```

#### 仅 reject

2. 追加到 `theory.yaml` 的 `validated_findings.ineffective`
3. 如果连续失败且方向已穷尽 → 标记 `never_retry: true`
4. 确认配置已回滚（与 Step 7 一致）

### 产出
- `backend/alpha_lab/experiments/<STRATEGY>/<EXP_ID>/narrative.md`
- 更新后的 `backend/alpha_lab/theories/<STRATEGY>/theory.yaml`（如 accept）
- 更新后的 signal YAML（如 accept）

---

## 各策略基线参考

| 策略 | Seed | WR | AR | P/L | 评估周期 |
|------|------|-----|------|-----|---------|
| dragon | 42 | 62.5% | 5.90% | 3.82 | T+20 |
| rally | 42 | 64.0% | 3.31% | — | T+10 |
| weekly | 42 | 67.6% | 1.31% | 3.33 | T+5 |

## 文件路径速查

| 用途 | 路径 |
|------|------|
| 理论文件 | `backend/alpha_lab/theories/<STRATEGY>/theory.yaml` |
| 信号定义 | `backend/alpha_lab/theories/<STRATEGY>/signals/*.yaml` |
| 策略配置 | `backend/alpha_lab/configs/<STRATEGY>.yaml` |
| 配置 schema | `backend/alpha_lab/configs/_schema.yaml` |
| 实验模板 | `backend/alpha_lab/experiments/_template/manifest.yaml` |
| 涨停归因 | `backend/alpha_lab/theories/<STRATEGY>/cohort_analysis.yaml` |
| Config loader | `backend/app/services/alpha_radar/engine/config_loader.py` |
| 归因脚本 | `docker compose exec api python -m scripts.cohort_analysis` |
| 回测脚本 | `docker compose exec api python -m scripts.alpha_radar_backtest` |
| Dragon 诊断 | `docker compose exec api python -m scripts.dragon_diagnostic` |
| Rally 诊断 | `docker compose exec api python -m scripts.rally_diagnostic` |
