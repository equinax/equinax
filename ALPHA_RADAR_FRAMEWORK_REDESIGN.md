# Alpha Radar 自迭代推荐框架设计

> 让 AI Agent 自主完成「假设 → 编码 → 回测 → 归因 → 迭代」的闭环，将 21 次手动 dragon 迭代压缩到一条可重放的流水线。

---

## 一、现状评估

### 做得好的（保留）

| 组件 | 现状 | 评价 |
|------|------|------|
| 策略/因子/评分分层 | `engine/factors/` + `engine/strategies/*/scoring.py` | ✅ 结构清晰，扩展点明确 |
| Theory.md | 每个策略一份，记录迭代历史和已验证发现 | ✅ 知识沉淀好 |
| 回测基础设施 | `alpha_radar_backtest.py`，批量加载+多日评估 | ✅ 生产级 |
| 诊断脚本 | `dragon_diagnostic.py` 做 winners vs losers 分析 | ✅ 已有后验分析能力 |
| 因子模块化 | 每个因子域独立文件 | ✅ 但文档性质强于可执行性 |

### 需要改进的

| 问题 | 影响 |
|------|------|
| 评分公式硬编码在 `scoring.py` 里 | 每次调参必须改代码，无法声明式驱动 |
| 理论记录是自由文本 | AI 无法程序化读取已验证结论，容易重复试错 |
| 迭代过程全靠人类+AI对话驱动 | 无法自动化、无法重放、无法并行实验 |
| 实验无结构化存档 | 无法对比实验、无法回溯具体参数快照 |
| 因子启停需改代码 | 不能声明式开关某个因子 |

---

## 二、你的期望中需要注意的问题

### 2.1 合理且可行的部分

1. **后验到先验的归因流程** — 这就是 `dragon_diagnostic.py` PART 4 已经在做的事。将它产品化为可复用 Skill 完全可行。
2. **理论结构化存储** — 当前 `theory.md` 升级为 `theory.yaml` + `theory.md` 双格式完全合理。
3. **因子可插拔** — 将权重/启停从代码提取到配置文件是标准重构。
4. **实验可追踪** — 基于文件系统的实验记录，git 友好，agent 友好。

### 2.2 需要修正的期望

#### ⚠️ "AI 自动改代码 → 跑回测 → 循环" 的风险

Dragon 的 21 次迭代历史证明：**全公式重设计是灾难性的**（WR 降 8-14pp），非线性变换也是灾难性的。真正有效的优化都是「一次改一个参数，小步验证」。

**建议**：将 95% 的迭代限制在 **配置层面**（权重调整、因子启停、阈值变更），而非让 AI 自由修改 Python 代码。只有在发现全新的因子信号时才允许代码级变更，且必须走 PR 审核。

#### ⚠️ "决策 Skill" 的范围界定

一个"决定当前应该做什么"的 Skill 本质上是一个编排器（Orchestrator）。如果它的决策空间太大（可以选择任意下一步），等于把所有问题甩给了一个万能入口，反而不可控。

**建议**：不要做一个自由决策器，而是做一个 **固定阶段流水线 + 阶段内可选动作**。流水线的步骤是固定的（收集 → 诊断 → 假设 → 应用 → 回测 → 评估 → 决定 → 归档），每一步内部可以有策略选择。

#### ⚠️ 后验分析的数据泄露问题

"看到今天涨停的股票 → 回到 20 天前 → 提取共性"这个流程如果不严格隔离发现期和验证期，会产生严重的数据泄露：

**建议**：
- **发现集**：用 60% 的历史涨停数据提取共性
- **验证集**：用剩余 40% 评估理论有效性
- **样本外测试**：用最近 N 天的真实推荐进行样本外验证
- 每个 Experiment 必须记录其使用的时间窗口

---

## 三、架构设计

### 3.1 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                    Agent Layer (Skills + Plans)               │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Analyst  │  │ Coder    │  │ Evaluator│  │Orchestrtr│    │
│  │  Skill   │  │  Skill   │  │  Skill   │  │  Plan    │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
│       │              │              │              │          │
├───────┼──────────────┼──────────────┼──────────────┼──────────┤
│       ▼              ▼              ▼              ▼          │
│              Knowledge Layer (Theories + Experiments)         │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  theories/                    experiments/               │ │
│  │  ├── dragon/                  ├── exp-2026-0217-001/    │ │
│  │  │   ├── theory.yaml          │   ├── manifest.yaml    │ │
│  │  │   ├── theory.md            │   ├── config_snapshot/  │ │
│  │  │   └── signals/             │   ├── results/          │ │
│  │  │       ├── sector_momentum  │   └── narrative.md      │ │
│  │  │       └── trend_quality    └──────────────────────── │ │
│  │  └── rally/                                             │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│              Execution Layer (Existing Engine, preserved)     │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │PolarsEng │  │ScoringEng│  │ Backtest │  │  Diag    │    │
│  │(指标计算) │  │(评分引擎) │  │(回测评估) │  │(因子诊断) │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│                                                              │
│  ← 这一层基本不动，只做「配置驱动化」改造 →                    │
└──────────────────────────────────────────────────────────────┘
```

### 3.2 原则

1. **Overlay，不替换** — 现有引擎作为稳定执行核心，新框架包裹在外层
2. **声明式优先** — 95% 的迭代通过配置变更完成，不改代码
3. **文件系统即数据库** — 实验/理论存储在 git 仓库内，agent 友好
4. **每次实验可重放** — 所有输入（配置、数据窗口、随机种子）都被记录
5. **人类始终在环** — Agent 提议变更，人类审核后促进

---

## 四、Code Structure 详细设计

### 4.1 目录结构

```
backend/
├── alpha_lab/                          # ← 新增：AI实验室根目录
│   ├── theories/                       # 理论存储
│   │   ├── dragon/
│   │   │   ├── theory.yaml             # 结构化理论定义（AI可读写）
│   │   │   ├── theory.md               # 人类可读理论文档（从yaml生成+补充）
│   │   │   └── signals/                # 信号定义目录
│   │   │       ├── sector_momentum.yaml
│   │   │       ├── trend_quality.yaml
│   │   │       └── _archive/           # 已证伪的信号
│   │   │           └── elg_flow.yaml
│   │   ├── rally/
│   │   │   ├── theory.yaml
│   │   │   ├── theory.md
│   │   │   └── signals/
│   │   └── weekly/
│   │       ├── theory.yaml
│   │       ├── theory.md
│   │       └── signals/
│   │
│   ├── experiments/                    # 实验存档
│   │   ├── dragon/
│   │   │   ├── exp-20260217-001/       # 单次实验目录
│   │   │   │   ├── manifest.yaml       # 实验元数据（假设、参数、时间窗口）
│   │   │   │   ├── config.yaml         # 本次实验使用的完整配置快照
│   │   │   │   ├── baseline.yaml       # 基线配置快照
│   │   │   │   ├── results.yaml        # 回测结果（WR/AR/P-L + per-stock）
│   │   │   │   ├── diagnosis.yaml      # 因子诊断结果
│   │   │   │   └── narrative.md        # 实验叙述和结论
│   │   │   └── exp-20260217-002/
│   │   └── rally/
│   │
│   ├── configs/                        # 策略配置（声明式）
│   │   ├── dragon.yaml                 # 当前生效的 dragon 配置
│   │   ├── rally.yaml
│   │   ├── weekly.yaml
│   │   └── _schema.yaml                # 配置 schema（约束参数范围）
│   │
│   └── cohorts/                        # 股票群组定义（用于归因分析）
│       ├── limit_up_leaders.yaml       # 涨停龙头群组定义
│       └── sector_rotators.yaml        # 板块轮动群组定义
│
├── app/services/alpha_radar/           # ← 现有代码，做配置驱动化改造
│   ├── engine/
│   │   ├── config_loader.py            # 新增：从 yaml 加载策略配置
│   │   ├── strategies/
│   │   │   └── dragon/
│   │   │       └── scoring.py          # 改造：读取 configs/dragon.yaml
│   │   └── ...
│   └── ...
```

### 4.2 Theory YAML 格式

```yaml
# alpha_lab/theories/dragon/theory.yaml
strategy: dragon
version: "22"           # 对应当前迭代版本
target: "T+20"
objective:
  win_rate: ">60%"
  avg_return: ">5%"
  profit_loss_ratio: ">3"

current_performance:
  win_rate: 62.5
  avg_return: 5.90
  profit_loss_ratio: 3.82
  eval_dates: 25
  seed: 42

core_thesis: |
  发掘中线即将起爆涨停的潜力票。
  关注主力资金吸筹蓄力、突破前的量价配合、板块热度轮动。
  避开分配阶段。

active_signals:
  - ref: signals/sector_momentum.yaml
  - ref: signals/trend_quality.yaml
  - ref: signals/anti_climax.yaml
  - ref: signals/main_strength.yaml
  - ref: signals/accumulation.yaml
  - ref: signals/consistency.yaml
  - ref: signals/buildup.yaml
  - ref: signals/elg_flow.yaml

penalties:
  - ref: signals/recent_spike_penalty.yaml
  - ref: signals/surge_penalty.yaml
  - ref: signals/ceiling_penalty.yaml
  - ref: signals/resistance_penalty.yaml

filters:
  - column: pct_chg
    op: abs_lte
    value: 5.0
  - column: near_limit_up
    op: eq
    value: false
  - column: is_st
    op: neq
    value: 1

backtest_top_n: 4

# 知识库：已验证的发现（AI不应重复试错）
validated_findings:
  effective:
    - id: sector_momentum_flip
      description: "板块动量从负信号翻转为正信号，权重递增至0.17均有提升"
      iteration: 3-8
    - id: reduce_main_strength
      description: "降低主力强度权重(0.15→0.10)，因为赢家的主力强度反而更低"
      iteration: 6
    - id: reduce_breakthrough_penalties
      description: "降低ceiling/resistance惩罚，对龙头候选者来说是突破信号"
      iteration: 19-21
    - id: top_n_tighten
      description: "Top-N从5→4，第5名一贯最弱"
      iteration: 14

  ineffective:
    - id: full_formula_redesign
      description: "全公式重设计灾难性(WR降8-14pp)"
      iteration: "multiple"
      never_retry: true
    - id: nonlinear_transforms
      description: "反转MS、封顶TQ等非线性变换灾难性"
      iteration: 11-12
      never_retry: true
    - id: interaction_terms
      description: "TQ×SM交互项替代ELG灾难性(AR=0.70%)"
      iteration: 13
      never_retry: true
    - id: return_5d_penalty
      description: "已涨=对龙头是特征而非缺陷"
      iteration: 5,10
      never_retry: true

factor_discrimination:
  - factor: trend_quality_20d
    winner_mean: 68.6
    loser_mean: 59.6
    delta: +8.96
    verdict: "最佳区分"
  - factor: sector_momentum_5d
    winner_mean: 1.66
    loser_mean: 0.32
    delta: +1.34
    verdict: "板块热=赢"
  - factor: main_strength_proxy
    winner_mean: 79.4
    loser_mean: 83.0
    delta: -3.6
    verdict: "低=赢"
  - factor: volume_consistency
    delta: ~0
    verdict: "无区分"
```

### 4.3 Signal YAML 格式

```yaml
# alpha_lab/theories/dragon/signals/sector_momentum.yaml
signal_id: sector_momentum
display_name: "板块动量"
description: "板块5日动量，归一化为正向信号。最可靠的优化杠杆。"

source_column: sector_momentum_5d
transform: "((value + 5.0) / 15.0).clip(0, 1) * 100"
component_name: sector_momentum_positive

weight: 0.17
direction: positive     # positive = 越高越好, negative = 越低越好

bounds:                 # AI 调参时的硬约束
  weight_min: 0.05
  weight_max: 0.25

evidence:
  - "Iter 3: 引入后 WR=57%, P/L提升至4.03"
  - "Iter 8: 权重提升至0.17均有改善"
  - "权重0.20开始过冲"

status: active          # active | archived | experimental
```

### 4.4 Strategy Config YAML 格式（声明式驱动评分）

```yaml
# alpha_lab/configs/dragon.yaml
strategy: dragon
version: "22"
description: "龙头涨停 — 主力吸筹+突破蓄力+量价一致"

score_column: dragon_score
eval_period: 20
requires_moneyflow: true
backtest_top_n: 4

# 评分公式：从这里生成，不再硬编码
scoring:
  positive_factors:
    - signal: sector_momentum_positive
      source: sector_momentum_5d
      transform: "((col + 5.0) / 15.0).clip(0.0, 1.0) * 100"
      weight: 0.17
    - signal: trend_quality
      source: trend_quality_20d
      fill_null: 50.0
      weight: 0.15
    - signal: anti_climax
      source: climax_score
      transform: "(1 - col) * 100"
      fill_null: 0.0
      weight: 0.15
    - signal: main_strength
      source: main_strength_proxy
      fill_null: 50.0
      weight: 0.10
    - signal: accumulation
      source: accumulation_score
      transform: "col * 100"
      fill_null: 0.0
      weight: 0.10
    - signal: consistency
      source: volume_consistency_score
      fill_null: 50.0
      weight: 0.05
    - signal: buildup
      source: volume_buildup_quality
      fill_null: 40.0
      weight: 0.05
    - signal: elg_flow
      source: elg_net_percentile
      fill_null: 50.0
      weight: 0.05

  penalties:
    - signal: recent_spike
      source: recent_vol_spike_max
      fill_null: 0.0
      weight: 0.10
    - signal: surge
      source: pct_chg
      transform: "((col.abs() - 3.0).clip(0.0, 4.0) / 4.0) * 100"
      fill_null: 0.0
      weight: 0.08
    - signal: ceiling
      source: price_position_60d
      transform: "((col - 0.80).clip(0.0, 0.20) / 0.20) * 100"
      fill_null: 0.5
      weight: 0.05
    - signal: resistance
      source: resistance_proximity_penalty
      fill_null: 0.0
      weight: 0.05

  filters:
    - column: pct_chg
      op: abs_lte
      value: 5.0
    - column: near_limit_up
      op: eq
      value: false
    - column: is_st
      op: neq
      value: 1

  regime_discount:
    enabled: true
    weight: 0.20

# 参数边界（AI自动调参时的硬约束）
parameter_bounds:
  weights:
    min: 0.02
    max: 0.30
    step: 0.01
  total_positive_weight:
    min: 0.70
    max: 0.90
  total_penalty_weight:
    min: 0.15
    max: 0.40
  backtest_top_n:
    min: 3
    max: 8
```

### 4.5 Experiment Manifest 格式

```yaml
# alpha_lab/experiments/dragon/exp-20260217-001/manifest.yaml
experiment_id: exp-20260217-001
strategy: dragon
created_at: "2026-02-17T10:30:00+08:00"
parent_version: "22"        # 基于哪个版本做的实验

hypothesis: |
  将 sector_momentum 权重从 0.17 提升到 0.19，
  因为该因子是最可靠的优化杠杆且历史验证到0.17均有提升。

changes:
  - path: "scoring.positive_factors[sector_momentum_positive].weight"
    from: 0.17
    to: 0.19

backtest_params:
  dates: 25
  seed: 42
  tabs: ["dragon"]

results:
  win_rate: null        # 回测后填入
  avg_return: null
  profit_loss_ratio: null
  vs_baseline:
    win_rate_delta: null
    avg_return_delta: null

decision: null          # accepted | rejected | needs_more_data
decision_reason: null
```

---

## 五、Workflow 设计

### 5.1 主流水线（8个阶段）

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                   Orchestrator Plan                             │
  │                                                                 │
  │   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐           │
  │   │COLLECT │──▶│DIAGNOSE│──▶│PROPOSE │──▶│ APPLY  │           │
  │   │收集数据 │   │归因分析 │   │提出假设 │   │应用变更 │           │
  │   └────────┘   └────────┘   └────────┘   └────────┘           │
  │                                               │                 │
  │                                               ▼                 │
  │   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐           │
  │   │DOCUMENT│◀──│DECIDE  │◀──│EVALUATE│◀──│BACKTEST│           │
  │   │归档记录 │   │接受/拒绝│   │评估结果 │   │运行回测 │           │
  │   └────────┘   └────────┘   └────────┘   └────────┘           │
  │       │              │                                          │
  │       │         ┌────┴────┐                                     │
  │       │         │ ITERATE │──▶ 回到 DIAGNOSE 或 PROPOSE         │
  │       │         │(继续迭代)│                                     │
  │       │         └─────────┘                                     │
  │       │                                                         │
  │       ▼                                                         │
  │   [完成] 输出最终实验报告                                         │
  └─────────────────────────────────────────────────────────────────┘
```

### 5.2 各阶段职责

| 阶段 | Skill | 输入 | 输出 | 可迭代 |
|------|-------|------|------|--------|
| **COLLECT** | analyst | 策略名, 时间范围 | 涨停/连板股列表 + T-N日因子快照 | 否 |
| **DIAGNOSE** | analyst | COLLECT 输出 + 现有理论 | 因子区分度报告 + 信号假设 | 是 |
| **PROPOSE** | analyst | DIAGNOSE 输出 + 已验证发现 | 具体参数变更提案 (manifest.yaml) | 是 |
| **APPLY** | coder | PROPOSE 的 manifest + 当前config | 更新后的 config.yaml | 否 |
| **BACKTEST** | evaluator | config.yaml + 回测参数 | results.yaml (WR/AR/P-L) | 否 |
| **EVALUATE** | evaluator | results.yaml + baseline | 对比报告 + 达标判断 | 否 |
| **DECIDE** | orchestrator | EVALUATE 输出 + 目标阈值 | accept/reject/iterate 决定 | 否 |
| **DOCUMENT** | analyst | 全流程产物 | narrative.md + theory更新 | 否 |

### 5.3 决策规则（DECIDE 阶段）

```yaml
# 不是自由决策，而是规则驱动
promotion_rules:
  # 必须满足全部条件才能 accept
  accept:
    - win_rate_delta: ">= 0"          # 不能比基线差
    - avg_return_delta: ">= 0"        # 不能比基线差
    - primary_metric_improvement: true # 主目标必须有改善
    - no_regression:                   # 其他指标不能大幅回退
        tolerance: -1.0               # 允许任一指标回退不超过1pp

  # 满足任一条件就 reject
  reject:
    - win_rate_delta: "< -2.0"        # WR 下降超过 2pp
    - avg_return_delta: "< -1.0"      # AR 下降超过 1%

  # 否则 iterate（最多 N 轮）
  max_iterations: 5
  max_iterations_action: "reject_and_document"
```

### 5.4 后验归因流程（COLLECT + DIAGNOSE 详解）

以 Dragon「龙头涨停」为例：

```
第一步 COLLECT — 收集后验样本
─────────────────────────────
输入: strategy=dragon, lookback_months=12
过程:
  1. 从 limit_list_daily 找出过去12个月所有涨停股
  2. 按连板天数分层 (1板, 2板, 3+板)
  3. 对每只涨停股，回溯到首次涨停前 T-5/T-10/T-20 日
  4. 提取那个时间点的全部因子快照（技术指标+资金面+板块面）
  5. 同时随机采样同日期的非涨停股作为对照组
输出: cohort_analysis.yaml (涨停组因子均值 vs 对照组因子均值)

第二步 DIAGNOSE — 归因分析
──────────────────────────
输入: cohort_analysis.yaml + theory.yaml (现有理论)
过程:
  1. 计算每个因子在涨停组vs对照组的区分度 (Cohen's d)
  2. 与现有 factor_discrimination 对比，发现新信号或确认旧信号
  3. 检查 validated_findings.ineffective，排除已证伪方向
  4. 分发现集(60%)和验证集(40%)，避免数据泄露
输出: 
  - diagnosis_report.yaml (因子区分度排名)
  - signal_candidates (候选新信号)
  - anti_patterns (应避免的方向)

关键约束:
  - 发现集和验证集时间窗口严格分离
  - 已标记 never_retry 的方向必须跳过
  - 输出必须引用具体数据点，不允许主观臆断
```

---

## 六、Agent Infra 设计

### 6.1 Skill 定义

#### Skill 1: `alpha-analyst` — 数据分析与理论生成

```
触发: 需要分析涨停/因子/归因/诊断时
能力:
  - 读取数据库（通过 docker compose exec api 调用诊断脚本）
  - 读写 alpha_lab/theories/ 下的 YAML/MD 文件
  - 读写 alpha_lab/cohorts/ 下的群组定义
  - 执行 alpha_radar_backtest.py / dragon_diagnostic.py
  - 对比实验结果与基线

禁止:
  - 不得修改 scoring.py 或任何引擎代码
  - 不得修改 alpha_lab/configs/ 下的生效配置
  - 不得直接操作数据库
```

#### Skill 2: `alpha-coder` — 配置编码与引擎适配

```
触发: 理论/假设确定后，需要转化为可执行配置时
能力:
  - 读写 alpha_lab/configs/*.yaml
  - 在约束范围内调整参数 (parameter_bounds)
  - 当需要新因子时，可在 engine/factors/ 下新增因子代码
  - 更新 engine/strategies/*/scoring.py 以支持新因子
  - 运行 lsp_diagnostics 确保无类型错误

禁止:
  - 不得修改超出 parameter_bounds 的参数
  - 不得删除已有因子代码
  - 不得修改回测/诊断脚本
  - 每次只能做一个变更（单一变量原则）
```

#### Skill 3: `alpha-evaluator` — 回测执行与结果评估

```
触发: 配置变更完成后，需要评估效果时
能力:
  - 执行 alpha_radar_backtest.py (docker compose exec api)
  - 解析回测输出为结构化结果
  - 与基线对比计算 delta
  - 生成 results.yaml 和对比报告
  - 按 promotion_rules 给出 accept/reject/iterate 建议

禁止:
  - 不得修改任何代码或配置
  - 不得改变回测参数（使用 manifest 中指定的参数）
```

### 6.2 Plan：Dragon 自迭代流程

```yaml
# 这是一个 Agent Plan，定义了完整的迭代工作流
name: dragon-self-iterate
description: |
  Dragon 策略自迭代流程。
  从后验归因开始，经过假设→编码→回测→评估→决策的完整循环。

prerequisites:
  - Docker 服务运行中
  - 数据库有最近12个月的 market_daily + limit_list_daily 数据

steps:
  1_collect:
    skill: alpha-analyst
    action: |
      1. 读取 alpha_lab/theories/dragon/theory.yaml 了解当前理论状态
      2. 从数据库中获取最近6个月的涨停数据
      3. 对涨停股回溯 T-5~T-20 提取因子快照
      4. 生成 cohort_analysis 输出到实验目录

  2_diagnose:
    skill: alpha-analyst
    action: |
      1. 加载 cohort_analysis 和现有 theory.yaml
      2. 计算因子区分度，与已知发现对比
      3. 检查 never_retry 清单，排除已证伪方向
      4. 输出诊断报告和信号候选

  3_propose:
    skill: alpha-analyst
    action: |
      1. 基于诊断结果，选择最有潜力的单一变更
      2. 检查 parameter_bounds 确保变更在允许范围内
      3. 生成 manifest.yaml（假设、变更、预期效果）
      4. 需要人类确认才能继续（或在自动模式下跳过）

  4_apply:
    skill: alpha-coder
    action: |
      1. 读取 manifest.yaml 中的变更定义
      2. 复制当前 config 为 baseline
      3. 应用变更到 config 副本
      4. 验证配置合法性（schema 校验）

  5_backtest:
    skill: alpha-evaluator
    action: |
      1. 使用修改后的 config 运行回测
      2. 参数: --seed 42 --tabs dragon (与基线一致)
      3. 解析输出为 results.yaml

  6_evaluate:
    skill: alpha-evaluator
    action: |
      1. 对比 results.yaml 与 baseline
      2. 按 promotion_rules 判断结果
      3. 输出评估报告

  7_decide:
    decision_rules:
      accept: "提交变更，更新 theory.yaml 版本号"
      reject: "回滚配置，记录失败原因"
      iterate: "回到 step 3，尝试不同假设（最多5轮）"

  8_document:
    skill: alpha-analyst
    action: |
      1. 生成 narrative.md 记录完整实验过程
      2. 如果 accepted，更新 theory.yaml 和 theory.md
      3. 将实验结果追加到 theory.yaml 的 validated_findings
```

---

## 七、实施路径

### Phase 1：配置驱动化改造（~1天）

**目标**：让现有评分引擎可以从 YAML 配置驱动，而非硬编码。

1. 创建 `alpha_lab/` 目录结构
2. 从现有 `scoring.py` 提取配置到 `configs/dragon.yaml` / `rally.yaml` / `weekly.yaml`
3. 实现 `config_loader.py`：读取 YAML → 生成 Polars 评分表达式
4. 修改 `DragonScoringEngine.score()` 为配置驱动模式
5. 验证：配置驱动的输出与硬编码输出完全一致

**验收标准**：`alpha_radar_backtest.py --seed 42` 结果与改造前完全一致。

### Phase 2：理论结构化 + 实验框架（~1天）

**目标**：将现有 theory.md 转为 theory.yaml + 实验框架。

1. 将三个 `theory.md` 内容结构化到 `theory.yaml`
2. 将每个因子定义为独立的 `signals/*.yaml`
3. 创建 `_schema.yaml` 定义参数约束
4. 创建实验模板（manifest.yaml 的空模板）
5. 将 `dragon_diagnostic.py` 的输出格式化为 diagnosis.yaml

**验收标准**：theory.yaml 完整覆盖 theory.md 中的所有信息。

### Phase 3：Agent Skills 实现（~1-2天）

**目标**：将分析-编码-评估封装为可复用的 Agent Skills。

1. 创建 `alpha-analyst` skill 文件
2. 创建 `alpha-coder` skill 文件
3. 创建 `alpha-evaluator` skill 文件
4. 创建 Orchestrator Plan（dragon 自迭代）
5. 端到端测试：跑通一轮完整的 collect → document 流程

**验收标准**：Agent 可以自主完成一轮 Dragon 策略迭代（带人类确认门控）。

### Phase 4：归因能力建设（~1天）

**目标**：实现后验归因分析的结构化管道。

1. 新建 `backend/scripts/cohort_analysis.py` — 从涨停股回溯提取因子
2. 输出标准格式（cohort_analysis.yaml）
3. 集成到 analyst skill 中
4. 加入数据泄露防护（发现集/验证集分离）

**验收标准**：给定一个策略方向，Agent 可以自动完成后验归因并输出结构化报告。

---

## 八、关键设计决策总结

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 框架定位 | Overlay (包裹) | 现有引擎成熟，不值得重写 |
| 理论格式 | YAML(机器) + MD(人类) | 兼顾可编程和可读性 |
| 迭代方式 | 声明式配置变更优先 | Dragon 经验证明代码级变更风险高 |
| 实验存储 | 文件系统 + git | Docker 环境友好，天然版本控制 |
| Agent 自主度 | 提议变更，默认需人类确认 | 可配置为自动模式（带严格护栏） |
| 因子管理 | 每因子独立 YAML | 清晰的启停/调参/归档 |
| 防重复试错 | theory.yaml 中 never_retry 清单 | 避免 AI 重蹈覆辙 |
| 数据泄露 | 强制发现/验证集分离 | 后验分析的核心风险 |
| 评分引擎 | 配置驱动 + 插件式 | 95% 配置变更 + 5% 新因子代码 |
| 变更安全 | parameter_bounds + promotion_rules | 硬约束 + 自动化门控 |

---

## 九、与现有代码的关系

```
现有代码（不删除）                    新增/改造
─────────────                      ──────────
engine/strategies/*/scoring.py  →  改造为读取 configs/*.yaml
engine/strategies/*/theory.md   →  保留作为人类文档，从 theory.yaml 同步
engine/factors/*.py             →  保留，新因子同样在此新增
engine/config.py                →  保留，configs/*.yaml 是其声明式扩展
alpha_radar_backtest.py         →  保留，evaluator skill 调用它
dragon_diagnostic.py            →  保留，analyst skill 调用它
performance_eval_service.py     →  保留，evaluator skill 调用它
polars_engine.py                →  保留不动
screener_service.py             →  保留，API 层不变
```

**核心原则：新框架是现有代码的「AI 操作界面」，不是替代品。**
