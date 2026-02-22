# Alpha Radar 宏观因子增强计划

## 概述

将指数研究（Index Research）中的 ICI、SCI、Dispersion 指标引入 Alpha Radar 选股系统，
作为宏观层面因子增强个股预测能力。核心理念：**市场环境决定策略有效性，行业环境决定个股弹性**。

## 动机

当前 Alpha Radar 的 market_regime_score 仅基于上证综指 5 日收益率 + 市场广度（breadth），
是一个粗糙的 0-100 单一信号。它只能做"打折"或"放弃操作"的二元决策，无法区分：

- **系统性主导期**（ICI 高 + 指数涨）→ 跟着大盘买就行，个股选择没那么重要
- **系统性风险期**（ICI 高 + 指数跌）→ 覆巢之下无完卵，应全面回避
- **行业轮动期**（ICI 中等）→ 选对板块比选对股更重要
- **个股分化期**（ICI 低）→ Alpha 选股价值最高，应加大置信度

这些微观结构在现有 regime_score 中完全丢失了。

## 设计原则

1. **定性可解释** — 每个因子都有直观的经济含义，不是黑箱
2. **最小侵入** — 不改变现有评分公式结构，通过现有机制（因子/折扣/门控）接入
3. **单变量迭代** — 每次只引入一个新因子，回测验证后再引入下一个
4. **可回退** — 每个因子都可通过 YAML 配置独立启停

## 新因子定义

### Factor 1: `market_coherence`（市场一致性）

| 字段 | 值 |
|------|------|
| 来源 | ICI（Index Coherence Index） |
| 粒度 | 全市场统一值（同一天所有股票看到相同值） |
| 计算 | 全市场股票与上证综指的滚动窗口（20d）相关系数均值 |
| 范围 | 0.0 ~ 1.0 |
| 经济含义 | 高 = 大盘驱动一切，个股选择价值低；低 = 个股分化严重，Alpha 选股价值高 |

**在评分中的作用：Score Modifier（反向）**

ICI 高时，个股选择效果差（大盘涨什么都涨，大盘跌什么都跌），应降低选股置信度。
ICI 低时，个股分化大，选股能力最能发挥价值，应提升置信度。

```yaml
# 接入方式 A：作为 regime_discount 的增强输入
# 现有 regime_discount 仅用 market_regime_score (0-100)
# 新方案：regime_score 计算公式中加入 ICI 分量
#
# regime_score_new = index_component + breadth_modifier + ici_modifier
# ici_modifier:
#   ICI > 0.5 且指数跌 → 额外 -15（系统性风险惩罚）
#   ICI > 0.5 且指数涨 → +5（顺势）
#   ICI < 0.25 → +10（分化期，选股价值高）
```

```yaml
# 接入方式 B：作为独立因子写入策略 YAML（更灵活）
- signal: market_coherence_discount
  source: market_coherence_ici    # 广播列：同一天所有行同值
  transform: "(0.5 - col).clip(-0.3, 0.3) / 0.3 * 100"  # ICI低→高分
  fill_null: 0.0
  weight: 0.05
```

**推荐：方式 A 优先尝试**，因为 ICI 本质上是 regime 信号而非个股信号，嵌入 regime 更自然。

---

### Factor 2: `sector_coherence`（行业一致性）

| 字段 | 值 |
|------|------|
| 来源 | SCI（Sector Coherence Index） |
| 粒度 | 按申万一级行业（31个），每个行业一个值 |
| 计算 | 行业内股票与行业指数的滚动窗口（20d）相关系数均值 |
| 范围 | 0.0 ~ 1.0（每个行业独立） |
| 经济含义 | 高 = 行业内个股齐涨齐跌（板块效应强）；低 = 行业内分化大 |

**在评分中的作用：Conditional Weight Modifier**

SCI 高的行业：行业指数回报几乎决定了个股回报 → sector_momentum 因子权重应增大。
SCI 低的行业：行业内个股走势独立 → 个股层面因子（accumulation, main_strength）更重要。

```yaml
# 接入方式：作为 sector_momentum 的动态加权条件
# 现有：sector_momentum_positive 固定 weight=0.17
# 新方案：
#   SCI > 0.6 → sector_momentum 权重上调（乘以 SCI 强度系数）
#   SCI < 0.3 → sector_momentum 权重下调（行业内分化大，板块信号弱）
#
# 实现：新增一个 sector_coherence_boost 因子
- signal: sector_coherence_boost
  source: sector_coherence_sci    # 按行业映射到个股的 SCI 值
  transform: "(col - 0.3).clip(0.0, 0.4) / 0.4 * 100"  # SCI>0.3开始加分
  fill_null: 0.0
  weight: 0.05
```

---

### Factor 3: `market_dispersion`（市场离散度）

| 字段 | 值 |
|------|------|
| 来源 | Dispersion Std（截面标准差） |
| 粒度 | 全市场统一值 |
| 计算 | 每日所有股票超额收益（stock_return - index_return）的截面标准差 |
| 范围 | 通常 1.0 ~ 5.0（百分比） |
| 经济含义 | 高 = 个股走势极度分化（可能有大机会也有大风险）；低 = 个股走势趋同 |

**在评分中的作用：Risk Gate / Volatility Adjustment**

Dispersion 极高时（>4%），市场处于混乱状态，选股不确定性大 → 应降低仓位/提高筛选门槛。
Dispersion 适中时（2-3%），个股有分化但不极端 → 选股最佳窗口。
Dispersion 极低时（<1.5%），个股齐步走 → 选谁都差不多，alpha 价值低。

```yaml
# 接入方式：作为 pre_filter 或 regime 门控
# 当日 dispersion > 4.5 → should_abstain（太混乱）
# 当日 dispersion < 1.0 → 降低 top_n（选谁都差不多）
```

---

## 数据流架构

```
                                    ┌─────────────────────┐
                                    │  index_research.py   │
                                    │  (已有计算逻辑)       │
                                    └──────────┬──────────┘
                                               │ 复用计算逻辑
                                               ▼
┌──────────────────┐           ┌─────────────────────────────┐
│ polars_engine.py │           │  market_factors.py (新文件)   │
│ load_market_     │ ────────► │  compute_market_factors()     │
│ regime()         │  扩展调用  │  - ICI (20d rolling)         │
│                  │           │  - SCI per industry (20d)    │
│                  │           │  - Dispersion (daily)        │
└──────────────────┘           └──────────────┬──────────────┘
                                               │
                                               ▼
                               ┌─────────────────────────────┐
                               │  load_market_regime() 返回值  │
                               │  增加字段：                    │
                               │  - ici_20d: float             │
                               │  - sci_by_industry: dict      │
                               │  - dispersion_std: float      │
                               │  - market_regime_v2: str      │
                               └──────────────┬──────────────┘
                                               │
                          ┌────────────────────┼────────────────────┐
                          ▼                    ▼                    ▼
                   regime_discount       DataFrame 广播列      market_gate
                   (ICI → 折扣)          (SCI → 按行业映射)     (dispersion → 门控)
```

### 关键设计决策

**Q1: ICI/SCI 计算放在哪里？**

**方案：放在 `polars_engine.py` 的 `load_market_regime()` 中扩展。**

理由：
- ICI/SCI 本质上是 regime 信号，和现有 regime 计算放在一起最自然
- 不需要从 index_research.py 导入（那是 API 层，不应被 service 层依赖）
- 但复用其计算逻辑：抽取核心计算到独立的 `market_factors.py` 工具函数

**Q2: SCI 是每个行业独立的，如何映射到个股？**

**方案：通过 `sw_industry_l1` 列做 JOIN。**

screener_service 已经有 `sw_industry_l1` 列在 DataFrame 中。
在 `_ensure_columns()` 阶段，根据 `sw_industry_l1` 查表映射对应行业的 SCI 值。

**Q3: 计算性能影响？**

ICI/SCI 计算需要全市场股票数据（5000+只股票 × 20 天滚动窗口），
在 index_research.py 中耗时约 7 秒。但 Alpha Radar 只需要**单日快照**：

- ICI：只需当天值，不需要整条时间序列 → 单日计算 < 0.5s
- SCI：只需当天 31 个行业值 → 单日计算 < 0.5s
- Dispersion：只需当天截面统计 → 单日计算 < 0.1s

**方案：针对单日快照优化，不复用 index_research.py 的整条时间序列逻辑。**

---

## 实施阶段

### Phase 1: 基础设施（数据管道）

**目标**：让 Alpha Radar 管道能获取到 ICI/SCI/Dispersion 数据

1. 新建 `backend/app/services/alpha_radar/engine/factors/market_factors.py`
   - `compute_ici(db, target_date, window=20) -> float`
   - `compute_sci(db, target_date, window=20) -> dict[str, float]`（行业名→SCI）
   - `compute_dispersion(db, target_date) -> float`
2. 扩展 `polars_engine.py` 的 `load_market_regime()` 返回值
   - 新增 `ici_20d`, `dispersion_std`, `market_regime_v2`（四分类）
3. 扩展 `screener_service.py`
   - 将 `ici_20d` 传递给 `ScoringEngine`
   - 将 SCI 按行业映射到 DataFrame 的新列 `sector_coherence_sci`
4. 回测脚本适配
   - `alpha_radar_backtest.py` 也需要能获取这些新数据

**验证**：运行现有回测，结果不变（新数据仅计算不使用）

### Phase 2: ICI → regime_discount 增强

**目标**：将 ICI 信号嵌入现有 regime 折扣机制

1. 修改 `load_market_regime()` 中的 `regime_score` 计算：
   ```python
   # 新增 ICI 分量
   if ici > 0.5 and ret_5d < -1.0:
       ici_modifier = -15.0  # 系统性风险
   elif ici > 0.5 and ret_5d > 1.0:
       ici_modifier = 5.0    # 系统性顺势
   elif ici < 0.25:
       ici_modifier = 10.0   # 个股分化期，选股价值高
   else:
       ici_modifier = 0.0
   
   regime_score = index_component + breadth_modifier + ici_modifier + breadth_today_modifier - fragility_penalty
   ```
2. 运行回测，对比基线

**验证**：dragon/rally/weekly/overnight 四策略回测，对比 WR/AR/P-L

### Phase 3: Dispersion → market_gate 门控

**目标**：在极端离散度下触发 abstain

1. 修改 `load_market_regime()` 的 `should_abstain` 逻辑：
   ```python
   # 新增 dispersion 门控
   extreme_dispersion = dispersion_std > 4.5
   if extreme_dispersion and hostile_signals >= 2:
       should_abstain = True
   ```
2. 运行回测，验证是否过滤掉了高风险日（可能减少交易次数但提升胜率）

**验证**：dragon 回测 WR 应提升或持平，交易天数可能减少

### Phase 4: SCI → sector_coherence 因子

**目标**：引入行业一致性作为个股评分因子

1. 在 `_ensure_columns()` 中增加 `sector_coherence_sci` 默认列
2. 在 `screener_service.py` 中将 SCI 映射到 DataFrame
3. 在 dragon.yaml 中添加新因子（初始小权重 0.03-0.05）：
   ```yaml
   - signal: sector_coherence_component
     source: sector_coherence_sci
     transform: "(col - 0.3).clip(0.0, 0.4) / 0.4 * 100"
     fill_null: 0.0
     weight: 0.03
   ```
4. 单变量回测

**验证**：dragon 回测对比，观察 SCI 因子的 Cohen's d

### Phase 5: 策略级差异化

**目标**：不同策略对宏观因子的反应应该不同

| 策略 | ICI 响应 | SCI 响应 | Dispersion 响应 |
|------|----------|----------|-----------------|
| dragon (涨停) | ICI 高+指数涨 → 加分（系统性行情利于涨停） | SCI 高 → 板块动量权重加大 | 高离散度 → 不回避（涨停就是极端事件） |
| rally (主升) | ICI 高+指数涨 → 强加分 | SCI 高 → sector_momentum 加权 | 极高离散度 → 回避 |
| weekly (周线) | ICI 中性 | SCI 低影响 | 高离散度 → 惩罚（低波策略怕波动） |
| overnight (隔夜) | ICI 高+指数跌 → 反转机会 | SCI 低 → 有利（分化=有个股机会） | 极高 → 回避 |

每个策略单独迭代，遵循单变量原则。

---

## 迭代顺序（推荐）

```
Iter 1: Phase 1 — 数据管道（不影响评分，纯基础设施）
Iter 2: Phase 2 — ICI → regime_discount（影响所有策略，风险最小）
Iter 3: Phase 3 — Dispersion → abstain 门控（减少坏交易）
Iter 4: Phase 4 — SCI → dragon 因子（单策略验证）
Iter 5+: Phase 5 — 其他策略差异化接入
```

## 风险与注意事项

1. **数据可用性** — ICI/SCI 需要 20 天滚动窗口，前 19 天为 NaN。回测起始日需要额外 20 天数据缓冲。
2. **过拟合** — 宏观因子（ICI/SCI）的横截面区分度可能不高（因为同一天所有股票看到相同的 ICI）。需要通过 Cohen's d 验证其实际区分力后再决定权重。
3. **前视偏差** — ICI 使用的是当日收盘数据，在 overnight 策略中需要用 T-1 的 ICI。Dragon/rally/weekly 使用快照日的 ICI 即可。
4. **计算成本** — 如果性能成为瓶颈，可以将 ICI/SCI 日度值预计算并缓存（Redis 或数据库表）。
5. **因子相关性** — ICI 与现有 `breadth_5d_avg` 可能高度相关（市场广度和一致性概念接近）。需要检查共线性。
6. **sector_momentum 与 SCI 的交互** — 现有 `sector_momentum_5d` 已隐含了部分行业信息。SCI 引入后需监控两者是否冗余。

## 成功标准

| 指标 | 基线（当前） | 目标（Phase 2 后） | 目标（Phase 4 后） |
|------|-------------|-------------------|-------------------|
| Dragon WR | 56.6% | ≥ 56.6%（不降） | ≥ 58% |
| Dragon AR | 2.61% | ≥ 2.61%（不降） | ≥ 3.0% |
| Overnight WR | TBD | 提升或持平 | — |
| 坏交易日过滤 | 无 | 识别≥50%的大跌日 | — |
