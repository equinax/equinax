# 龙头涨停 (Dragon Leader) 理论纲领

## 核心理念

发掘中线即将起爆涨停的潜力票。以 T+20 收益率和胜率为核心评估指标。
关注主力资金吸筹蓄力、突破前的量价配合、板块热度轮动，以及避开分配阶段。

## 当前性能 (Iter 21, 25-date backtest)

| 指标 | 基线 | 当前 | 目标 |
|------|------|------|------|
| WR | 56.0% | **62.5%** | >60% ✅ |
| AR | 2.80% | **5.90%** | >5% ✅ |
| P/L | 2.97 | **3.82** | >3 ✅ |

## 选股逻辑

### 正向因子 (权重合计 0.82)

| 因子 | 权重 | 说明 |
|------|------|------|
| sector_momentum_positive | 0.17 | 板块5日动量，归一化为正向信号。**最可靠的优化杠杆** |
| trend_quality | 0.15 | trend_quality_20d，**最佳区分指标**（赢家vs输家 delta=+8.96） |
| anti_climax | 0.15 | (1-climax_score)×100，远离高潮分配区 |
| main_strength | 0.10 | main_strength_proxy，主力资金参与度。赢家值反而更低 |
| accumulation | 0.10 | accumulation_score×100，持续性资金吸筹 |
| consistency | 0.05 | volume_consistency_score，成交量稳定性。零区分度 |
| buildup | 0.05 | volume_buildup_quality，量能蓄力 |
| elg_flow | 0.05 | elg_net_percentile，超大单净流入分位数。零区分度 |

### 惩罚因子 (权重合计 0.28)

| 因子 | 权重 | 说明 |
|------|------|------|
| recent_spike_penalty | 0.10 | recent_vol_spike_max，近期异常量价冲击 |
| surge_penalty | 0.08 | 当日涨跌幅>3%部分的惩罚 |
| ceiling_penalty | 0.05 | price_position_60d>0.80触发。从0.08降至0.05：接近高位是突破前兆 |
| resistance_penalty | 0.05 | resistance_proximity_penalty。从0.08降至0.05：阻力位附近是突破信号 |

### 前置过滤
- `pct_chg.abs() <= 5.0`: 排除当日大幅波动
- `near_limit_up == False`: 排除涨停板
- `is_st != 1`: 排除ST股
- 需加载 moneyflow 数据 (elg_net_percentile, mf_net_percentile)
- `backtest_top_n = 4`（从5降至4，过滤掉最低置信度推荐）

## 评估指标
- 主指标: T+20 收益率
- 胜率目标: > 60%
- 平均收益目标: > 5%

## 关键发现

### 有效的优化手段
1. **板块动量**是最可靠的正向杠杆：从负信号翻转为正信号，权重递增至0.17均有提升，但0.20开始过冲
2. **降低主力强度权重** (0.15→0.10)：赢家的主力强度反而更低（delta=-3.6）
3. **降低突破型惩罚** (ceiling/resistance 0.08→0.05)：对龙头候选者来说，接近高位/阻力位是看涨突破信号
4. **Top-N收紧** (5→4)：第5名推荐一贯最弱，移除后WR+1.2pp、AR+1.27pp

### 无效/有害的优化手段
1. **全部公式重设计**: 灾难性（WR降8-14pp）
2. **非线性变换**（反转MS、封顶TQ）: 灾难性（WR降6pp+）
3. **交互项**（TQ×SM替代ELG）: 灾难性（AR=0.70%）
4. **return_5d惩罚**: 一致性地杀死AR，"已经在涨"对龙头是特征而非缺陷
5. **trend_quality权重超过0.15**: 虽是最佳区分指标，提权反而损害AR
6. **条件型惩罚缩放**: 无增益
7. **SM权重超过0.17**: 0.18微弱下降，0.20崩溃

### 因子区分度 (赢家 vs 输家)

| 因子 | 赢家均值 | 输家均值 | Delta | 结论 |
|------|---------|---------|-------|------|
| trend_quality_20d | 68.6 | 59.6 | +8.96 | **最佳区分** |
| return_5d | 1.96 | 4.65 | -2.69 | 已涨=输 |
| main_strength_proxy | 79.4 | 83.0 | -3.6 | 低=赢 |
| sector_momentum_5d | 1.66 | 0.32 | +1.34 | 板块热=赢 |
| volume_consistency | — | — | ~0 | 无区分 |
| accumulation_score | — | — | ~0 | 无区分 |
| elg_net_percentile | — | — | ~0 | 无区分 |

## 迭代记录

详见 `scoring.py` 模块级docstring，完整记录21次迭代的参数变化和结果。

关键里程碑：
- 基线: WR=56.0%, AR=2.80%
- Iter 6 (MS↓, SM↑): WR=62.0%, AR=3.67% — 首次突破60%胜率
- Iter 8 (VCS↓, SM↑): WR=60.0%, AR=4.41% — AR新高
- Iter 14 (top_n 5→4): WR=61.2%, AR=5.68% — 首次突破5%收益目标
- Iter 19+21 (ceiling/resistance↓): WR=62.5%, AR=5.90% — 当前最佳
