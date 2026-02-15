# 周内短线 (Weekly T+6) 理论纲领

## 核心理念

做稳定的5个交易日内短线收益。不追高、不追涨停、不赌方向，选择
**低波动、高一致性、蓄力充分** 的品种，在 T+1 买入后 T+6 评估收益。

## 选股逻辑

### 正向因子
- **anti_climax (0.20)**: 远离高潮顶部，避免追高。climax_score 越低越好。
- **consistency (0.15)**: volume_consistency_score 高 = 成交量稳定可靠，无异常波动。
- **consolidation (0.15)**: post_spike_consolidation 高 = 放量后充分整理，筹码沉淀。
- **trend_quality (0.10)**: trend_quality_20d 高 = 20日趋势质量好，走势健康。
- **buildup (0.10)**: volume_buildup_quality 高 = 量能逐步堆积，蓄力充分。
- **accumulation (0.10)**: accumulation_score 高 = 有资金持续流入信号。
- **momentum_quality (0.05)**: momentum_quality_ratio 高 = 动量质量好，非虚涨。

### 惩罚因子
- **recent_spike_penalty (0.10)**: 近期有异常放量冲高，波动风险大。
- **resistance_penalty (0.10)**: 接近阻力位，短期上行空间受限。
- **surge_penalty (0.05)**: 当日涨跌幅超过3%，已有较大波动。

### 前置过滤
- `pct_chg.abs() <= 5.0`: 排除当日大幅波动的票
- `near_limit_up == False`: 排除涨停板
- `is_st != 1`: 排除ST股

## 评估指标
- 主指标: T+6 收益率 (以 T+1 开盘买入价为基准)
- 胜率目标: > 65%
- 平均收益目标: > 1.5%

## 迭代记录
- v1.0: 初始公式设计，基于全景/聪明钱/深度价值三套公式的共性提炼
