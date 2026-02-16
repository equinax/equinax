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

## 公式能力边界

### 擅长场景（超卖反弹）
公式在低位蓄力环境下表现最佳。100% WR 日期的共性: price_position_60d < 0.20, return_5d 为负, ma_alignment 偏低。当前因子组合(anti_climax + consolidation + consistency)天然构成超卖反弹选择器。

### 结构性弱点（均匀热市场）
当市场全面上涨后(如 02-10), 所有候选的因子值趋同(ma_alignment ~98, price_position_60d 0.35-0.58), 线性加权公式无法产生区分度。这是信息量问题, 非权重问题。

### 已验证无效的因子
| 因子 | 方向 | 失败原因 | 迭代 |
|------|------|----------|------|
| price_position_60d | 惩罚高位 | 均匀热市场下阈值不触发, 其他日期误伤赢家 | Iter 16 |
| price_position_60d | 奖励低位 | 改善3个日期但伤害3个, 零和博弈 | Iter 16 |
| moneyflow | 任意 | Regime-dependent, 信号方向不一致 | Iter 16 |
| ma_alignment | 任意 | Regime-dependent, 与胜负无稳定相关 | Iter 16 |
| return_5d | 惩罚高位 | 与 price_position_60d 同理 | Iter 16 |

## 迭代记录
- v1.0: 初始公式设计，基于全景/聪明钱/深度价值三套公式的共性提炼
- v1.0 Iter 16: 超卖反弹假设检验。两次尝试(penalty + bonus)均退步, 回滚。确认公式在线性加权框架下已达局部最优。WR=67.6%, AR=1.31%, P/L=3.33。
