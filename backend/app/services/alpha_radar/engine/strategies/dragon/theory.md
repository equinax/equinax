# 龙头先锋 (Dragon Leader) 理论纲领

## 核心理念

发掘中线即将起爆涨停的潜力票。以 Buy+20, Buy+30 效益最大化为评估指标。
关注主力资金吸筹蓄力、突破前的量价配合、以及避开分配阶段。

## 选股逻辑

### 正向因子
- **main_strength (0.15)**: main_strength_proxy 高 = 主力资金参与度强。
  (Iter 15: 从0.20降至0.15，避免过度加权动量)
- **consistency (0.15)**: volume_consistency_score 高 = 成交量稳定可靠。
- **anti_climax (0.15)**: 远离高潮分配区。
  (Iter 15: 新增，此前缺失是0%胜率的首要根因)
- **accumulation (0.10)**: accumulation_score 高 = 持续性资金吸筹。
- **trend_quality (0.10)**: 趋势质量健康。
- **buildup (0.05)**: 量能蓄力。
- **elg_flow (0.05)**: 超大单净流入分位数。
  (Iter 15: 从0.10降至0.05，同日资金流是滞后指标)

### 惩罚因子
- **recent_spike_penalty (0.10)**: 近期异常量价冲击。
- **surge_penalty (0.08)**: 当日涨跌幅过大。
  (Iter 15: 从0.05提至0.08)
- **ceiling_penalty (0.08)**: price_position_60d > 0.80 时触发，
  股价接近60日高位，分配风险大。(Iter 15 新增)
- **resistance_penalty (0.08)**: 阻力位压制。
  (Iter 15: 从0.05提至0.08)
- **sector_overheat_penalty (0.05)**: sector_momentum_5d > 5% 时触发，
  板块过热容易均值回归。(Iter 15 新增)
- **sector_weakness_penalty (0.02)**: 板块弱势。

### 前置过滤
- `pct_chg.abs() <= 5.0`: 排除当日大幅波动
- `near_limit_up == False`: 排除涨停板
- `is_st != 1`: 排除ST股
- 需要加载 moneyflow 数据 (elg_net_percentile, mf_net_percentile)

## 评估指标
- 主指标: T+20 收益率
- 辅助指标: T+30 收益率
- 胜率目标: > 55%
- 平均收益目标: > 5%

## 关键发现
1. close_strength 在所有 tab 中都是有害信号 (追高)，已移除
2. 同日 moneyflow 是滞后指标，权重不超过 0.05
3. anti_climax 是全局最重要的因子之一，缺失直接导致0%胜率
4. price_position_60d > 0.80 有效捕捉分配区
5. sector_momentum_5d > 5% 信号板块过热均值回归

## 迭代记录
- v1.0 (Iter 15): 从 scoring.py `calculate_dragon_leader_score()` 原样迁移
  - 重大重设计：新增 anti_climax, ceiling_penalty, sector_overheat_penalty
  - 降低 elg (0.10→0.05), main_strength (0.20→0.15)
  - 提高 surge (0.05→0.08), resistance (0.05→0.08)
  - 移除 mf_net_percentile (过噪声)
