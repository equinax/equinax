# 主升浪 (Main Rally) 理论纲领

## 核心理念

捕捉正在进入主升浪阶段的个股 —— MA多头排列、量能阶梯式放大、
趋势质量高、尚未到达高潮顶部。重点关注大盘股的板块轮动节奏。

## 选股逻辑

### 正向因子
- **alignment (0.20)**: ma_alignment_score 高 = MA5/10/20/60 多头排列程度强。
- **trend_quality (0.15)**: trend_quality_20d 高 = 20日趋势方向明确、波动可控。
- **buildup (0.15)**: volume_buildup_quality 高 = 量能有序堆积，非突发放量。
- **accumulation (0.10)**: accumulation_score 高 = 持续性资金流入。
- **consolidation (0.10)**: post_spike_consolidation 高 = 放量后整理充分。
- **momentum_quality (0.10)**: momentum_quality_ratio 高 = 动量健康度。
- **anti_climax (0.05)**: 远离高潮区域。

### 惩罚因子
- **recent_spike_penalty (0.10)**: 近期异常量价冲击。
- **resistance_penalty (0.05)**: 阻力位压制。

### 前置过滤
- `near_limit_up == False`: 排除涨停板
- `is_st != 1`: 排除ST股

## 评估指标
- 主指标: T+5 收益率
- 胜率目标: > 60%

## 迭代记录
- v1.0: 从 scoring.py `calculate_main_rally_score()` 原样迁移，保留全部权重和注释
