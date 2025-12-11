import { z } from 'zod'

// 策略类型枚举
export const strategyTypeEnum = z.enum([
  'trend_following',
  'momentum',
  'mean_reversion',
  'arbitrage',
  'other',
])

export type StrategyType = z.infer<typeof strategyTypeEnum>

// 策略表单 Schema - 与后端验证规则保持一致
export const strategyFormSchema = z.object({
  name: z.string().min(3, '策略名称至少需要3个字符'),
  description: z.string().optional().nullable(),
  strategyType: strategyTypeEnum.default('trend_following'),
  code: z.string().min(1, '策略代码不能为空'),
  indicatorsUsed: z.array(z.string()).default([]),
})

export type StrategyFormValues = z.infer<typeof strategyFormSchema>

// 默认代码模板
export const defaultCode = `class MyStrategy(bt.Strategy):
    """
    自定义策略模板

    参数:
        period: 均线周期
    """
    params = (
        ('period', 20),
    )

    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.close()
`

// 默认表单值
export const defaultStrategyValues: StrategyFormValues = {
  name: '',
  description: '',
  strategyType: 'trend_following',
  code: defaultCode,
  indicatorsUsed: [],
}

// 策略类型选项（用于 UI）
export const strategyTypeOptions = [
  { value: 'trend_following', label: '趋势跟踪' },
  { value: 'momentum', label: '动量策略' },
  { value: 'mean_reversion', label: '均值回归' },
  { value: 'arbitrage', label: '套利策略' },
  { value: 'other', label: '其他' },
] as const
