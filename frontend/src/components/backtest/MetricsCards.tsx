import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatPercent, formatCurrency } from '@/lib/utils'
import { TrendingUp, TrendingDown, Activity, BarChart3 } from 'lucide-react'
import type { BacktestResultDetailResponse } from '@/api/generated/schemas'

interface MetricsCardsProps {
  metrics: BacktestResultDetailResponse
}

interface MetricRowProps {
  label: string
  value: number | string | null | undefined
  format: 'percent' | 'currency' | 'number' | 'ratio' | 'days'
  colorize?: boolean
}

function MetricRow({ label, value, format, colorize = false }: MetricRowProps) {
  const formatValue = () => {
    if (value === null || value === undefined) return '-'

    const numValue = Number(value)
    if (isNaN(numValue)) return String(value)

    switch (format) {
      case 'percent':
        return formatPercent(numValue)
      case 'currency':
        return formatCurrency(numValue)
      case 'ratio':
        return numValue.toFixed(2)
      case 'days':
        return `${numValue} 天`
      case 'number':
      default:
        return numValue.toLocaleString()
    }
  }

  const getColorClass = () => {
    if (!colorize || value === null || value === undefined) return ''

    const numValue = Number(value)
    if (isNaN(numValue)) return ''

    // For drawdown, negative is bad (show in red)
    // For returns/ratios, negative is bad, positive is good
    if (format === 'percent' && label.includes('回撤')) {
      return numValue < 0 ? 'text-loss' : ''
    }

    return numValue >= 0 ? 'text-profit' : 'text-loss'
  }

  return (
    <div className="flex items-center justify-between py-2">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className={`font-medium ${getColorClass()}`}>{formatValue()}</span>
    </div>
  )
}

export function MetricsCards({ metrics }: MetricsCardsProps) {
  const metricGroups = [
    {
      title: '收益指标',
      icon: TrendingUp,
      items: [
        { label: '总收益', value: metrics.total_return, format: 'percent' as const, colorize: true },
        { label: '年化收益', value: metrics.annual_return, format: 'percent' as const, colorize: true },
        { label: '最终价值', value: metrics.final_value, format: 'currency' as const },
      ],
    },
    {
      title: '风险指标',
      icon: TrendingDown,
      items: [
        { label: '最大回撤', value: metrics.max_drawdown, format: 'percent' as const, colorize: true },
        { label: '最大回撤持续', value: metrics.max_drawdown_duration, format: 'days' as const },
        { label: '波动率', value: metrics.volatility, format: 'percent' as const },
      ],
    },
    {
      title: '风险调整收益',
      icon: Activity,
      items: [
        { label: 'Sharpe 比率', value: metrics.sharpe_ratio, format: 'ratio' as const },
        { label: 'Sortino 比率', value: metrics.sortino_ratio, format: 'ratio' as const },
        { label: 'Calmar 比率', value: metrics.calmar_ratio, format: 'ratio' as const },
      ],
    },
    {
      title: '交易统计',
      icon: BarChart3,
      items: [
        { label: '总交易次数', value: metrics.total_trades, format: 'number' as const },
        { label: '盈利交易', value: metrics.winning_trades, format: 'number' as const },
        { label: '亏损交易', value: metrics.losing_trades, format: 'number' as const },
        { label: '胜率', value: metrics.win_rate, format: 'percent' as const },
        { label: '盈亏比', value: metrics.profit_factor, format: 'ratio' as const },
      ],
    },
  ]

  return (
    <div className="grid gap-4 md:grid-cols-2">
      {metricGroups.map((group) => (
        <Card key={group.title}>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-base">
              <group.icon className="h-4 w-4 text-muted-foreground" />
              {group.title}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="divide-y">
              {group.items.map((item) => (
                <MetricRow key={item.label} {...item} />
              ))}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
