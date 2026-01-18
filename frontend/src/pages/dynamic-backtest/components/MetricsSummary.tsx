import { Card, CardContent } from '@/components/ui/card'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'
import { TrendingUp, TrendingDown, Activity, BarChart3, Target, Percent } from 'lucide-react'

export function MetricsSummary() {
  const { metrics, equityCurve } = useDynamicBacktestStore()
  
  if (!metrics || equityCurve.length === 0) {
    return null
  }
  
  const items = [
    {
      label: '总收益',
      value: metrics.totalReturn,
      format: 'percent',
      icon: TrendingUp,
      colorize: true,
    },
    {
      label: '年化收益',
      value: metrics.annualReturn,
      format: 'percent',
      icon: BarChart3,
      colorize: true,
    },
    {
      label: '最大回撤',
      value: -metrics.maxDrawdown,
      format: 'percent',
      icon: TrendingDown,
      colorize: true,
    },
    {
      label: '夏普比率',
      value: metrics.sharpeRatio,
      format: 'number',
      icon: Activity,
      colorize: false,
    },
    {
      label: '胜率',
      value: metrics.winRate,
      format: 'percent',
      icon: Target,
      colorize: false,
    },
    {
      label: '超额收益',
      value: metrics.excessReturn,
      format: 'percent',
      icon: Percent,
      colorize: true,
    },
  ]
  
  const formatValue = (value: number, format: string) => {
    if (format === 'percent') {
      return (value * 100).toFixed(2) + '%'
    }
    return value.toFixed(2)
  }
  
  const getColorClass = (value: number, colorize: boolean) => {
    if (!colorize) return ''
    if (value > 0) return 'text-green-600 dark:text-green-400'
    if (value < 0) return 'text-red-600 dark:text-red-400'
    return ''
  }
  
  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2">
      {items.map(item => (
        <Card key={item.label} className="bg-muted/30">
          <CardContent className="p-2">
            <div className="flex items-center gap-2 text-muted-foreground mb-1">
              <item.icon className="h-3.5 w-3.5" />
              <span className="text-xs">{item.label}</span>
            </div>
            <p className={`text-lg font-semibold font-mono ${getColorClass(item.value, item.colorize)}`}>
              {item.value > 0 && item.colorize ? '+' : ''}
              {formatValue(item.value, item.format)}
            </p>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
