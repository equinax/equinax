import { useMemo } from 'react'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { formatPercent, formatCurrency } from '@/lib/utils'

interface MetricsData {
  stock_code: string
  total_return?: number | string | null
  annual_return?: number | string | null
  sharpe_ratio?: number | string | null
  sortino_ratio?: number | string | null
  calmar_ratio?: number | string | null
  max_drawdown?: number | string | null
  volatility?: number | string | null
  total_trades?: number | null
  winning_trades?: number | null
  losing_trades?: number | null
  win_rate?: number | string | null
  profit_factor?: number | string | null
  final_value?: number | string | null
}

interface MultiMetricsTableProps {
  data: Record<string, MetricsData> | null | undefined
}

interface MetricConfig {
  key: keyof MetricsData
  label: string
  format: 'percent' | 'currency' | 'ratio' | 'number'
  colorize?: boolean
  invertColor?: boolean  // for metrics where negative is bad (like max_drawdown shown as positive)
}

const METRICS: MetricConfig[] = [
  { key: 'total_return', label: '总收益', format: 'percent', colorize: true },
  { key: 'annual_return', label: '年化收益', format: 'percent', colorize: true },
  { key: 'sharpe_ratio', label: 'Sharpe 比率', format: 'ratio', colorize: true },
  { key: 'sortino_ratio', label: 'Sortino 比率', format: 'ratio', colorize: true },
  { key: 'calmar_ratio', label: 'Calmar 比率', format: 'ratio', colorize: true },
  { key: 'max_drawdown', label: '最大回撤', format: 'percent', colorize: true, invertColor: true },
  { key: 'volatility', label: '波动率', format: 'percent' },
  { key: 'total_trades', label: '交易次数', format: 'number' },
  { key: 'winning_trades', label: '盈利交易', format: 'number' },
  { key: 'losing_trades', label: '亏损交易', format: 'number' },
  { key: 'win_rate', label: '胜率', format: 'percent' },
  { key: 'profit_factor', label: '盈亏比', format: 'ratio' },
  { key: 'final_value', label: '最终价值', format: 'currency' },
]

function formatValue(value: unknown, format: MetricConfig['format']): string {
  if (value === null || value === undefined) return '-'

  const num = typeof value === 'string' ? parseFloat(value) : Number(value)
  if (isNaN(num)) return '-'

  switch (format) {
    case 'percent':
      return formatPercent(num)
    case 'currency':
      return formatCurrency(num)
    case 'ratio':
      return num.toFixed(2)
    case 'number':
      return num.toLocaleString()
    default:
      return String(num)
  }
}

function getColorClass(
  value: unknown,
  colorize: boolean | undefined,
  invertColor: boolean | undefined
): string {
  if (!colorize) return ''

  const num = typeof value === 'string' ? parseFloat(value) : Number(value)
  if (isNaN(num)) return ''

  // For inverted colors (like max_drawdown), positive values are bad
  if (invertColor) {
    return num > 0 ? 'text-loss' : ''
  }

  return num > 0 ? 'text-profit' : num < 0 ? 'text-loss' : ''
}

export function MultiMetricsTable({ data }: MultiMetricsTableProps) {
  const stockCodes = useMemo(() => {
    if (!data || typeof data !== 'object') return []
    return Object.keys(data)
  }, [data])

  if (stockCodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-[200px] bg-muted/30 rounded-lg">
        <p className="text-muted-foreground">暂无指标数据</p>
      </div>
    )
  }

  return (
    <div className="rounded-lg border overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="sticky left-0 bg-background min-w-[120px]">指标</TableHead>
            {stockCodes.map(code => (
              <TableHead key={code} className="text-center min-w-[120px]">
                {code}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {METRICS.map(metric => (
            <TableRow key={metric.key}>
              <TableCell className="sticky left-0 bg-background font-medium">
                {metric.label}
              </TableCell>
              {stockCodes.map(code => {
                const stockData = data![code]
                const value = stockData?.[metric.key]
                const colorClass = getColorClass(value, metric.colorize, metric.invertColor)

                return (
                  <TableCell key={code} className={`text-center ${colorClass}`}>
                    {formatValue(value, metric.format)}
                  </TableCell>
                )
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
