import ReactECharts from 'echarts-for-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Loader2, BarChart3 } from 'lucide-react'
import { useGetDistributionApiV1AnalyticsJobIdDistributionGet } from '@/api/generated/analytics/analytics'
import { formatPercent } from '@/lib/utils'

interface ReturnDistributionChartProps {
  jobId: string
  metric?: 'total_return' | 'sharpe_ratio' | 'max_drawdown' | 'win_rate'
}

const metricLabels: Record<string, string> = {
  total_return: '总收益',
  sharpe_ratio: 'Sharpe比率',
  max_drawdown: '最大回撤',
  win_rate: '胜率',
}

export function ReturnDistributionChart({ jobId, metric = 'total_return' }: ReturnDistributionChartProps) {
  const { data, isLoading, error } = useGetDistributionApiV1AnalyticsJobIdDistributionGet(
    jobId,
    { metric, buckets: 20 },
    { query: { enabled: !!jobId } }
  )

  if (isLoading) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center h-80">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    )
  }

  if (error || !data) {
    return (
      <Card>
        <CardContent className="flex items-center justify-center h-80 text-muted-foreground">
          无法加载分布数据
        </CardContent>
      </Card>
    )
  }

  // Format bucket labels based on metric type
  const formatValue = (value: number) => {
    if (metric === 'total_return' || metric === 'max_drawdown' || metric === 'win_rate') {
      return formatPercent(value)
    }
    return value.toFixed(2)
  }

  // Prepare chart data
  const bucketLabels = data.buckets.map((b, i) => {
    if (i === data.buckets.length - 1) {
      return `${formatValue(b.range_min)}+`
    }
    return `${formatValue(b.range_min)}`
  })

  const bucketCounts = data.buckets.map(b => b.count)

  // Determine bar colors based on metric
  const getBarColor = (rangeMin: number, rangeMax: number) => {
    if (metric === 'max_drawdown') {
      // For drawdown, less negative is better
      return rangeMin >= -0.1 ? '#22c55e' : rangeMin >= -0.2 ? '#eab308' : '#ef4444'
    }
    // For return/sharpe/win_rate, higher is better
    const midpoint = (rangeMin + rangeMax) / 2
    if (metric === 'total_return') {
      return midpoint >= 0.1 ? '#22c55e' : midpoint >= 0 ? '#eab308' : '#ef4444'
    }
    if (metric === 'sharpe_ratio') {
      return midpoint >= 1 ? '#22c55e' : midpoint >= 0 ? '#eab308' : '#ef4444'
    }
    // win_rate
    return midpoint >= 0.5 ? '#22c55e' : '#eab308'
  }

  const barColors = data.buckets.map(b => getBarColor(b.range_min, b.range_max))

  const option = {
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (params: { dataIndex: number; value: number }[]) => {
        const bucket = data.buckets[params[0].dataIndex]
        const stocks = bucket.stock_codes?.slice(0, 5).join(', ') || ''
        return `
          <div style="padding: 8px;">
            <div style="font-weight: 600; margin-bottom: 4px;">
              ${formatValue(bucket.range_min)} ~ ${formatValue(bucket.range_max)}
            </div>
            <div>数量: <strong>${bucket.count}</strong></div>
            ${stocks ? `<div style="font-size: 11px; color: #999; margin-top: 4px;">${stocks}${(bucket.stock_codes?.length || 0) > 5 ? '...' : ''}</div>` : ''}
          </div>
        `
      },
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '10%',
      top: '10%',
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      data: bucketLabels,
      axisLabel: {
        rotate: 45,
        fontSize: 10,
        color: '#888',
      },
      axisLine: { lineStyle: { color: '#333' } },
    },
    yAxis: {
      type: 'value',
      name: '股票数量',
      nameTextStyle: { color: '#888' },
      axisLabel: { color: '#888' },
      splitLine: { lineStyle: { color: '#333', type: 'dashed' } },
    },
    series: [
      {
        name: '数量',
        type: 'bar',
        data: bucketCounts.map((count, i) => ({
          value: count,
          itemStyle: { color: barColors[i] },
        })),
        barWidth: '80%',
        markLine: data.statistics?.mean != null ? {
          silent: true,
          symbol: 'none',
          lineStyle: { color: '#3b82f6', width: 2, type: 'dashed' },
          data: [
            {
              name: '均值',
              xAxis: bucketLabels.findIndex((_, i) => {
                const bucket = data.buckets[i]
                return data.statistics?.mean != null &&
                  bucket.range_min <= (data.statistics.mean as number) &&
                  (data.statistics.mean as number) < bucket.range_max
              }),
              label: {
                formatter: `均值: ${formatValue(data.statistics.mean as number)}`,
                position: 'end',
              },
            },
          ],
        } : undefined,
      },
    ],
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <BarChart3 className="h-5 w-5" />
          {metricLabels[metric]}分布
          <span className="text-sm font-normal text-muted-foreground">
            ({data.total_count} 只股票)
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ReactECharts option={option} style={{ height: '300px' }} />

        {/* Statistics summary */}
        {data.statistics && (
          <div className="mt-4 grid grid-cols-5 gap-2 text-center text-sm border-t pt-4">
            {data.statistics.min != null && (
              <div>
                <div className="text-muted-foreground text-xs">最小</div>
                <div className="font-medium">{formatValue(data.statistics.min as number)}</div>
              </div>
            )}
            {data.statistics.p25 != null && (
              <div>
                <div className="text-muted-foreground text-xs">P25</div>
                <div className="font-medium">{formatValue(data.statistics.p25 as number)}</div>
              </div>
            )}
            {data.statistics.median != null && (
              <div>
                <div className="text-muted-foreground text-xs">中位数</div>
                <div className="font-medium">{formatValue(data.statistics.median as number)}</div>
              </div>
            )}
            {data.statistics.p75 != null && (
              <div>
                <div className="text-muted-foreground text-xs">P75</div>
                <div className="font-medium">{formatValue(data.statistics.p75 as number)}</div>
              </div>
            )}
            {data.statistics.max != null && (
              <div>
                <div className="text-muted-foreground text-xs">最大</div>
                <div className="font-medium">{formatValue(data.statistics.max as number)}</div>
              </div>
            )}
          </div>
        )}

        {/* Outliers */}
        {data.outliers && ((data.outliers.best?.length ?? 0) > 0 || (data.outliers.worst?.length ?? 0) > 0) && (
          <div className="mt-4 grid grid-cols-2 gap-4 border-t pt-4">
            {data.outliers.best && data.outliers.best.length > 0 && (
              <div>
                <div className="text-xs text-muted-foreground mb-2">表现最佳</div>
                <div className="space-y-1">
                  {data.outliers.best.slice(0, 3).map((item, i) => {
                    const stockCode = (item as Record<string, unknown>).stock_code as string
                    const value = (item as Record<string, unknown>).value as number
                    return (
                      <div key={i} className="flex justify-between text-sm">
                        <span className="font-mono">{stockCode}</span>
                        <span className="text-profit font-medium">{formatValue(value)}</span>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
            {data.outliers.worst && data.outliers.worst.length > 0 && (
              <div>
                <div className="text-xs text-muted-foreground mb-2">表现最差</div>
                <div className="space-y-1">
                  {data.outliers.worst.slice(0, 3).map((item, i) => {
                    const stockCode = (item as Record<string, unknown>).stock_code as string
                    const value = (item as Record<string, unknown>).value as number
                    return (
                      <div key={i} className="flex justify-between text-sm">
                        <span className="font-mono">{stockCode}</span>
                        <span className="text-loss font-medium">{formatValue(value)}</span>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
