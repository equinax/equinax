import { useMemo } from 'react'
import ReactECharts from 'echarts-for-react'
import { useTheme } from '@/components/theme-provider'
import type { MonthlyReturns } from '@/types/backtest'

const STOCK_COLORS = [
  '#3b82f6',  // 蓝色
  '#22c55e',  // 绿色
  '#f97316',  // 橙色
  '#a855f7',  // 紫色
  '#ef4444',  // 红色
  '#06b6d4',  // 青色
  '#78716c',  // 棕色
  '#64748b',  // 灰蓝色
]

interface MultiMonthlyReturnsChartProps {
  data: Record<string, MonthlyReturns> | null | undefined
  height?: number
}

export function MultiMonthlyReturnsChart({ data, height = 350 }: MultiMonthlyReturnsChartProps) {
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const { stockCodes, months, seriesData } = useMemo(() => {
    if (!data || typeof data !== 'object') {
      return { stockCodes: [], months: [], seriesData: [] }
    }

    const codes = Object.keys(data)

    // Collect all months across all stocks
    const monthSet = new Set<string>()
    Object.values(data).forEach(monthlyData => {
      if (monthlyData && typeof monthlyData === 'object') {
        Object.keys(monthlyData).forEach(month => monthSet.add(month))
      }
    })
    const sortedMonths = Array.from(monthSet).sort()

    // Build series data for each stock
    const series = codes.map((stockCode, index) => {
      const monthlyData = data[stockCode] || {}
      return {
        name: stockCode,
        type: 'bar' as const,
        data: sortedMonths.map(month => {
          const value = monthlyData[month]
          return typeof value === 'number' ? value : 0
        }),
        itemStyle: {
          color: STOCK_COLORS[index % STOCK_COLORS.length],
        },
      }
    })

    return { stockCodes: codes, months: sortedMonths, seriesData: series }
  }, [data])

  const option = useMemo(() => ({
    tooltip: {
      trigger: 'axis' as const,
      axisPointer: { type: 'shadow' as const },
      formatter: (params: { seriesName: string; value: number; color: string; name: string }[]) => {
        const month = params[0]?.name || ''
        const items = params
          .map(p => `<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:${p.color};"></span>${p.seriesName}: ${(p.value * 100).toFixed(2)}%`)
          .join('<br/>')
        return `${month}<br/>${items}`
      },
    },
    legend: {
      data: stockCodes,
      top: 0,
      textStyle: { color: isDark ? '#a1a1aa' : '#71717a' },
    },
    grid: {
      top: 40,
      left: 60,
      right: 20,
      bottom: 60,
    },
    xAxis: {
      type: 'category' as const,
      data: months,
      axisLabel: {
        rotate: 45,
        color: isDark ? '#a1a1aa' : '#71717a',
        fontSize: 10,
      },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    yAxis: {
      type: 'value' as const,
      axisLabel: {
        formatter: (value: number) => `${(value * 100).toFixed(0)}%`,
        color: isDark ? '#a1a1aa' : '#71717a',
      },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
      splitLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    series: seriesData,
  }), [stockCodes, months, seriesData, isDark])

  if (!data || Object.keys(data).length === 0) {
    return (
      <div
        className="flex items-center justify-center bg-muted/30 rounded-lg"
        style={{ height }}
      >
        <p className="text-muted-foreground">暂无月度收益数据</p>
      </div>
    )
  }

  return (
    <div className="rounded-lg border bg-card p-4">
      <ReactECharts
        option={option}
        style={{ height }}
        notMerge={true}
      />
    </div>
  )
}
