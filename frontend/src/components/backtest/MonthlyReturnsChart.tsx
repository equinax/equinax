import { useMemo, useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { useTheme } from '@/components/theme-provider'
import { Button } from '@/components/ui/button'
import { BarChart3, Grid3X3 } from 'lucide-react'
import type { MonthlyReturns } from '@/types/backtest'

interface MonthlyReturnsChartProps {
  data: MonthlyReturns | { [key: string]: unknown } | null | undefined
  height?: number
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export function MonthlyReturnsChart({ data, height = 300 }: MonthlyReturnsChartProps) {
  const [viewMode, setViewMode] = useState<'heatmap' | 'bar'>('bar')
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const normalizedData = useMemo(() => {
    if (!data || typeof data !== 'object') return {}

    const result: MonthlyReturns = {}
    for (const [key, value] of Object.entries(data)) {
      if (typeof value === 'number') {
        result[key] = value
      }
    }
    return result
  }, [data])

  const years = useMemo(() => {
    const yearSet = new Set<string>()
    Object.keys(normalizedData).forEach(key => {
      const year = key.split('-')[0]
      if (year) yearSet.add(year)
    })
    return Array.from(yearSet).sort()
  }, [normalizedData])

  const heatmapOption = useMemo(() => ({
    tooltip: {
      position: 'top',
      formatter: (params: { data: [number, number, number] }) => {
        const [monthIndex, yearIndex, value] = params.data
        const year = years[yearIndex]
        const month = MONTHS[monthIndex]
        return `${year} ${month}: ${(value * 100).toFixed(2)}%`
      },
    },
    grid: {
      top: 30,
      left: 60,
      right: 20,
      bottom: 30,
    },
    xAxis: {
      type: 'category' as const,
      data: MONTHS,
      splitArea: { show: true },
      axisLabel: { color: isDark ? '#a1a1aa' : '#71717a' },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    yAxis: {
      type: 'category' as const,
      data: years,
      splitArea: { show: true },
      axisLabel: { color: isDark ? '#a1a1aa' : '#71717a' },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    visualMap: {
      min: -0.15,
      max: 0.15,
      calculable: true,
      orient: 'horizontal' as const,
      left: 'center',
      top: 0,
      textStyle: { color: isDark ? '#a1a1aa' : '#71717a' },
      inRange: {
        color: ['#ef4444', '#fca5a5', '#ffffff', '#86efac', '#22c55e'],
      },
      formatter: (value: number) => `${(value * 100).toFixed(0)}%`,
    },
    series: [{
      type: 'heatmap',
      data: years.flatMap((year, yearIndex) =>
        MONTHS.map((_, monthIndex) => {
          const key = `${year}-${String(monthIndex + 1).padStart(2, '0')}`
          const value = normalizedData[key] ?? 0
          return [monthIndex, yearIndex, value]
        })
      ),
      label: {
        show: true,
        formatter: (params: { data: [number, number, number] }) => {
          const value = params.data[2]
          if (value === 0) return ''
          return `${(value * 100).toFixed(0)}%`
        },
        color: isDark ? '#fff' : '#000',
        fontSize: 10,
      },
      emphasis: {
        itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.5)' },
      },
    }],
  }), [normalizedData, years, isDark])

  const barOption = useMemo(() => {
    const sortedEntries = Object.entries(normalizedData).sort(([a], [b]) => a.localeCompare(b))

    return {
      tooltip: {
        trigger: 'axis' as const,
        formatter: (params: { name: string; value: number }[]) => {
          const p = params[0]
          return `${p.name}: ${(p.value * 100).toFixed(2)}%`
        },
      },
      grid: {
        top: 20,
        left: 60,
        right: 20,
        bottom: 60,
      },
      xAxis: {
        type: 'category' as const,
        data: sortedEntries.map(([key]) => key),
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
      series: [{
        type: 'bar',
        data: sortedEntries.map(([_, value]) => ({
          value,
          itemStyle: {
            color: value >= 0 ? '#22c55e' : '#ef4444',
          },
        })),
      }],
    }
  }, [normalizedData, isDark])

  if (Object.keys(normalizedData).length === 0) {
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
    <div className="space-y-2">
      <div className="flex justify-end gap-2">
        <Button
          variant={viewMode === 'bar' ? 'default' : 'outline'}
          size="sm"
          onClick={() => setViewMode('bar')}
        >
          <BarChart3 className="h-4 w-4 mr-1" />
          柱状图
        </Button>
        <Button
          variant={viewMode === 'heatmap' ? 'default' : 'outline'}
          size="sm"
          onClick={() => setViewMode('heatmap')}
        >
          <Grid3X3 className="h-4 w-4 mr-1" />
          热力图
        </Button>
      </div>
      <div className="rounded-lg border bg-card p-4">
        <ReactECharts
          option={viewMode === 'heatmap' ? heatmapOption : barOption}
          style={{ height }}
          notMerge={true}
        />
      </div>
    </div>
  )
}
