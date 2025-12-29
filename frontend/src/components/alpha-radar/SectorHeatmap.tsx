import { useMemo, useRef, useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useTheme } from '@/components/theme-provider'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import { useGetSectorHeatmapApiV1AlphaRadarSectorHeatmapGet } from '@/api/generated/alpha-radar/alpha-radar'
import type { SectorMetric, TimeMode, SectorL1Item } from '@/api/generated/schemas'
import {
  getHeatmapGradient,
  getValueColor,
  formatMetricValue,
  getMetricLabel,
  getContrastTextColor,
} from '@/lib/sector-colors'
import { cn } from '@/lib/utils'

interface SectorHeatmapProps {
  timeMode: TimeMode
  selectedDate?: Date
  dateRange?: { from?: Date; to?: Date }
  onSectorClick?: (industryL1: string, industryL2?: string) => void
}

const METRICS: { value: SectorMetric; label: string }[] = [
  { value: 'change', label: '涨跌幅' },
  { value: 'amount', label: '成交额' },
  { value: 'main_strength', label: '主力强度' },
  { value: 'score', label: '综合评分' },
]

export function SectorHeatmap({
  timeMode,
  selectedDate,
  dateRange,
  onSectorClick,
}: SectorHeatmapProps) {
  const [metric, setMetric] = useState<SectorMetric>('change')
  const chartRef = useRef<ReactECharts>(null)
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  // Fetch heatmap data
  const { data, isLoading } = useGetSectorHeatmapApiV1AlphaRadarSectorHeatmapGet({
    metric,
    mode: timeMode,
    date: selectedDate?.toISOString().split('T')[0],
    start_date: dateRange?.from?.toISOString().split('T')[0],
    end_date: dateRange?.to?.toISOString().split('T')[0],
  })

  const { steps, progress } = useComputingProgress(isLoading, 'heatmap')

  // Transform data to ECharts treemap format
  const treemapData = useMemo(() => {
    if (!data?.sectors) return []

    return data.sectors.map((sector: SectorL1Item) => ({
      name: sector.name,
      value: Number(sector.size_value) || 0,
      itemStyle: {
        color: getValueColor(
          Number(sector.value) || 0,
          Number(data.min_value) || 0,
          Number(data.max_value) || 0,
          metric,
          isDark
        ),
      },
      // Custom data for tooltip
      _data: {
        stockCount: sector.stock_count,
        value: Number(sector.value) || 0,
        avgChangePct: Number(sector.avg_change_pct) || 0,
        totalAmount: Number(sector.total_amount) || 0,
        avgMainStrength: Number(sector.avg_main_strength) || 0,
        avgScore: Number(sector.avg_score) || 0,
        upCount: sector.up_count,
        downCount: sector.down_count,
      },
      children: sector.children?.map((child) => ({
        name: child.name,
        value: Number(child.size_value) || 0,
        itemStyle: {
          color: getValueColor(
            Number(child.value) || 0,
            Number(data.min_value) || 0,
            Number(data.max_value) || 0,
            metric,
            isDark
          ),
        },
        _data: {
          stockCount: child.stock_count,
          value: Number(child.value) || 0,
          avgChangePct: Number(child.avg_change_pct) || 0,
          totalAmount: Number(child.total_amount) || 0,
          avgMainStrength: Number(child.avg_main_strength) || 0,
          avgScore: Number(child.avg_score) || 0,
          upCount: child.up_count,
          downCount: child.down_count,
        },
      })) || [],
    }))
  }, [data, metric, isDark])

  // Get gradient for visual map
  const gradient = useMemo(() => getHeatmapGradient(metric, isDark), [metric, isDark])

  // ECharts option
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const option = useMemo<any>(() => {
    const textColor = isDark ? '#e5e7eb' : '#374151'
    const borderColor = isDark ? '#374151' : '#d1d5db'

    return {
      tooltip: {
        formatter: (params: {
          name: string
          data?: {
            _data?: {
              stockCount: number
              value: number
              avgChangePct: number
              totalAmount: number
              upCount: number
              downCount: number
            }
          }
        }) => {
          const d = params.data?._data
          if (!d) return params.name

          const changeColor = d.avgChangePct >= 0
            ? (isDark ? '#f07575' : '#c93b3b')
            : (isDark ? '#4cc38a' : '#288a5b')

          // Format amount to 亿
          const amountYi = (d.totalAmount / 100000000).toFixed(2)

          return `
            <div style="padding: 8px 12px; min-width: 180px;">
              <div style="font-weight: 600; margin-bottom: 8px; font-size: 14px;">${params.name}</div>
              <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="color: #9ca3af;">股票数</span>
                <span>${d.stockCount}</span>
              </div>
              <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="color: #9ca3af;">涨跌幅</span>
                <span style="color: ${changeColor}; font-family: monospace;">
                  ${d.avgChangePct >= 0 ? '+' : ''}${d.avgChangePct.toFixed(2)}%
                </span>
              </div>
              <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                <span style="color: #9ca3af;">成交额</span>
                <span style="font-family: monospace;">${amountYi}亿</span>
              </div>
              <div style="display: flex; justify-content: space-between;">
                <span style="color: #9ca3af;">涨/跌</span>
                <span>
                  <span style="color: ${isDark ? '#f07575' : '#c93b3b'};">${d.upCount}</span>
                  <span style="color: #9ca3af;"> / </span>
                  <span style="color: ${isDark ? '#4cc38a' : '#288a5b'};">${d.downCount}</span>
                </span>
              </div>
            </div>
          `
        },
        backgroundColor: isDark ? '#1f2937' : '#ffffff',
        borderColor: borderColor,
        textStyle: {
          color: textColor,
        },
      },
      series: [
        {
          type: 'treemap',
          roam: false,
          nodeClick: 'zoomToNode',
          breadcrumb: {
            show: true,
            left: 'center',
            top: 8,
            height: 22,
            itemStyle: {
              color: isDark ? '#374151' : '#f3f4f6',
              borderColor: borderColor,
              textStyle: {
                color: textColor,
              },
            },
            emphasis: {
              itemStyle: {
                color: isDark ? '#4b5563' : '#e5e7eb',
              },
            },
          },
          levels: [
            {
              // Root level
              itemStyle: {
                borderWidth: 0,
                gapWidth: 2,
              },
            },
            {
              // L1 - Main sectors
              itemStyle: {
                borderWidth: 3,
                borderColor: borderColor,
                gapWidth: 2,
              },
              upperLabel: {
                show: true,
                height: 28,
                backgroundColor: 'transparent',
                padding: [4, 8],
                formatter: (params: { name: string; data?: { _data?: { avgChangePct: number } } }) => {
                  const changePct = params.data?._data?.avgChangePct
                  const changeStr = changePct !== undefined
                    ? ` ${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`
                    : ''
                  return `{name|${params.name}}{change|${changeStr}}`
                },
                rich: {
                  name: {
                    fontSize: 13,
                    fontWeight: 600,
                    color: textColor,
                  },
                  change: {
                    fontSize: 11,
                    fontFamily: 'monospace',
                    color: '#9ca3af',
                    padding: [0, 0, 0, 6],
                  },
                },
              },
              label: {
                show: true,
                formatter: '{b}',
                fontSize: 11,
                color: textColor,
              },
            },
            {
              // L2 - Sub-sectors
              itemStyle: {
                borderWidth: 1,
                borderColor: borderColor,
                gapWidth: 1,
              },
              label: {
                show: true,
                formatter: (params: { name: string; data?: { _data?: { avgChangePct: number } } }) => {
                  const changePct = params.data?._data?.avgChangePct
                  if (changePct === undefined) return params.name
                  return `${params.name}\n${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`
                },
                fontSize: 10,
                color: (params: { data?: { itemStyle?: { color: string } } }) => {
                  const bgColor = params.data?.itemStyle?.color
                  if (!bgColor) return textColor
                  return getContrastTextColor(bgColor) === 'light' ? '#ffffff' : '#1f2937'
                },
                lineHeight: 14,
              },
            },
          ],
          data: treemapData,
        },
      ],
    }
  }, [treemapData, isDark, gradient])

  // Handle chart click
  const handleChartClick = (params: { name?: string; treePathInfo?: { name: string }[] }) => {
    if (!onSectorClick || !params.treePathInfo) return

    const path = params.treePathInfo
    if (path.length >= 2) {
      // L1 click
      onSectorClick(path[1].name)
    }
    if (path.length >= 3) {
      // L2 click
      onSectorClick(path[1].name, path[2].name)
    }
  }

  if (isLoading) {
    return (
      <Card>
        <CardHeader className="pb-3 pt-3">
          <div className="flex items-center gap-4">
            <CardTitle className="text-lg">行业热力图</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          <ComputingConsole
            title="正在加载行业数据..."
            steps={steps}
            progress={progress}
          />
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader className="pb-3 pt-3">
        <div className="flex items-center gap-4">
          <CardTitle className="text-lg shrink-0">行业热力图</CardTitle>

          {/* Metric Switcher */}
          <div className="flex items-center gap-1">
            {METRICS.map((m) => (
              <Button
                key={m.value}
                variant={metric === m.value ? 'default' : 'ghost'}
                size="sm"
                onClick={() => setMetric(m.value)}
                className={cn(
                  'text-xs h-7 px-3',
                  metric === m.value && 'bg-primary text-primary-foreground'
                )}
              >
                {m.label}
              </Button>
            ))}
          </div>

          <div className="flex-1" />

          {/* Stats summary */}
          {data && (
            <div className="flex items-center gap-4 text-xs text-muted-foreground">
              <span>
                {getMetricLabel(metric)}范围:
                <span className="font-mono ml-1">
                  {formatMetricValue(Number(data.min_value), metric)} ~ {formatMetricValue(Number(data.max_value), metric)}
                </span>
              </span>
              <span>
                均值:
                <span className="font-mono ml-1">
                  {formatMetricValue(Number(data.market_avg), metric)}
                </span>
              </span>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        {treemapData.length > 0 ? (
          <ReactECharts
            ref={chartRef}
            option={option}
            style={{ height: 480 }}
            onEvents={{
              click: handleChartClick,
            }}
          />
        ) : (
          <div className="h-[480px] flex items-center justify-center text-muted-foreground">
            暂无数据
          </div>
        )}
      </CardContent>
    </Card>
  )
}
