/**
 * Sector Heatmap Component
 *
 * Displays industry performance as a bidirectional diverging bar chart:
 * - L1 industries in horizontal row (sorted by change: gain left, loss right)
 * - Click to expand L2 vertically (gainers up, losers down)
 * - Full-width responsive layout
 */

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useTheme } from '@/components/theme-provider'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import { useGetSectorHeatmapApiV1AlphaRadarSectorHeatmapGet } from '@/api/generated/alpha-radar/alpha-radar'
import type { SectorMetric, TimeMode } from '@/api/generated/schemas'
import { formatMetricValue, getMetricLabel } from '@/lib/sector-colors'
import { DivergingBarChart } from './sector-heatmap/DivergingBarChart'
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
  const { theme } = useTheme()

  const isDark =
    theme === 'dark' ||
    (theme === 'system' &&
      window.matchMedia('(prefers-color-scheme: dark)').matches)

  // Fetch heatmap data
  const { data, isLoading } = useGetSectorHeatmapApiV1AlphaRadarSectorHeatmapGet(
    {
      metric,
      mode: timeMode,
      date: selectedDate?.toISOString().split('T')[0],
      start_date: dateRange?.from?.toISOString().split('T')[0],
      end_date: dateRange?.to?.toISOString().split('T')[0],
    }
  )

  const { steps, progress } = useComputingProgress(isLoading, 'heatmap')

  // Handle L2 click
  const handleL2Click = (l1Name: string, l2Name: string) => {
    onSectorClick?.(l1Name, l2Name)
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
                  {formatMetricValue(Number(data.min_value), metric)} ~{' '}
                  {formatMetricValue(Number(data.max_value), metric)}
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
        <div className="text-xs text-muted-foreground mb-2">
          点击展开/收起二级行业 | 按指标值从大到小排列
        </div>
        <DivergingBarChart
          sectors={data?.sectors}
          metric={metric}
          isDark={isDark}
          minValue={Number(data?.min_value) || 0}
          maxValue={Number(data?.max_value) || 0}
          onL2Click={handleL2Click}
        />
      </CardContent>
    </Card>
  )
}
