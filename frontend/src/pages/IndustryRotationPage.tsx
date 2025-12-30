/**
 * Industry Rotation Page
 *
 * Displays a date × industry matrix heatmap for sector rotation analysis.
 * Features:
 * - Multiple sorting options with column animation
 * - Multi-metric display in cells
 * - Algorithm signals (momentum, reversal, divergence)
 * - Rich tooltips with top stocks and metrics
 */

import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { keepPreviousData } from '@tanstack/react-query'
import { ArrowLeft } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import { useGetSectorRotationApiV1AlphaRadarSectorRotationGet } from '@/api/generated/alpha-radar/alpha-radar'
import { RotationMatrix } from '@/components/industry-rotation/RotationMatrix'

// Metric options for cell display
export type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

const METRIC_OPTIONS: { key: MetricKey; label: string }[] = [
  { key: 'change', label: '涨跌幅' },
  { key: 'volume', label: '成交量' },
  { key: 'flow', label: '资金流入' },
  { key: 'momentum', label: '动量' },
]

export default function IndustryRotationPage() {
  const navigate = useNavigate()
  // Default: show change only, others can be toggled
  const [visibleMetrics, setVisibleMetrics] = useState<MetricKey[]>(['change'])

  // Toggle metric visibility (at least one must be selected)
  const toggleMetric = (key: MetricKey) => {
    setVisibleMetrics((prev) => {
      if (prev.includes(key)) {
        // Don't allow deselecting the last one
        if (prev.length === 1) return prev
        return prev.filter((m) => m !== key)
      }
      return [...prev, key]
    })
  }

  // Fetch rotation data (fixed 60 days)
  const { data, isLoading, isFetching } = useGetSectorRotationApiV1AlphaRadarSectorRotationGet(
    {
      days: 60,
    },
    {
      query: {
        placeholderData: keepPreviousData,
      },
    }
  )

  // Loading state
  const showInitialLoading = isLoading && !data
  const { steps, progress } = useComputingProgress(showInitialLoading, 'heatmap')

  return (
    <div className="space-y-4">
      {/* Page Header */}
      <div className="flex items-center gap-4">
        <Button variant="ghost" size="sm" onClick={() => navigate(-1)}>
          <ArrowLeft className="h-4 w-4 mr-1" />
          返回
        </Button>
        <h1 className="text-2xl font-bold">行业轮动雷达</h1>
        {data && (
          <span className="text-sm text-muted-foreground">
            {data.stats.total_industries} 个行业 · {data.stats.trading_days} 个交易日
          </span>
        )}
      </div>

      {/* Matrix */}
      <Card>
        <CardHeader className="pb-2 pt-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <CardTitle className="text-lg">轮动矩阵</CardTitle>
              {isFetching && !showInitialLoading && (
                <span className="text-xs text-muted-foreground animate-pulse">
                  更新中...
                </span>
              )}
            </div>

            {/* Metric Chips */}
            <div className="flex items-center gap-2">
              {METRIC_OPTIONS.map((opt) => {
                const isSelected = visibleMetrics.includes(opt.key)
                return (
                  <button
                    key={opt.key}
                    onClick={() => toggleMetric(opt.key)}
                    className={`px-3 py-1 text-sm rounded-md transition-colors ${
                      isSelected
                        ? 'bg-primary text-primary-foreground'
                        : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                    }`}
                  >
                    {opt.label}
                  </button>
                )
              })}
            </div>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {showInitialLoading ? (
            <ComputingConsole
              title="正在计算行业轮动数据..."
              steps={steps}
              progress={progress}
            />
          ) : data ? (
            <RotationMatrix data={data} visibleMetrics={visibleMetrics} />
          ) : (
            <div className="flex items-center justify-center h-32 text-muted-foreground">
              暂无数据
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
