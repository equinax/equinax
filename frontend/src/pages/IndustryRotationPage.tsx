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

import { useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { keepPreviousData } from '@tanstack/react-query'
import { ArrowLeft } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import { useGetSectorRotationApiV1AlphaRadarSectorRotationGet } from '@/api/generated/alpha-radar/alpha-radar'
import { RotationSortBy } from '@/api/generated/schemas'
import { RotationMatrix, ColorRangeBar } from '@/components/industry-rotation'

// Metric options for cell display
export type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

const METRIC_OPTIONS: { key: MetricKey; label: string; activeColor: string }[] = [
  { key: 'change', label: '涨跌幅', activeColor: 'bg-[#c93b3b]' },
  { key: 'volume', label: '成交量', activeColor: 'bg-[#2989c9]' },
  { key: 'flow', label: '资金流入', activeColor: 'bg-[#c47a30]' },
  { key: 'momentum', label: '动量', activeColor: 'bg-[#7a2eb0]' },
]

// Highlight range for color filter
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

export default function IndustryRotationPage() {
  const navigate = useNavigate()
  // Default: show change only, others can be toggled
  const [visibleMetrics, setVisibleMetrics] = useState<MetricKey[]>(['change'])
  // Track last selected metric for color range bar
  const [lastSelectedMetric, setLastSelectedMetric] = useState<MetricKey>('change')
  // Sort order for columns
  const [sortBy, setSortBy] = useState<RotationSortBy>('upstream')
  // Highlight range for filtering cells by color
  const [highlightRange, setHighlightRange] = useState<HighlightRange | null>(null)

  // Toggle metric visibility (at least one must be selected)
  const toggleMetric = useCallback((key: MetricKey) => {
    setVisibleMetrics((prev) => {
      if (prev.includes(key)) {
        // Don't allow deselecting the last one
        if (prev.length === 1) return prev
        return prev.filter((m) => m !== key)
      }
      // When selecting a new metric, update lastSelectedMetric
      setLastSelectedMetric(key)
      return [...prev, key]
    })
  }, [])

  // Handle color range bar hover
  const handleColorRangeHover = useCallback((range: { min: number; max: number } | null) => {
    if (range) {
      setHighlightRange({ metric: lastSelectedMetric, ...range })
    } else {
      setHighlightRange(null)
    }
  }, [lastSelectedMetric])

  // Fetch rotation data (fixed 60 days)
  const { data, isLoading, isFetching } = useGetSectorRotationApiV1AlphaRadarSectorRotationGet(
    {
      days: 60,
      sort_by: sortBy,
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

            {/* Color Range Bar + Metric Group Buttons */}
            <div className="flex items-center gap-3">
              {/* Color range filter bar */}
              <ColorRangeBar
                metric={lastSelectedMetric}
                onHoverRange={handleColorRangeHover}
              />

              {/* Metric Group Buttons */}
              <div className="flex items-center border rounded-md overflow-hidden">
                {METRIC_OPTIONS.map((opt, idx) => {
                  const isSelected = visibleMetrics.includes(opt.key)
                  const isFirst = idx === 0
                  return (
                    <button
                      key={opt.key}
                      onClick={() => toggleMetric(opt.key)}
                      className={`px-2.5 py-1 text-xs font-medium transition-colors ${
                        isSelected
                          ? `${opt.activeColor} text-white`
                          : 'bg-background text-muted-foreground hover:text-foreground hover:bg-muted'
                      } ${!isFirst ? 'border-l' : ''}`}
                    >
                      {opt.label}
                    </button>
                  )
                })}
              </div>
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
            <RotationMatrix
              data={data}
              visibleMetrics={visibleMetrics}
              highlightRange={highlightRange}
              sortBy={sortBy}
              onSortChange={setSortBy}
            />
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
