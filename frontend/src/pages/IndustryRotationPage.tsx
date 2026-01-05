/**
 * Industry Rotation Page
 *
 * Displays a date × industry matrix heatmap for sector rotation analysis.
 * Features:
 * - Full-height layout with fixed header
 * - Infinite scroll to load more historical data
 * - Multiple sorting options with column animation
 * - Multi-metric display in cells
 * - Algorithm signals (momentum, reversal, divergence)
 * - Rich tooltips with top stocks and metrics
 */

import { useState, useCallback, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { keepPreviousData } from '@tanstack/react-query'
import { ArrowLeft } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import {
  useGetSectorRotationApiV1AlphaRadarSectorRotationGet,
  getSectorRotationApiV1AlphaRadarSectorRotationGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import { SectorRotationResponse } from '@/api/generated/schemas'
import { RotationMatrix, ColorRangeBar } from '@/components/industry-rotation'

// Metric options for cell display
export type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

// 3-state metric: off -> raw -> weighted -> off
export type MetricState = 'off' | 'raw' | 'weighted'

// Only change and volume support weighted mode
const METRIC_OPTIONS: { key: MetricKey; label: string; activeColor: string; supportsWeighted: boolean }[] = [
  { key: 'change', label: '涨跌幅', activeColor: 'bg-[#c93b3b]', supportsWeighted: true },
  { key: 'volume', label: '成交量', activeColor: 'bg-[#2989c9]', supportsWeighted: true },
  { key: 'flow', label: '资金流入', activeColor: 'bg-[#c47a30]', supportsWeighted: false },
  { key: 'momentum', label: '动量', activeColor: 'bg-[#7a2eb0]', supportsWeighted: false },
]

// Get button label with weighted indicator
const getMetricLabel = (opt: typeof METRIC_OPTIONS[0], state: MetricState): string => {
  if (state === 'weighted') return `${opt.label}(加权)`
  return opt.label
}

// Cycle through states: off -> raw -> weighted -> off
const cycleMetricState = (current: MetricState, supportsWeighted: boolean): MetricState => {
  if (current === 'off') return 'raw'
  if (current === 'raw' && supportsWeighted) return 'weighted'
  return 'off' // weighted -> off, or raw (no weighted) -> off
}

// Initial load and pagination size
const DAYS_PER_PAGE = 40

// Highlight range for color filter
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

/**
 * Get the date before a given date string (YYYY-MM-DD format)
 */
function getDateBefore(dateStr: string): string {
  const date = new Date(dateStr)
  date.setDate(date.getDate() - 1)
  return date.toISOString().split('T')[0]
}

/**
 * Merge two SectorRotationResponse objects (append new data to existing)
 */
function mergeRotationData(
  existing: SectorRotationResponse,
  newData: SectorRotationResponse
): SectorRotationResponse {
  // Combine trading days (new data is older, append at end)
  const allDays = [...existing.trading_days, ...newData.trading_days]
  // Remove duplicates while preserving order (most recent first)
  const uniqueDays = [...new Set(allDays)]

  // Merge each industry's cells
  const mergedIndustries = existing.industries.map((ind) => {
    const newInd = newData.industries.find((i) => i.code === ind.code)
    return {
      ...ind,
      cells: [...ind.cells, ...(newInd?.cells || [])],
    }
  })

  return {
    ...existing,
    trading_days: uniqueDays,
    industries: mergedIndustries,
    stats: {
      ...existing.stats,
      trading_days: uniqueDays.length,
    },
  }
}

export default function IndustryRotationPage() {
  const navigate = useNavigate()
  // 3-state metrics: off/raw/weighted (default: change=raw, others=off)
  const [metricStates, setMetricStates] = useState<Record<MetricKey, MetricState>>({
    change: 'raw',
    volume: 'off',
    flow: 'off',
    momentum: 'off',
  })
  // Track last selected metric for color range bar
  const [lastSelectedMetric, setLastSelectedMetric] = useState<MetricKey>('change')
  // Highlight range for filtering cells by color
  const [highlightRange, setHighlightRange] = useState<HighlightRange | null>(null)

  // Infinite scroll state
  const [allData, setAllData] = useState<SectorRotationResponse | null>(null)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const loadMoreRef = useRef(false) // Prevent duplicate loads

  // Derive visibleMetrics from metricStates (any non-off metric is visible)
  const visibleMetrics = METRIC_OPTIONS.filter((opt) => metricStates[opt.key] !== 'off').map((opt) => opt.key)

  // Toggle metric state: off -> raw -> weighted -> raw (for metrics with weighted support)
  const toggleMetric = useCallback((key: MetricKey) => {
    const opt = METRIC_OPTIONS.find((o) => o.key === key)
    if (!opt) return

    setMetricStates((prev) => {
      const currentState = prev[key]
      const nextState = cycleMetricState(currentState, opt.supportsWeighted)

      // Ensure at least one metric is visible
      if (nextState === 'off') {
        const otherVisible = Object.entries(prev).some(([k, v]) => k !== key && v !== 'off')
        if (!otherVisible) {
          // Can't turn off the last one - stay at raw
          return { ...prev, [key]: 'raw' }
        }
      }

      // When turning on a new metric, update lastSelectedMetric
      if (currentState === 'off') {
        setLastSelectedMetric(key)
      }

      return { ...prev, [key]: nextState }
    })
  }, [])

  // Handle color range bar hover
  const handleColorRangeHover = useCallback(
    (range: { min: number; max: number } | null) => {
      if (range) {
        setHighlightRange({ metric: lastSelectedMetric, ...range })
      } else {
        setHighlightRange(null)
      }
    },
    [lastSelectedMetric]
  )

  // Initial data fetch (30 days) - backend always returns upstream order
  const { data: initialData, isLoading, isFetching } = useGetSectorRotationApiV1AlphaRadarSectorRotationGet(
    {
      days: DAYS_PER_PAGE,
    },
    {
      query: {
        placeholderData: keepPreviousData,
      },
    }
  )

  // Update allData when initial data changes
  useEffect(() => {
    if (initialData) {
      setAllData(initialData)
      setHasMore(initialData.trading_days.length === DAYS_PER_PAGE)
    }
  }, [initialData])

  // Load more data - backend always returns upstream order
  const loadMore = useCallback(async () => {
    if (isLoadingMore || !hasMore || !allData || loadMoreRef.current) return

    const earliestDate = allData.trading_days[allData.trading_days.length - 1]
    if (!earliestDate) return

    loadMoreRef.current = true
    setIsLoadingMore(true)

    try {
      const moreData = await getSectorRotationApiV1AlphaRadarSectorRotationGet({
        days: DAYS_PER_PAGE,
        end_date: getDateBefore(earliestDate),
      })

      setAllData((prev) => (prev ? mergeRotationData(prev, moreData) : moreData))
      setHasMore(moreData.trading_days.length === DAYS_PER_PAGE)
    } catch (error) {
      console.error('Failed to load more data:', error)
    } finally {
      setIsLoadingMore(false)
      loadMoreRef.current = false
    }
  }, [isLoadingMore, hasMore, allData])

  // Loading state
  const showInitialLoading = isLoading && !allData
  const { steps, progress } = useComputingProgress(showInitialLoading, 'heatmap')

  return (
    <div className="h-[calc(100vh-32px)] flex flex-col">
      {/* Matrix Card - fills remaining height */}
      <Card className="flex-1 flex flex-col min-h-0">
        <CardHeader className="flex-shrink-0 pb-2 pt-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Button variant="ghost" size="sm" onClick={() => navigate(-1)}>
                <ArrowLeft className="h-4 w-4 mr-1" />
                返回
              </Button>
              <CardTitle className="text-lg">轮动矩阵</CardTitle>
              {isFetching && !showInitialLoading && (
                <span className="text-xs text-muted-foreground animate-pulse">
                  更新中...
                </span>
              )}
            </div>

            {/* Color Range Bar + Metric Group Buttons */}
            <div className="flex items-center gap-3">
              {/* Color range filter bar - uses weighted scale when metric is weighted */}
              <ColorRangeBar
                metric={lastSelectedMetric}
                isWeighted={metricStates[lastSelectedMetric] === 'weighted'}
                onHoverRange={handleColorRangeHover}
              />

              {/* Metric Group Buttons - 3 states: off/raw/weighted */}
              <div className="flex items-center border rounded-md overflow-hidden">
                {METRIC_OPTIONS.map((opt, idx) => {
                  const state = metricStates[opt.key]
                  const isFirst = idx === 0
                  return (
                    <button
                      key={opt.key}
                      onClick={() => toggleMetric(opt.key)}
                      className={`px-2.5 py-1 text-xs font-medium transition-colors ${
                        state === 'weighted'
                          ? `${opt.activeColor} text-white ring-2 ring-white/30 ring-inset`
                          : state === 'raw'
                            ? `${opt.activeColor} text-white`
                            : 'bg-background text-muted-foreground hover:text-foreground hover:bg-muted'
                      } ${!isFirst ? 'border-l' : ''}`}
                    >
                      {getMetricLabel(opt, state)}
                    </button>
                  )
                })}
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex-1 min-h-0 p-4 pt-0 flex flex-col">
          {showInitialLoading ? (
            <ComputingConsole title="正在计算行业轮动数据..." steps={steps} progress={progress} />
          ) : allData ? (
            <RotationMatrix
              data={allData}
              visibleMetrics={visibleMetrics}
              metricStates={metricStates}
              highlightRange={highlightRange}
              isLoadingMore={isLoadingMore}
              hasMore={hasMore}
              onLoadMore={loadMore}
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
