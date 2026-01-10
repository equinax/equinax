/**
 * ETF Rotation Page
 *
 * Displays a date × sub-category matrix heatmap for ETF rotation analysis.
 * Features:
 * - All sub-categories expanded by default (flat view)
 * - SVG-based matrix with grouped category headers
 * - Infinite scroll for historical data
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
  useGetEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet,
  getEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfRotationFlatResponse } from '@/api/generated/schemas'
import { EtfRotationMatrix } from '@/components/etf-rotation'

// Initial load and pagination size
const DAYS_PER_PAGE = 60

/**
 * Get the date before a given date string (YYYY-MM-DD format)
 */
function getDateBefore(dateStr: string): string {
  const date = new Date(dateStr)
  date.setDate(date.getDate() - 1)
  return date.toISOString().split('T')[0]
}

/**
 * Merge two EtfRotationFlatResponse objects (append new data to existing)
 */
function mergeRotationData(
  existing: EtfRotationFlatResponse,
  newData: EtfRotationFlatResponse
): EtfRotationFlatResponse {
  // Combine trading days (new data is older, append at end)
  const allDays = [...existing.trading_days, ...newData.trading_days]
  // Remove duplicates while preserving order (most recent first)
  const uniqueDays = [...new Set(allDays)]

  // Merge each sub-category's cells
  const mergedSubCategories = existing.sub_categories.map((col) => {
    const newCol = newData.sub_categories.find((c) => c.name === col.name)
    return {
      ...col,
      cells: [...col.cells, ...(newCol?.cells || [])],
    }
  })

  return {
    ...existing,
    trading_days: uniqueDays,
    sub_categories: mergedSubCategories,
    days: uniqueDays.length,
  }
}

export default function EtfRotationPage() {
  const navigate = useNavigate()

  // Infinite scroll state
  const [allData, setAllData] = useState<EtfRotationFlatResponse | null>(null)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [hasMore, setHasMore] = useState(true)
  const loadMoreRef = useRef(false)

  // Initial data fetch
  const { data: initialData, isLoading, isFetching } = useGetEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet(
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

  // Load more data
  const loadMore = useCallback(async () => {
    if (isLoadingMore || !hasMore || !allData || loadMoreRef.current) return

    const earliestDate = allData.trading_days[allData.trading_days.length - 1]
    if (!earliestDate) return

    loadMoreRef.current = true
    setIsLoadingMore(true)

    try {
      const moreData = await getEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet({
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
              <CardTitle className="text-lg">ETF 轮动矩阵</CardTitle>
              {isFetching && !showInitialLoading && (
                <span className="text-xs text-muted-foreground animate-pulse">
                  更新中...
                </span>
              )}
            </div>

            {/* Legend */}
            <div className="flex items-center gap-4 text-xs text-muted-foreground">
              <span>最近 {allData?.trading_days.length || DAYS_PER_PAGE} 个交易日</span>
              <div className="flex items-center gap-2">
                <span className="w-3 h-3 rounded bg-red-500" />
                <span>上涨</span>
                <span className="w-3 h-3 rounded bg-green-500" />
                <span>下跌</span>
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent className="flex-1 min-h-0 p-4 pt-0 flex flex-col">
          {showInitialLoading ? (
            <ComputingConsole title="正在计算ETF轮动数据..." steps={steps} progress={progress} />
          ) : allData ? (
            <EtfRotationMatrix
              data={allData}
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
