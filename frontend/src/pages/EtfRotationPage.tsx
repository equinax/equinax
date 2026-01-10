/**
 * ETF Rotation Page
 *
 * Displays a date × sub-category matrix heatmap for ETF rotation analysis.
 * Features:
 * - All sub-categories expanded by default (flat view)
 * - SVG-based matrix with grouped category headers
 * - Infinite scroll for historical data
 * - Optional prediction overlay (toggle)
 */

import { useState, useCallback, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { keepPreviousData } from '@tanstack/react-query'
import { ArrowLeft, Target } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Slider } from '@/components/ui/slider'
import { ComputingConsole } from '@/components/ui/computing-console'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import {
  useGetEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet,
  getEtfRotationFlatApiV1AlphaRadarEtfRotationFlatGet,
  useGetEtfPredictionApiV1AlphaRadarEtfPredictionGet,
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

  // Prediction overlay state
  const [showPrediction, setShowPrediction] = useState(false)
  const [predictionDate, setPredictionDate] = useState<string | null>(null)
  const [predictionTopN, setPredictionTopN] = useState(5)

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
      // Set default prediction date to first trading day
      if (!predictionDate && initialData.trading_days.length > 0) {
        setPredictionDate(initialData.trading_days[0])
      }
    }
  }, [initialData, predictionDate])

  // Fetch prediction data when enabled
  const { data: predictionData } = useGetEtfPredictionApiV1AlphaRadarEtfPredictionGet(
    {
      date: predictionDate || undefined,
      min_score: 0,
    },
    {
      query: {
        enabled: showPrediction && !!predictionDate,
        placeholderData: keepPreviousData,
      },
    }
  )

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

            {/* Prediction Toggle */}
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <Switch
                  id="prediction-toggle"
                  checked={showPrediction}
                  onCheckedChange={setShowPrediction}
                />
                <Label htmlFor="prediction-toggle" className="text-xs flex items-center gap-1 cursor-pointer">
                  <Target className="h-3 w-3" />
                  明日预测
                </Label>
              </div>
              {showPrediction && (
                <>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">前</span>
                    <Slider
                      value={[predictionTopN]}
                      onValueChange={(values: number[]) => setPredictionTopN(values[0])}
                      min={5}
                      max={80}
                      step={5}
                      className="w-20"
                    />
                    <span className="text-xs text-muted-foreground w-4">{predictionTopN}</span>
                  </div>
                  {predictionDate && (
                    <span className="text-xs text-muted-foreground">
                      基于 {predictionDate}
                    </span>
                  )}
                </>
              )}
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
              showPrediction={showPrediction}
              predictionData={predictionData}
              predictionDate={predictionDate}
              onPredictionDateChange={setPredictionDate}
              predictionTopN={predictionTopN}
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
