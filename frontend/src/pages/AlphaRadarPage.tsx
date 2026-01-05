import { useState, useMemo, useCallback, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import type { SortingState, Updater } from '@tanstack/react-table'
import { format } from 'date-fns'
import {
  useGetDashboardApiV1AlphaRadarDashboardGet,
  useGetScreenerApiV1AlphaRadarScreenerGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { ScreenerTab, TimeMode } from '@/api/generated/schemas'
import { MarketDashboard } from '@/components/alpha-radar/MarketDashboard'
import { RadarDataTable } from '@/components/alpha-radar/RadarDataTable'
import { TimeController } from '@/components/alpha-radar/TimeController'
import { SectorHeatmap } from '@/components/alpha-radar/SectorHeatmap'

// Tab configuration
const TABS = [
  { value: 'panorama', label: '全景综合', description: '动量+估值+质量+主力+技术' },
  { value: 'smart', label: '聪明钱吸筹', description: '主力强度+量价+价格位置' },
  { value: 'value', label: '深度价值', description: '低估+质量+稳定+分红' },
  { value: 'trend', label: '超级趋势', description: '动量+突破+量能+趋势' },
] as const

// Map frontend sorting to backend sort_by parameter
const sortFieldMap: Record<string, string> = {
  code: 'code',
  composite_score: 'score',
  change_pct: 'change',
  main_strength_proxy: 'main_strength',
  valuation_percentile: 'valuation',
}

export default function AlphaRadarPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  // Time controller state
  const [timeMode, setTimeMode] = useState<TimeMode>('snapshot')
  const [dateRange, setDateRange] = useState<{ from?: Date; to?: Date }>({})

  // Parse date string to Date object (avoiding timezone issues)
  const parseDateString = (dateStr: string): Date => {
    // Parse as local date to avoid timezone shift
    const [year, month, day] = dateStr.split('-').map(Number)
    return new Date(year, month - 1, day)
  }

  // Format Date to string (avoiding timezone issues)
  const formatDateString = (date: Date): string => {
    return format(date, 'yyyy-MM-dd')
  }

  // Initialize selectedDate from URL params
  const [selectedDate, setSelectedDate] = useState<Date | undefined>(() => {
    const dateParam = searchParams.get('date')
    return dateParam ? parseDateString(dateParam) : undefined
  })

  // Sync selectedDate to URL
  useEffect(() => {
    const currentDateParam = searchParams.get('date')
    const newDateStr = selectedDate ? formatDateString(selectedDate) : undefined

    if (newDateStr !== currentDateParam) {
      setSearchParams(prev => {
        if (newDateStr) {
          prev.set('date', newDateStr)
        } else {
          prev.delete('date')
        }
        return prev
      }, { replace: true })
    }
  }, [selectedDate, searchParams, setSearchParams])

  // Tab state
  const [activeTab, setActiveTab] = useState<ScreenerTab>('panorama')

  // Pagination state
  const [page, setPage] = useState(1)
  const pageSize = 50

  // Sorting state
  const [sorting, setSorting] = useState<SortingState>([])

  // Compute sort parameters
  const sortBy = useMemo(() => {
    if (sorting.length === 0) return 'score'
    const field = sorting[0].id
    return sortFieldMap[field] || 'score'
  }, [sorting])

  const sortOrder = useMemo(() => {
    if (sorting.length === 0) return 'desc'
    return sorting[0].desc ? 'desc' : 'asc'
  }, [sorting])

  // Fetch dashboard data
  const { data: dashboard, isLoading: isLoadingDashboard } = useGetDashboardApiV1AlphaRadarDashboardGet({
    mode: timeMode,
    date: selectedDate?.toISOString().split('T')[0],
    start_date: dateRange.from?.toISOString().split('T')[0],
    end_date: dateRange.to?.toISOString().split('T')[0],
  })

  // Fetch screener data
  const { data: screener, isLoading: isLoadingScreener } = useGetScreenerApiV1AlphaRadarScreenerGet({
    tab: activeTab,
    mode: timeMode,
    date: selectedDate?.toISOString().split('T')[0],
    start_date: dateRange.from?.toISOString().split('T')[0],
    end_date: dateRange.to?.toISOString().split('T')[0],
    page,
    page_size: pageSize,
    sort_by: sortBy as 'score' | 'change' | 'volume' | 'valuation' | 'main_strength' | 'code',
    sort_order: sortOrder as 'asc' | 'desc',
  })

  // Loading lock - prevent rapid date changes while data is loading
  const isAnyLoading = isLoadingDashboard || isLoadingScreener

  // Handle date change with loading lock
  const handleDateChange = useCallback((date: Date | undefined) => {
    if (isAnyLoading) return // Ignore clicks while loading
    setSelectedDate(date)
  }, [isAnyLoading])

  // Handle date range change with loading lock
  const handleDateRangeChange = useCallback((range: { from?: Date; to?: Date }) => {
    if (isAnyLoading) return
    setDateRange(range)
  }, [isAnyLoading])

  // Handle tab change - reset to page 1
  const handleTabChange = (tab: string) => {
    setActiveTab(tab as ScreenerTab)
    setPage(1)
  }

  // Handle sorting changes - reset to page 1
  const handleSortingChange = useCallback((updaterOrValue: Updater<SortingState>) => {
    setSorting((old) => {
      const newSorting = typeof updaterOrValue === 'function' ? updaterOrValue(old) : updaterOrValue
      setPage(1)
      return newSorting
    })
  }, [])

  // Handle time mode change
  const handleTimeModeChange = (mode: TimeMode) => {
    setTimeMode(mode)
    setPage(1)
  }

  const totalPages = screener?.pages || 1

  return (
    <div className="space-y-4">
      {/* Header + Time Controller */}
      <div className="flex items-center gap-4">
        <div className="shrink-0">
          <h1 className="text-2xl font-bold">α Radar</h1>
          <p className="text-sm text-muted-foreground font-mono h-5">
            {selectedDate
              ? selectedDate.toISOString().split('T')[0]
              : screener?.date ?? <span className="invisible">0000-00-00</span>}
          </p>
        </div>
        <div className="flex-1">
          <TimeController
            mode={timeMode}
            onModeChange={handleTimeModeChange}
            selectedDate={selectedDate}
            onDateChange={handleDateChange}
            dateRange={dateRange}
            onDateRangeChange={handleDateRangeChange}
            disabled={isAnyLoading}
            defaultActiveDate={screener?.date ?? undefined}
          />
        </div>
      </div>

      {/* Market Dashboard - 4 Cards */}
      <MarketDashboard data={dashboard} isLoading={isLoadingDashboard} />

      {/* Sector Heatmap */}
      <SectorHeatmap
        timeMode={timeMode}
        selectedDate={selectedDate}
        dateRange={dateRange}
      />

      {/* Intelligent Screener */}
      <Card>
        <CardHeader className="pb-3 pt-3">
          <div className="flex items-center gap-4">
            {/* Title */}
            <CardTitle className="text-lg shrink-0">智能选股</CardTitle>

            {/* Tabs */}
            <Tabs value={activeTab} onValueChange={handleTabChange} className="shrink-0">
              <TabsList>
                {TABS.map((tab) => (
                  <TabsTrigger key={tab.value} value={tab.value} className="text-xs px-3">
                    {tab.label}
                  </TabsTrigger>
                ))}
              </TabsList>
            </Tabs>

            {/* Description */}
            <span className="text-xs text-muted-foreground shrink-0">
              {TABS.find((t) => t.value === activeTab)?.description}
            </span>

            {/* Spacer */}
            <div className="flex-1" />

            {/* Count */}
            <span className="text-sm text-muted-foreground shrink-0">
              共 {screener?.total?.toLocaleString() || 0} 只
            </span>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {/* Data Table */}
          <RadarDataTable
            data={screener?.items || []}
            isLoading={isLoadingScreener}
            sorting={sorting}
            onSortingChange={handleSortingChange}
            timeMode={timeMode}
            activeDate={screener?.date ?? undefined}
          />

          {/* Pagination */}
          {screener && screener.pages > 1 && (
            <div className="flex items-center justify-between mt-4 pt-4 border-t">
              <p className="text-sm text-muted-foreground">
                第 {page} / {totalPages} 页，共 {screener.total.toLocaleString()} 条
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1 || isLoadingScreener}
                >
                  <ChevronLeft className="h-4 w-4" />
                  上一页
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages || isLoadingScreener}
                >
                  下一页
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
