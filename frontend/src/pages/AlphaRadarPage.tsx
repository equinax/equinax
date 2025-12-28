import { useState, useMemo, useCallback } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ChevronLeft, ChevronRight, Radar } from 'lucide-react'
import type { SortingState, Updater } from '@tanstack/react-table'
import {
  useGetDashboardApiV1AlphaRadarDashboardGet,
  useGetScreenerApiV1AlphaRadarScreenerGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { ScreenerTab, TimeMode } from '@/api/generated/schemas'
import { MarketDashboard } from '@/components/alpha-radar/MarketDashboard'
import { RadarDataTable } from '@/components/alpha-radar/RadarDataTable'
import { TimeController } from '@/components/alpha-radar/TimeController'

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
  // Time controller state
  const [timeMode, setTimeMode] = useState<TimeMode>('snapshot')
  const [selectedDate, setSelectedDate] = useState<Date | undefined>(undefined)
  const [dateRange, setDateRange] = useState<{ from?: Date; to?: Date }>({})

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
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="h-10 w-10 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
          <Radar className="h-6 w-6 text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold">Alpha Radar</h1>
          <p className="text-muted-foreground text-sm">
            市场发现与智能选股
            {screener?.date && (
              <span className="ml-2">
                · 数据日期: <span className="font-mono">{screener.date}</span>
              </span>
            )}
          </p>
        </div>
      </div>

      {/* Time Controller */}
      <TimeController
        mode={timeMode}
        onModeChange={handleTimeModeChange}
        selectedDate={selectedDate}
        onDateChange={setSelectedDate}
        dateRange={dateRange}
        onDateRangeChange={setDateRange}
      />

      {/* Market Dashboard - 4 Cards */}
      <MarketDashboard data={dashboard} isLoading={isLoadingDashboard} />

      {/* Intelligent Screener */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg">智能选股</CardTitle>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">
                共 {screener?.total?.toLocaleString() || 0} 只
              </span>
            </div>
          </div>
          {/* Tab Selector */}
          <Tabs value={activeTab} onValueChange={handleTabChange} className="mt-2">
            <TabsList className="grid w-full grid-cols-4">
              {TABS.map((tab) => (
                <TabsTrigger key={tab.value} value={tab.value} className="text-xs sm:text-sm">
                  {tab.label}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
          <p className="text-xs text-muted-foreground mt-2">
            {TABS.find((t) => t.value === activeTab)?.description}
          </p>
        </CardHeader>
        <CardContent className="pt-0">
          {/* Data Table */}
          <RadarDataTable
            data={screener?.items || []}
            isLoading={isLoadingScreener}
            sorting={sorting}
            onSortingChange={handleSortingChange}
            timeMode={timeMode}
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
