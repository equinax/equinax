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
  useGetEtfScreenerApiV1AlphaRadarEtfScreenerGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfCategory, ScreenerTab, TimeMode } from '@/api/generated/schemas'
import { MarketDashboard } from '@/components/alpha-radar/MarketDashboard'
import { RadarDataTable } from '@/components/alpha-radar/RadarDataTable'
import { EtfDataTable } from '@/components/alpha-radar/EtfDataTable'
import { TimeController } from '@/components/alpha-radar/TimeController'
import { SectorHeatmap } from '@/components/alpha-radar/SectorHeatmap'
import { EtfCategoryHeatmap } from '@/components/alpha-radar/EtfCategoryHeatmap'
import { TomorrowPrediction } from '@/components/alpha-radar/TomorrowPrediction'

// Radar mode type
type RadarMode = 'stock' | 'etf'

// Tab configuration for stock screener
const STOCK_TABS = [
  { value: 'panorama', label: '全景综合', description: '动量+估值+质量+主力+技术' },
  { value: 'smart', label: '聪明钱吸筹', description: '主力强度+量价+价格位置' },
  { value: 'value', label: '深度价值', description: '低估+质量+稳定+分红' },
  { value: 'trend', label: '超级趋势', description: '动量+突破+量能+趋势' },
] as const

// Tab configuration for ETF screener
const ETF_TABS = [
  { value: 'all', label: '全部', description: '所有ETF' },
  { value: 'broad', label: '宽基/大盘', description: '沪深300/中证500/科创50' },
  { value: 'sector', label: '行业', description: '银行/证券/医药/消费' },
  { value: 'theme', label: '赛道', description: '半导体/新能源/AI' },
  { value: 'cross_border', label: '跨境/QDII', description: '纳指/标普/恒生科技' },
  { value: 'commodity', label: '商品', description: '黄金/豆粕/原油' },
  { value: 'bond', label: '债券', description: '国债/城投债' },
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

  // Initialize radarMode from URL params (default: stock)
  const [radarMode, setRadarMode] = useState<RadarMode>(() => {
    const modeParam = searchParams.get('mode')
    return modeParam === 'etf' ? 'etf' : 'stock'
  })

  // Time controller state
  const [timeMode, setTimeMode] = useState<TimeMode>('snapshot')
  const [dateRange, setDateRange] = useState<{ from?: Date; to?: Date }>({})

  // Initialize selectedDate from URL params
  const [selectedDate, setSelectedDate] = useState<Date | undefined>(() => {
    const dateParam = searchParams.get('date')
    return dateParam ? parseDateString(dateParam) : undefined
  })

  // Tab state (for stock screener)
  const [activeTab, setActiveTab] = useState<ScreenerTab>('panorama')

  // Initialize ETF category from URL params (default: all)
  const [etfCategory, setEtfCategory] = useState<EtfCategory | 'all'>(() => {
    const categoryParam = searchParams.get('category')
    const validCategories = ['all', 'broad', 'sector', 'theme', 'cross_border', 'commodity', 'bond']
    return validCategories.includes(categoryParam || '') ? (categoryParam as EtfCategory | 'all') : 'all'
  })

  // Sync state to URL (radarMode, selectedDate, etfCategory)
  useEffect(() => {
    setSearchParams(prev => {
      // Sync radarMode
      if (radarMode === 'etf') {
        prev.set('mode', 'etf')
      } else {
        prev.delete('mode')
      }

      // Sync selectedDate
      if (selectedDate) {
        prev.set('date', formatDateString(selectedDate))
      } else {
        prev.delete('date')
      }

      // Sync etfCategory (only when in ETF mode)
      if (radarMode === 'etf' && etfCategory !== 'all') {
        prev.set('category', etfCategory)
      } else {
        prev.delete('category')
      }

      return prev
    }, { replace: true })
  }, [radarMode, selectedDate, etfCategory, setSearchParams])

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
    date: selectedDate ? formatDateString(selectedDate) : undefined,
    start_date: dateRange.from ? formatDateString(dateRange.from) : undefined,
    end_date: dateRange.to ? formatDateString(dateRange.to) : undefined,
  })

  // Fetch stock screener data (only when in stock mode)
  const { data: screener, isLoading: isLoadingScreener } = useGetScreenerApiV1AlphaRadarScreenerGet(
    {
      tab: activeTab,
      mode: timeMode,
      date: selectedDate ? formatDateString(selectedDate) : undefined,
      start_date: dateRange.from ? formatDateString(dateRange.from) : undefined,
      end_date: dateRange.to ? formatDateString(dateRange.to) : undefined,
      page,
      page_size: pageSize,
      sort_by: sortBy as 'score' | 'change' | 'volume' | 'valuation' | 'main_strength' | 'code',
      sort_order: sortOrder as 'asc' | 'desc',
    },
    { query: { enabled: radarMode === 'stock' } }
  )

  // Fetch ETF screener data (only when in ETF mode)
  const { data: etfScreener, isLoading: isLoadingEtfScreener } = useGetEtfScreenerApiV1AlphaRadarEtfScreenerGet(
    {
      category: etfCategory === 'all' ? undefined : (etfCategory as EtfCategory),
      date: selectedDate ? formatDateString(selectedDate) : undefined,
      page,
      page_size: pageSize,
      sort_by: 'amount',
      sort_order: 'desc',
      representative_only: true,
    },
    { query: { enabled: radarMode === 'etf' } }
  )

  // Loading lock - prevent rapid date changes while data is loading
  const isAnyLoading = isLoadingDashboard || isLoadingScreener || isLoadingEtfScreener

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

  // Handle radar mode change - reset to page 1
  const handleRadarModeChange = (mode: RadarMode) => {
    setRadarMode(mode)
    setPage(1)
  }

  // Handle tab change (stock screener) - reset to page 1
  const handleTabChange = (tab: string) => {
    setActiveTab(tab as ScreenerTab)
    setPage(1)
  }

  // Handle ETF category change - reset to page 1
  const handleEtfCategoryChange = (category: string) => {
    setEtfCategory(category as EtfCategory | 'all')
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

  // Get active screener data based on mode
  const activeScreener = radarMode === 'stock' ? screener : etfScreener
  const isLoadingActiveScreener = radarMode === 'stock' ? isLoadingScreener : isLoadingEtfScreener
  const totalPages = activeScreener?.pages || 1

  return (
    <div className="space-y-4">
      {/* Header + Time Controller */}
      <div className="flex items-center gap-4">
        <div className="shrink-0">
          <h1 className="text-2xl font-bold">α Radar</h1>
          <p className="text-sm text-muted-foreground font-mono h-5">
            {selectedDate
              ? formatDateString(selectedDate)
              : activeScreener?.date ?? <span className="invisible">0000-00-00</span>}
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
            defaultActiveDate={activeScreener?.date ?? undefined}
          />
        </div>
        {/* Mode Toggle */}
        <div className="flex items-center gap-1 border rounded-lg p-1 shrink-0">
          <Button
            variant={radarMode === 'stock' ? 'default' : 'ghost'}
            size="sm"
            className="text-xs h-7 px-3"
            onClick={() => handleRadarModeChange('stock')}
          >
            股票
          </Button>
          <Button
            variant={radarMode === 'etf' ? 'default' : 'ghost'}
            size="sm"
            className="text-xs h-7 px-3"
            onClick={() => handleRadarModeChange('etf')}
          >
            ETF
          </Button>
        </div>
      </div>

      {/* Market Dashboard - 4 Cards */}
      <MarketDashboard data={dashboard} isLoading={isLoadingDashboard} />

      {/* Sector Heatmap - Only show in stock mode */}
      {radarMode === 'stock' && (
        <SectorHeatmap
          timeMode={timeMode}
          selectedDate={selectedDate}
          dateRange={dateRange}
        />
      )}

      {/* ETF Category Heatmap - Only show in ETF mode */}
      {radarMode === 'etf' && (
        <EtfCategoryHeatmap selectedDate={selectedDate} />
      )}

      {/* Tomorrow Prediction - Only show in ETF mode */}
      {radarMode === 'etf' && (
        <TomorrowPrediction selectedDate={selectedDate} />
      )}

      {/* Screener Section */}
      <Card>
        <CardHeader className="pb-3 pt-3">
          <div className="flex items-center gap-4">
            {/* Title */}
            <CardTitle className="text-lg shrink-0">
              {radarMode === 'stock' ? '智能选股' : 'ETF雷达'}
            </CardTitle>

            {/* Tabs - conditional based on mode */}
            {radarMode === 'stock' ? (
              <>
                <Tabs value={activeTab} onValueChange={handleTabChange} className="shrink-0">
                  <TabsList>
                    {STOCK_TABS.map((tab) => (
                      <TabsTrigger key={tab.value} value={tab.value} className="text-xs px-3">
                        {tab.label}
                      </TabsTrigger>
                    ))}
                  </TabsList>
                </Tabs>
                <span className="text-xs text-muted-foreground shrink-0">
                  {STOCK_TABS.find((t) => t.value === activeTab)?.description}
                </span>
              </>
            ) : (
              <>
                <Tabs value={etfCategory} onValueChange={handleEtfCategoryChange} className="shrink-0">
                  <TabsList>
                    {ETF_TABS.map((tab) => (
                      <TabsTrigger key={tab.value} value={tab.value} className="text-xs px-3">
                        {tab.label}
                      </TabsTrigger>
                    ))}
                  </TabsList>
                </Tabs>
                <span className="text-xs text-muted-foreground shrink-0">
                  {ETF_TABS.find((t) => t.value === etfCategory)?.description}
                </span>
              </>
            )}

            {/* Spacer */}
            <div className="flex-1" />

            {/* Count */}
            <span className="text-sm text-muted-foreground shrink-0">
              共 {activeScreener?.total?.toLocaleString() || 0} 只
            </span>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {/* Data Table - conditional based on mode */}
          {radarMode === 'stock' ? (
            <RadarDataTable
              data={screener?.items || []}
              isLoading={isLoadingScreener}
              sorting={sorting}
              onSortingChange={handleSortingChange}
              timeMode={timeMode}
              activeDate={screener?.date ?? undefined}
            />
          ) : (
            <EtfDataTable
              data={etfScreener?.items || []}
              isLoading={isLoadingEtfScreener}
              activeDate={etfScreener?.date ?? undefined}
            />
          )}

          {/* Pagination */}
          {activeScreener && activeScreener.pages > 1 && (
            <div className="flex items-center justify-between mt-4 pt-4 border-t">
              <p className="text-sm text-muted-foreground">
                第 {page} / {totalPages} 页，共 {activeScreener.total.toLocaleString()} 条
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1 || isLoadingActiveScreener}
                >
                  <ChevronLeft className="h-4 w-4" />
                  上一页
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages || isLoadingActiveScreener}
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
