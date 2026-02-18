import { useState, useMemo, useCallback, useEffect } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ChevronLeft, ChevronRight, BarChart3, X } from 'lucide-react'
import type { SortingState, Updater, RowSelectionState } from '@tanstack/react-table'
import { format } from 'date-fns'
import { motion, AnimatePresence } from 'motion/react'
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
  { value: 'overnight', label: '隔夜超短', description: '确定性隔夜机会·T+1买T+2卖' },
  { value: 'weekly', label: '周内短线', description: '稳定T+6，偏防守' },
  { value: 'rally', label: '主升浪', description: 'MA多头+量能阶梯+趋势质量' },
  { value: 'dragon', label: '龙头先锋', description: '主力吸筹+突破蓄力+量价一致' },
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
  const navigate = useNavigate()
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({})

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

  // Tab state (for stock screener) - restore from URL
  const [activeTab, setActiveTab] = useState<ScreenerTab>(() => {
    const tabParam = searchParams.get('tab')
    const validTabs = STOCK_TABS.map(t => t.value) as readonly string[]
    return validTabs.includes(tabParam || '') ? (tabParam as ScreenerTab) : 'overnight'
  })

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

      // Sync activeTab (only when in stock mode and not default)
      if (radarMode === 'stock' && activeTab !== 'weekly') {
        prev.set('tab', activeTab)
      } else {
        prev.delete('tab')
      }

      // Sync etfCategory (only when in ETF mode)
      if (radarMode === 'etf' && etfCategory !== 'all') {
        prev.set('category', etfCategory)
      } else {
        prev.delete('category')
      }

      return prev
    }, { replace: true })
  }, [radarMode, selectedDate, activeTab, etfCategory, setSearchParams])

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

  const handleDateChange = useCallback((date: Date | undefined) => {
    setSelectedDate(date)
    setRowSelection({})
  }, [])

  const handleDateRangeChange = useCallback((range: { from?: Date; to?: Date }) => {
    setDateRange(range)
  }, [])

  // Handle radar mode change - reset to page 1
  const handleRadarModeChange = (mode: RadarMode) => {
    setRadarMode(mode)
    setPage(1)
    setRowSelection({})
  }

  // Handle tab change (stock screener) - reset to page 1
  const handleTabChange = (tab: string) => {
    setActiveTab(tab as ScreenerTab)
    setPage(1)
    setRowSelection({})
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
    setRowSelection({})
  }

  const handleMultiBrowse = useCallback(() => {
    const selectedCodes = Object.keys(rowSelection)
    if (selectedCodes.length === 0) return
    const dateParam = screener?.date ?? (selectedDate ? formatDateString(selectedDate) : '')
    const params = new URLSearchParams()
    params.set('codes', selectedCodes.join(','))
    if (dateParam) params.set('date', dateParam)
    const items = screener?.items
    if (items) {
      const labelsMap: Record<string, string[]> = {}
      for (const item of items) {
        if (selectedCodes.includes(item.code) && item.quant_labels?.length) {
          labelsMap[item.code] = item.quant_labels
        }
      }
      if (Object.keys(labelsMap).length > 0) {
        params.set('labels', JSON.stringify(labelsMap))
      }
    }
    if (activeTab !== 'weekly') {
      params.set('tab', activeTab)
    }
    navigate(`/alpha-radar/multi-browse?${params.toString()}`)
  }, [rowSelection, screener?.date, screener?.items, selectedDate, activeTab, navigate])

  const handleSelectTopN = useCallback((n: number) => {
    const items = screener?.items
    if (!items) return
    const selection: RowSelectionState = {}
    for (let i = 0; i < Math.min(n, items.length); i++) {
      selection[items[i].code] = true
    }
    setRowSelection(selection)
  }, [screener?.items])

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
            disabled={false}
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

            {/* Quick select top N - stock mode only */}
            {radarMode === 'stock' && (
              <div className="flex items-center gap-1 shrink-0">
                {[3, 5, 10, 20].map((n) => (
                  <Button
                    key={n}
                    variant="ghost"
                    size="sm"
                    className="h-6 px-2 text-xs text-muted-foreground hover:text-foreground"
                    onClick={() => handleSelectTopN(n)}
                  >
                    前{n}
                  </Button>
                ))}
              </div>
            )}

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
              rowSelection={rowSelection}
              onRowSelectionChange={setRowSelection}
              abstain={screener?.abstain}
              abstainReason={screener?.abstain_reason ?? undefined}
              dashboard={dashboard}
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

      <AnimatePresence>
        {radarMode === 'stock' && Object.keys(rowSelection).length > 0 && (
          <motion.div
            initial={{ y: 100, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            exit={{ y: 100, opacity: 0 }}
            transition={{ type: 'spring', damping: 25, stiffness: 300 }}
            className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50"
          >
            <div className="flex items-center gap-3 bg-background/95 backdrop-blur-sm border rounded-full shadow-lg px-4 py-2">
              <span className="text-sm text-muted-foreground">
                已选 <span className="font-medium text-foreground">{Object.keys(rowSelection).length}</span> 只
              </span>
              <Button
                size="sm"
                onClick={handleMultiBrowse}
                className="rounded-full gap-1.5"
              >
                <BarChart3 className="h-4 w-4" />
                浏览K线
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setRowSelection({})}
                className="rounded-full h-8 w-8 p-0"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
