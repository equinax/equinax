import { useState, useMemo, useCallback, useEffect } from 'react'
import { useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  LineChart,
  Calendar,
  List,
  BarChart3,
  AlertCircle,
  Eye,
  EyeOff,
  Loader2,
} from 'lucide-react'
import { MultiEquityCurveChart } from './MultiEquityCurveChart'
import { MultiMonthlyReturnsChart } from './MultiMonthlyReturnsChart'
import { MultiTradesTable } from './MultiTradesTable'
import { MultiMetricsTable } from './MultiMetricsTable'
import {
  getBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet,
  getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey,
  getBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet,
  getGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGetQueryKey,
  getBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet,
  getGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGetQueryKey,
} from '@/api/generated/backtests/backtests'
import {
  getStockApiV1StocksCodeGet,
  getGetStockApiV1StocksCodeGetQueryKey,
} from '@/api/generated/stocks/stocks'
import type { EquityCurvePoint, TradeRecord, MonthlyReturns } from '@/types/backtest'
import { getChartPalette } from '@/lib/market-colors'
import { useTheme } from '@/components/theme-provider'
import { cn } from '@/lib/utils'

// 默认显示的最大股票数量
const DEFAULT_VISIBLE_STOCKS = 20
// 最大同时加载的股票数量
const MAX_CONCURRENT_LOAD = 50

interface BacktestResult {
  id: string
  stock_code: string
}

interface ResultComparisonViewProps {
  jobId: string
  results: BacktestResult[] | null | undefined
}

function LoadingSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-8 w-full" />
      <Skeleton className="h-[350px] w-full" />
    </div>
  )
}

export function ResultComparisonView({ jobId, results }: ResultComparisonViewProps) {
  const [activeTab, setActiveTab] = useState('equity')
  const [showChart, setShowChart] = useState(true)
  // visibleStocks: 记录要显示的股票（在图表上显示的）
  const [visibleStocks, setVisibleStocks] = useState<Set<string>>(new Set())
  // loadingStocks: 正在加载中的股票
  const [loadingStocks, setLoadingStocks] = useState<Set<string>>(new Set())

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const chartPalette = useMemo(() => getChartPalette(isDark), [isDark])
  const grayColor = isDark ? '#52525b' : '#a1a1aa'

  const queryClient = useQueryClient()

  // 创建 stock_code -> result 映射
  const stockResultMap = useMemo(() => {
    const map = new Map<string, BacktestResult>()
    results?.forEach(r => map.set(r.stock_code, r))
    return map
  }, [results])

  // 所有股票代码（全部结果，不限制数量）
  const allStockCodes = useMemo(() => {
    return results?.map(r => r.stock_code) ?? []
  }, [results])

  // 初始化时显示前 N 只股票
  useEffect(() => {
    if (results && results.length > 0 && visibleStocks.size === 0) {
      const initialVisible = new Set(
        results.slice(0, DEFAULT_VISIBLE_STOCKS).map(r => r.stock_code)
      )
      setVisibleStocks(initialVisible)
    }
  }, [results, visibleStocks.size])

  // 要加载数据的股票（可见的股票，限制并发数量）
  const stocksToLoad = useMemo(() => {
    const visible = Array.from(visibleStocks)
    return visible.slice(0, MAX_CONCURRENT_LOAD)
  }, [visibleStocks])

  // 获取要加载的 results
  const resultsToLoad = useMemo(() => {
    return stocksToLoad
      .map(code => stockResultMap.get(code))
      .filter((r): r is BacktestResult => r !== undefined)
  }, [stocksToLoad, stockResultMap])

  // 切换股票显示/隐藏状态
  const toggleStock = useCallback((stockCode: string) => {
    setVisibleStocks(prev => {
      const next = new Set(prev)
      if (next.has(stockCode)) {
        next.delete(stockCode)
      } else {
        next.add(stockCode)
        // 如果数据还没加载，标记为 loading
        const result = stockResultMap.get(stockCode)
        if (result) {
          const queryKey = getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey(jobId, result.id)
          const cached = queryClient.getQueryData(queryKey)
          if (!cached) {
            setLoadingStocks(current => new Set(current).add(stockCode))
          }
        }
      }
      return next
    })
  }, [stockResultMap, jobId, queryClient])

  // 获取基础信息（只加载可见股票）
  const resultQueries = useQueries({
    queries: resultsToLoad.map(result => ({
      queryKey: getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey(jobId, result.id),
      queryFn: ({ signal }) => getBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet(jobId, result.id, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id,
    })),
  })

  // 获取权益曲线（只加载可见股票）
  const equityQueries = useQueries({
    queries: resultsToLoad.map(result => ({
      queryKey: getGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGetQueryKey(jobId, result.id),
      queryFn: ({ signal }) => getBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet(jobId, result.id, undefined, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id && showChart,
    })),
  })

  // 获取交易记录（只加载可见股票）
  const tradesQueries = useQueries({
    queries: resultsToLoad.map(result => ({
      queryKey: getGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGetQueryKey(jobId, result.id, { page_size: 200 }),
      queryFn: ({ signal }) => getBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet(jobId, result.id, { page_size: 200 }, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id,
    })),
  })

  // 悬停的股票代码（用于按需加载股票信息）
  const [hoveredStock, setHoveredStock] = useState<string | null>(null)

  // 按需加载股票信息（只在悬停时加载）
  const stockInfoQuery = useQuery({
    queryKey: getGetStockApiV1StocksCodeGetQueryKey(hoveredStock ?? ''),
    queryFn: ({ signal }) => getStockApiV1StocksCodeGet(hoveredStock!, signal),
    staleTime: 30 * 60 * 1000, // 30 分钟缓存
    enabled: !!hoveredStock,
  })

  // 获取悬停股票的信息
  const hoveredStockInfo = useMemo(() => {
    if (!hoveredStock || !stockInfoQuery.data) return null
    return {
      name: stockInfoQuery.data.code_name ?? '',
      industry: stockInfoQuery.data.industry ?? undefined,
    }
  }, [hoveredStock, stockInfoQuery.data])

  // 更新 loadingStocks 状态
  useEffect(() => {
    const stillLoading = new Set<string>()
    resultsToLoad.forEach((result, index) => {
      if (resultQueries[index]?.isLoading || equityQueries[index]?.isLoading) {
        stillLoading.add(result.stock_code)
      }
    })
    setLoadingStocks(stillLoading)
  }, [resultQueries, equityQueries, resultsToLoad])

  const isLoading = resultQueries.some(q => q.isLoading) ||
    (showChart && equityQueries.some(q => q.isLoading)) ||
    tradesQueries.some(q => q.isLoading)
  const hasError = resultQueries.some(q => q.isError)

  // 聚合数据
  const aggregatedData = useMemo(() => {
    const equityCurves: Record<string, EquityCurvePoint[]> = {}
    const monthlyReturns: Record<string, MonthlyReturns> = {}
    const trades: Record<string, TradeRecord[]> = {}
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const metrics: Record<string, any> = {}

    // 处理所有已加载股票的基础数据
    resultQueries.forEach((query, index) => {
      if (!query.data) return

      const result = query.data
      const stockCode = resultsToLoad?.[index]?.stock_code || result.stock_code

      if (!stockCode) return

      // 月度收益
      if (result.monthly_returns && typeof result.monthly_returns === 'object') {
        monthlyReturns[stockCode] = result.monthly_returns as MonthlyReturns
      }

      // 指标
      metrics[stockCode] = {
        stock_code: stockCode,
        total_return: result.total_return,
        annual_return: result.annual_return,
        sharpe_ratio: result.sharpe_ratio,
        sortino_ratio: result.sortino_ratio,
        calmar_ratio: result.calmar_ratio,
        max_drawdown: result.max_drawdown,
        volatility: result.volatility,
        total_trades: result.total_trades,
        winning_trades: result.winning_trades,
        losing_trades: result.losing_trades,
        win_rate: result.win_rate,
        profit_factor: result.profit_factor,
        final_value: result.final_value,
      }
    })

    // 聚合权益曲线
    equityQueries.forEach((query, index) => {
      if (!query.data) return
      const stockCode = resultsToLoad?.[index]?.stock_code
      if (!stockCode) return

      equityCurves[stockCode] = query.data.map(p => ({
        date: p.date,
        value: Number(p.value),
        drawdown: p.drawdown ? Number(p.drawdown) : undefined,
      }))
    })

    // 聚合交易记录
    tradesQueries.forEach((query, index) => {
      if (!query.data?.items) return
      const stockCode = resultsToLoad?.[index]?.stock_code
      if (!stockCode) return

      trades[stockCode] = query.data.items.map(t => ({
        id: t.id,
        stock_code: t.stock_code,
        entry_date: t.entry_date,
        exit_date: t.exit_date ?? undefined,
        entry_price: Number(t.entry_price),
        exit_price: t.exit_price ? Number(t.exit_price) : undefined,
        size: t.size,
        pnl: t.pnl ? Number(t.pnl) : 0,
        pnl_percent: t.pnl_percent ? Number(t.pnl_percent) : 0,
        type: t.direction as 'long' | 'short',
        bars_held: t.bars_held ?? undefined,
      })) as TradeRecord[]
    })

    return { equityCurves, monthlyReturns, trades, metrics }
  }, [resultQueries, equityQueries, tradesQueries, resultsToLoad])

  // 拖拽选择状态
  const [isDragging, setIsDragging] = useState(false)
  const [dragAction, setDragAction] = useState<'show' | 'hide' | null>(null)

  // 全局 mouseup 监听
  useEffect(() => {
    const handleMouseUp = () => {
      setIsDragging(false)
      setDragAction(null)
    }
    window.addEventListener('mouseup', handleMouseUp)
    return () => window.removeEventListener('mouseup', handleMouseUp)
  }, [])

  if (!results || results.length === 0) {
    return (
      <div className="flex items-center justify-center h-[300px] text-muted-foreground">
        <p>暂无回测结果可供对比</p>
      </div>
    )
  }

  if (hasError) {
    return (
      <div className="flex flex-col items-center justify-center h-[300px] text-muted-foreground">
        <AlertCircle className="h-12 w-12 mb-4" />
        <p>加载数据失败</p>
      </div>
    )
  }

  const totalResults = results.length
  const visibleCount = visibleStocks.size

  // 图例组件
  const legendElement = (
    <TooltipProvider delayDuration={300}>
      <div className="flex flex-wrap gap-1.5 justify-center max-h-48 overflow-y-auto p-2 bg-muted/30 rounded-lg">
        {allStockCodes.map((stockCode, index) => {
          const isVisible = visibleStocks.has(stockCode)
          const isLoadingStock = loadingStocks.has(stockCode)
          const color = chartPalette[index % chartPalette.length]
          // 只有当前悬停的股票才显示详细信息
          const stockInfo = hoveredStock === stockCode ? hoveredStockInfo : null

          return (
            <Tooltip key={stockCode}>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  className={cn(
                    'flex items-center gap-1 px-1.5 py-0.5 rounded text-xs transition-all select-none',
                    'hover:bg-muted/80',
                    'cursor-pointer',
                    isVisible ? 'opacity-100' : 'opacity-50'
                  )}
                  onMouseDown={(e) => {
                    e.preventDefault()
                    setIsDragging(true)
                    const action = isVisible ? 'hide' : 'show'
                    setDragAction(action)
                    toggleStock(stockCode)
                  }}
                  onMouseEnter={() => {
                    // 设置悬停股票，触发按需加载
                    setHoveredStock(stockCode)
                    // 拖拽逻辑
                    if (!isDragging || !dragAction) return
                    if ((dragAction === 'show' && !isVisible) ||
                        (dragAction === 'hide' && isVisible)) {
                      toggleStock(stockCode)
                    }
                  }}
                  onMouseLeave={() => {
                    setHoveredStock(null)
                  }}
                >
                  {isLoadingStock ? (
                    <Loader2 className="w-2 h-2 animate-spin" style={{ color }} />
                  ) : (
                    <div
                      className="w-2 h-2 rounded-full transition-colors flex-shrink-0"
                      style={{ backgroundColor: isVisible ? color : grayColor }}
                    />
                  )}
                  <span className={cn(
                    'transition-colors truncate max-w-[80px]',
                    isVisible ? 'text-foreground' : 'text-muted-foreground line-through'
                  )}>
                    {stockCode}
                  </span>
                </button>
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-xs">
                <div className="space-y-1">
                  <div className="font-medium">{stockCode}</div>
                  {stockInfo?.name ? (
                    <div className="text-sm text-muted-foreground">{stockInfo.name}</div>
                  ) : stockInfoQuery.isLoading && hoveredStock === stockCode ? (
                    <div className="text-sm text-muted-foreground flex items-center gap-1">
                      <Loader2 className="w-3 h-3 animate-spin" />
                      加载中...
                    </div>
                  ) : null}
                  {stockInfo?.industry && (
                    <div className="text-xs text-muted-foreground">行业: {stockInfo.industry}</div>
                  )}
                  <div className="text-xs text-muted-foreground">
                    {isVisible ? '点击隐藏' : '点击显示'}
                  </div>
                </div>
              </TooltipContent>
            </Tooltip>
          )
        })}
      </div>
    </TooltipProvider>
  )

  return (
    <div className="space-y-4">
      {/* 简化的控制栏 */}
      <div className="flex flex-wrap items-center justify-between gap-2 p-3 bg-muted/50 rounded-lg">
        <div className="flex items-center gap-3">
          <div className="text-sm text-muted-foreground">
            共 <span className="font-medium text-foreground">{totalResults}</span> 只股票
          </div>
          <Badge variant="secondary" className="gap-1">
            <Eye className="h-3 w-3" />
            显示 {visibleCount} 只
          </Badge>
          {loadingStocks.size > 0 && (
            <Badge variant="outline" className="gap-1">
              <Loader2 className="h-3 w-3 animate-spin" />
              加载中 {loadingStocks.size}
            </Badge>
          )}
          <span className="text-xs text-muted-foreground">
            点击图例切换显示，拖拽批量选择
          </span>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowChart(!showChart)}
            className="gap-1"
          >
            {showChart ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            {showChart ? '隐藏图表' : '显示图表'}
          </Button>
        </div>
      </div>

      {/* 图例区域 */}
      {legendElement}

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="equity" className="gap-1" disabled={!showChart}>
            <LineChart className="h-4 w-4" />
            <span className="hidden sm:inline">权益曲线</span>
          </TabsTrigger>
          <TabsTrigger value="monthly" className="gap-1">
            <Calendar className="h-4 w-4" />
            <span className="hidden sm:inline">月度收益</span>
          </TabsTrigger>
          <TabsTrigger value="trades" className="gap-1">
            <List className="h-4 w-4" />
            <span className="hidden sm:inline">交易记录</span>
          </TabsTrigger>
          <TabsTrigger value="metrics" className="gap-1">
            <BarChart3 className="h-4 w-4" />
            <span className="hidden sm:inline">详细指标</span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="equity" className="mt-4">
          {!showChart ? (
            <div className="flex items-center justify-center h-[300px] text-muted-foreground">
              <div className="text-center">
                <EyeOff className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>图表已隐藏</p>
                <Button variant="outline" size="sm" className="mt-2" onClick={() => setShowChart(true)}>
                  显示图表
                </Button>
              </div>
            </div>
          ) : isLoading && visibleStocks.size > 0 ? (
            <LoadingSkeleton />
          ) : (
            <MultiEquityCurveChart
              data={aggregatedData.equityCurves}
              trades={aggregatedData.trades}
              height={400}
              allStockCodes={Array.from(visibleStocks)}
              hiddenStocks={new Set()}
              showLegend={false}
            />
          )}
        </TabsContent>

        <TabsContent value="monthly" className="mt-4">
          {isLoading && visibleStocks.size > 0 ? (
            <LoadingSkeleton />
          ) : (
            <MultiMonthlyReturnsChart
              data={aggregatedData.monthlyReturns}
              height={350}
              allStockCodes={Array.from(visibleStocks)}
              hiddenStocks={new Set()}
              showLegend={false}
            />
          )}
        </TabsContent>

        <TabsContent value="trades" className="mt-4">
          {visibleStocks.size === 0 ? (
            <div className="flex items-center justify-center h-[300px] text-muted-foreground">
              <div className="text-center">
                <AlertCircle className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>请点击图例选择至少一只股票</p>
              </div>
            </div>
          ) : isLoading ? (
            <LoadingSkeleton />
          ) : (
            <MultiTradesTable data={aggregatedData.trades} />
          )}
        </TabsContent>

        <TabsContent value="metrics" className="mt-4">
          {isLoading ? (
            <LoadingSkeleton />
          ) : (
            <MultiMetricsTable data={aggregatedData.metrics} />
          )}
        </TabsContent>
      </Tabs>
    </div>
  )
}
