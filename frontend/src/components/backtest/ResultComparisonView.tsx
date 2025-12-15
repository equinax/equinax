import { useState, useMemo, useCallback, useEffect } from 'react'
import { useQueries } from '@tanstack/react-query'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  LineChart,
  Calendar,
  List,
  BarChart3,
  AlertCircle,
  Eye,
  EyeOff,
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
import type { EquityCurvePoint, TradeRecord, MonthlyReturns } from '@/types/backtest'

// 默认显示的最大股票数量（图表显示）
const DEFAULT_VISIBLE_STOCKS = 20
// 最大加载的股票数量
const MAX_STOCKS_TO_LOAD = 100

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
  // hiddenStocks: 记录被隐藏的股票（点击图例后变灰的）
  const [hiddenStocks, setHiddenStocks] = useState<Set<string>>(new Set())

  // 限制加载的股票数量
  const limitedResults = useMemo(() => {
    if (!results) return []
    return results.slice(0, MAX_STOCKS_TO_LOAD)
  }, [results])

  // 所有股票代码
  const allStockCodes = useMemo(() => {
    return limitedResults.map(r => r.stock_code)
  }, [limitedResults])

  // 初始化时隐藏超过默认显示数量的股票
  useEffect(() => {
    if (results && results.length > DEFAULT_VISIBLE_STOCKS) {
      const stocksToHide = new Set(
        results.slice(DEFAULT_VISIBLE_STOCKS, MAX_STOCKS_TO_LOAD).map(r => r.stock_code)
      )
      setHiddenStocks(stocksToHide)
    }
  }, [results])

  // 切换股票显示/隐藏状态
  const toggleStock = useCallback((stockCode: string) => {
    setHiddenStocks(prev => {
      const next = new Set(prev)
      if (next.has(stockCode)) {
        next.delete(stockCode)
      } else {
        next.add(stockCode)
      }
      return next
    })
  }, [])

  // 可见（未隐藏）的股票
  const visibleResults = useMemo(() => {
    return limitedResults.filter(r => !hiddenStocks.has(r.stock_code))
  }, [limitedResults, hiddenStocks])

  // 获取基础信息（所有加载的股票）
  const resultQueries = useQueries({
    queries: limitedResults.map(result => ({
      queryKey: getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey(jobId, result.id),
      queryFn: ({ signal }) => getBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet(jobId, result.id, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id,
    })),
  })

  // 获取所有已加载股票的权益曲线（使用缓存，只在显示图表时）
  const equityQueries = useQueries({
    queries: limitedResults.map(result => ({
      queryKey: getGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGetQueryKey(jobId, result.id),
      queryFn: ({ signal }) => getBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet(jobId, result.id, undefined, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id && showChart,
    })),
  })

  // 获取所有已加载股票的交易记录
  const tradesQueries = useQueries({
    queries: limitedResults.map(result => ({
      queryKey: getGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGetQueryKey(jobId, result.id, { page_size: 200 }),
      queryFn: ({ signal }) => getBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet(jobId, result.id, { page_size: 200 }, signal),
      staleTime: 5 * 60 * 1000,
      enabled: !!jobId && !!result.id,
    })),
  })

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
      const stockCode = limitedResults?.[index]?.stock_code || result.stock_code

      if (!stockCode) return

      // 月度收益（所有股票都处理，用于图例显示）
      if (result.monthly_returns && typeof result.monthly_returns === 'object') {
        // 只为可见股票添加月度收益数据
        if (!hiddenStocks.has(stockCode)) {
          monthlyReturns[stockCode] = result.monthly_returns as MonthlyReturns
        }
      }

      // 指标（所有股票都显示）
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

    // 聚合权益曲线（根据 hiddenStocks 过滤）
    equityQueries.forEach((query, index) => {
      if (!query.data) return
      const stockCode = limitedResults?.[index]?.stock_code
      if (!stockCode || hiddenStocks.has(stockCode)) return

      equityCurves[stockCode] = query.data.map(p => ({
        date: p.date,
        value: Number(p.value),
        drawdown: p.drawdown ? Number(p.drawdown) : undefined,
      }))
    })

    // 聚合交易记录（根据 hiddenStocks 过滤）
    tradesQueries.forEach((query, index) => {
      if (!query.data?.items) return
      const stockCode = limitedResults?.[index]?.stock_code
      if (!stockCode || hiddenStocks.has(stockCode)) return

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
  }, [resultQueries, equityQueries, tradesQueries, limitedResults, hiddenStocks])

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
  const visibleCount = visibleResults.length

  return (
    <div className="space-y-4">
      {/* 简化的控制栏 */}
      <div className="flex flex-wrap items-center justify-between gap-2 p-3 bg-muted/50 rounded-lg">
        <div className="flex items-center gap-3">
          <div className="text-sm text-muted-foreground">
            共 <span className="font-medium text-foreground">{totalResults}</span> 只股票
            {totalResults > MAX_STOCKS_TO_LOAD && (
              <span className="text-xs ml-1">(已加载 {MAX_STOCKS_TO_LOAD})</span>
            )}
          </div>
          <Badge variant="secondary" className="gap-1">
            <Eye className="h-3 w-3" />
            显示 {visibleCount} 只
          </Badge>
          <span className="text-xs text-muted-foreground">
            点击图例切换显示
          </span>
        </div>

        <div className="flex items-center gap-2">
          {/* 显示/隐藏图表切换 */}
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
          ) : isLoading && visibleResults.length > 0 ? (
            <LoadingSkeleton />
          ) : (
            <MultiEquityCurveChart
              data={aggregatedData.equityCurves}
              trades={aggregatedData.trades}
              height={400}
              allStockCodes={allStockCodes}
              hiddenStocks={hiddenStocks}
              onToggleStock={toggleStock}
            />
          )}
        </TabsContent>

        <TabsContent value="monthly" className="mt-4">
          {isLoading && visibleResults.length > 0 ? (
            <LoadingSkeleton />
          ) : (
            <MultiMonthlyReturnsChart
              data={aggregatedData.monthlyReturns}
              height={350}
              allStockCodes={allStockCodes}
              hiddenStocks={hiddenStocks}
              onToggleStock={toggleStock}
            />
          )}
        </TabsContent>

        <TabsContent value="trades" className="mt-4">
          {visibleResults.length === 0 ? (
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
