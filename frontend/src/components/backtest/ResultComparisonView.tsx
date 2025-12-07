import { useState, useMemo } from 'react'
import { useQueries } from '@tanstack/react-query'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Skeleton } from '@/components/ui/skeleton'
import { LineChart, Calendar, List, BarChart3, AlertCircle } from 'lucide-react'
import { MultiEquityCurveChart } from './MultiEquityCurveChart'
import { MultiMonthlyReturnsChart } from './MultiMonthlyReturnsChart'
import { MultiTradesTable } from './MultiTradesTable'
import { MultiMetricsTable } from './MultiMetricsTable'
import {
  getBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet,
  getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey,
} from '@/api/generated/backtests/backtests'
import type { EquityCurvePoint, TradeRecord, MonthlyReturns } from '@/types/backtest'

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

  // Parallel fetch all result details
  const resultQueries = useQueries({
    queries: (results ?? []).map(result => ({
      queryKey: getGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGetQueryKey(jobId, result.id),
      queryFn: ({ signal }) => getBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet(jobId, result.id, signal),
      staleTime: 5 * 60 * 1000, // 5 minutes
      enabled: !!jobId && !!result.id,
    })),
  })

  const isLoading = resultQueries.some(q => q.isLoading)
  const hasError = resultQueries.some(q => q.isError)

  // Aggregate data by stock_code
  const aggregatedData = useMemo(() => {
    const equityCurves: Record<string, EquityCurvePoint[]> = {}
    const monthlyReturns: Record<string, MonthlyReturns> = {}
    const trades: Record<string, TradeRecord[]> = {}
    const metrics: Record<string, any> = {}

    resultQueries.forEach((query, index) => {
      if (!query.data) return

      const result = query.data
      const stockCode = results?.[index]?.stock_code || result.stock_code

      if (!stockCode) return

      // Equity curve
      if (Array.isArray(result.equity_curve)) {
        equityCurves[stockCode] = result.equity_curve
      }

      // Monthly returns
      if (result.monthly_returns && typeof result.monthly_returns === 'object') {
        monthlyReturns[stockCode] = result.monthly_returns
      }

      // Trades
      if (Array.isArray(result.trades)) {
        trades[stockCode] = result.trades
      }

      // Metrics
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

    return { equityCurves, monthlyReturns, trades, metrics }
  }, [resultQueries, results])

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

  return (
    <div className="space-y-4">
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="equity" className="gap-1">
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
          {isLoading ? (
            <LoadingSkeleton />
          ) : (
            <MultiEquityCurveChart data={aggregatedData.equityCurves} trades={aggregatedData.trades} height={400} />
          )}
        </TabsContent>

        <TabsContent value="monthly" className="mt-4">
          {isLoading ? (
            <LoadingSkeleton />
          ) : (
            <MultiMonthlyReturnsChart data={aggregatedData.monthlyReturns} height={350} />
          )}
        </TabsContent>

        <TabsContent value="trades" className="mt-4">
          {isLoading ? (
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
