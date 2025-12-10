import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from '@/components/ui/sheet'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { Badge } from '@/components/ui/badge'
import { formatPercent } from '@/lib/utils'
import { LineChart, Calendar, List, BarChart3, AlertCircle } from 'lucide-react'
import {
  useGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet,
  useGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet,
  useGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet,
} from '@/api/generated/backtests/backtests'
import { EquityCurveWithIndicators } from './EquityCurveWithIndicators'
import { MonthlyReturnsChart } from './MonthlyReturnsChart'
import { TradesTable } from './TradesTable'
import { MetricsCards } from './MetricsCards'
import type { EquityCurvePoint, TradeRecord, MonthlyReturns } from '@/types/backtest'
import type { BacktestResultDetailResponse } from '@/api/generated/schemas'

interface ResultDetailSheetProps {
  jobId: string
  resultId: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function LoadingSkeleton() {
  return (
    <div className="space-y-6">
      {/* Quick stats skeleton */}
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="space-y-2">
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-8 w-24" />
          </div>
        ))}
      </div>
      {/* Chart skeleton */}
      <Skeleton className="h-[400px] w-full" />
    </div>
  )
}

function QuickStats({ data }: { data: BacktestResultDetailResponse }) {
  const stats = [
    {
      label: '总收益',
      value: data.total_return,
      format: 'percent' as const,
    },
    {
      label: 'Sharpe',
      value: data.sharpe_ratio,
      format: 'ratio' as const,
    },
    {
      label: '最大回撤',
      value: data.max_drawdown,
      format: 'percent' as const,
    },
    {
      label: '胜率',
      value: data.win_rate,
      format: 'percent' as const,
    },
  ]

  return (
    <div className="grid grid-cols-4 gap-4">
      {stats.map((stat) => {
        const numValue = Number(stat.value)
        const isNegative = !isNaN(numValue) && numValue < 0
        const isPositive = !isNaN(numValue) && numValue > 0

        let colorClass = ''
        if (stat.label === '最大回撤') {
          colorClass = isNegative ? 'text-loss' : ''
        } else if (stat.format === 'percent') {
          colorClass = isPositive ? 'text-profit' : isNegative ? 'text-loss' : ''
        }

        const displayValue = stat.value == null
          ? '-'
          : stat.format === 'percent'
            ? formatPercent(numValue)
            : stat.format === 'ratio'
              ? numValue.toFixed(2)
              : numValue.toLocaleString()

        return (
          <Card key={stat.label}>
            <CardContent className="pt-4 pb-3">
              <p className="text-xs text-muted-foreground">{stat.label}</p>
              <p className={`text-xl font-bold ${colorClass}`}>{displayValue}</p>
            </CardContent>
          </Card>
        )
      })}
    </div>
  )
}

export function ResultDetailSheet({ jobId, resultId, open, onOpenChange }: ResultDetailSheetProps) {
  // Fetch main result detail
  const { data, isLoading, error } = useGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet(
    jobId,
    resultId || '',
    {
      query: {
        enabled: !!resultId && open,
        staleTime: 5 * 60 * 1000, // 5 minutes
      },
    }
  )

  // Fetch equity curve data separately
  const { data: equityData } = useGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet(
    jobId,
    resultId || '',
    undefined, // no date filter
    {
      query: {
        enabled: !!resultId && open,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch trades data separately
  const { data: tradesData } = useGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet(
    jobId,
    resultId || '',
    { page_size: 200 }, // fetch more trades for display
    {
      query: {
        enabled: !!resultId && open,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full sm:max-w-[90vw] overflow-y-auto">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            {data ? (
              <>
                <Badge variant="outline">{data.stock_code}</Badge>
                回测详情
              </>
            ) : (
              '加载中...'
            )}
          </SheetTitle>
          {data && (
            <SheetDescription>
              {data.status === 'completed' ? (
                <span className="text-profit">已完成</span>
              ) : data.status === 'failed' ? (
                <span className="text-loss">执行失败</span>
              ) : (
                data.status
              )}
            </SheetDescription>
          )}
        </SheetHeader>

        <div className="mt-6">
          {isLoading && <LoadingSkeleton />}

          {error && (
            <div className="flex flex-col items-center justify-center h-[300px] text-muted-foreground">
              <AlertCircle className="h-12 w-12 mb-4" />
              <p>加载失败</p>
              <p className="text-sm">{String(error)}</p>
            </div>
          )}

          {data && (
            <div className="space-y-6">
              {/* Quick Stats */}
              <QuickStats data={data} />

              {/* Tabbed Content */}
              <Tabs defaultValue="equity" className="w-full">
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
                  <EquityCurveWithIndicators
                    stockCode={data.stock_code}
                    equityCurve={equityData?.map(p => ({
                      date: p.date,
                      value: Number(p.value),
                      drawdown: p.drawdown ? Number(p.drawdown) : undefined,
                    })) as EquityCurvePoint[] | undefined}
                    trades={tradesData?.items?.map(t => ({
                      ...t,
                      entry_price: Number(t.entry_price),
                      exit_price: t.exit_price ? Number(t.exit_price) : undefined,
                      pnl: t.pnl ? Number(t.pnl) : 0,
                      pnl_percent: t.pnl_percent ? Number(t.pnl_percent) : 0,
                      type: t.direction as 'long' | 'short',
                    })) as TradeRecord[] | undefined}
                    height={500}
                  />
                </TabsContent>

                <TabsContent value="monthly" className="mt-4">
                  <MonthlyReturnsChart
                    data={data.monthly_returns as MonthlyReturns | undefined}
                    height={280}
                  />
                </TabsContent>

                <TabsContent value="trades" className="mt-4">
                  <TradesTable trades={tradesData?.items?.map(t => ({
                    ...t,
                    entry_price: Number(t.entry_price),
                    exit_price: t.exit_price ? Number(t.exit_price) : undefined,
                    pnl: t.pnl ? Number(t.pnl) : 0,
                    pnl_percent: t.pnl_percent ? Number(t.pnl_percent) : 0,
                    type: t.direction as 'long' | 'short',
                  })) as TradeRecord[] | undefined} />
                </TabsContent>

                <TabsContent value="metrics" className="mt-4">
                  <MetricsCards metrics={data} />
                </TabsContent>
              </Tabs>
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
