import { useEffect, useMemo, useRef } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ArrowLeft } from 'lucide-react'
import { StockChart } from '@/components/stock/StockChart'
import type { PriceLine, VerticalMarker } from '@/components/stock/StockChart'
import { ChartSyncManager } from '@/lib/dynamic-backtest/chart-sync'
import { useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost } from '@/api/generated/alpha-radar/alpha-radar'
import { useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { cn } from '@/lib/utils'
import type { IChartApi } from 'lightweight-charts'

// Helper to format percentage strings
const formatPercent = (val: string | null | undefined) => {
  if (!val) return '—'
  return `${parseFloat(val).toFixed(2)}%`
}

// Helper to format number strings
const formatNumber = (val: string | null | undefined) => {
  if (!val) return '—'
  return parseFloat(val).toFixed(2)
}

// Helper for color based on value (A-share: Red=Up, Green=Down)
const getValueColor = (val: string | null | undefined, isWinRate = false) => {
  if (!val) return 'text-muted-foreground'
  const num = parseFloat(val)
  
  if (isWinRate) {
    if (num >= 60) return 'text-red-500 font-medium'
    if (num < 50) return 'text-green-500 font-medium'
    return 'text-foreground'
  }

  if (num > 0) return 'text-red-500 font-medium'
  if (num < 0) return 'text-green-500 font-medium'
  return 'text-muted-foreground'
}

// Helper for assessment badge color
const getAssessmentColor = (assessment: string) => {
  if (assessment.includes('✅')) return 'bg-green-100 text-green-800 hover:bg-green-100/80 dark:bg-green-900/30 dark:text-green-400'
  if (assessment.includes('👍')) return 'bg-blue-100 text-blue-800 hover:bg-blue-100/80 dark:bg-blue-900/30 dark:text-blue-400'
  if (assessment.includes('⚠️')) return 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100/80 dark:bg-yellow-900/30 dark:text-yellow-400'
  if (assessment.includes('❌')) return 'bg-red-100 text-red-800 hover:bg-red-100/80 dark:bg-red-900/30 dark:text-red-400'
  return 'bg-secondary text-secondary-foreground'
}

const EVAL_PERIODS = [3, 5, 10, 20] as const
const PERIOD_LABELS: Record<number, string> = {
  3: 'Buy+3',
  5: 'Buy+5',
  10: 'Buy+10',
  20: 'Buy+20',
}
const PERIOD_COLORS: Record<number, string> = {
  3: '#f59e0b',   // amber — Buy+3
  5: '#8b5cf6',   // violet — Buy+5
  10: '#06b6d4',  // cyan — Buy+10
  20: '#ec4899',  // pink — Buy+20
}

export default function MultiStockBrowsePage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  
  const codes = useMemo(() => 
    searchParams.get('codes')?.split(',').filter(Boolean) ?? [], 
    [searchParams]
  )
  const date = searchParams.get('date') ?? ''

  const syncManagerRef = useRef<ChartSyncManager>(new ChartSyncManager())

  const evalMutation = useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost()

  useEffect(() => {
    return () => {
      syncManagerRef.current.reset()
    }
  }, [])

  useEffect(() => {
    if (codes.length > 0 && date) {
      evalMutation.mutate({ data: { codes, date, base_price: 't1_open', periods: [...EVAL_PERIODS] } })
    }
  }, [codes, date])

  const stockMap = useMemo(() => {
    if (!evalMutation.data?.stocks) return {}
    return evalMutation.data.stocks.reduce((acc, stock) => {
      acc[stock.code] = stock
      return acc
    }, {} as Record<string, typeof evalMutation.data.stocks[0]>)
  }, [evalMutation.data])

  const verticalMarkers = useMemo((): VerticalMarker[] => {
    if (!evalMutation.data?.period_dates) return []
    const markers: VerticalMarker[] = []
    for (const period of EVAL_PERIODS) {
      const dateStr = (evalMutation.data.period_dates as Record<string, string | null>)?.[String(period)]
      if (dateStr) {
        markers.push({
          date: dateStr,
          color: PERIOD_COLORS[period],
          label: PERIOD_LABELS[period],
        })
      }
    }
    return markers
  }, [evalMutation.data?.period_dates])

  const priceLinesMap = useMemo(() => {
    const map: Record<string, PriceLine[]> = {}
    for (const code of codes) {
      const stockInfo = stockMap[code]
      const buyPrice = stockInfo?.buy_price ? parseFloat(String(stockInfo.buy_price)) : null
      map[code] = buyPrice ? [
        { price: buyPrice, color: '#3b82f6', label: '成本', lineStyle: 'solid' as const },
        { price: buyPrice * 0.95, color: '#ef4444', label: '止损 -5%', lineStyle: 'dashed' as const },
      ] : []
    }
    return map
  }, [codes, stockMap])

  const chartReadyCallbacks = useMemo(() => {
    const map: Record<string, (chart: IChartApi) => void> = {}
    for (const code of codes) {
      map[code] = (chart: IChartApi) => {
        syncManagerRef.current.register(code, chart)
      }
    }
    return map
  }, [codes])

  const periods = EVAL_PERIODS

  // Find stats for a specific period
  const getStatsForPeriod = (period: number) => {
    return evalMutation.data?.period_stats?.find(s => s.period === period)
  }

  if (codes.length === 0 || !date) {
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <h1 className="text-2xl font-bold tracking-tight">多股浏览</h1>
        </div>
        <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
          请提供有效的股票代码和日期
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" className="-ml-2 gap-1 text-muted-foreground hover:text-foreground" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-4 w-4" />
            返回选股
          </Button>
        </div>
        <div className="flex items-baseline justify-between">
          <h1 className="text-3xl font-bold tracking-tight">多股浏览</h1>
          <span className="text-muted-foreground font-mono">
            {date} · {codes.length}只股票
          </span>
        </div>
      </div>

      {/* Performance Panel */}
      <div className="border bg-background">
        <div className="flex items-center gap-2 px-3 py-2 border-b bg-[#d1b2ad]/35 backdrop-blur-[2px]">
          <span className="text-sm font-medium">表现评估</span>
          {evalMutation.isPending && <Skeleton className="h-5 w-24" />}
          {evalMutation.data && (
            <Badge variant="outline" className={cn("font-normal text-xs px-2 py-0", getAssessmentColor(evalMutation.data.assessment))}>
              {evalMutation.data.assessment}
            </Badge>
          )}
        </div>
        <div className="px-3 py-2">
          {evalMutation.isPending ? (
            <div className="space-y-2">
              <Skeleton className="h-6 w-full" />
              <Skeleton className="h-6 w-full" />
            </div>
          ) : evalMutation.data ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-1.5 font-medium text-muted-foreground w-24">指标</th>
                    {periods.map(p => (
                      <th key={p} className="text-right py-1.5 font-medium text-muted-foreground">{PERIOD_LABELS[p]}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b border-muted/50">
                    <td className="py-1.5 font-medium">胜率</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.win_rate
                      return (
                        <td key={p} className={cn("text-right py-1.5", getValueColor(val, true))}>
                          {formatPercent(val)}
                        </td>
                      )
                    })}
                  </tr>
                  <tr className="border-b border-muted/50">
                    <td className="py-1.5 font-medium">平均收益</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.avg_return
                      return (
                        <td key={p} className={cn("text-right py-1.5", getValueColor(val))}>
                          {formatPercent(val)}
                        </td>
                      )
                    })}
                  </tr>
                  <tr className="border-b border-muted/50">
                    <td className="py-1.5 font-medium">盈亏比</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.profit_loss_ratio
                      return (
                        <td key={p} className="text-right py-1.5 font-mono">
                          {formatNumber(val)}
                        </td>
                      )
                    })}
                  </tr>
                  <tr>
                    <td className="py-1.5 font-medium">上涨/下跌</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      if (!stats) return <td key={p} className="text-right py-1.5 text-muted-foreground">—</td>
                      return (
                        <td key={p} className="text-right py-1.5 font-mono text-xs">
                          <span className="text-red-500">{stats.win_count}</span>
                          <span className="text-muted-foreground mx-1">/</span>
                          <span className="text-green-500">{stats.lose_count}</span>
                        </td>
                      )
                    })}
                  </tr>
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-3 text-sm text-muted-foreground">
              暂无评估数据
            </div>
          )}
        </div>
      </div>

      {/* Charts List */}
      <div className="space-y-0">
        {codes.map((code, index) => (
          <StockChartItem
            key={code}
            code={code}
            date={date}
            isFirst={index === 0}
            stockInfo={stockMap[code]}
            evalDone={!evalMutation.isPending}
            priceLines={priceLinesMap[code] ?? []}
            verticalMarkers={verticalMarkers}
            onChartReady={chartReadyCallbacks[code]}
          />
        ))}
      </div>
    </div>
  )
}

interface StockChartItemProps {
  code: string
  date: string
  isFirst: boolean
  stockInfo: { name: string; buy_price?: string | number | null; returns?: Record<string, string | null> } | undefined
  evalDone: boolean
  priceLines: PriceLine[]
  verticalMarkers: VerticalMarker[]
  onChartReady: (chart: IChartApi) => void
}

function StockChartItem({ code, date, isFirst, stockInfo, evalDone, priceLines, verticalMarkers, onChartReady }: StockChartItemProps) {
  const { data: klineData } = useGetKlineApiV1StocksCodeKlineGet(
    code,
    { limit: 1000 },
    { query: { enabled: !!code, staleTime: 5 * 60 * 1000 } }
  )

  const displayName = stockInfo?.name ?? klineData?.code_name ?? (evalDone ? '—' : '')
  const buyPrice = stockInfo?.buy_price ? parseFloat(String(stockInfo.buy_price)).toFixed(2) : null

  return (
    <div className={cn("relative border-x border-b bg-background", isFirst && "border-t")}>
      <div className="absolute left-0 top-0 z-10 flex items-center gap-2 bg-[#d1b2ad]/35 px-2 py-1 text-sm backdrop-blur-[2px]">
        <span className="font-mono font-medium">{code}</span>
        <span className="text-muted-foreground">{displayName}</span>
        {buyPrice && (
          <span className="font-mono text-xs">¥{buyPrice}</span>
        )}
        <div className="flex items-center gap-2 ml-1">
          {EVAL_PERIODS.map(p => {
            const ret = stockInfo?.returns?.[String(p)]
            if (!ret) return null
            return (
              <span key={p} className={cn("font-mono text-xs flex items-center", getValueColor(ret))}>
                <span className="inline-block w-1.5 h-1.5 rounded-full mr-1" style={{ backgroundColor: PERIOD_COLORS[p] }} />
                {PERIOD_LABELS[p]}: {formatPercent(ret)}
              </span>
            )
          })}
        </div>
      </div>

      <div className="h-[300px] w-full">
        <StockChart
          code={code}
          endDate={date}
          height={300}
          priceLines={priceLines}
          verticalMarkers={verticalMarkers}
          onChartReady={onChartReady}
          minimal={true}
        />
      </div>
    </div>
  )
}