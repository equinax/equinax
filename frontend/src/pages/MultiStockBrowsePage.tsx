import { useEffect, useMemo, useRef, useCallback, useState } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { useQueries } from '@tanstack/react-query'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ArrowLeft, FileDown, Loader2 } from 'lucide-react'
import { StockChart } from '@/components/stock/StockChart'
import type { PriceLine, VerticalMarker } from '@/components/stock/StockChart'
import { ChartSyncManager } from '@/lib/dynamic-backtest/chart-sync'
import { useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost } from '@/api/generated/alpha-radar/alpha-radar'
import { getKlineApiV1StocksCodeKlineGet, getGetKlineApiV1StocksCodeKlineGetQueryKey, useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { cn } from '@/lib/utils'
import { QUANT_LABEL_CN, formatMv, formatVol } from '@/lib/quant-labels'
import { generateStockReport } from '@/lib/generate-report'
import type { ReportStock } from '@/lib/generate-report'
import type { IChartApi, ISeriesApi, SeriesType } from 'lightweight-charts'
import { DateAxisBar } from '@/components/stock/DateAxisBar'

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

const PRICE_LINE_PERIODS = [3, 5, 10] as const
const PRICE_LINE_LABELS: Record<number, string> = { 3: 'B3', 5: 'B5', 10: 'B10', 20: 'B20' }

export default function MultiStockBrowsePage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  
  const codes = useMemo(() => 
    searchParams.get('codes')?.split(',').filter(Boolean) ?? [], 
    [searchParams]
  )
  const date = searchParams.get('date') ?? ''
  const labelsMap = useMemo<Record<string, string[]>>(() => {
    try {
      const raw = searchParams.get('labels')
      return raw ? JSON.parse(raw) : {}
    } catch {
      return {}
    }
  }, [searchParams])

  const syncManagerRef = useRef<ChartSyncManager>(new ChartSyncManager())
  const [isExporting, setIsExporting] = useState(false)

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

  const klineQueries = useQueries({
    queries: codes.map(code => ({
      queryKey: getGetKlineApiV1StocksCodeKlineGetQueryKey(code, { limit: 1000 }),
      queryFn: () => getKlineApiV1StocksCodeKlineGet(code, { limit: 1000 }),
      staleTime: 5 * 60 * 1000,
      enabled: !!code,
    })),
  })

  const klineQueryData = klineQueries.map(q => q.data)

  const sharedDates = useMemo(() => {
    const dateSet = new Set<string>()
    for (const data of klineQueryData) {
      if (data?.data) {
        for (const d of data.data) {
          dateSet.add(d.date)
        }
      }
    }
    return Array.from(dateSet).sort()
  }, [klineQueryData])

  const dateAxisChartReadyCallback = useCallback((chart: IChartApi, series: ISeriesApi<SeriesType>) => {
    syncManagerRef.current.register('__date_axis__', chart, series)
  }, [])

  const priceLinesMap = useMemo(() => {
    const map: Record<string, PriceLine[]> = {}
    for (const code of codes) {
      const stockInfo = stockMap[code]
      const buyPrice = stockInfo?.buy_price ? parseFloat(String(stockInfo.buy_price)) : null
      if (!buyPrice) {
        map[code] = []
        continue
      }

      const lines: PriceLine[] = [
        { price: buyPrice, color: '#3b82f6', label: '成本', lineStyle: 'solid' as const },
        { price: buyPrice * 0.95, color: '#ef4444', label: '止损 -5%', lineStyle: 'dashed' as const },
      ]

      for (const period of PRICE_LINE_PERIODS) {
        const ret = stockInfo?.returns?.[String(period)]
        if (ret != null) {
          const retPct = parseFloat(String(ret))
          const periodPrice = buyPrice * (1 + retPct / 100)
          lines.push({
            price: periodPrice,
            color: PERIOD_COLORS[period],
            label: PRICE_LINE_LABELS[period],
            lineStyle: 'solid' as const,
          })
        }
      }

      map[code] = lines
    }
    return map
  }, [codes, stockMap])

  const chartReadyCallbacks = useMemo(() => {
    const map: Record<string, (chart: IChartApi, series: ISeriesApi<SeriesType>) => void> = {}
    for (const code of codes) {
      map[code] = (chart: IChartApi, series: ISeriesApi<SeriesType>) => {
        syncManagerRef.current.register(code, chart, series)
      }
    }
    return map
  }, [codes])

  const dataLoadedCallbacks = useMemo(() => {
    const map: Record<string, () => void> = {}
    for (const code of codes) {
      map[code] = () => {
        syncManagerRef.current.applyCurrentRange(code)
      }
    }
    return map
  }, [codes])

  const periods = EVAL_PERIODS

  // Find stats for a specific period
  const getStatsForPeriod = (period: number) => {
    return evalMutation.data?.period_stats?.find(s => s.period === period)
  }

  const handleExportReport = useCallback(async () => {
    if (isExporting) return
    setIsExporting(true)
    try {
      const savedRange = syncManagerRef.current.getRange()

      const charts = codes
        .map(code => ({ code, chart: syncManagerRef.current.getChart(code) }))
        .filter((e): e is { code: string; chart: import('lightweight-charts').IChartApi } => e.chart !== null)

      for (const { chart } of charts) {
        chart.timeScale().fitContent()
      }
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))

      const reportStocks: ReportStock[] = []
      for (const { code, chart } of charts) {
        const canvas = chart.takeScreenshot()
        const chartImage = canvas.toDataURL('image/png')
        const info = stockMap[code]

        reportStocks.push({
          code,
          name: info?.name ?? code,
          buyPrice: info?.buy_price ? parseFloat(String(info.buy_price)) : undefined,
          totalMv: info?.total_mv,
          circMv: info?.circ_mv,
          volume: info?.volume,
          turnover: info?.turnover,
          peTtm: info?.pe_ttm,
          pbMrq: info?.pb_mrq,
          quantLabels: labelsMap[code],
          chartImage,
        })
      }

      if (savedRange) {
        syncManagerRef.current.setRange(savedRange)
      }

      if (reportStocks.length > 0) {
        await generateStockReport(date, reportStocks)
      }
    } finally {
      setIsExporting(false)
    }
  }, [codes, date, stockMap, labelsMap, isExporting])

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
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" className="-ml-2 gap-1 text-muted-foreground hover:text-foreground" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-4 w-4" />
            返回选股
          </Button>
          <span className="text-muted-foreground">·</span>
          <h1 className="text-xl font-bold tracking-tight">多股浏览</h1>
        </div>
        <span className="text-muted-foreground font-mono text-sm">
          {date} · {codes.length}只股票
        </span>
      </div>

      {/* Performance Panel */}
      <div className="border bg-background">
        <div className="flex items-center gap-2 px-3 py-2 border-b">
          <span className="text-sm font-medium">表现评估</span>
          {evalMutation.isPending && <Skeleton className="h-5 w-24" />}
          {evalMutation.data && (
            <Badge variant="outline" className={cn("font-normal text-xs px-2 py-0", getAssessmentColor(evalMutation.data.assessment))}>
              {evalMutation.data.assessment}
            </Badge>
          )}
          <Button
            variant="outline"
            size="sm"
            className="ml-auto h-7 gap-1.5 text-xs"
            disabled={isExporting || codes.length === 0}
            onClick={handleExportReport}
          >
            {isExporting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <FileDown className="h-3.5 w-3.5" />}
            导出报告
          </Button>
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
        <DateAxisBar
          sharedDates={sharedDates}
          endDate={date}
          onChartReady={dateAxisChartReadyCallback}
        />
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
            onDataLoaded={dataLoadedCallbacks[code]}
            sharedDates={sharedDates}
            quantLabels={labelsMap[code]}
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
  stockInfo: {
    name: string
    buy_price?: string | number | null
    returns?: Record<string, string | null>
    total_mv?: string | number | null
    circ_mv?: string | number | null
    volume?: string | number | null
    turnover?: string | number | null
    pe_ttm?: string | number | null
    pb_mrq?: string | number | null
  } | undefined
  evalDone: boolean
  priceLines: PriceLine[]
  verticalMarkers: VerticalMarker[]
  onChartReady: (chart: IChartApi, series: ISeriesApi<SeriesType>) => void
  onDataLoaded: () => void
  sharedDates: string[]
  quantLabels?: string[]
}

function StockChartItem({ code, date, isFirst, stockInfo, evalDone, priceLines, verticalMarkers, onChartReady, onDataLoaded, sharedDates, quantLabels }: StockChartItemProps) {
  const { data: klineData } = useGetKlineApiV1StocksCodeKlineGet(
    code,
    { limit: 1000 },
    { query: { enabled: !!code, staleTime: 5 * 60 * 1000 } }
  )

  const displayName = stockInfo?.name ?? klineData?.code_name ?? (evalDone ? '—' : '')
  const buyPrice = stockInfo?.buy_price ? parseFloat(String(stockInfo.buy_price)).toFixed(2) : null

  return (
    <div className={cn("relative border-x border-b bg-background", isFirst && "border-t")}>
      <div className="absolute left-0 right-0 top-0 z-10 flex items-center gap-2 bg-[#d1b2ad]/35 px-2 py-1 text-sm backdrop-blur-[2px]">
        <span className="font-mono font-medium">{code}</span>
        <span className="text-muted-foreground">{displayName}</span>
        {buyPrice && (
          <span className="font-mono text-xs">¥{buyPrice}</span>
        )}
        {stockInfo && (stockInfo.total_mv || stockInfo.volume || stockInfo.pe_ttm) && (
          <>
            {stockInfo.total_mv != null && <span className="text-xs text-muted-foreground">市值 {formatMv(stockInfo.total_mv)}</span>}
            {stockInfo.circ_mv != null && <span className="text-xs text-muted-foreground">流值 {formatMv(stockInfo.circ_mv)}</span>}
            {stockInfo.volume != null && <span className="text-xs text-muted-foreground">量 {formatVol(stockInfo.volume)}</span>}
            {stockInfo.turnover != null && <span className="text-xs text-muted-foreground">换手 {parseFloat(String(stockInfo.turnover)).toFixed(1)}%</span>}
            {stockInfo.pe_ttm != null && <span className="text-xs text-muted-foreground">PE {parseFloat(String(stockInfo.pe_ttm)).toFixed(1)}</span>}
            {stockInfo.pb_mrq != null && <span className="text-xs text-muted-foreground">PB {parseFloat(String(stockInfo.pb_mrq)).toFixed(2)}</span>}
          </>
        )}
        {quantLabels && quantLabels.length > 0 && (
          <span className="text-xs">
            <span className="text-muted-foreground">推荐:</span>
            {quantLabels.map(l => (
              <span key={l} className="text-amber-600 dark:text-amber-400 ml-0.5">{QUANT_LABEL_CN[l] ?? l}</span>
            ))}
          </span>
        )}
        <div className="flex items-center gap-2 ml-auto">
          {EVAL_PERIODS.map(p => {
            const ret = stockInfo?.returns?.[String(p)]
            if (!ret) return null
            return (
              <span key={p} className={cn("font-mono text-xs flex items-center", getValueColor(ret))}>
                <span className="inline-block w-1.5 h-1.5 rounded-full mr-1" style={{ backgroundColor: PERIOD_COLORS[p] }} />
                {PRICE_LINE_LABELS[p] ?? PERIOD_LABELS[p]}: {formatPercent(ret)}
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
          onDataLoaded={onDataLoaded}
          minimal={true}
          sharedDates={sharedDates}
        />
      </div>
    </div>
  )
}