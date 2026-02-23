import { useEffect, useMemo, useRef, useCallback, useState } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { useQueries } from '@tanstack/react-query'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ArrowLeft, FileDown, Loader2 } from 'lucide-react'
import { StockChart } from '@/components/stock/StockChart'
import type { PriceLine, VerticalMarker, HoverData } from '@/components/stock/StockChart'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { ChartSyncManager } from '@/lib/dynamic-backtest/chart-sync'
import { useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost } from '@/api/generated/alpha-radar/alpha-radar'
import { getKlineApiV1StocksCodeKlineGet, getGetKlineApiV1StocksCodeKlineGetQueryKey, useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { cn } from '@/lib/utils'
import { QUANT_LABEL_CN, formatMv, formatVol } from '@/lib/quant-labels'
import { generateMarkdownReport } from '@/lib/generate-report'
import type { MarkdownReportStock, MarkdownPeriodStats } from '@/lib/generate-report'
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

const EVAL_PERIODS_BY_TAB: Record<string, readonly number[]> = {
  weekly: [3, 6],
  rally: [3, 5, 10],
  dragon: [3, 5, 10, 20],
  overnight: [1, 2],
}
const DEFAULT_EVAL_PERIODS = [3, 5, 10, 20] as const
const PERIOD_LABELS: Record<number, string> = {
  1: 'Buy+1',
  2: 'Buy+2',
  3: 'Buy+3',
  5: 'Buy+5',
  6: 'Buy+6',
  10: 'Buy+10',
  20: 'Buy+20',
}
const OVERNIGHT_PERIOD_LABELS: Record<number, string> = {
  1: '隔夜(B0→B1)',
  2: '两日(B0→B2)',
}
const getPeriodLabel = (p: number, tab?: string) =>
  (tab === 'overnight' ? OVERNIGHT_PERIOD_LABELS[p] : undefined) ?? PERIOD_LABELS[p]
const PERIOD_COLORS: Record<number, string> = {
  1: '#f97316',   // orange
  2: '#14b8a6',   // teal
  3: '#f59e0b',   // amber
  5: '#8b5cf6',   // violet
  6: '#10b981',   // emerald
  10: '#06b6d4',  // cyan
  20: '#ec4899',  // pink
}

const PRICE_LINE_PERIODS_BY_TAB: Record<string, readonly number[]> = {
  weekly: [3, 6],
  rally: [3, 5, 10],
  dragon: [3, 5, 10, 20],
  overnight: [1, 2],
}
const DEFAULT_PRICE_LINE_PERIODS = [3, 5, 10] as const
const PRICE_LINE_LABELS: Record<number, string> = { 1: 'B1', 2: 'B2', 3: 'B3', 5: 'B5', 6: 'B6', 10: 'B10', 20: 'B20' }

const STOCK_TAB_LABELS: Record<string, string> = {
  weekly: '周内短线',
  rally: '大盘主升',
  dragon: '龙头涨停',
  overnight: '隔夜超短',
}

// 申万一级行业名称 → 行业指数代码 映射
const SW_INDUSTRY_NAME_TO_INDEX: Record<string, string> = {
  "农林牧渔": "sw.801010",
  "基础化工": "sw.801030",
  "钢铁": "sw.801040",
  "有色金属": "sw.801050",
  "电子": "sw.801080",
  "家用电器": "sw.801110",
  "食品饮料": "sw.801120",
  "纺织服饰": "sw.801130",
  "轻工制造": "sw.801140",
  "医药生物": "sw.801150",
  "公用事业": "sw.801160",
  "交通运输": "sw.801170",
  "房地产": "sw.801180",
  "商贸零售": "sw.801200",
  "社会服务": "sw.801210",
  "综合": "sw.801230",
  "建筑材料": "sw.801710",
  "建筑装饰": "sw.801720",
  "电力设备": "sw.801730",
  "国防军工": "sw.801740",
  "计算机": "sw.801750",
  "传媒": "sw.801760",
  "通信": "sw.801770",
  "银行": "sw.801780",
  "非银金融": "sw.801790",
  "汽车": "sw.801880",
  "机械设备": "sw.801890",
  "煤炭": "sw.801950",
  "石油石化": "sw.801960",
  "环保": "sw.801970",
  "美容护理": "sw.801980",
}

export default function MultiStockBrowsePage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  
  const codes = useMemo(() => 
    searchParams.get('codes')?.split(',').filter(Boolean) ?? [], 
    [searchParams]
  )
  const date = searchParams.get('date') ?? ''
  const tab = searchParams.get('tab') ?? ''
  const tabLabel = STOCK_TAB_LABELS[tab] ?? ''
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
  const [industryDialog, setIndustryDialog] = useState<{ name: string; code: string } | null>(null)

  const handleGoBack = useCallback(() => {
    const params = new URLSearchParams()
    if (date) params.set('date', date)
    if (tab) params.set('tab', tab)
    navigate(`/alpha-radar?${params.toString()}`)
  }, [navigate, date, tab])

  const evalMutation = useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost()

  useEffect(() => {
    return () => {
      syncManagerRef.current.reset()
    }
  }, [])

  useEffect(() => {
    if (codes.length > 0 && date) {
      const tabPeriods = EVAL_PERIODS_BY_TAB[tab] ?? DEFAULT_EVAL_PERIODS
      evalMutation.mutate({ data: { codes, date, base_price: 't1_open', periods: [...tabPeriods], tab: tab || undefined } })
    }
  }, [codes, date])

  const stockMap = useMemo(() => {
    if (!evalMutation.data?.stocks) return {}
    return evalMutation.data.stocks.reduce((acc, stock) => {
      acc[stock.code] = stock
      return acc
    }, {} as Record<string, typeof evalMutation.data.stocks[0]>)
  }, [evalMutation.data])

  const evalPeriods = useMemo(() => EVAL_PERIODS_BY_TAB[tab] ?? DEFAULT_EVAL_PERIODS, [tab])

  const verticalMarkers = useMemo((): VerticalMarker[] => {
    if (!evalMutation.data?.period_dates) return []
    const markers: VerticalMarker[] = []
    for (const period of evalPeriods) {
      const dateStr = (evalMutation.data.period_dates as Record<string, string | null>)?.[String(period)]
      if (dateStr) {
        markers.push({
          date: dateStr,
          color: PERIOD_COLORS[period],
          label: getPeriodLabel(period, tab),
        })
      }
    }
    return markers
  }, [evalMutation.data?.period_dates, evalPeriods])

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

  const priceLinePeriods = useMemo(() => PRICE_LINE_PERIODS_BY_TAB[tab] ?? DEFAULT_PRICE_LINE_PERIODS, [tab])

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

      for (const period of priceLinePeriods) {
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
  }, [codes, stockMap, priceLinePeriods])

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

  const handleIndustryClick = useCallback((industryName: string, indexCode: string) => {
    setIndustryDialog({ name: industryName, code: indexCode })
  }, [])

  const industryVerticalMarker = useMemo((): VerticalMarker[] => {
    if (!date) return []
    return [{ date, color: '#3b82f6', label: '推荐日', lineStyle: 'dashed' }]
  }, [date])

  const periods = evalPeriods

  // Find stats for a specific period
  const getStatsForPeriod = (period: number) => {
    return evalMutation.data?.period_stats?.find(s => s.period === period)
  }

  const handleExportReport = useCallback(() => {
    if (isExporting || !evalMutation.data) return
    setIsExporting(true)
    try {
      const mdStocks: MarkdownReportStock[] = codes.map(code => {
        const info = stockMap[code]
        const stockData = evalMutation.data!.stocks.find(s => s.code === code)
        return {
          code,
          name: info?.name ?? code,
          buyPrice: info?.buy_price ? parseFloat(String(info.buy_price)) : undefined,
          refPrice: stockData?.ref_price
            ? parseFloat(String(stockData.ref_price))
            : undefined,
          totalMv: info?.total_mv,
          circMv: info?.circ_mv,
          volume: info?.volume,
          turnover: info?.turnover,
          peTtm: info?.pe_ttm,
          pbMrq: info?.pb_mrq,
          quantLabels: labelsMap[code],
          returns: info?.returns as Record<string, string | null> | undefined,
          limitUpCount: stockData?.limit_up_count ?? undefined,
          maxConsecLimitUp: stockData?.max_consec_limit_up ?? undefined,
        }
      })

      const mdPeriodStats: MarkdownPeriodStats[] = (evalMutation.data!.period_stats ?? []).map(s => ({
        period: s.period,
        winRate: s.win_rate as string | null | undefined,
        avgReturn: s.avg_return as string | null | undefined,
        avgWin: s.avg_win as string | null | undefined,
        avgLoss: s.avg_loss as string | null | undefined,
        profitLossRatio: s.profit_loss_ratio as string | null | undefined,
        winCount: s.win_count,
        loseCount: s.lose_count,
        total: s.total,
      }))

      generateMarkdownReport({
        date,
        tab,
        tabLabel,
        assessment: evalMutation.data!.assessment,
        stocks: mdStocks,
        periodStats: mdPeriodStats,
        periods: evalPeriods,
      })
    } finally {
      setIsExporting(false)
    }
  }, [codes, date, tab, tabLabel, stockMap, labelsMap, evalMutation.data, isExporting])

  if (codes.length === 0 || !date) {
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={handleGoBack}>
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
          <Button variant="ghost" size="sm" className="-ml-2 gap-1 text-muted-foreground hover:text-foreground" onClick={handleGoBack}>
            <ArrowLeft className="h-4 w-4" />
            返回选股
          </Button>
          <span className="text-muted-foreground">·</span>
          <h1 className="text-xl font-bold tracking-tight">多股浏览</h1>
          {tabLabel && (
            <>
              <span className="text-muted-foreground">·</span>
              <Badge variant="secondary" className="font-normal text-xs">{tabLabel}</Badge>
            </>
          )}
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
                      <th key={p} className="text-right py-1.5 font-medium text-muted-foreground">{getPeriodLabel(p, tab)}</th>
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
                  {tab === 'dragon' && (
                    <>
                      <tr className="border-t border-muted/50">
                        <td className="py-1.5 font-medium">涨停成功率</td>
                        {periods.map(p => {
                          const stocksWithLU = evalMutation.data!.stocks.filter(
                            s => s.limit_up_count != null && s.limit_up_count > 0
                          )
                          const total = evalMutation.data!.stocks.length
                          if (total === 0) return <td key={p} className="text-right py-1.5 text-muted-foreground">—</td>
                          const rate = (stocksWithLU.length / total * 100).toFixed(1)
                          return (
                            <td key={p} className={cn("text-right py-1.5", parseFloat(rate) >= 50 ? 'text-red-500 font-medium' : 'text-foreground')}>
                              {rate}%
                            </td>
                          )
                        })}
                      </tr>
                      <tr className="border-t border-muted/50">
                        <td className="py-1.5 font-medium">平均涨停次数</td>
                        {periods.map(p => {
                          const stats = getStatsForPeriod(p)
                          const val = stats?.avg_limit_up_count
                          return (
                            <td key={p} className="text-right py-1.5 font-mono">
                              {val != null ? parseFloat(String(val)).toFixed(1) : '—'}
                            </td>
                          )
                        })}
                      </tr>
                      <tr className="border-t border-muted/50">
                        <td className="py-1.5 font-medium">平均最大连板</td>
                        {periods.map(p => {
                          const stats = getStatsForPeriod(p)
                          const val = stats?.avg_max_consec_limit_up
                          return (
                            <td key={p} className="text-right py-1.5 font-mono">
                              {val != null ? parseFloat(String(val)).toFixed(1) : '—'}
                            </td>
                          )
                        })}
                      </tr>
                    </>
                  )}
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
            evalPeriods={evalPeriods}
            tab={tab}
            onIndustryClick={handleIndustryClick}
          />
        ))}
      </div>

      <Dialog open={!!industryDialog} onOpenChange={(open) => !open && setIndustryDialog(null)}>
        <DialogContent className="max-w-4xl p-0">
          <DialogHeader className="px-4 pt-4 pb-0">
            <DialogTitle className="text-base font-medium">
              {industryDialog?.name} 行业指数
              <span className="ml-2 text-xs font-mono text-muted-foreground">{industryDialog?.code}</span>
            </DialogTitle>
          </DialogHeader>
          {industryDialog && (
            <div className="px-2 pb-2">
              <StockChart
                code={industryDialog.code}
                height={400}
                endDate={date}
                verticalMarkers={industryVerticalMarker}
              />
            </div>
          )}
        </DialogContent>
      </Dialog>
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
    sw_industry_l1?: string | null
    limit_up_count?: number | null
    max_consec_limit_up?: number | null
  } | undefined
  evalDone: boolean
  priceLines: PriceLine[]
  verticalMarkers: VerticalMarker[]
  onChartReady: (chart: IChartApi, series: ISeriesApi<SeriesType>) => void
  onDataLoaded: () => void
  sharedDates: string[]
  quantLabels?: string[]
  evalPeriods: readonly number[]
  tab: string
  onIndustryClick?: (industryName: string, indexCode: string) => void
}

function StockChartItem({ code, date, isFirst, stockInfo, evalDone, priceLines, verticalMarkers, onChartReady, onDataLoaded, sharedDates, quantLabels, evalPeriods, tab, onIndustryClick }: StockChartItemProps) {
  const { data: klineData } = useGetKlineApiV1StocksCodeKlineGet(
    code,
    { limit: 1000 },
    { query: { enabled: !!code, staleTime: 5 * 60 * 1000 } }
  )

  const [hoverData, setHoverData] = useState<HoverData | null>(null)

  const displayName = stockInfo?.name ?? klineData?.code_name ?? (evalDone ? '—' : '')
  const buyPrice = stockInfo?.buy_price ? parseFloat(String(stockInfo.buy_price)) : null

  const recDayOhlc = useMemo(() => {
    if (!klineData?.data || !date) return null
    const dayData = klineData.data.find(d => d.date === date)
    if (!dayData) return null
    return {
      open: Number(dayData.open) || 0,
      high: Number(dayData.high) || 0,
      low: Number(dayData.low) || 0,
      close: Number(dayData.close) || 0,
      preclose: Number(dayData.preclose) || 0,
      change_pct: dayData.preclose ? ((Number(dayData.close) - Number(dayData.preclose)) / Number(dayData.preclose)) * 100 : 0,
    }
  }, [klineData?.data, date])

  const activeOhlc = hoverData
    ? { open: hoverData.open, high: hoverData.high, low: hoverData.low, close: hoverData.close, preclose: hoverData.preclose, change_pct: hoverData.change_pct }
    : recDayOhlc

  const ohlcColor = (val: number) => {
    if (!activeOhlc) return 'text-muted-foreground'
    if (val > activeOhlc.preclose) return 'text-red-500'
    if (val < activeOhlc.preclose) return 'text-green-500'
    return 'text-muted-foreground'
  }

  const isLimitUp = useMemo(() => {
    if (!activeOhlc || tab !== 'dragon') return false
    const { close, high, change_pct } = activeOhlc
    if (close !== high || change_pct < 4.5) return false
    const is20pctBoard = code.startsWith('sh.688') || code.startsWith('sz.30')
    return is20pctBoard ? change_pct >= 19.5 : change_pct >= 9.5
  }, [activeOhlc, tab, code])

  const cumulativeReturn = useMemo(() => {
    if (!hoverData || buyPrice == null || buyPrice === 0) return null
    return ((hoverData.close - buyPrice) / buyPrice) * 100
  }, [hoverData, buyPrice])

  const tradingDaysSinceBuy = useMemo(() => {
    if (!hoverData || !klineData?.data || !date) return null
    const dates = klineData.data.map(d => d.date).sort()
    const buyIdx = dates.indexOf(date)
    const hoverIdx = dates.indexOf(hoverData.date)
    if (buyIdx < 0 || hoverIdx < 0) return null
    return hoverIdx - buyIdx
  }, [hoverData, klineData?.data, date])

  return (
    <div className={cn("relative border-x border-b bg-background", isFirst && "border-t")}>
      <div className="absolute left-0 right-0 top-0 z-10 flex items-center gap-2 bg-[#d1b2ad]/35 px-2 py-1 text-sm backdrop-blur-[2px]">
        <span className="font-mono font-medium">{code}</span>
        <span className="text-muted-foreground">{displayName}</span>
        {stockInfo?.sw_industry_l1 && (
          <span
            className={cn(
              "text-xs text-muted-foreground/70",
              SW_INDUSTRY_NAME_TO_INDEX[stockInfo.sw_industry_l1] && "cursor-pointer hover:text-foreground hover:underline"
            )}
            onClick={() => {
              const indexCode = SW_INDUSTRY_NAME_TO_INDEX[stockInfo.sw_industry_l1!]
              if (indexCode && onIndustryClick) {
                onIndustryClick(stockInfo.sw_industry_l1!, indexCode)
              }
            }}
          >
            {stockInfo.sw_industry_l1}
          </span>
        )}
        {activeOhlc && (
          <>
            <span className="text-xs text-muted-foreground">开 <span className={cn("font-mono", ohlcColor(activeOhlc.open))}>{activeOhlc.open.toFixed(2)}</span></span>
            <span className="text-xs text-muted-foreground">收 <span className={cn("font-mono", ohlcColor(activeOhlc.close))}>{activeOhlc.close.toFixed(2)}</span></span>
            <span className="text-xs text-muted-foreground">高 <span className={cn("font-mono", ohlcColor(activeOhlc.high))}>{activeOhlc.high.toFixed(2)}</span></span>
            <span className="text-xs text-muted-foreground">低 <span className={cn("font-mono", ohlcColor(activeOhlc.low))}>{activeOhlc.low.toFixed(2)}</span></span>
            <span className={cn("font-mono text-xs font-medium", activeOhlc.change_pct > 0 ? 'text-red-500' : activeOhlc.change_pct < 0 ? 'text-green-500' : 'text-muted-foreground')}>
              {activeOhlc.change_pct > 0 ? '+' : ''}{activeOhlc.change_pct.toFixed(2)}%
            </span>
            <span className="text-xs text-muted-foreground">量 {formatVol(hoverData?.volume ?? stockInfo?.volume)}</span>
            {isLimitUp && (
              <span className="text-xs font-bold text-red-600 bg-red-100 dark:bg-red-900/40 dark:text-red-400 px-1 rounded">涨停</span>
            )}
          </>
        )}
        {stockInfo && (stockInfo.total_mv || stockInfo.pe_ttm) && (
          <>
            {stockInfo.total_mv != null && <span className="text-xs text-muted-foreground">市值 {formatMv(stockInfo.total_mv)}</span>}
            {stockInfo.circ_mv != null && <span className="text-xs text-muted-foreground">流值 {formatMv(stockInfo.circ_mv)}</span>}
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
          {stockInfo?.limit_up_count != null && stockInfo.limit_up_count > 0 && (
            <span className="font-mono text-xs text-red-500">
              涨停{stockInfo.limit_up_count}次
              {stockInfo.max_consec_limit_up != null && stockInfo.max_consec_limit_up > 1 && ` 连板${stockInfo.max_consec_limit_up}`}
            </span>
          )}
          {cumulativeReturn != null && tradingDaysSinceBuy != null && (
            <span className={cn("font-mono text-xs font-medium", cumulativeReturn > 0 ? 'text-red-500' : cumulativeReturn < 0 ? 'text-green-500' : 'text-muted-foreground')}>
              B{tradingDaysSinceBuy}: {cumulativeReturn > 0 ? '+' : ''}{cumulativeReturn.toFixed(2)}%
            </span>
          )}
          {evalPeriods.map(p => {
            const ret = stockInfo?.returns?.[String(p)]
            if (!ret) return null
            return (
              <span key={p} className={cn("font-mono text-xs flex items-center", getValueColor(ret))}>
                <span className="inline-block w-1.5 h-1.5 rounded-full mr-1" style={{ backgroundColor: PERIOD_COLORS[p] }} />
                {PRICE_LINE_LABELS[p] ?? getPeriodLabel(p, tab)}: {formatPercent(ret)}
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
          onHoverData={setHoverData}
          minimal={true}
          sharedDates={sharedDates}
        />
      </div>
    </div>
  )
}