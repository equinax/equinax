import { useState, useMemo, useEffect, useCallback } from 'react'
import { format, parseISO, isWeekend, differenceInDays } from 'date-fns'
import { zhCN } from 'date-fns/locale'
import {
  useGetDataCoverageApiV1DataMapCoverageGet,
  useGetDataHeatmapApiV1DataMapHeatmapGet,
  useGetTableGapsApiV1DataMapGapsTableGet,
  useTriggerBackfillApiV1DataMapBackfillPost,
  useGetDateDetailApiV1DataMapDateDetailTableGet,
  useTriggerDateBackfillApiV1DataMapDateBackfillPost,
  useRefreshAssetMetaApiV1DataMapRefreshAssetMetaPost,
} from '@/api/generated/data-map/data-map'
import { useGetSyncJobApiV1DataSyncJobJobIdGet } from '@/api/generated/data-sync/data-sync'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { cn } from '@/lib/utils'
  import { 
  Map as MapIcon, 
  AlertTriangle, 
  CheckCircle, 
  CheckCircle2,
  Download, 
  DownloadCloud,
  ChevronRight, 
  Loader2,
  RefreshCw
} from 'lucide-react'
import customInstance from '@/api/mutator'

// --- Helper Functions ---

const formatNumber = (num: number) => {
  if (num >= 100000000) return (num / 100000000).toFixed(2) + '亿'
  if (num >= 10000) return (num / 10000).toFixed(1) + '万'
  return num.toLocaleString()
}

const getStatusColor = (status: string) => {
  switch (status?.toLowerCase()) {
    case 'ok': return 'text-emerald-500 bg-emerald-500/10 border-emerald-500/20'
    case 'stale': return 'text-amber-500 bg-amber-500/10 border-amber-500/20'
    case 'gap': return 'text-orange-500 bg-orange-500/10 border-orange-500/20'
    case 'empty': return 'text-red-500 bg-red-500/10 border-red-500/20'
    default: return 'text-muted-foreground bg-muted border-border'
  }
}

const getFillStyle = (count: number, expected: number) => {
  if (!expected || expected === 0) return { backgroundColor: 'hsl(var(--muted) / 0.3)' }
  if (count === 0) return { backgroundColor: 'hsl(var(--muted) / 0.5)' }

  const ratio = count / expected
  
  if (ratio >= 0.95) {
    // Green
    const alpha = Math.min(0.3 + (ratio * 0.5), 0.9)
    return { backgroundColor: `hsla(142, 70%, 40%, ${alpha})`, color: '#ecfdf5' }
  } else if (ratio >= 0.5) {
    // Amber
    const alpha = 0.3 + ((ratio - 0.5) * 1.2)
    return { backgroundColor: `hsla(38, 92%, 50%, ${alpha})`, color: '#fffbeb' }
  } else {
    // Red
    const alpha = 0.3 + (ratio * 0.8)
    return { backgroundColor: `hsla(0, 84%, 60%, ${alpha})`, color: '#fef2f2' }
  }
}

// --- Components ---

// Polling component for active backfill jobs
const BackfillPoller = ({ 
  jobId, 
  onStatusUpdate 
}: { 
  jobId: string, 
  onStatusUpdate: (status: string, records?: number) => void 
}) => {
  const { data } = useGetSyncJobApiV1DataSyncJobJobIdGet(jobId, {
    query: {
      refetchInterval: (query) => {
        const status = query.state.data?.status
        return (status === 'queued' || status === 'running') ? 2000 : false
      }
    }
  })

  useEffect(() => {
    if (data?.status) {
      onStatusUpdate(data.status, data.records_imported)
    }
  }, [data?.status, data?.records_imported, onStatusUpdate])

  return null
}



const GapDetailPanel = ({ 
  tableId, 
  days, 
  startDate,
  endDate,
  onClose,
  backfillingRanges,
  onStartBackfill
}: { 
  tableId: string, 
  days: number, 
  startDate?: string,
  endDate?: string,
  onClose: () => void,
  backfillingRanges: Map<string, { jobId: string, status: string, records?: number }>,
  onStartBackfill: (start: string, end: string) => void
}) => {
  const queryClient = useQueryClient()
  const { data: gapData, isLoading, refetch: refetchGaps } = useGetTableGapsApiV1DataMapGapsTableGet(tableId, { days, start_date: startDate, end_date: endDate })

  const [sparseBackfillStatus, setSparseBackfillStatus] = useState<Map<string, { status: string, before: number, after: number, expected: number, message: string }>>(new Map())
  const [isSparseBackfilling, setIsSparseBackfilling] = useState(false)

  const handleSparseBackfillAll = useCallback(async () => {
    if (!gapData?.sparse_dates?.length) return
    setIsSparseBackfilling(true)
    
    const dates = gapData.sparse_dates
      .filter(s => {
        const existing = sparseBackfillStatus.get(s.date)
        return !existing || existing.status === 'error'
      })
      .map(s => s.date)
    
    if (dates.length === 0) { setIsSparseBackfilling(false); return }

    try {
      const resp = await customInstance<{
        table: string
        results: Array<{ date: string, before: number, after: number, expected: number, status: string, message: string }>
        total_improved: number
        errors: string[]
      }>({
        url: '/api/v1/data-map/sparse-backfill',
        method: 'POST',
        data: { table: tableId, dates },
      })

      setSparseBackfillStatus(prev => {
        const next = new Map(prev)
        for (const r of resp.results) {
          next.set(r.date, { status: r.status, before: r.before, after: r.after, expected: r.expected, message: r.message })
        }
        return next
      })
      refetchGaps()
      queryClient.invalidateQueries({ queryKey: [`/api/v1/data-map/heatmap`] })
    } catch {
      for (const d of dates) {
        setSparseBackfillStatus(prev => {
          const next = new Map(prev)
          next.set(d, { status: 'error', before: 0, after: 0, expected: 0, message: '请求失败' })
          return next
        })
      }
    } finally {
      setIsSparseBackfilling(false)
    }
  }, [gapData, tableId, sparseBackfillStatus, refetchGaps, queryClient])

  const handleSparseBackfillOne = useCallback(async (dateStr: string) => {
    setSparseBackfillStatus(prev => {
      const next = new Map(prev)
      next.set(dateStr, { status: 'running', before: 0, after: 0, expected: 0, message: '' })
      return next
    })

    try {
      const resp = await customInstance<{
        table: string
        results: Array<{ date: string, before: number, after: number, expected: number, status: string, message: string }>
        total_improved: number
        errors: string[]
      }>({
        url: '/api/v1/data-map/sparse-backfill',
        method: 'POST',
        data: { table: tableId, dates: [dateStr] },
      })

      const r = resp.results[0]
      if (r) {
        setSparseBackfillStatus(prev => {
          const next = new Map(prev)
          next.set(r.date, { status: r.status, before: r.before, after: r.after, expected: r.expected, message: r.message })
          return next
        })
      }
      refetchGaps()
      queryClient.invalidateQueries({ queryKey: [`/api/v1/data-map/heatmap`] })
    } catch {
      setSparseBackfillStatus(prev => {
        const next = new Map(prev)
        next.set(dateStr, { status: 'error', before: 0, after: 0, expected: 0, message: '请求失败' })
        return next
      })
    }
  }, [tableId, refetchGaps, queryClient])

  // Group consecutive dates into ranges
  const gapRanges = useMemo(() => {
    if (!gapData?.missing_dates || gapData.missing_dates.length === 0) return []
    
    const sortedDates = [...gapData.missing_dates].sort()
    const ranges: { start: string, end: string, count: number }[] = []
    
    let currentStart = sortedDates[0]
    let currentEnd = sortedDates[0]
    let count = 1

    for (let i = 1; i < sortedDates.length; i++) {
      const date = sortedDates[i]
      const prevDate = parseISO(currentEnd)
      const currDate = parseISO(date)
      const diff = differenceInDays(currDate, prevDate)

      if (diff === 1) {
        currentEnd = date
        count++
      } else {
        ranges.push({ start: currentStart, end: currentEnd, count })
        currentStart = date
        currentEnd = date
        count = 1
      }
    }
    ranges.push({ start: currentStart, end: currentEnd, count })
    return ranges
  }, [gapData])

  if (isLoading) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground">
        <Loader2 className="w-6 h-6 animate-spin mr-2" />
        Loading gap details...
      </div>
    )
  }

  if (!gapData) return null

  const coverageRatio = gapData.reference_dates > 0 
    ? (gapData.covered_dates / gapData.reference_dates * 100).toFixed(1) 
    : '0.0'

  return (
    <div className="h-full flex flex-col bg-card/30 backdrop-blur-xl border-l border-border/50 w-[350px]">
      <div className="p-4 border-b border-border/50 flex items-center justify-between bg-card/50">
        <div>
          <h3 className="font-semibold text-foreground">{gapData.display_name}</h3>
          <p className="text-xs text-muted-foreground font-mono">{gapData.table}</p>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} className="h-8 w-8">
          <ChevronRight className="w-4 h-4" />
        </Button>
      </div>

      <div className="p-4 grid grid-cols-2 gap-3 border-b border-border/50 bg-card/20">
        <div>
          <div className="text-xs text-muted-foreground">Coverage</div>
          <div className={cn("text-xl font-bold font-mono", Number(coverageRatio) > 95 ? "text-emerald-500" : "text-amber-500")}>
            {coverageRatio}%
          </div>
        </div>
        <div>
          <div className="text-xs text-muted-foreground">Missing Days</div>
          <div className="text-xl font-bold font-mono text-foreground">
            {gapData.missing_dates?.length || 0}
          </div>
        </div>
      </div>

      <ScrollArea className="flex-1 p-4">
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-orange-500" />
              Missing Ranges
            </h4>
            {gapRanges.length > 1 && (() => {
              const allDone = gapRanges.every(r => backfillingRanges.get(`${r.start}-${r.end}`)?.status === 'success')
              const anyActive = gapRanges.some(r => {
                const s = backfillingRanges.get(`${r.start}-${r.end}`)?.status
                return s === 'queued' || s === 'running'
              })
              return (
                <Button
                  size="sm"
                  variant="default"
                  className="h-7 text-xs gap-1.5"
                  disabled={allDone || anyActive}
                  onClick={() => {
                    gapRanges.forEach(r => {
                      const s = backfillingRanges.get(`${r.start}-${r.end}`)?.status
                      if (s !== 'success' && s !== 'queued' && s !== 'running') {
                        onStartBackfill(r.start, r.end)
                      }
                    })
                  }}
                >
                  {anyActive ? (
                    <Loader2 className="w-3 h-3 animate-spin" />
                  ) : (
                    <DownloadCloud className="w-3 h-3" />
                  )}
                  {allDone ? '已全部补全' : anyActive ? '补全中...' : '补全全部'}
                </Button>
              )
            })()}
           </div>

          {gapRanges.length === 0 && (gapData.sparse_dates?.length ?? 0) === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm bg-emerald-500/5 rounded-lg border border-emerald-500/10">
              <CheckCircle className="w-8 h-8 text-emerald-500 mx-auto mb-2" />
              No gaps found in this period!
            </div>
          ) : gapRanges.length > 0 ? (
            gapRanges.map((range, idx) => {
              const rangeKey = `${range.start}-${range.end}`
              const backfillState = backfillingRanges.get(rangeKey)
              const status = backfillState?.status
              
              return (
                <div key={idx} className="bg-background/50 border border-border/50 rounded-lg p-3 text-sm">
                  <div className="flex justify-between items-start mb-2">
                    <div className="font-mono text-xs text-foreground/80">
                      {range.start} ~ {range.end}
                    </div>
                    <Badge variant="secondary" className="text-[10px] h-5">
                      {range.count} days
                    </Badge>
                  </div>
                  <Button 
                    size="sm" 
                    variant={status === 'failed' ? 'destructive' : 'outline'}
                    className={cn(
                      "w-full h-7 text-xs gap-1.5 transition-colors",
                      status === 'success' && "bg-emerald-500/10 text-emerald-500 border-emerald-500/20 hover:bg-emerald-500/20",
                      !status && "hover:bg-primary hover:text-primary-foreground"
                    )}
                    onClick={() => onStartBackfill(range.start, range.end)}
                    disabled={status === 'queued' || status === 'running' || status === 'success'}
                  >
                    {status === 'queued' && (
                      <>
                        <Loader2 className="w-3 h-3 animate-spin" />
                        Queued...
                      </>
                    )}
                    {status === 'running' && (
                      <>
                        <Loader2 className="w-3 h-3 animate-spin" />
                        Running...
                      </>
                    )}
                    {status === 'success' && (
                      <>
                        <CheckCircle2 className="w-3 h-3" />
                        {backfillState?.records ? `✓ ${backfillState.records} records` : '✓ Done'}
                      </>
                    )}
                    {status === 'failed' && (
                      <>
                        <AlertTriangle className="w-3 h-3" />
                        Failed - Retry
                      </>
                    )}
                    {!status && (
                      <>
                        <Download className="w-3 h-3" />
                        Backfill Range
                      </>
                    )}
                  </Button>
                </div>
              )
            })
          ) : null}

          {(gapData.sparse_dates?.length ?? 0) > 0 && (
            <>
              <div className="flex items-center justify-between mt-4">
                <h4 className="text-sm font-medium flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-amber-500" />
                  Sparse Dates
                  <Badge variant="secondary" className="text-[10px] h-4 font-mono">&lt;95%</Badge>
                </h4>
                {(() => {
                  const allDone = gapData.sparse_dates!.every(s => {
                    const st = sparseBackfillStatus.get(s.date)?.status
                    return st === 'improved' || st === 'best_effort'
                  })
                  return (
                    <Button
                      size="sm"
                      variant="default"
                      className="h-7 text-xs gap-1.5"
                      disabled={allDone || isSparseBackfilling}
                      onClick={handleSparseBackfillAll}
                    >
                      {isSparseBackfilling ? (
                        <Loader2 className="w-3 h-3 animate-spin" />
                      ) : (
                        <DownloadCloud className="w-3 h-3" />
                      )}
                      {allDone ? '已全部补全' : isSparseBackfilling ? '补全中...' : `补全全部(${gapData.sparse_dates!.length})`}
                    </Button>
                  )
                })()}
              </div>
              {gapData.sparse_dates!.map((s) => {
                const pct = s.expected > 0 ? (s.actual / s.expected * 100).toFixed(1) : '0'
                const backfillResult = sparseBackfillStatus.get(s.date)
                const isRunning = backfillResult?.status === 'running'
                const isDone = backfillResult?.status === 'improved' || backfillResult?.status === 'best_effort'
                const isError = backfillResult?.status === 'error'

                return (
                  <div key={s.date} className={cn(
                    "border rounded-lg px-3 py-2 text-xs transition-colors",
                    isDone ? "bg-emerald-500/5 border-emerald-500/15" :
                    isError ? "bg-red-500/5 border-red-500/15" :
                    "bg-amber-500/5 border-amber-500/10"
                  )}>
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-foreground/80">{s.date}</span>
                      <div className="flex items-center gap-2">
                        <span className={cn("font-mono", isDone ? "text-emerald-500" : "text-amber-500")}>
                          {isDone && backfillResult ? `${backfillResult.after.toLocaleString()}/${backfillResult.expected.toLocaleString()}` : `${s.actual.toLocaleString()}/${s.expected.toLocaleString()}`}
                          <span className="ml-1 opacity-70">({isDone && backfillResult ? (backfillResult.after / backfillResult.expected * 100).toFixed(1) : pct}%)</span>
                        </span>
                        {!isDone && !isSparseBackfilling && tableId !== 'market_daily_index' && (
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-6 w-6 p-0"
                            disabled={isRunning}
                            onClick={() => handleSparseBackfillOne(s.date)}
                          >
                            {isRunning ? (
                              <Loader2 className="w-3 h-3 animate-spin" />
                            ) : (
                              <Download className="w-3 h-3 text-muted-foreground hover:text-foreground" />
                            )}
                          </Button>
                        )}
                      </div>
                    </div>
                    {isDone && backfillResult?.message && (
                      <div className="mt-1 text-[10px] text-emerald-600">
                        <CheckCircle2 className="w-3 h-3 inline mr-1" />
                        {backfillResult.message}
                      </div>
                    )}
                    {isError && backfillResult?.message && (
                      <div className="mt-1 text-[10px] text-red-500">
                        <AlertTriangle className="w-3 h-3 inline mr-1" />
                        {backfillResult.message}
                      </div>
                    )}
                  </div>
                )
              })}
            </>
          )}
        </div>
      </ScrollArea>
    </div>
  )
}

// --- Date Detail Dialog ---

const DateDetailDialog = ({
  table,
  date,
  open,
  onOpenChange,
  refetchHeatmap
}: {
  table: string,
  date: string,
  open: boolean,
  onOpenChange: (open: boolean) => void,
  refetchHeatmap: () => void
}) => {
  const queryClient = useQueryClient()
  const { data, isLoading, refetch } = useGetDateDetailApiV1DataMapDateDetailTableGet(table, { date }, {
    query: { enabled: !!table && !!date && open }
  })

  const { mutate: triggerBackfill } = useTriggerDateBackfillApiV1DataMapDateBackfillPost()
  const [backfillingTypes, setBackfillingTypes] = useState<Set<string>>(new Set())

  const isMarketTable = table.startsWith('market_daily_')

  const handleBackfill = (assetTypes: string[]) => {
    assetTypes.forEach(t => setBackfillingTypes(prev => new Set(prev).add(t)))

    triggerBackfill({
      data: {
        table,
        date,
        asset_types: assetTypes
      }
    }, {
      onSuccess: () => {
        refetch()
        refetchHeatmap()
        queryClient.invalidateQueries({ queryKey: [`/api/v1/data-map/heatmap`] })
      },
      onSettled: () => {
        assetTypes.forEach(t => setBackfillingTypes(prev => {
          const next = new Set(prev)
          next.delete(t)
          return next
        }))
      }
    })
  }

  const getAssetTypeName = (type: string) => {
    switch (type) {
      case 'STOCK': return '股票'
      case 'ETF': return 'ETF'
      case 'INDEX': return '指数'
      default: return type
    }
  }

  const TABLE_LABELS: Record<string, string> = {
    market_daily_stock: '股票行情',
    market_daily_etf: 'ETF行情',
    market_daily_index: '指数行情',
    indicator_valuation: '估值指标',
    moneyflow_daily: '资金流向',
    limit_list_daily: '涨跌停',
    adjust_factor: '复权因子',
    stock_style_exposure: '风格因子',
    market_regime: '市场环境',
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[420px] bg-background/95 backdrop-blur-xl border-border/50">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-base">
            <span>{date}</span>
            <Badge variant="outline" className="font-mono font-normal text-xs">{TABLE_LABELS[table] ?? table}</Badge>
          </DialogTitle>
        </DialogHeader>

        {isLoading ? (
          <div className="py-12 flex justify-center">
            <Loader2 className="w-8 h-8 animate-spin text-muted-foreground/50" />
          </div>
        ) : data ? (
          <div className="space-y-4">
            <div className="flex items-center justify-between bg-muted/30 p-3 rounded-lg border border-border/50">
              <div className="text-xs text-muted-foreground">
                <div className="mb-1">记录数</div>
                <div className="flex items-baseline gap-1">
                  <span className="font-mono text-lg font-semibold text-foreground">{formatNumber(data.total_actual)}</span>
                  {data.total_expected > 0 && (
                    <>
                      <span className="text-[10px]">/</span>
                      <span className="font-mono text-sm">{formatNumber(data.total_expected)}</span>
                    </>
                  )}
                </div>
              </div>
              {isMarketTable && data.total_expected > 0 && (() => {
                const ratio = data.total_actual / data.total_expected
                const isComplete = ratio >= 0.99
                return (
                  <div className="text-right">
                    <div className={cn("font-mono text-lg font-semibold", isComplete ? "text-emerald-500" : "text-amber-500")}>
                      {(ratio * 100).toFixed(1)}%
                    </div>
                    <div className="h-1.5 w-24 bg-muted mt-1 rounded-full overflow-hidden">
                      <div 
                        className={cn("h-full rounded-full transition-all duration-500", isComplete ? "bg-emerald-500" : "bg-amber-500")} 
                        style={{ width: `${Math.min(ratio * 100, 100)}%` }}
                      />
                    </div>
                  </div>
                )
              })()}
            </div>

            {isMarketTable && data.breakdown.length > 0 && (() => {
              const item = data.breakdown[0]
              const ratio = item.expected > 0 ? item.actual / item.expected : 0
              const isComplete = ratio >= 0.99
              const isBackfilling = backfillingTypes.has(item.asset_type)
              const missing = Math.max(0, item.expected - item.actual)

              return isComplete ? (
                <div className="flex items-center justify-center gap-2 py-3 text-sm text-emerald-500 bg-emerald-500/5 rounded-lg border border-emerald-500/10">
                  <CheckCircle2 className="w-4 h-4" />
                  数据完整
                </div>
              ) : (
                <Button
                  variant="outline"
                  className="w-full h-9 text-xs gap-2 hover:bg-primary/5 hover:text-primary hover:border-primary/20 transition-colors"
                  onClick={() => handleBackfill([item.asset_type])}
                  disabled={isBackfilling}
                >
                  {isBackfilling ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Download className="w-3.5 h-3.5" />
                  )}
                  补全{getAssetTypeName(item.asset_type)}数据
                  <span className="text-muted-foreground ml-1">（缺 {formatNumber(missing)} 条）</span>
                </Button>
              )
            })()}

            {!isMarketTable && data.total_actual === 0 && (
              <div className="flex items-center justify-center gap-2 py-3 text-sm text-muted-foreground bg-muted/20 rounded-lg border border-border/50">
                <AlertTriangle className="w-4 h-4 text-amber-500" />
                暂无数据
              </div>
            )}
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

// --- Main Page Component ---

export default function DataMapPage() {
  const [days, setDays] = useState(60)
  const [selectedYear, setSelectedYear] = useState<string>("")
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [cellDetail, setCellDetail] = useState<{ table: string, date: string } | null>(null)
  const [backfillingRanges, setBackfillingRanges] = useState<Map<string, { jobId: string, status: string, records?: number }>>(new Map())
  // Compute date range for year selection
  const dateRange = useMemo(() => {
    if (!selectedYear) return { start_date: undefined, end_date: undefined }
    const year = Number(selectedYear)
    const now = new Date()
    const currentYear = now.getFullYear()
    return {
      start_date: `${year}-01-01`,
      end_date: year >= currentYear ? `${currentYear}-12-31` : `${year}-12-31`,
    }
  }, [selectedYear])

  const queryClient = useQueryClient()
  const { data: coverageData, refetch: refetchCoverage } = useGetDataCoverageApiV1DataMapCoverageGet()
  const { data: heatmapData, isLoading: isHeatmapLoading, refetch: refetchHeatmap } = useGetDataHeatmapApiV1DataMapHeatmapGet({ days, start_date: dateRange.start_date, end_date: dateRange.end_date })
  
  const { mutate: triggerBackfill } = useTriggerBackfillApiV1DataMapBackfillPost()

  const { mutate: refreshAssetMeta, isPending: isRefreshingMeta } = useRefreshAssetMetaApiV1DataMapRefreshAssetMetaPost()
  const [metaResult, setMetaResult] = useState<{ stocks: number, indices: number, etfs: number } | null>(null)

  const handleRefreshMeta = useCallback(() => {
    setMetaResult(null)
    refreshAssetMeta(undefined, {
      onSuccess: (data) => {
        setMetaResult({
          stocks: data.stocks_upserted ?? 0,
          indices: data.indices_upserted ?? 0,
          etfs: data.etfs_upserted ?? 0,
        })
        refetchCoverage()
        refetchHeatmap()
        setTimeout(() => setMetaResult(null), 5000)
      }
    })
  }, [refreshAssetMeta, refetchCoverage, refetchHeatmap])

  const handleStartBackfill = useCallback((start: string, end: string) => {
    if (!selectedTable) return
    
    const rangeKey = `${start}-${end}`
    
    triggerBackfill({
      data: {
        table: selectedTable,
        start_date: start,
        end_date: end
      }
    }, {
      onSuccess: (data) => {
        setBackfillingRanges(prev => {
          const next = new Map(prev)
          next.set(rangeKey, { jobId: data.job_id, status: 'queued' })
          return next
        })
      },
      onError: () => {
        setBackfillingRanges(prev => {
          const next = new Map(prev)
          next.set(rangeKey, { jobId: '', status: 'failed' })
          return next
        })
      }
    })
  }, [selectedTable, triggerBackfill])

  const handleJobStatusUpdate = useCallback((rangeKey: string, status: string, records?: number) => {
    setBackfillingRanges(prev => {
      const current = prev.get(rangeKey)
      if (current?.status === status && current?.records === records) return prev
      
      const next = new Map(prev)
      next.set(rangeKey, { ...current!, status, records })
      return next
    })

    if (status === 'success') {
      // Trigger refetches — invalidate gaps query so GapDetailPanel's own hook refreshes
      refetchCoverage()
      refetchHeatmap()
      queryClient.invalidateQueries({ queryKey: [`/api/v1/data-map/gaps/${selectedTable}`] })
      
      // Remove from map after delay to show success state
      setTimeout(() => {
        setBackfillingRanges(prev => {
          const next = new Map(prev)
          next.delete(rangeKey)
          return next
        })
      }, 3000)
    }
  }, [refetchCoverage, refetchHeatmap, queryClient, selectedTable])

  const coverageMap = useMemo(() => {
    if (!coverageData?.tables) return new Map()
    return new Map(coverageData.tables.map(t => [t.table, t]))
  }, [coverageData])

  return (
    <div className="h-[calc(100vh-2rem)] -m-4 flex flex-col bg-background text-foreground overflow-hidden">
      {/* Render pollers for active jobs */}
      {Array.from(backfillingRanges.entries()).map(([key, value]) => {
        if (value.status === 'queued' || value.status === 'running') {
          return (
            <BackfillPoller 
              key={value.jobId} 
              jobId={value.jobId} 
              onStatusUpdate={(status, records) => handleJobStatusUpdate(key, status, records)} 
            />
          )
        }
        return null
      })}

      {/* Header */}
      <header className="flex-shrink-0 h-14 border-b border-border/40 bg-background/80 backdrop-blur-md flex items-center justify-between px-6 z-10">
        <div className="flex items-center gap-3">
          <div className="p-1.5 bg-primary/10 rounded-md">
            <MapIcon className="w-5 h-5 text-primary" />
          </div>
          <div>
            <h1 className="text-base font-semibold tracking-tight">数据地图</h1>
            <p className="text-[10px] text-muted-foreground">Data Coverage & Integrity Map</p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="h-8 text-xs gap-1.5"
            onClick={handleRefreshMeta}
            disabled={isRefreshingMeta}
          >
            {isRefreshingMeta ? (
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
            ) : (
              <RefreshCw className="w-3.5 h-3.5" />
            )}
            更新资产列表
          </Button>
          {metaResult && (
            <div className="text-[10px] text-muted-foreground font-mono animate-in fade-in slide-in-from-left-2 duration-300">
              股票{metaResult.stocks} · 指数{metaResult.indices} · ETF{metaResult.etfs}
            </div>
          )}
          <Tabs value={selectedYear ? "" : days.toString()} onValueChange={(v) => { setSelectedYear(""); setDays(Number(v)) }} className="h-8">
            <TabsList className="h-8 bg-muted/50 p-0.5">
              {[30, 60, 90, 180, 365].map(d => (
                <TabsTrigger 
                  key={d} 
                  value={d.toString()}
                  className="h-7 text-xs px-3 data-[state=active]:bg-background data-[state=active]:shadow-sm"
                >
                  {d} Days
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
          <Select value={selectedYear} onValueChange={(v) => {
            setSelectedYear(v)
          }}>
            <SelectTrigger className={cn("h-8 w-[100px] text-xs", selectedYear ? "bg-background shadow-sm" : "bg-muted/50")}>
              <SelectValue placeholder="Select Year" />
            </SelectTrigger>
            <SelectContent>
              {Array.from({ length: new Date().getFullYear() - 2015 + 1 }, (_, i) => 2015 + i).reverse().map(y => (
                <SelectItem key={y} value={y.toString()} className="text-xs">{y}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </header>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 flex flex-col min-w-0">


          {/* Heatmap Section */}
          <div className="flex-1 overflow-hidden relative bg-background">
            {isHeatmapLoading ? (
              <div className="absolute inset-0 flex items-center justify-center">
                <Loader2 className="w-8 h-8 animate-spin text-primary/50" />
              </div>
            ) : heatmapData ? (
              <div className="h-full overflow-auto custom-scrollbar">
                <div className="inline-block min-w-full align-top">
                  <table className="border-collapse w-full">
                    <thead className="sticky top-0 z-20 bg-background/95 backdrop-blur shadow-sm">
                      <tr>
                        <th className="sticky left-0 z-30 bg-background/95 backdrop-blur border-b border-r border-border/50 p-2 text-left min-w-[120px]">
                          <div className="text-xs font-medium text-muted-foreground pl-2">Date</div>
                        </th>
                        {heatmapData.tables.map(tableKey => {
                          const cov = coverageMap.get(tableKey)
                          return (
                          <th 
                            key={tableKey} 
                            className={cn(
                              "border-b border-r border-border/50 px-2 py-1.5 min-w-[110px] text-left cursor-pointer hover:bg-muted/50 transition-colors align-top",
                              selectedTable === tableKey && "bg-primary/5 border-b-primary/30"
                            )}
                            onClick={() => setSelectedTable(tableKey === selectedTable ? null : tableKey)}
                          >
                            <div className="flex items-center gap-1.5 mb-0.5">
                              <span className={cn("text-xs font-medium whitespace-nowrap", selectedTable === tableKey ? "text-primary" : "text-foreground")}>
                                {heatmapData.table_labels[tableKey]}
                              </span>
                              {cov && (
                                <Badge variant="outline" className={cn("text-[7px] px-1 py-0 h-3 leading-none flex-shrink-0", getStatusColor(cov.status))}>
                                  {cov.status}
                                </Badge>
                              )}
                            </div>
                            <div className="text-[9px] text-muted-foreground font-mono font-normal opacity-70">
                              {tableKey}
                            </div>
                            {cov && (
                              <div className="flex items-baseline gap-1 mt-0.5 text-[9px] font-mono tabular-nums text-muted-foreground">
                                <span className="text-foreground/70 font-medium">{formatNumber(cov.row_count)}</span>
                                <span className="opacity-50">/</span>
                                <span>{formatNumber(cov.symbol_count)}sym</span>
                                {cov.gap_days > 0 && (
                                  <span className="text-orange-500 font-medium ml-auto">{cov.gap_days}gap</span>
                                )}
                              </div>
                            )}
                          </th>
                          )
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {heatmapData.rows.map((row) => {
                        const dateObj = parseISO(row.date)
                        const isWknd = isWeekend(dateObj)
                        
                        return (
                          <tr key={row.date} className="group hover:bg-muted/5">
                            <td className={cn(
                              "sticky left-0 z-10 border-b border-r border-border/50 p-2 text-xs font-mono bg-background/95 backdrop-blur group-hover:bg-muted/5",
                              isWknd && "text-muted-foreground/50"
                            )}>
                              <div className="pl-2 flex items-center gap-2">
                                <span>{format(dateObj, 'yyyy-MM-dd')}</span>
                                <span className="text-[10px] opacity-60">{format(dateObj, 'EEE', { locale: zhCN })}</span>
                              </div>
                            </td>
                            {heatmapData.tables.map(tableKey => {
                              const count = row.cells[tableKey] ?? 0
                              const expected = heatmapData.expected_counts[tableKey] ?? 0
                              const style = getFillStyle(count, expected)
                              
                              return (
                                <td 
                                  key={`${row.date}-${tableKey}`} 
                                  className={cn(
                                    "border-b border-r border-border/30 p-0 relative transition-colors cursor-pointer hover:brightness-90",
                                    selectedTable === tableKey && "ring-1 ring-inset ring-primary/10"
                                  )}
                                  onClick={() => setCellDetail({ table: tableKey, date: row.date })}
                                >
                                  <div 
                                    className="w-full h-10 flex items-center justify-center text-[10px] font-mono cursor-default"
                                    style={style}
                                  >
                                    {count > 0 ? (count >= 10000 ? (count/10000).toFixed(1)+'w' : count) : '—'}
                                  </div>
                                </td>
                              )
                            })}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </div>
        </div>

        {/* Gap Detail Panel (Slide-in) */}
        <div className={cn(
          "transition-all duration-300 ease-in-out overflow-hidden border-l border-border/50 bg-background shadow-xl z-20",
          selectedTable ? "w-[350px] opacity-100 translate-x-0" : "w-0 opacity-0 translate-x-full"
        )}>
          {selectedTable && (
            <GapDetailPanel 
              tableId={selectedTable} 
              days={days}
              startDate={dateRange.start_date}
              endDate={dateRange.end_date}
              onClose={() => setSelectedTable(null)}
              backfillingRanges={backfillingRanges}
              onStartBackfill={handleStartBackfill}
            />
          )}
        </div>
      </div>

      <DateDetailDialog 
        table={cellDetail?.table ?? ''} 
        date={cellDetail?.date ?? ''} 
        open={!!cellDetail} 
        onOpenChange={(open) => !open && setCellDetail(null)} 
        refetchHeatmap={refetchHeatmap}
      />
    </div>
  )
}
