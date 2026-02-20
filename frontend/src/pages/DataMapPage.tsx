import { useState, useMemo, useEffect, useCallback } from 'react'
import { format, parseISO, isWeekend, differenceInDays } from 'date-fns'
import { zhCN } from 'date-fns/locale'
import {
  useGetDataCoverageApiV1DataMapCoverageGet,
  useGetDataHeatmapApiV1DataMapHeatmapGet,
  useGetTableGapsApiV1DataMapGapsTableGet,
  useTriggerBackfillApiV1DataMapBackfillPost,
} from '@/api/generated/data-map/data-map'
import { useGetSyncJobApiV1DataSyncJobJobIdGet } from '@/api/generated/data-sync/data-sync'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { 
  Map as MapIcon, 
  AlertTriangle, 
  CheckCircle, 
  CheckCircle2,
  Download, 
  ChevronRight, 
  Loader2
} from 'lucide-react'

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
  onClose,
  backfillingRanges,
  onStartBackfill
}: { 
  tableId: string, 
  days: number, 
  onClose: () => void,
  backfillingRanges: Map<string, { jobId: string, status: string, records?: number }>,
  onStartBackfill: (start: string, end: string) => void
}) => {
  const { data: gapData, isLoading } = useGetTableGapsApiV1DataMapGapsTableGet(tableId, { days })

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
           </div>

          {gapRanges.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm bg-emerald-500/5 rounded-lg border border-emerald-500/10">
              <CheckCircle className="w-8 h-8 text-emerald-500 mx-auto mb-2" />
              No gaps found in this period!
            </div>
          ) : (
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
          )}
        </div>
      </ScrollArea>
    </div>
  )
}

// --- Main Page Component ---

export default function DataMapPage() {
  const [days, setDays] = useState(60)
  const [selectedTable, setSelectedTable] = useState<string | null>(null)
  const [backfillingRanges, setBackfillingRanges] = useState<Map<string, { jobId: string, status: string, records?: number }>>(new Map())
  
  const queryClient = useQueryClient()
  const { data: coverageData, refetch: refetchCoverage } = useGetDataCoverageApiV1DataMapCoverageGet()
  const { data: heatmapData, isLoading: isHeatmapLoading, refetch: refetchHeatmap } = useGetDataHeatmapApiV1DataMapHeatmapGet({ days })
  
  const { mutate: triggerBackfill } = useTriggerBackfillApiV1DataMapBackfillPost()

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

        <div className="flex items-center gap-4">
          <Tabs value={days.toString()} onValueChange={(v) => setDays(Number(v))} className="h-8">
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
                                <span>{format(dateObj, 'MM-dd')}</span>
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
                                    "border-b border-r border-border/30 p-0 relative transition-colors",
                                    selectedTable === tableKey && "ring-1 ring-inset ring-primary/10"
                                  )}
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
              onClose={() => setSelectedTable(null)}
              backfillingRanges={backfillingRanges}
              onStartBackfill={handleStartBackfill}
            />
          )}
        </div>
      </div>
    </div>
  )
}
