import { useState, useMemo } from 'react'
import { format, parseISO, isWeekend, differenceInDays } from 'date-fns'
import { zhCN } from 'date-fns/locale'
import {
  useGetDataCoverageApiV1DataMapCoverageGet,
  useGetDataHeatmapApiV1DataMapHeatmapGet,
  useGetTableGapsApiV1DataMapGapsTableGet,
  useTriggerBackfillApiV1DataMapBackfillPost,
  useGetMarketDailyBreakdownApiV1DataMapMarketDailyBreakdownGet,
} from '@/api/generated/data-map/data-map'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { 
  Map as MapIcon, 
  Calendar, 
  AlertTriangle, 
  CheckCircle, 
  Download, 
  ChevronRight, 
  Loader2,
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

const CoverageCard = ({ table, onClick, isSelected }: { table: any, onClick: () => void, isSelected: boolean }) => {
  const isMarketDaily = table.table === 'market_daily'
  const { data: breakdown } = useGetMarketDailyBreakdownApiV1DataMapMarketDailyBreakdownGet({
    query: { enabled: isMarketDaily }
  })

  return (
    <div 
      onClick={onClick}
      className={cn(
        "flex-shrink-0 w-[280px] p-4 rounded-xl border cursor-pointer transition-all duration-200 hover:shadow-md",
        isSelected 
          ? "border-primary bg-primary/5 ring-1 ring-primary/20" 
          : "border-border/50 bg-card/50 hover:bg-card/80 hover:border-border"
      )}
    >
      <div className="flex justify-between items-start mb-3">
        <div>
          <h3 className="font-medium text-sm text-foreground/90">{table.display_name}</h3>
          <p className="text-xs text-muted-foreground font-mono mt-0.5">{table.table}</p>
        </div>
        <Badge variant="outline" className={cn("text-[10px] px-1.5 py-0 h-5", getStatusColor(table.status))}>
          {table.status}
        </Badge>
      </div>

      <div className="grid grid-cols-2 gap-2 mb-3">
        <div className="bg-background/50 rounded p-2">
          <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Rows</div>
          <div className="text-lg font-semibold font-mono tracking-tight">{formatNumber(table.row_count)}</div>
        </div>
        <div className="bg-background/50 rounded p-2">
          <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Symbols</div>
          <div className="text-lg font-semibold font-mono tracking-tight">{formatNumber(table.symbol_count)}</div>
        </div>
      </div>

      {isMarketDaily && breakdown?.breakdowns ? (
        <div className="flex gap-1 mb-3">
          {breakdown.breakdowns.map((b: any) => (
            <div key={b.asset_type} className="flex-1 bg-background/30 rounded px-1.5 py-1 text-center">
              <div className="text-[9px] text-muted-foreground">{b.asset_type}</div>
              <div className="text-xs font-mono">{formatNumber(b.row_count)}</div>
            </div>
          ))}
        </div>
      ) : null}

      <div className="flex items-center justify-between text-xs text-muted-foreground border-t border-border/30 pt-2 mt-auto">
        <div className="flex items-center gap-1.5">
          <Calendar className="w-3 h-3" />
          <span>{table.earliest_date ? format(parseISO(table.earliest_date), 'yyyy-MM-dd') : '-'}</span>
        </div>
        {table.gap_days > 0 && (
          <div className="flex items-center gap-1 text-orange-500 font-medium">
            <AlertTriangle className="w-3 h-3" />
            <span>{table.gap_days} gaps</span>
          </div>
        )}
      </div>
    </div>
  )
}

const GapDetailPanel = ({ tableId, days, onClose }: { tableId: string, days: number, onClose: () => void }) => {
  const { data: gapData, isLoading } = useGetTableGapsApiV1DataMapGapsTableGet(tableId, { days })
  const { mutate: triggerBackfill } = useTriggerBackfillApiV1DataMapBackfillPost()
  const [backfillingRanges, setBackfillingRanges] = useState<Set<string>>(new Set())
  const [backfillMessage, setBackfillMessage] = useState<string | null>(null)

  const handleBackfill = (start: string, end: string) => {
    const rangeKey = `${start}-${end}`
    setBackfillingRanges(prev => new Set(prev).add(rangeKey))
    
    triggerBackfill({
      data: {
        table: tableId,
        start_date: start,
        end_date: end
      }
    }, {
      onSuccess: (data) => {
        setBackfillMessage(`Backfill started: ${data.message}`)
        setTimeout(() => setBackfillMessage(null), 5000)
      },
      onError: () => {
        setBackfillMessage('Failed to start backfill')
        setTimeout(() => setBackfillMessage(null), 5000)
        setBackfillingRanges(prev => {
          const next = new Set(prev)
          next.delete(rangeKey)
          return next
        })
      }
    })
  }

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

          {backfillMessage && (
            <div className="text-xs px-3 py-2 rounded-lg bg-primary/10 border border-primary/20 text-primary">
              {backfillMessage}
            </div>
          )}

          {gapRanges.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm bg-emerald-500/5 rounded-lg border border-emerald-500/10">
              <CheckCircle className="w-8 h-8 text-emerald-500 mx-auto mb-2" />
              No gaps found in this period!
            </div>
          ) : (
            gapRanges.map((range, idx) => {
              const rangeKey = `${range.start}-${range.end}`
              const isProcessing = backfillingRanges.has(rangeKey)
              
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
                    variant="outline" 
                    className="w-full h-7 text-xs gap-1.5 hover:bg-primary hover:text-primary-foreground transition-colors"
                    onClick={() => handleBackfill(range.start, range.end)}
                    disabled={isProcessing}
                  >
                    {isProcessing ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                    {isProcessing ? 'Queued' : 'Backfill Range'}
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
  
  const { data: coverageData, isLoading: isCoverageLoading } = useGetDataCoverageApiV1DataMapCoverageGet()
  const { data: heatmapData, isLoading: isHeatmapLoading } = useGetDataHeatmapApiV1DataMapHeatmapGet({ days })

  // Sort tables by status priority (Gap > Stale > OK > Empty) then by name
  const sortedTables = useMemo(() => {
    if (!coverageData?.tables) return []
    const priority = { 'gap': 0, 'stale': 1, 'ok': 2, 'empty': 3 }
    return [...coverageData.tables].sort((a, b) => {
      const pA = priority[a.status.toLowerCase() as keyof typeof priority] ?? 4
      const pB = priority[b.status.toLowerCase() as keyof typeof priority] ?? 4
      if (pA !== pB) return pA - pB
      return a.display_name.localeCompare(b.display_name)
    })
  }, [coverageData])

  return (
    <div className="h-[calc(100vh-2rem)] -m-4 flex flex-col bg-background text-foreground overflow-hidden">
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
          
          {/* Coverage Cards Section */}
          <div className="flex-shrink-0 border-b border-border/40 bg-muted/5">
            <ScrollArea className="w-full whitespace-nowrap">
              <div className="flex gap-3 p-4 min-w-max">
                {isCoverageLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="w-[280px] h-[140px] rounded-xl border border-border/50 bg-card/30 animate-pulse" />
                  ))
                ) : (
                  sortedTables.map(table => (
                    <CoverageCard 
                      key={table.table} 
                      table={table} 
                      isSelected={selectedTable === table.table}
                      onClick={() => setSelectedTable(table.table === selectedTable ? null : table.table)}
                    />
                  ))
                )}
              </div>
            </ScrollArea>
          </div>

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
                        {heatmapData.tables.map(tableKey => (
                          <th 
                            key={tableKey} 
                            className={cn(
                              "border-b border-r border-border/50 p-2 min-w-[100px] text-left cursor-pointer hover:bg-muted/50 transition-colors",
                              selectedTable === tableKey && "bg-primary/5 border-b-primary/30"
                            )}
                            onClick={() => setSelectedTable(tableKey === selectedTable ? null : tableKey)}
                          >
                            <div className="flex flex-col gap-0.5">
                              <span className={cn("text-xs font-medium whitespace-nowrap", selectedTable === tableKey ? "text-primary" : "text-foreground")}>
                                {heatmapData.table_labels[tableKey]}
                              </span>
                              <span className="text-[9px] text-muted-foreground font-mono font-normal opacity-70">
                                {tableKey}
                              </span>
                            </div>
                          </th>
                        ))}
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
                                  <TooltipProvider delayDuration={0}>
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <div 
                                          className="w-full h-10 flex items-center justify-center text-[10px] font-mono cursor-default"
                                          style={style}
                                        >
                                          {count > 0 ? (count >= 10000 ? (count/10000).toFixed(1)+'w' : count) : '—'}
                                        </div>
                                      </TooltipTrigger>
                                      <TooltipContent side="top" className="text-xs">
                                        <div className="font-bold mb-1">{heatmapData.table_labels[tableKey]}</div>
                                        <div className="text-muted-foreground mb-1">{row.date}</div>
                                        <div className="flex gap-4">
                                          <div>Count: <span className="text-foreground font-mono">{count}</span></div>
                                          <div>Expected: <span className="text-foreground font-mono">{expected}</span></div>
                                        </div>
                                      </TooltipContent>
                                    </Tooltip>
                                  </TooltipProvider>
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
            />
          )}
        </div>
      </div>
    </div>
  )
}
