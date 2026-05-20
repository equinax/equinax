import { useState, useMemo } from 'react'
import { format } from 'date-fns'
import { RefreshCw, TrendingUp, X } from 'lucide-react'
import { TimeController } from '@/components/alpha-radar/TimeController'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useGetLowPositionRadarApiV1AlphaRadarLowPositionGet } from '@/api/generated/alpha-radar/alpha-radar'
import { cn } from '@/lib/utils'

const TAG_COLOR_MAP: Record<string, string> = {
  强相对: 'bg-red-500/15 text-red-600 dark:text-red-400 border-red-500/30',
  低位首板: 'bg-red-500/15 text-red-600 dark:text-red-400 border-red-500/30',
  蓄势充分: 'bg-amber-500/15 text-amber-600 dark:text-amber-400 border-amber-500/30',
  补涨预备: 'bg-amber-500/15 text-amber-600 dark:text-amber-400 border-amber-500/30',
  均线多头: 'bg-blue-500/15 text-blue-600 dark:text-blue-400 border-blue-500/30',
  趋势启动: 'bg-blue-500/15 text-blue-600 dark:text-blue-400 border-blue-500/30',
  主力净流入: 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30',
  资金异动: 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30',
}

const DEFAULT_TAG_COLOR = 'bg-muted text-muted-foreground border-border'

function TagChip({ label }: { label: string }) {
  const color = TAG_COLOR_MAP[label] ?? DEFAULT_TAG_COLOR
  return (
    <span
      className={cn(
        'inline-flex items-center whitespace-nowrap rounded-full border px-2 py-0.5 text-[10px] font-medium leading-none',
        color,
      )}
    >
      {label}
    </span>
  )
}

function fmtPct(v: number | null | undefined, mult = 1): string {
  if (v == null || Number.isNaN(v)) return '—'
  return (v * mult).toFixed(2) + '%'
}

function fmtNum(v: number | null | undefined, digits = 2): string {
  if (v == null || Number.isNaN(v)) return '—'
  return v.toFixed(digits)
}

function fmtYi(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return '—'
  return v.toFixed(2) + '亿'
}

function changeClass(v: number | null | undefined): string {
  if (v == null) return ''
  if (v > 0) return 'text-profit'
  if (v < 0) return 'text-loss'
  return ''
}

function moneyClass(v: number | null | undefined): string {
  if (v == null) return ''
  if (v > 0) return 'text-profit'
  if (v < 0) return 'text-loss'
  return ''
}

export default function LowPositionPage() {
  const [selectedDate, setSelectedDate] = useState<Date | undefined>(new Date())
  const [dateRange, setDateRange] = useState<{ from?: Date; to?: Date }>({})
  const [selectedSector, setSelectedSector] = useState<string | null>(null)

  const dateStr = selectedDate ? format(selectedDate, 'yyyy-MM-dd') : ''

  const { data, isLoading, isError, refetch, isFetching } =
    useGetLowPositionRadarApiV1AlphaRadarLowPositionGet({
      date: dateStr || undefined,
    })

  const sectors = useMemo(() => {
    const list = data?.sectors ?? []
    return [...list].sort((a, b) => b.heat_score - a.heat_score)
  }, [data?.sectors])

  const stocks = useMemo(() => {
    const list = data?.stocks ?? []
    const filtered = selectedSector
      ? list.filter((s) => s.industry_l1 === selectedSector)
      : list
    return [...filtered].sort((a, b) => b.radar_score - a.radar_score)
  }, [data?.stocks, selectedSector])

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <div className="shrink-0">
          <h1 className="text-2xl font-bold tracking-tight">主线低位雷达</h1>
          <p className="text-xs text-muted-foreground">板块热度与低位启动股扫描</p>
        </div>
        <div className="flex-1">
          <TimeController
            mode="snapshot"
            onModeChange={() => {}}
            selectedDate={selectedDate}
            onDateChange={setSelectedDate}
            dateRange={dateRange}
            onDateRangeChange={setDateRange}
            showModeToggle={false}
            defaultActiveDate={data?.date ?? undefined}
          />
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => refetch()}
          disabled={isFetching}
          className="shrink-0"
        >
          <RefreshCw className={cn('h-4 w-4 mr-1', isFetching && 'animate-spin')} />
          刷新
        </Button>
      </div>

      {isError ? (
        <Card>
          <CardContent className="py-12 text-center space-y-3">
            <p className="text-sm text-muted-foreground">数据加载失败</p>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
              <RefreshCw className="h-4 w-4 mr-1" />
              重试
            </Button>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-12 gap-4">
          {/* Sector pane */}
          <Card className="col-span-12 lg:col-span-5">
            <CardHeader className="pb-3 pt-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-base flex items-center gap-2">
                  <TrendingUp className="h-4 w-4" />
                  热点板块
                </CardTitle>
                <span className="text-xs text-muted-foreground">
                  {sectors.length} 个板块
                </span>
              </div>
            </CardHeader>
            <CardContent className="pt-0">
              {isLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 8 }).map((_, i) => (
                    <Skeleton key={i} className="h-9 w-full" />
                  ))}
                </div>
              ) : sectors.length === 0 ? (
                <div className="py-12 text-center text-sm text-muted-foreground">
                  暂无板块数据
                </div>
              ) : (
                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-muted/40 hover:bg-muted/40">
                        <TableHead className="h-8 text-xs whitespace-nowrap">板块</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">热度</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">涨停</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">中位涨幅</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">主力(亿)</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">最高板</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">晋级率</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sectors.map((s) => {
                        const active = selectedSector === s.name
                        return (
                          <TableRow
                            key={s.name}
                            data-active={active}
                            onClick={() =>
                              setSelectedSector(active ? null : s.name)
                            }
                            className={cn(
                              'cursor-pointer transition-colors',
                              active && 'bg-primary/10 hover:bg-primary/15',
                            )}
                          >
                            <TableCell className="py-1.5 font-medium text-sm whitespace-nowrap">
                              {s.name}
                            </TableCell>
                            <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                              {fmtNum(s.heat_score, 1)}
                            </TableCell>
                            <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                              {s.limit_up_count}
                            </TableCell>
                            <TableCell
                              className={cn(
                                'py-1.5 text-right text-sm font-mono whitespace-nowrap',
                                changeClass(s.median_change_pct),
                              )}
                            >
                              {fmtPct(s.median_change_pct)}
                            </TableCell>
                            <TableCell
                              className={cn(
                                'py-1.5 text-right text-sm font-mono whitespace-nowrap',
                                moneyClass(s.main_inflow_yi),
                              )}
                            >
                              {fmtYi(s.main_inflow_yi)}
                            </TableCell>
                            <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                              {s.max_board_height}
                            </TableCell>
                            <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                              {fmtPct(s.promotion_rate, 100)}
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Stock pane */}
          <Card className="col-span-12 lg:col-span-7">
            <CardHeader className="pb-3 pt-3">
              <div className="flex items-center justify-between gap-2">
                <CardTitle className="text-base flex items-center gap-2">
                  雷达股票
                  {selectedSector && (
                    <span className="text-xs font-normal text-muted-foreground">
                      · 已筛选「{selectedSector}」
                    </span>
                  )}
                </CardTitle>
                <div className="flex items-center gap-2">
                  {selectedSector && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => setSelectedSector(null)}
                    >
                      <X className="h-3 w-3 mr-1" />
                      全部
                    </Button>
                  )}
                  <span className="text-xs text-muted-foreground">
                    {stocks.length} 只
                  </span>
                </div>
              </div>
            </CardHeader>
            <CardContent className="pt-0">
              {isLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 10 }).map((_, i) => (
                    <Skeleton key={i} className="h-9 w-full" />
                  ))}
                </div>
              ) : stocks.length === 0 ? (
                <div className="py-12 text-center text-sm text-muted-foreground">
                  {selectedSector ? '该板块暂无雷达股票' : '暂无雷达股票'}
                </div>
              ) : (
                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-muted/40 hover:bg-muted/40">
                        <TableHead className="h-8 text-xs whitespace-nowrap">代码</TableHead>
                        <TableHead className="h-8 text-xs whitespace-nowrap">名称</TableHead>
                        <TableHead className="h-8 text-xs whitespace-nowrap">行业</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">收盘</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">涨幅</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">雷达分</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">相对强度</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">主力分位</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">均线</TableHead>
                        <TableHead className="h-8 text-xs text-right whitespace-nowrap">量能</TableHead>
                        <TableHead className="h-8 text-xs whitespace-nowrap">标签</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {stocks.map((s) => (
                        <TableRow key={s.code}>
                          <TableCell className="py-1.5 font-mono text-xs whitespace-nowrap">
                            {s.code}
                          </TableCell>
                          <TableCell className="py-1.5 font-medium text-sm whitespace-nowrap">
                            {s.name}
                          </TableCell>
                          <TableCell className="py-1.5 text-xs text-muted-foreground whitespace-nowrap">
                            {s.industry_l1 ?? '—'}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                            {fmtNum(s.close, 2)}
                          </TableCell>
                          <TableCell
                            className={cn(
                              'py-1.5 text-right text-sm font-mono whitespace-nowrap',
                              changeClass(s.change_pct),
                            )}
                          >
                            {fmtPct(s.change_pct)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono font-semibold whitespace-nowrap">
                            {fmtNum(s.radar_score, 1)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                            {fmtNum(s.rs_vs_sector, 1)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                            {fmtNum(s.elg_net_percentile, 1)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                            {fmtNum(s.ma_alignment_score, 1)}
                          </TableCell>
                          <TableCell className="py-1.5 text-right text-sm font-mono whitespace-nowrap">
                            {fmtNum(s.volume_buildup_quality, 1)}
                          </TableCell>
                          <TableCell className="py-1.5 min-w-[300px]">
                            <div className="flex flex-wrap gap-1">
                              {s.tags?.map((t) => <TagChip key={t} label={t} />)}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  )
}
