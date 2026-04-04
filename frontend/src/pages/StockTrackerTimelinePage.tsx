import { useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, RefreshCw, Calendar } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  useGetTimelineApiV1StockTrackerTracksTsCodeTimelineGet,
  useSyncDailyApiV1StockTrackerTracksTsCodeSyncDailyPost,
  getGetTimelineApiV1StockTrackerTracksTsCodeTimelineGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { TimelineDayRead } from '@/api/generated/schemas'

const formatDateFull = (dateStr: string) => {
  const d = new Date(dateStr)
  const weekdays = ['日', '一', '二', '三', '四', '五', '六']
  return `${d.getMonth() + 1}月${d.getDate()}日 周${weekdays[d.getDay()]}`
}

const getPctChgColor = (pctChg: number | null | undefined) => {
  if (pctChg === null || pctChg === undefined) return 'text-muted-foreground'
  if (pctChg > 0) return 'text-profit'
  if (pctChg < 0) return 'text-loss'
  return 'text-muted-foreground'
}

const formatPctChg = (pctChg: number | null | undefined) => {
  if (pctChg === null || pctChg === undefined) return '-'
  const sign = pctChg > 0 ? '+' : ''
  return `${sign}${pctChg.toFixed(2)}%`
}

const patternColors: Record<string, string> = {
  甲: 'bg-green-500/20 text-green-400',
  乙: 'bg-red-500/20 text-red-400',
  丙: 'bg-yellow-500/20 text-yellow-400',
  丁: 'bg-orange-500/20 text-orange-400',
  戊: 'bg-blue-500/20 text-blue-400',
  己: 'bg-purple-500/20 text-purple-400',
  庚: 'bg-emerald-500/20 text-emerald-400',
  辛: 'bg-rose-500/20 text-rose-400',
  壬: 'bg-cyan-500/20 text-cyan-400',
  癸: 'bg-indigo-500/20 text-indigo-400',
}

function FilledDayCard({
  day,
  onClick,
}: {
  day: TimelineDayRead
  onClick: () => void
}) {
  const pctChg = day.pct_chg ?? null
  return (
    <Card
      className="cursor-pointer hover:bg-muted/50 transition-colors"
      onClick={onClick}
    >
      <CardContent className="p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium">
            {formatDateFull(day.trade_date)}
          </span>
          {day.pattern && (
            <Badge
              variant="secondary"
              className={`text-xs font-bold ${patternColors[day.pattern] || ''}`}
            >
              {day.pattern}
            </Badge>
          )}
        </div>

        <div className="mb-2">
          <span
            className={`text-xl font-bold font-mono ${getPctChgColor(pctChg)}`}
          >
            {formatPctChg(pctChg)}
          </span>
        </div>

        <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
          <span>
            开{' '}
            <span className="font-mono text-foreground">
              {day.open != null ? day.open.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            高{' '}
            <span className="font-mono text-foreground">
              {day.high != null ? day.high.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            低{' '}
            <span className="font-mono text-foreground">
              {day.low != null ? day.low.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            收{' '}
            <span className="font-mono text-foreground">
              {day.close != null ? day.close.toFixed(2) : '-'}
            </span>
          </span>
        </div>

        {day.mood && (
          <div className="mt-2 text-xs text-muted-foreground">{day.mood}</div>
        )}
      </CardContent>
    </Card>
  )
}

function BlankDayCard({ day }: { day: TimelineDayRead }) {
  const pctChg = day.pct_chg ?? null
  return (
    <Card className="border-dashed opacity-60">
      <CardContent className="p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm font-medium text-muted-foreground">
            {formatDateFull(day.trade_date)}
          </span>
        </div>

        <div className="mb-2">
          <span
            className={`text-xl font-bold font-mono ${getPctChgColor(pctChg)}`}
          >
            {formatPctChg(pctChg)}
          </span>
        </div>

        <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-muted-foreground/60">
          <span>
            开{' '}
            <span className="font-mono">
              {day.open != null ? day.open.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            高{' '}
            <span className="font-mono">
              {day.high != null ? day.high.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            低{' '}
            <span className="font-mono">
              {day.low != null ? day.low.toFixed(2) : '-'}
            </span>
          </span>
          <span>
            收{' '}
            <span className="font-mono">
              {day.close != null ? day.close.toFixed(2) : '-'}
            </span>
          </span>
        </div>

        <div className="mt-2 text-xs text-muted-foreground/40 italic">
          未记录
        </div>
      </CardContent>
    </Card>
  )
}

export default function StockTrackerTimelinePage() {
  const { tsCode } = useParams<{ tsCode: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { data: timeline, isLoading } =
    useGetTimelineApiV1StockTrackerTracksTsCodeTimelineGet(tsCode || '', {
      days: 20,
    })

  const syncMutation =
    useSyncDailyApiV1StockTrackerTracksTsCodeSyncDailyPost({
      mutation: {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey:
              getGetTimelineApiV1StockTrackerTracksTsCodeTimelineGetQueryKey(
                tsCode || ''
              ),
          })
        },
      },
    })

  const handleSync = () => {
    if (!tsCode) return
    syncMutation.mutate({ tsCode, params: { days: 60 } })
  }

  const items = timeline || []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => navigate('/stock-tracker')}
          >
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Calendar className="h-6 w-6 text-primary" />
              <span className="font-mono">{tsCode}</span>
              <span className="text-lg font-normal text-muted-foreground">
                每日时间线
              </span>
            </h1>
            <p className="text-muted-foreground text-sm">
              最近 20 个交易日走势记录
            </p>
          </div>
        </div>
        <Button
          variant="outline"
          onClick={handleSync}
          disabled={syncMutation.isPending}
        >
          <RefreshCw
            className={`h-4 w-4 mr-1 ${syncMutation.isPending ? 'animate-spin' : ''}`}
          />
          {syncMutation.isPending ? '同步中...' : '同步数据'}
        </Button>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
          {Array.from({ length: 10 }).map((_, i) => (
            <Card key={i}>
              <CardContent className="p-4">
                <Skeleton className="h-4 w-20 mb-2" />
                <Skeleton className="h-8 w-full mb-2" />
                <Skeleton className="h-4 w-16" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : items.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            <div className="flex flex-col items-center gap-3">
              <Calendar className="h-10 w-10 opacity-40" />
              <p className="text-lg">暂无交易日数据</p>
              <p className="text-sm">点击「同步数据」从市场日线导入</p>
            </div>
          </CardContent>
        </Card>
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
          {items.map((day: TimelineDayRead) =>
            day.has_entry && day.entry_id ? (
              <FilledDayCard
                key={day.trade_date}
                day={day}
                onClick={() =>
                  navigate(`/stock-tracker/${tsCode}/${day.entry_id}`)
                }
              />
            ) : (
              <BlankDayCard key={day.trade_date} day={day} />
            )
          )}
        </div>
      )}
    </div>
  )
}
