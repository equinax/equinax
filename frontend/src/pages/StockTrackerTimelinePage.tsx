import { useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, RefreshCw, Calendar } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import {
  useGetTimelineApiV1StockTrackerTracksTsCodeTimelineGet,
  useSyncDailyApiV1StockTrackerTracksTsCodeSyncDailyPost,
  getGetTimelineApiV1StockTrackerTracksTsCodeTimelineGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { TimelineDayRead } from '@/api/generated/schemas'
import DayChart from '@/components/stock-tracker/DayChart'

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
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-2">
          {Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="border rounded-lg">
              <Skeleton className="w-full h-[180px] rounded-lg" />
            </div>
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
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-2">
          {items.map((day: TimelineDayRead) => {
            const hasEntry = day.has_entry && day.entry_id
            const scores = day.scores_summary
              ? (day.scores_summary as { market: number; sector: number; stock: number })
              : null
            const keyPoints = day.key_points
              ? (day.key_points as Record<string, { time: string; price: number }>)
              : null

            return hasEntry ? (
              <div
                key={day.trade_date}
                className="border rounded-lg hover:border-primary/50 transition-colors cursor-pointer"
                onClick={() =>
                  navigate(`/stock-tracker/${tsCode}/${day.entry_id}`)
                }
              >
                <DayChart
                  mode="thumbnail"
                  tradeDate={day.trade_date}
                  open={day.open ?? null}
                  high={day.high ?? null}
                  low={day.low ?? null}
                  close={day.close ?? null}
                  preClose={day.pre_close ?? null}
                  pctChg={day.pct_chg ?? null}
                  keyPoints={keyPoints}
                  scores={scores}
                  pattern={day.pattern ?? null}
                  notes={day.notes ?? null}
                  width={220}
                  height={180}
                />
              </div>
            ) : (
              <div
                key={day.trade_date}
                className="border border-dashed rounded-lg opacity-60"
              >
                <DayChart
                  mode="thumbnail"
                  tradeDate={day.trade_date}
                  open={day.open ?? null}
                  high={day.high ?? null}
                  low={day.low ?? null}
                  close={day.close ?? null}
                  preClose={day.pre_close ?? null}
                  pctChg={day.pct_chg ?? null}
                  width={220}
                  height={180}
                />
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
