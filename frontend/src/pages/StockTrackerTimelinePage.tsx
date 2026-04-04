import { useState, useMemo, useRef, useCallback, useEffect, useLayoutEffect } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, RefreshCw, Calendar, Loader2 } from 'lucide-react'
import { useQueryClient, useInfiniteQuery } from '@tanstack/react-query'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import {
  getTimelineApiV1StockTrackerTracksTsCodeTimelineGet,
  useSyncDailyApiV1StockTrackerTracksTsCodeSyncDailyPost,
  getGetTimelineApiV1StockTrackerTracksTsCodeTimelineGetQueryKey,
  useCreateEntryForDateApiV1StockTrackerTracksTsCodeEntriesCreateForDatePost,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { TimelineDayRead } from '@/api/generated/schemas'
import DayChart from '@/components/stock-tracker/DayChart'

const COLS = 10
const PAGE_SIZE = 20
const SYNC_DAYS = 20

function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size))
  }
  return chunks
}

function formatDateShort(dateStr: string): string {
  const cleaned = dateStr.replace(/-/g, '')
  if (cleaned.length === 8) {
    return `${cleaned.slice(4, 6)}-${cleaned.slice(6, 8)}`
  }
  return dateStr
}

export default function StockTrackerTimelinePage() {
  const { tsCode } = useParams<{ tsCode: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const sentinelRef = useRef<HTMLDivElement>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const prevScrollHeightRef = useRef<number>(0)
  const [creatingDate, setCreatingDate] = useState<string | null>(null)

  const {
    data,
    isLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['stock-tracker-timeline', tsCode],
    queryFn: ({ pageParam }) =>
      getTimelineApiV1StockTrackerTracksTsCodeTimelineGet(tsCode || '', {
        days: PAGE_SIZE,
        before: pageParam ?? undefined,
      }),
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => {
      if (!lastPage || lastPage.length < PAGE_SIZE) return undefined
      const oldest = lastPage[lastPage.length - 1]
      return oldest?.trade_date ?? undefined
    },
    enabled: !!tsCode,
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
          queryClient.invalidateQueries({
            queryKey: ['stock-tracker-timeline', tsCode],
          })
        },
      },
    })

  const createEntryMutation =
    useCreateEntryForDateApiV1StockTrackerTracksTsCodeEntriesCreateForDatePost({
      mutation: {
        onSuccess: (entry) => {
          queryClient.invalidateQueries({
            queryKey: ['stock-tracker-timeline', tsCode],
          })
          navigate(`/stock-tracker/${tsCode}/${entry.id}`)
        },
        onSettled: () => {
          setCreatingDate(null)
        },
      },
    })

  const handleSync = () => {
    if (!tsCode) return
    syncMutation.mutate({
      tsCode,
      params: {
        days: syncInfo.before ? SYNC_DAYS : 60,
        before: syncInfo.before ?? undefined,
      },
    })
  }

  const handleCreateEntry = (tradeDate: string) => {
    if (!tsCode || creatingDate) return
    setCreatingDate(tradeDate)
    createEntryMutation.mutate({
      tsCode,
      params: { trade_date: tradeDate },
    })
  }

  const handleObserver = useCallback(
    (entries: IntersectionObserverEntry[]) => {
      const [entry] = entries
      if (entry.isIntersecting && hasNextPage && !isFetchingNextPage) {
        const container = scrollContainerRef.current
        if (container) {
          prevScrollHeightRef.current = container.scrollHeight
        }
        fetchNextPage()
      }
    },
    [fetchNextPage, hasNextPage, isFetchingNextPage],
  )

  useEffect(() => {
    const el = sentinelRef.current
    if (!el) return
    const observer = new IntersectionObserver(handleObserver, {
      root: scrollContainerRef.current,
      rootMargin: '200px',
    })
    observer.observe(el)
    return () => observer.disconnect()
  }, [handleObserver])

  useLayoutEffect(() => {
    const container = scrollContainerRef.current
    if (!container || !prevScrollHeightRef.current) return
    const newScrollHeight = container.scrollHeight
    const diff = newScrollHeight - prevScrollHeightRef.current
    if (diff > 0) {
      container.scrollTop += diff
    }
    prevScrollHeightRef.current = 0
  }, [data])

  const items = useMemo(() => {
    if (!data?.pages) return []
    const all = data.pages.flatMap((page) => page)
    return [...all].reverse()
  }, [data])

  const rows = useMemo(() => chunkArray(items, COLS), [items])

  const syncInfo = useMemo(() => {
    if (!items.length) return { label: `最近${SYNC_DAYS}日`, before: undefined }
    const noEntryItems = items.filter((d) => !d.has_entry)
    if (noEntryItems.length > 0) {
      const oldest = noEntryItems[0].trade_date
      const newest = noEntryItems[noEntryItems.length - 1].trade_date
      return {
        label: `${formatDateShort(oldest)} ~ ${formatDateShort(newest)}`,
        before: undefined,
      }
    }
    const oldestDate = items[0].trade_date
    return {
      label: `${formatDateShort(oldestDate)} 之前${SYNC_DAYS}日`,
      before: oldestDate,
    }
  }, [items])

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="sticky top-0 z-10 bg-background pb-4">
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
                共 {items.length} 个交易日
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
            {syncMutation.isPending
              ? '同步中...'
              : `同步数据 (${syncInfo.label})`}
          </Button>
        </div>
      </div>

      <div ref={scrollContainerRef} className="flex-1 overflow-y-auto min-h-0">
        {isLoading ? (
          <div className="flex">
            {Array.from({ length: COLS }).map((_, i) => (
              <div
                key={i}
                className="flex-1 min-w-0 border border-border -ml-px first:ml-0"
              >
                <Skeleton className="w-full h-[140px]" />
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
          <div className="flex flex-col gap-y-3">
            <div ref={sentinelRef} className="h-1" />
            {isFetchingNextPage && (
              <div className="flex justify-center py-2">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            )}
            {rows.map((row, rowIdx) => (
              <div key={rowIdx} className="flex">
                {row.map((day: TimelineDayRead) => {
                  const hasEntry = day.has_entry && day.entry_id
                  const scores = day.scores_summary
                    ? (day.scores_summary as {
                        market: number
                        sector: number
                        stock: number
                      })
                    : null
                  const keyPoints = day.key_points
                    ? (day.key_points as Record<
                        string,
                        { time: string; price: number }
                      >)
                    : null
                  const isCreating = creatingDate === day.trade_date

                  return hasEntry ? (
                    <div
                      key={day.trade_date}
                      className="flex-1 min-w-0 border border-border -ml-px first:ml-0 hover:border-primary/50 hover:z-10 transition-colors cursor-pointer"
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
                      />
                    </div>
                  ) : (
                    <div
                      key={day.trade_date}
                      className={`flex-1 min-w-0 border border-dashed border-border -ml-px first:ml-0 cursor-pointer hover:border-primary/50 hover:z-10 transition-colors ${isCreating ? 'opacity-50' : 'opacity-60'}`}
                      onClick={() => handleCreateEntry(day.trade_date)}
                    >
                      {isCreating ? (
                        <div className="w-full h-[140px] flex items-center justify-center">
                          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                        </div>
                      ) : (
                        <DayChart
                          mode="thumbnail"
                          tradeDate={day.trade_date}
                          open={day.open ?? null}
                          high={day.high ?? null}
                          low={day.low ?? null}
                          close={day.close ?? null}
                          preClose={day.pre_close ?? null}
                          pctChg={day.pct_chg ?? null}
                        />
                      )}
                    </div>
                  )
                })}
                {row.length < COLS &&
                  Array.from({ length: COLS - row.length }).map((_, i) => (
                    <div
                      key={`empty-${i}`}
                      className="flex-1 min-w-0 border border-dashed border-border -ml-px opacity-30"
                    />
                  ))}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
