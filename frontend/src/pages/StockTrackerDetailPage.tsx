import { useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, Pencil } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import {
  useGetEntryApiV1StockTrackerEntriesEntryIdGet,
  useUpdateEntryApiV1StockTrackerEntriesEntryIdPatch,
  getGetEntryApiV1StockTrackerEntriesEntryIdGetQueryKey,
  useGetSketchApiV1StockTrackerEntriesEntryIdSketchGet,
  useUpsertSketchApiV1StockTrackerEntriesEntryIdSketchPut,
  getGetSketchApiV1StockTrackerEntriesEntryIdSketchGetQueryKey,
  useListTracksApiV1StockTrackerTracksGet,
  useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet,
  useGetMinuteDataApiV1StockTrackerEntriesEntryIdMinuteDataGet,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { MinuteDataResponse } from '@/types/minute-data'
import { getSectionSum } from '@/lib/score-utils'
import DayChart from '@/components/stock-tracker/DayChart'
import CompactScoring from '@/components/stock-tracker/CompactScoring'
import OperationsPanel from '@/components/stock-tracker/OperationsPanel'

const formatDateFull = (dateStr: string) => {
  const d = new Date(dateStr)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

const getPctChgColor = (pctChg: number | null) => {
  if (pctChg === null || pctChg === undefined) return 'text-muted-foreground'
  if (pctChg > 0) return 'text-profit'
  if (pctChg < 0) return 'text-loss'
  return 'text-muted-foreground'
}

const formatPctChg = (pctChg: number | null) => {
  if (pctChg === null || pctChg === undefined) return '-'
  const sign = pctChg > 0 ? '+' : ''
  return `${sign}${pctChg.toFixed(2)}%`
}

export default function StockTrackerDetailPage() {
  const { tsCode, entryId } = useParams<{
    tsCode: string
    entryId: string
  }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { data: entry, isLoading } =
    useGetEntryApiV1StockTrackerEntriesEntryIdGet(entryId || '')

  const { data: tracks } = useListTracksApiV1StockTrackerTracksGet()
  const stockName = tracks?.find((t) => t.ts_code === tsCode)?.stock_name

  const { data: scoreData } =
    useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet(entryId || '', {
      query: { enabled: !!entryId },
    })
  const scores = scoreData?.scores
    ? {
      market: getSectionSum(scoreData.scores.market as Record<string, number>),
      sector: getSectionSum(scoreData.scores.sector as Record<string, number>),
      stock: getSectionSum(scoreData.scores.stock as Record<string, number>),
    }
    : null

  const { data: minuteRawData, isLoading: minuteLoading, isError: minuteError } =
    useGetMinuteDataApiV1StockTrackerEntriesEntryIdMinuteDataGet(
      entryId || '',
      { freq: '5' },
      { query: { enabled: !!entryId } }
    )
  const minuteData = minuteRawData as MinuteDataResponse | undefined
  const minuteCandles = minuteData?.candles ?? null

  const [showMinuteLine, setShowMinuteLine] = useState(true)
  const [showOhlcPoints, setShowOhlcPoints] = useState(true)

  const [editingNotes, setEditingNotes] = useState(false)
  const [notesValue, setNotesValue] = useState('')

  const { data: sketchData } =
    useGetSketchApiV1StockTrackerEntriesEntryIdSketchGet(entryId || '', {
      query: { enabled: !!entryId },
    })

  const sketchMutation =
    useUpsertSketchApiV1StockTrackerEntriesEntryIdSketchPut({
      mutation: {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey:
              getGetSketchApiV1StockTrackerEntriesEntryIdSketchGetQueryKey(
                entryId || ''
              ),
          })
          queryClient.invalidateQueries({
            queryKey:
              getGetEntryApiV1StockTrackerEntriesEntryIdGetQueryKey(
                entryId || ''
              ),
          })
        },
      },
    })

  const handleSketchSave = (
    keyPoints: Record<string, { time: string; price: number }>
  ) => {
    if (!entryId) return
    sketchMutation.mutate({
      entryId,
      data: { key_points: keyPoints },
    })
  }

  const updateMutation = useUpdateEntryApiV1StockTrackerEntriesEntryIdPatch({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: getGetEntryApiV1StockTrackerEntriesEntryIdGetQueryKey(
            entryId || ''
          ),
        })
        setEditingNotes(false)
      },
    },
  })

  const startEditing = () => {
    setNotesValue((entry?.notes as string) || '')
    setEditingNotes(true)
  }

  const handleSaveNotes = () => {
    if (!entryId) return
    updateMutation.mutate({
      entryId,
      data: {
        notes: notesValue || undefined,
      },
    })
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-6 w-96" />
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-4">
          <div className="space-y-3">
            <Skeleton className="h-[440px]" />
            <Skeleton className="h-10" />
            <Skeleton className="h-24" />
          </div>
          <Skeleton className="h-[440px]" />
        </div>
      </div>
    )
  }

  if (!entry) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        未找到该条目
      </div>
    )
  }

  const pctChg = entry.pct_chg as number | null

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4 flex-wrap">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => navigate(`/stock-tracker/${tsCode}`)}
        >
          <ArrowLeft className="h-5 w-5" />
        </Button>
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-baseline gap-1.5">
            <h1 className="text-2xl font-bold truncate max-w-[200px]">
              {stockName || tsCode}
            </h1>
            {stockName && (
              <span className="text-xs text-muted-foreground font-mono">
                {tsCode}
              </span>
            )}
          </div>
          <span className="text-lg text-muted-foreground">
            {formatDateFull(entry.trade_date)}
          </span>
          <Badge
            variant="secondary"
            className={`font-mono font-bold ${getPctChgColor(pctChg)}`}
          >
            {formatPctChg(pctChg)}
          </Badge>
          {sketchData?.auto_pattern && (
            <Badge variant="outline" className="font-bold">
              {sketchData.auto_pattern as string}
            </Badge>
          )}
          {entry.pattern && !sketchData?.auto_pattern && (
            <Badge variant="outline" className="font-bold">
              {entry.pattern as string}
            </Badge>
          )}
          <span className="text-xs text-muted-foreground font-mono">
            昨收 {entry.pre_close != null ? (entry.pre_close as number).toFixed(2) : '-'}
            {' | '}开 {entry.open != null ? (entry.open as number).toFixed(2) : '-'}
            {' | '}高{' '}
            <span
              className={
                entry.high != null &&
                  entry.pre_close != null &&
                  (entry.high as number) > (entry.pre_close as number)
                  ? 'text-red-500'
                  : ''
              }
            >
              {entry.high != null ? (entry.high as number).toFixed(2) : '-'}
            </span>
            {' | '}低{' '}
            <span
              className={
                entry.low != null &&
                  entry.pre_close != null &&
                  (entry.low as number) < (entry.pre_close as number)
                  ? 'text-green-500'
                  : ''
              }
            >
              {entry.low != null ? (entry.low as number).toFixed(2) : '-'}
            </span>
            {' | '}收 {entry.close != null ? (entry.close as number).toFixed(2) : '-'}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,720px)_1fr] gap-4 items-start">
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-xs">
            <button
              className={`px-2 py-0.5 rounded text-xs transition-colors ${showMinuteLine ? 'bg-blue-500/20 text-blue-400' : 'bg-muted text-muted-foreground'}`}
              onClick={() => setShowMinuteLine(!showMinuteLine)}
            >
              分时
            </button>
            <button
              className={`px-2 py-0.5 rounded text-xs transition-colors ${showOhlcPoints ? 'bg-amber-500/20 text-amber-400' : 'bg-muted text-muted-foreground'}`}
              onClick={() => setShowOhlcPoints(!showOhlcPoints)}
            >
              OHLC
            </button>
          </div>
          <DayChart
            mode="detail"
            tradeDate={entry.trade_date}
            open={entry.open as number | null}
            high={entry.high as number | null}
            low={entry.low as number | null}
            close={entry.close as number | null}
            preClose={entry.pre_close as number | null}
            pctChg={entry.pct_chg as number | null}
            keyPoints={
              (sketchData?.key_points as Record<
                string,
                { time: string; price: number }
              > | undefined) ?? null
            }
            scores={scores}
            detailedScores={
              (scoreData?.scores as Record<string, Record<string, number>> | undefined) ?? null
            }
            autoPattern={
              (sketchData?.auto_pattern as string | undefined) ?? null
            }
            onSave={handleSketchSave}
            isSaving={sketchMutation.isPending}
            minuteCandles={minuteCandles}
            showMinuteLine={showMinuteLine}
            showOhlcPoints={showOhlcPoints}
            minuteLoading={minuteLoading}
            minuteError={minuteError}
          />

          <CompactScoring entryId={entryId || ''} />

          <div>
            {editingNotes ? (
              <div className="space-y-2">
                <Textarea
                  value={notesValue}
                  onChange={(e) => setNotesValue(e.target.value)}
                  placeholder="今日操作思路、复盘要点..."
                  rows={4}
                />
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    onClick={handleSaveNotes}
                    disabled={updateMutation.isPending}
                  >
                    {updateMutation.isPending ? '保存中...' : '保存'}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setEditingNotes(false)}
                  >
                    取消
                  </Button>
                </div>
              </div>
            ) : (
              <div
                className="group cursor-pointer"
                onClick={startEditing}
              >
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-muted-foreground font-medium">备注</span>
                  <Pencil className="h-3 w-3 text-muted-foreground/0 group-hover:text-muted-foreground transition-colors" />
                </div>
                <p className="text-sm text-muted-foreground whitespace-pre-wrap mt-0.5">
                  {(entry.notes as string) || '暂无备注'}
                </p>
              </div>
            )}
          </div>
        </div>

        <div className="border rounded-lg p-4">
          <OperationsPanel entryId={entryId || ''} />
        </div>
      </div>
    </div>
  )
}
