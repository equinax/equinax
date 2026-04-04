import { useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { ArrowLeft, Pencil } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  useGetEntryApiV1StockTrackerEntriesEntryIdGet,
  useUpdateEntryApiV1StockTrackerEntriesEntryIdPatch,
  getGetEntryApiV1StockTrackerEntriesEntryIdGetQueryKey,
  useGetSketchApiV1StockTrackerEntriesEntryIdSketchGet,
  useUpsertSketchApiV1StockTrackerEntriesEntryIdSketchPut,
  getGetSketchApiV1StockTrackerEntriesEntryIdSketchGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import DayChart from '@/components/stock-tracker/DayChart'
import ScoringPanel from '@/components/stock-tracker/ScoringPanel'
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

  const [editingNotes, setEditingNotes] = useState(false)
  const [notesValue, setNotesValue] = useState('')
  const [moodValue, setMoodValue] = useState('')

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
          // Also refresh entry to update pattern
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
    setMoodValue((entry?.mood as string) || '')
    setEditingNotes(true)
  }

  const handleSaveNotes = () => {
    if (!entryId) return
    updateMutation.mutate({
      entryId,
      data: {
        notes: notesValue || undefined,
        mood: moodValue || undefined,
      },
    })
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-6 w-96" />
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-4">
          <Skeleton className="h-[440px]" />
          <Skeleton className="h-[440px]" />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Skeleton className="h-64" />
          <Skeleton className="h-64" />
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
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => navigate(`/stock-tracker/${tsCode}`)}
        >
          <ArrowLeft className="h-5 w-5" />
        </Button>
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold font-mono">{tsCode}</h1>
          <span className="text-lg text-muted-foreground">
            {formatDateFull(entry.trade_date)}
          </span>
          <Badge
            variant="secondary"
            className={`font-mono font-bold ${getPctChgColor(pctChg)}`}
          >
            {formatPctChg(pctChg)}
          </Badge>
          {entry.pattern && (
            <Badge variant="outline" className="font-bold">
              {entry.pattern as string}
            </Badge>
          )}
        </div>
      </div>

      {/* OHLC Summary Bar */}
      <div className="flex items-center gap-6 text-sm border-b pb-3">
        <span className="text-muted-foreground">
          昨收{' '}
          <span className="font-mono text-foreground">
            {entry.pre_close != null
              ? (entry.pre_close as number).toFixed(2)
              : '-'}
          </span>
        </span>
        <span className="text-muted-foreground">
          开{' '}
          <span className="font-mono text-foreground">
            {entry.open != null ? (entry.open as number).toFixed(2) : '-'}
          </span>
        </span>
        <span className="text-muted-foreground">
          高{' '}
          <span className="font-mono text-foreground">
            {entry.high != null ? (entry.high as number).toFixed(2) : '-'}
          </span>
        </span>
        <span className="text-muted-foreground">
          低{' '}
          <span className="font-mono text-foreground">
            {entry.low != null ? (entry.low as number).toFixed(2) : '-'}
          </span>
        </span>
        <span className="text-muted-foreground">
          收{' '}
          <span className="font-mono text-foreground">
            {entry.close != null
              ? (entry.close as number).toFixed(2)
              : '-'}
          </span>
        </span>
      </div>

      {/* Upper section: DayChart + ScoringPanel */}
      <div className="grid grid-cols-1 lg:grid-cols-[1fr_340px] gap-4">
        {/* Left: DayChart in detail mode */}
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
          scores={null}
          autoPattern={
            (sketchData?.auto_pattern as string | undefined) ?? null
          }
          onSave={handleSketchSave}
          isSaving={sketchMutation.isPending}
        />

        {/* Right: ScoringPanel */}
        <div className="border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">评分</h3>
          <ScoringPanel entryId={entryId || ''} />
        </div>
      </div>

      {/* Lower section: OperationsPanel + Notes */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left: OperationsPanel */}
        <div className="border rounded-lg p-4">
          <h3 className="text-sm font-medium mb-2">操作记录</h3>
          <OperationsPanel entryId={entryId || ''} />
        </div>

        {/* Right: Notes */}
        <div className="border rounded-lg p-4">
          <div className="space-y-4">
            {/* Current info */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">心态</span>
                <span className="text-sm text-muted-foreground">
                  {(entry.mood as string) || '未记录'}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">天干</span>
                <span className="text-sm">
                  {(entry.pattern as string) || '未识别'}
                </span>
              </div>
            </div>

            <div className="border-t pt-4">
              {editingNotes ? (
                <div className="space-y-3">
                  <div className="grid gap-2">
                    <Label>心态</Label>
                    <Input
                      value={moodValue}
                      onChange={(e) => setMoodValue(e.target.value)}
                      placeholder="乐观 / 焦虑 / 冷静 / ..."
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label>备注</Label>
                    <Textarea
                      value={notesValue}
                      onChange={(e) => setNotesValue(e.target.value)}
                      placeholder="今日操作思路、复盘要点..."
                      rows={6}
                    />
                  </div>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      onClick={handleSaveNotes}
                      disabled={updateMutation.isPending}
                    >
                      {updateMutation.isPending
                        ? '保存中...'
                        : '保存'}
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
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">备注</span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={startEditing}
                    >
                      <Pencil className="h-3 w-3 mr-1" />
                      编辑
                    </Button>
                  </div>
                  <p className="text-sm text-muted-foreground whitespace-pre-wrap">
                    {(entry.notes as string) || '暂无备注'}
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
