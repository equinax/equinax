import { useState, useMemo, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import { Slider } from '@/components/ui/slider'
import { Plus, Pencil, Trash2, X, Check, ChevronDown, ChevronUp } from 'lucide-react'
import {
  useListOperationsApiV1StockTrackerEntriesEntryIdOperationsGet,
  useCreateOperationApiV1StockTrackerEntriesEntryIdOperationsPost,
  useUpdateOperationApiV1StockTrackerOperationsOpIdPatch,
  useDeleteOperationApiV1StockTrackerOperationsOpIdDelete,
  getListOperationsApiV1StockTrackerEntriesEntryIdOperationsGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { TradeOperationRead } from '@/api/generated/schemas'
import type { MinuteCandle } from '@/types/minute-data'

const OP_TYPES = [
  { value: 'buy', label: '买入', color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/30' },
  { value: 'sell', label: '卖出', color: 'text-green-400', bg: 'bg-green-500/10 border-green-500/30' },
]

const getOpLabel = (opType: string) =>
  OP_TYPES.find((t) => t.value === opType) ?? { value: opType, label: opType, color: 'text-muted-foreground', bg: '' }

const TIME_PRESETS = [
  '09:30', '09:35', '09:40', '09:45', '09:50',
  '10:00', '10:30', '11:00', '11:30',
  '13:00', '13:30', '14:00', '14:30',
  '14:45', '14:50', '14:55', '15:00',
]

function findCandlePrice(time: string, candles: MinuteCandle[] | null | undefined): number | null {
  if (!candles || candles.length === 0) return null
  const exact = candles.find((c) => c.time === time)
  if (exact) return exact.close
  const timeMinutes = parseTimeToMinutes(time)
  if (timeMinutes === null) return null
  let closest: MinuteCandle | null = null
  let minDiff = Infinity
  for (const c of candles) {
    const cm = parseTimeToMinutes(c.time)
    if (cm === null) continue
    const diff = Math.abs(cm - timeMinutes)
    if (diff < minDiff) {
      minDiff = diff
      closest = c
    }
  }
  return closest ? closest.close : null
}

function parseTimeToMinutes(t: string): number | null {
  const parts = t.split(':')
  if (parts.length !== 2) return null
  const h = parseInt(parts[0], 10)
  const m = parseInt(parts[1], 10)
  if (isNaN(h) || isNaN(m)) return null
  return h * 60 + m
}

function formatLots(shares: number): string {
  const lots = Math.round(shares / 100)
  return `${lots}手(${shares}股)`
}

interface FormState {
  op_type: string
  op_time: string
  price: string
  quantity: string
  emotion: string
  notes: string
}

const EMPTY_FORM: FormState = {
  op_type: 'buy',
  op_time: '',
  price: '',
  quantity: '',
  emotion: '',
  notes: '',
}

interface OperationsPanelProps {
  entryId: string
  minuteCandles?: MinuteCandle[] | null
  dayHigh?: number | null
  dayLow?: number | null
  dayOpen?: number | null
  dayClose?: number | null
  preClose?: number | null
}

export default function OperationsPanel({
  entryId,
  minuteCandles,
  dayHigh,
  dayLow,
  dayOpen: _dayOpen,
  dayClose: _dayClose,
  preClose: _preClose,
}: OperationsPanelProps) {
  void _dayOpen; void _dayClose; void _preClose
  const queryClient = useQueryClient()
  const [showAddForm, setShowAddForm] = useState(false)
  const [form, setForm] = useState<FormState>(EMPTY_FORM)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editForm, setEditForm] = useState<FormState>(EMPTY_FORM)

  const invalidateOps = () =>
    queryClient.invalidateQueries({
      queryKey:
        getListOperationsApiV1StockTrackerEntriesEntryIdOperationsGetQueryKey(entryId),
    })

  const { data: operations } =
    useListOperationsApiV1StockTrackerEntriesEntryIdOperationsGet(entryId, {
      query: { enabled: !!entryId },
    })

  const createMutation =
    useCreateOperationApiV1StockTrackerEntriesEntryIdOperationsPost({
      mutation: {
        onSuccess: () => {
          invalidateOps()
          setForm(EMPTY_FORM)
          setShowAddForm(false)
        },
      },
    })

  const updateMutation = useUpdateOperationApiV1StockTrackerOperationsOpIdPatch({
    mutation: {
      onSuccess: () => {
        invalidateOps()
        setEditingId(null)
      },
    },
  })

  const deleteMutation = useDeleteOperationApiV1StockTrackerOperationsOpIdDelete({
    mutation: { onSuccess: invalidateOps },
  })

  // 手→股
  const handleCreate = () => {
    const lotsNum = form.quantity ? parseInt(form.quantity, 10) : undefined
    createMutation.mutate({
      entryId,
      data: {
        op_type: form.op_type,
        op_time: form.op_time || undefined,
        price: form.price ? parseFloat(form.price) : undefined,
        quantity: lotsNum != null ? lotsNum * 100 : undefined,
        emotion: form.emotion || undefined,
        notes: form.notes || undefined,
      },
    })
  }

  const handleUpdate = (opId: string) => {
    const lotsNum = editForm.quantity ? parseInt(editForm.quantity, 10) : undefined
    updateMutation.mutate({
      opId,
      data: {
        op_type: editForm.op_type || undefined,
        op_time: editForm.op_time || undefined,
        price: editForm.price ? parseFloat(editForm.price) : undefined,
        quantity: lotsNum != null ? lotsNum * 100 : undefined,
        emotion: editForm.emotion || undefined,
        notes: editForm.notes || undefined,
      },
    })
  }

  // 股→手
  const startEdit = (op: TradeOperationRead) => {
    setEditingId(op.id)
    const sharesNum = op.quantity != null ? Number(op.quantity) : 0
    setEditForm({
      op_type: op.op_type,
      op_time: (op.op_time as string) ?? '',
      price: op.price != null ? String(op.price) : '',
      quantity: sharesNum > 0 ? String(Math.round(sharesNum / 100)) : '',
      emotion: (op.emotion as string) ?? '',
      notes: (op.notes as string) ?? '',
    })
  }

  const opList = (operations ?? []) as TradeOperationRead[]

  const priceRange = useMemo(() => {
    const high = dayHigh ?? null
    const low = dayLow ?? null
    if (high == null || low == null) return null
    const range = high - low
    const margin = Math.max(range * 0.05, 0.01)
    return {
      min: Math.max(0, Math.round((low - margin) * 100) / 100),
      max: Math.round((high + margin) * 100) / 100,
    }
  }, [dayHigh, dayLow])

  const handleTimeSelect = useCallback(
    (time: string, formState: FormState, setFormState: (f: FormState) => void) => {
      const price = findCandlePrice(time, minuteCandles)
      setFormState({
        ...formState,
        op_time: time,
        ...(price != null ? { price: price.toFixed(2) } : {}),
      })
    },
    [minuteCandles]
  )

  const summary = useMemo(() => {
    if (opList.length === 0) return null
    let buyCost = 0
    let buyShares = 0
    let sellRevenue = 0
    let sellShares = 0
    for (const op of opList) {
      const price = op.price != null ? Number(op.price) : 0
      const qty = op.quantity != null ? Number(op.quantity) : 0
      if (op.op_type === 'buy') {
        buyCost += price * qty
        buyShares += qty
      } else if (op.op_type === 'sell') {
        sellRevenue += price * qty
        sellShares += qty
      }
    }
    const net = sellRevenue - buyCost
    return { buyCost, buyShares, sellRevenue, sellShares, net }
  }, [opList])

  const renderOpForm = (
    formState: FormState,
    setFormState: (f: FormState) => void,
    onSubmit: () => void,
    onCancel: () => void,
    isPending: boolean,
    submitLabel: string
  ) => (
    <OpForm
      formState={formState}
      setFormState={setFormState}
      onSubmit={onSubmit}
      onCancel={onCancel}
      isPending={isPending}
      submitLabel={submitLabel}
      priceRange={priceRange}
      minuteCandles={minuteCandles}
      handleTimeSelect={handleTimeSelect}
    />
  )

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <h3 className="text-sm font-medium">操作记录</h3>
        <span className="text-xs text-muted-foreground">
          {opList.length} 条
        </span>
        <div className="ml-auto">
          {!showAddForm && (
            <Button
              size="sm"
              variant="outline"
              className="h-6 text-xs px-2"
              onClick={() => {
                setForm(EMPTY_FORM)
                setShowAddForm(true)
              }}
            >
              <Plus className="h-3 w-3 mr-1" />
              新增
            </Button>
          )}
        </div>
      </div>

      {showAddForm &&
        renderOpForm(
          form,
          setForm,
          handleCreate,
          () => setShowAddForm(false),
          createMutation.isPending,
          '添加'
        )}

      {opList.length === 0 && !showAddForm && (
        <div className="text-center py-4 text-muted-foreground text-sm">
          暂无操作记录
        </div>
      )}

      <div className="space-y-1">
        {opList.map((op) => {
          const opConfig = getOpLabel(op.op_type)

          if (editingId === op.id) {
            return (
              <div key={op.id}>
                {renderOpForm(
                  editForm,
                  setEditForm,
                  () => handleUpdate(op.id),
                  () => setEditingId(null),
                  updateMutation.isPending,
                  '更新'
                )}
              </div>
            )
          }

          const shares = op.quantity != null ? Number(op.quantity) : null

          return (
            <div
              key={op.id}
              className="flex items-center gap-1.5 text-xs group py-1 border-b border-border/30 last:border-0"
            >
              <Badge variant="outline" className={`text-[10px] px-1 py-0 h-4 shrink-0 ${opConfig.color}`}>
                {opConfig.label}
              </Badge>
              {op.op_time && (
                <span className="text-muted-foreground font-mono">
                  {op.op_time as string}
                </span>
              )}
              {op.price != null && (
                <span className="font-mono">
                  ¥{Number(op.price).toFixed(2)}
                </span>
              )}
              {shares != null && (
                <span className="text-muted-foreground font-mono">
                  {formatLots(shares)}
                </span>
              )}
              {op.emotion && (
                <span className="text-muted-foreground truncate">
                  {op.emotion as string}
                </span>
              )}
              {op.notes && (
                <span className="text-muted-foreground truncate flex-1 min-w-0">
                  {op.notes as string}
                </span>
              )}
              <div className="flex gap-0.5 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-5 w-5"
                  onClick={() => startEdit(op)}
                >
                  <Pencil className="h-2.5 w-2.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-5 w-5 text-destructive"
                  onClick={() => deleteMutation.mutate({ opId: op.id })}
                >
                  <Trash2 className="h-2.5 w-2.5" />
                </Button>
              </div>
            </div>
          )
        })}
      </div>

      {summary && (
        <div className="border-t border-border/50 pt-2 mt-2 space-y-1 text-xs font-mono">
          <div className="flex justify-between">
            <span className="text-muted-foreground">买入</span>
            <span className="text-red-400">
              {formatLots(summary.buyShares)} · ¥{summary.buyCost.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">卖出</span>
            <span className="text-green-400">
              {formatLots(summary.sellShares)} · ¥{summary.sellRevenue.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between border-t border-border/30 pt-1">
            <span className="text-muted-foreground">净现金流</span>
            <span
              className={
                summary.net > 0
                  ? 'text-red-400'
                  : summary.net < 0
                    ? 'text-green-400'
                    : 'text-muted-foreground'
              }
            >
              {summary.net > 0 ? '+' : ''}¥{summary.net.toFixed(2)}
            </span>
          </div>
        </div>
      )}
    </div>
  )
}


interface OpFormProps {
  formState: FormState
  setFormState: (f: FormState) => void
  onSubmit: () => void
  onCancel: () => void
  isPending: boolean
  submitLabel: string
  priceRange: { min: number; max: number } | null
  minuteCandles?: MinuteCandle[] | null
  handleTimeSelect: (time: string, formState: FormState, setFormState: (f: FormState) => void) => void
}

function OpForm({
  formState,
  setFormState,
  onSubmit,
  onCancel,
  isPending,
  submitLabel,
  priceRange,
  minuteCandles,
  handleTimeSelect,
}: OpFormProps) {
  const [showExtra, setShowExtra] = useState(false)

  const priceNum = formState.price ? parseFloat(formState.price) : 0
  const qtyNum = formState.quantity ? parseInt(formState.quantity, 10) : 0

  return (
    <div className="space-y-2 border rounded-lg p-2 bg-muted/20">
      <div className="flex gap-1">
        {OP_TYPES.map((t) => (
          <button
            key={t.value}
            type="button"
            className={`flex-1 text-xs font-medium py-1 rounded border transition-colors ${
              formState.op_type === t.value
                ? t.bg + ' ' + t.color
                : 'border-border/50 text-muted-foreground hover:text-foreground'
            }`}
            onClick={() => setFormState({ ...formState, op_type: t.value })}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div>
        <div className="flex flex-wrap gap-0.5">
          {TIME_PRESETS.map((t) => (
            <button
              key={t}
              type="button"
              className={`text-[10px] font-mono px-1 py-0.5 rounded transition-colors ${
                formState.op_time === t
                  ? 'bg-primary/20 text-primary border border-primary/40'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
              }`}
              onClick={() => handleTimeSelect(t, formState, setFormState)}
            >
              {t}
            </button>
          ))}
          <Input
            className="h-5 w-14 text-[10px] font-mono px-1 inline-flex"
            placeholder="HH:MM"
            value={formState.op_time}
            onChange={(e) => {
              const val = e.target.value
              setFormState({ ...formState, op_time: val })
              if (/^\d{2}:\d{2}$/.test(val)) {
                const price = findCandlePrice(val, minuteCandles)
                if (price != null) {
                  setFormState({ ...formState, op_time: val, price: price.toFixed(2) })
                }
              }
            }}
          />
        </div>
      </div>

      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground w-6 shrink-0">价格</span>
          <Input
            className="h-6 w-20 text-xs font-mono px-1"
            type="number"
            step="0.01"
            placeholder="0.00"
            value={formState.price}
            onChange={(e) => setFormState({ ...formState, price: e.target.value })}
          />
          {formState.price && (
            <span className="text-[10px] text-muted-foreground font-mono">
              ¥{parseFloat(formState.price || '0').toFixed(2)}
            </span>
          )}
        </div>
        {priceRange && (
          <Slider
            min={priceRange.min}
            max={priceRange.max}
            step={0.01}
            value={[priceNum || priceRange.min]}
            onValueChange={([v]) =>
              setFormState({ ...formState, price: v.toFixed(2) })
            }
            className="py-1"
          />
        )}
      </div>

      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground w-6 shrink-0">数量</span>
          <Input
            className="h-6 w-16 text-xs font-mono px-1"
            type="number"
            step="1"
            min="0"
            placeholder="手"
            value={formState.quantity}
            onChange={(e) => setFormState({ ...formState, quantity: e.target.value })}
          />
          <span className="text-[10px] text-muted-foreground font-mono">
            {qtyNum > 0 ? `${qtyNum}手(${qtyNum * 100}股)` : '手'}
          </span>
        </div>
        <Slider
          min={0}
          max={100}
          step={1}
          value={[qtyNum]}
          onValueChange={([v]) =>
            setFormState({ ...formState, quantity: String(v) })
          }
          className="py-1"
        />
      </div>

      <button
        type="button"
        className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground transition-colors"
        onClick={() => setShowExtra(!showExtra)}
      >
        {showExtra ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        心态 / 备注
      </button>
      {showExtra && (
        <div className="space-y-1.5">
          <Input
            className="h-6 text-xs"
            placeholder="心态: 冷静 / 紧张 / 兴奋..."
            value={formState.emotion}
            onChange={(e) => setFormState({ ...formState, emotion: e.target.value })}
          />
          <Textarea
            className="text-xs min-h-[36px]"
            placeholder="备注..."
            rows={2}
            value={formState.notes}
            onChange={(e) => setFormState({ ...formState, notes: e.target.value })}
          />
        </div>
      )}

      <div className="flex gap-1.5 justify-end">
        <Button size="sm" variant="outline" className="h-6 text-xs px-2" onClick={onCancel}>
          <X className="h-3 w-3 mr-0.5" />
          取消
        </Button>
        <Button size="sm" className="h-6 text-xs px-2" onClick={onSubmit} disabled={isPending}>
          <Check className="h-3 w-3 mr-0.5" />
          {isPending ? '...' : submitLabel}
        </Button>
      </div>
    </div>
  )
}
