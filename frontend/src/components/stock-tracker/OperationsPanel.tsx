import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import { Plus, Pencil, Trash2, X, Check } from 'lucide-react'
import {
  useListOperationsApiV1StockTrackerEntriesEntryIdOperationsGet,
  useCreateOperationApiV1StockTrackerEntriesEntryIdOperationsPost,
  useUpdateOperationApiV1StockTrackerOperationsOpIdPatch,
  useDeleteOperationApiV1StockTrackerOperationsOpIdDelete,
  getListOperationsApiV1StockTrackerEntriesEntryIdOperationsGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { TradeOperationRead } from '@/api/generated/schemas'

const OP_TYPES = [
  { value: 'buy', label: '买入', color: 'text-red-400' },
  { value: 'sell', label: '卖出', color: 'text-green-400' },
  { value: 'add', label: '加仓', color: 'text-red-300' },
  { value: 'reduce', label: '减仓', color: 'text-green-300' },
]

const getOpLabel = (opType: string) =>
  OP_TYPES.find((t) => t.value === opType) ?? { value: opType, label: opType, color: 'text-muted-foreground' }

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
}

export default function OperationsPanel({ entryId }: OperationsPanelProps) {
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

  const handleCreate = () => {
    createMutation.mutate({
      entryId,
      data: {
        op_type: form.op_type,
        op_time: form.op_time || undefined,
        price: form.price ? parseFloat(form.price) : undefined,
        quantity: form.quantity ? parseInt(form.quantity, 10) : undefined,
        emotion: form.emotion || undefined,
        notes: form.notes || undefined,
      },
    })
  }

  const handleUpdate = (opId: string) => {
    updateMutation.mutate({
      opId,
      data: {
        op_type: editForm.op_type || undefined,
        op_time: editForm.op_time || undefined,
        price: editForm.price ? parseFloat(editForm.price) : undefined,
        quantity: editForm.quantity ? parseInt(editForm.quantity, 10) : undefined,
        emotion: editForm.emotion || undefined,
        notes: editForm.notes || undefined,
      },
    })
  }

  const startEdit = (op: TradeOperationRead) => {
    setEditingId(op.id)
    setEditForm({
      op_type: op.op_type,
      op_time: (op.op_time as string) ?? '',
      price: op.price != null ? String(op.price) : '',
      quantity: op.quantity != null ? String(op.quantity) : '',
      emotion: (op.emotion as string) ?? '',
      notes: (op.notes as string) ?? '',
    })
  }

  const opList = (operations ?? []) as TradeOperationRead[]

  const renderOpForm = (
    formState: FormState,
    setFormState: (f: FormState) => void,
    onSubmit: () => void,
    onCancel: () => void,
    isPending: boolean,
    submitLabel: string
  ) => (
    <div className="space-y-3 border rounded-lg p-3 bg-muted/20">
      <div className="grid grid-cols-2 gap-2">
        <div className="grid gap-1">
          <Label className="text-xs">类型</Label>
          <select
            className="h-8 rounded-md border bg-background px-2 text-sm"
            value={formState.op_type}
            onChange={(e) =>
              setFormState({ ...formState, op_type: e.target.value })
            }
          >
            {OP_TYPES.map((t) => (
              <option key={t.value} value={t.value}>
                {t.label}
              </option>
            ))}
          </select>
        </div>
        <div className="grid gap-1">
          <Label className="text-xs">时间</Label>
          <Input
            className="h-8 text-sm"
            placeholder="HH:MM"
            value={formState.op_time}
            onChange={(e) =>
              setFormState({ ...formState, op_time: e.target.value })
            }
          />
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <div className="grid gap-1">
          <Label className="text-xs">价格</Label>
          <Input
            className="h-8 text-sm"
            type="number"
            step="0.01"
            placeholder="0.00"
            value={formState.price}
            onChange={(e) =>
              setFormState({ ...formState, price: e.target.value })
            }
          />
        </div>
        <div className="grid gap-1">
          <Label className="text-xs">数量</Label>
          <Input
            className="h-8 text-sm"
            type="number"
            step="100"
            placeholder="0"
            value={formState.quantity}
            onChange={(e) =>
              setFormState({ ...formState, quantity: e.target.value })
            }
          />
        </div>
      </div>
      <div className="grid gap-1">
        <Label className="text-xs">心态</Label>
        <Input
          className="h-8 text-sm"
          placeholder="冷静 / 紧张 / 兴奋 / ..."
          value={formState.emotion}
          onChange={(e) =>
            setFormState({ ...formState, emotion: e.target.value })
          }
        />
      </div>
      <div className="grid gap-1">
        <Label className="text-xs">备注</Label>
        <Textarea
          className="text-sm min-h-[60px]"
          placeholder="操作理由..."
          rows={2}
          value={formState.notes}
          onChange={(e) =>
            setFormState({ ...formState, notes: e.target.value })
          }
        />
      </div>
      <div className="flex gap-2 justify-end">
        <Button size="sm" variant="outline" onClick={onCancel}>
          <X className="h-3 w-3 mr-1" />
          取消
        </Button>
        <Button size="sm" onClick={onSubmit} disabled={isPending}>
          <Check className="h-3 w-3 mr-1" />
          {isPending ? '保存中...' : submitLabel}
        </Button>
      </div>
    </div>
  )

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm text-muted-foreground">
          {opList.length} 条记录
        </span>
        {!showAddForm && (
          <Button
            size="sm"
            variant="outline"
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
        <div className="text-center py-8 text-muted-foreground text-sm">
          暂无操作记录
        </div>
      )}

      <div className="space-y-2">
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

          return (
            <div
              key={op.id}
              className="flex items-start justify-between gap-2 border rounded-lg p-2.5 bg-muted/10"
            >
              <div className="space-y-1 flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className={`text-xs ${opConfig.color}`}>
                    {opConfig.label}
                  </Badge>
                  {op.op_time && (
                    <span className="text-xs text-muted-foreground font-mono">
                      {op.op_time as string}
                    </span>
                  )}
                  {op.price != null && (
                    <span className="text-xs font-mono">
                      ¥{Number(op.price).toFixed(2)}
                    </span>
                  )}
                  {op.quantity != null && (
                    <span className="text-xs text-muted-foreground">
                      ×{op.quantity}
                    </span>
                  )}
                </div>
                {op.emotion && (
                  <div className="text-xs text-muted-foreground">
                    心态: {op.emotion as string}
                  </div>
                )}
                {op.notes && (
                  <div className="text-xs text-muted-foreground truncate">
                    {op.notes as string}
                  </div>
                )}
              </div>
              <div className="flex gap-1 shrink-0">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  onClick={() => startEdit(op)}
                >
                  <Pencil className="h-3 w-3" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 text-destructive"
                  onClick={() => deleteMutation.mutate({ opId: op.id })}
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
