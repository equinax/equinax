import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { BookMarked, Plus, Trash2 } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import {
  useListTracksApiV1StockTrackerTracksGet,
  useCreateTrackApiV1StockTrackerTracksPost,
  useDeleteTrackApiV1StockTrackerTracksTsCodeDelete,
  getListTracksApiV1StockTrackerTracksGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'
import type { AssetSearchResult } from '@/api/generated/schemas'
import StockSearchCombobox from '@/components/stock-tracker/StockSearchCombobox'

const formatDate = (dateStr: string) => {
  const d = new Date(dateStr)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

export default function StockTrackerPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null)

  // Form state
  const [selectedStock, setSelectedStock] = useState<AssetSearchResult | null>(null)
  const [formWatchReason, setFormWatchReason] = useState('')
  const [formSector, setFormSector] = useState('')
  const [formTags, setFormTags] = useState('')

  const { data: tracks, isLoading } = useListTracksApiV1StockTrackerTracksGet()

  const createMutation = useCreateTrackApiV1StockTrackerTracksPost({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: getListTracksApiV1StockTrackerTracksGetQueryKey() })
        setDialogOpen(false)
        resetForm()
      },
    },
  })

  const deleteMutation = useDeleteTrackApiV1StockTrackerTracksTsCodeDelete({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: getListTracksApiV1StockTrackerTracksGetQueryKey() })
        setDeleteConfirm(null)
      },
    },
  })

  const resetForm = () => {
    setSelectedStock(null)
    setFormWatchReason('')
    setFormSector('')
    setFormTags('')
  }

  const handleCreate = () => {
    if (!selectedStock) return
    const tags = formTags
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean)
    createMutation.mutate({
      data: {
        ts_code: selectedStock.code,
        stock_name: selectedStock.name || undefined,
        watch_reason: formWatchReason.trim() || undefined,
        sector: formSector.trim() || undefined,
        tags: tags.length > 0 ? tags : undefined,
      },
    })
  }

  const handleDelete = (tsCode: string) => {
    deleteMutation.mutate({ tsCode })
  }

  const items = tracks || []

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <BookMarked className="h-6 w-6 text-primary" />
            个股追踪
          </h1>
          <p className="text-muted-foreground text-sm">
            追踪关注股票的每日走势与操作记录
          </p>
        </div>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="h-4 w-4 mr-1" />
              添加追踪
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>添加股票追踪</DialogTitle>
              <DialogDescription>
                搜索并选择要追踪的股票
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-4 py-4">
              <div className="grid gap-2">
                <Label>股票 *</Label>
                <StockSearchCombobox
                  value={selectedStock}
                  onSelect={setSelectedStock}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="watch_reason">关注原因</Label>
                <Input
                  id="watch_reason"
                  placeholder="龙头股 / 技术突破 / ..."
                  value={formWatchReason}
                  onChange={(e) => setFormWatchReason(e.target.value)}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="sector">所属板块</Label>
                <Input
                  id="sector"
                  placeholder="白酒 / 半导体 / ..."
                  value={formSector}
                  onChange={(e) => setFormSector(e.target.value)}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="tags">标签（逗号分隔）</Label>
                <Input
                  id="tags"
                  placeholder="龙头, 趋势, 短线"
                  value={formTags}
                  onChange={(e) => setFormTags(e.target.value)}
                />
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setDialogOpen(false)}>
                取消
              </Button>
              <Button
                onClick={handleCreate}
                disabled={!selectedStock || createMutation.isPending}
              >
                {createMutation.isPending ? '添加中...' : '确认添加'}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {/* Track List */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">
            追踪列表
            <span className="ml-2 text-sm font-normal text-muted-foreground">
              共 {items.length} 只
            </span>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[120px]">代码</TableHead>
                <TableHead>名称</TableHead>
                <TableHead>标签</TableHead>
                <TableHead>关注原因</TableHead>
                <TableHead>板块</TableHead>
                <TableHead className="text-center">添加时间</TableHead>
                <TableHead className="w-[60px]" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 7 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-4 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : items.length === 0 ? (
                <TableRow>
                  <TableCell
                    colSpan={7}
                    className="text-center py-12 text-muted-foreground"
                  >
                    <div className="flex flex-col items-center gap-2">
                      <BookMarked className="h-8 w-8 opacity-40" />
                      <p>暂无追踪股票</p>
                      <p className="text-xs">点击「添加追踪」开始</p>
                    </div>
                  </TableCell>
                </TableRow>
              ) : (
                items.map((track) => (
                  <TableRow
                    key={track.id}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => navigate(`/stock-tracker/${track.ts_code}`)}
                  >
                    <TableCell className="font-mono text-sm">
                      {track.ts_code}
                    </TableCell>
                    <TableCell className="font-medium">
                      {track.stock_name || '-'}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {(track.tags as string[] | null)?.map((tag: string) => (
                          <Badge key={tag} variant="secondary" className="text-xs">
                            {tag}
                          </Badge>
                        ))}
                      </div>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground max-w-[200px] truncate">
                      {track.watch_reason || '-'}
                    </TableCell>
                    <TableCell className="text-sm">
                      {track.sector || '-'}
                    </TableCell>
                    <TableCell className="text-center text-sm text-muted-foreground">
                      {formatDate(track.created_at)}
                    </TableCell>
                    <TableCell>
                      {deleteConfirm === track.ts_code ? (
                        <Button
                          variant="destructive"
                          size="sm"
                          onClick={(e) => {
                            e.stopPropagation()
                            handleDelete(track.ts_code)
                          }}
                          disabled={deleteMutation.isPending}
                        >
                          确认
                        </Button>
                      ) : (
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8 text-muted-foreground hover:text-destructive"
                          onClick={(e) => {
                            e.stopPropagation()
                            setDeleteConfirm(track.ts_code)
                          }}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  )
}
