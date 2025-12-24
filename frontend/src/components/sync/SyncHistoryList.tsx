/**
 * Sync history list with pagination.
 */

import { useGetSyncHistoryApiV1DataSyncHistoryGet } from '@/api/generated/data-sync/data-sync'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Loader2, ChevronLeft, ChevronRight } from 'lucide-react'

// Format date time
function formatDateTime(dateStr: string | null | undefined): string {
  if (!dateStr) return '-'
  const d = new Date(dateStr)
  return d.toLocaleString('zh-CN', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

// Status badge component
function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    success: 'bg-green-500/10 text-green-500',
    running: 'bg-blue-500/10 text-blue-500',
    queued: 'bg-yellow-500/10 text-yellow-500',
    failed: 'bg-red-500/10 text-red-500',
  }

  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${styles[status] || 'bg-gray-500/10 text-gray-500'}`}>
      {status}
    </span>
  )
}

interface SyncHistoryListProps {
  onSelect: (id: string) => void
  page: number
  onPageChange: (page: number) => void
}

const PAGE_SIZE = 10

export function SyncHistoryList({ onSelect, page, onPageChange }: SyncHistoryListProps) {
  const { data, isLoading } = useGetSyncHistoryApiV1DataSyncHistoryGet(
    { limit: PAGE_SIZE, offset: page * PAGE_SIZE },
    {
      query: {
        refetchInterval: 30000,
      },
    }
  )

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0

  if (isLoading && !data) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!data?.items || data.items.length === 0) {
    return (
      <div className="text-muted-foreground text-center py-12">
        暂无同步历史
      </div>
    )
  }

  return (
    <div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>时间</TableHead>
            <TableHead>类型</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>耗时</TableHead>
            <TableHead className="text-right">记录数</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.items.map((item) => (
            <TableRow
              key={item.id}
              className="cursor-pointer hover:bg-muted/50 transition-colors"
              onClick={() => onSelect(item.id)}
            >
              <TableCell className="font-medium">
                {formatDateTime(item.started_at)}
              </TableCell>
              <TableCell>{item.sync_type}</TableCell>
              <TableCell>
                <StatusBadge status={item.status} />
              </TableCell>
              <TableCell>
                {item.duration_seconds ? `${item.duration_seconds.toFixed(1)}s` : '-'}
              </TableCell>
              <TableCell className="text-right">
                {(item.records_imported ?? 0).toLocaleString()}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-4 px-2">
          <div className="text-sm text-muted-foreground">
            共 {data.total} 条记录
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => onPageChange(page - 1)}
              disabled={page === 0}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="text-sm">
              {page + 1} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => onPageChange(page + 1)}
              disabled={page >= totalPages - 1}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
