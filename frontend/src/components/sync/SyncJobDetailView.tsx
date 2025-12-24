/**
 * Sync job detail view showing event timeline.
 */

import { useGetSyncJobDetailApiV1DataSyncJobJobIdDetailGet } from '@/api/generated/data-sync/data-sync'
import { Loader2, CheckCircle, Clock, Info, XCircle, AlertCircle } from 'lucide-react'

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

// Event log item component
function EventLogItem({ event }: { event: { type: string; timestamp: string; data: Record<string, unknown> } }) {
  const icons: Record<string, React.ReactNode> = {
    plan: <Clock className="h-4 w-4 text-gray-500" />,
    progress: <Loader2 className="h-4 w-4 text-blue-500" />,
    step_complete: <CheckCircle className="h-4 w-4 text-green-500" />,
    job_complete: <CheckCircle className="h-4 w-4 text-green-600" />,
    error: <XCircle className="h-4 w-4 text-red-500" />,
  }

  const message = (event.data?.message as string) || event.type
  const recordsCount = event.data?.records_count as number | undefined
  const durationSeconds = event.data?.duration_seconds as number | undefined
  const detail = event.data?.detail as string | undefined

  return (
    <div className="flex items-start gap-3 text-sm py-2 border-b border-border/50 last:border-0">
      <div className="mt-0.5">{icons[event.type] || <Info className="h-4 w-4 text-gray-400" />}</div>
      <div className="flex-1 min-w-0">
        <div className="font-medium truncate">{message}</div>
        <div className="text-xs text-muted-foreground">
          {new Date(event.timestamp).toLocaleTimeString('zh-CN')}
        </div>
        {(recordsCount !== undefined || durationSeconds !== undefined || detail) && (
          <div className="text-xs text-muted-foreground mt-0.5">
            {recordsCount !== undefined && <span>{recordsCount} 条</span>}
            {durationSeconds !== undefined && <span className="ml-2">({durationSeconds}s)</span>}
            {detail && <span className="ml-2 text-green-500/80">- {detail}</span>}
          </div>
        )}
      </div>
    </div>
  )
}

interface SyncJobDetailViewProps {
  jobId: string
}

export function SyncJobDetailView({ jobId }: SyncJobDetailViewProps) {
  const { data, isLoading, error } = useGetSyncJobDetailApiV1DataSyncJobJobIdDetailGet(jobId)

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center gap-2 py-8 text-red-500 justify-center">
        <AlertCircle className="h-5 w-5" />
        <span>加载失败</span>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className="grid grid-cols-3 gap-4 text-sm">
        <div>
          <span className="text-muted-foreground">状态: </span>
          <StatusBadge status={data.status} />
        </div>
        <div>
          <span className="text-muted-foreground">耗时: </span>
          <span className="font-medium">{data.duration_seconds?.toFixed(1)}s</span>
        </div>
        <div>
          <span className="text-muted-foreground">导入: </span>
          <span className="font-medium">{data.records_imported?.toLocaleString()} 条</span>
        </div>
      </div>

      {/* Sync Type & Time */}
      <div className="text-sm text-muted-foreground">
        <span>{data.sync_type} 同步</span>
        <span className="mx-2">|</span>
        <span>{new Date(data.started_at).toLocaleString('zh-CN')}</span>
      </div>

      {/* Event Timeline */}
      <div className="border-t pt-4">
        <h4 className="font-medium mb-3 text-sm">执行日志</h4>
        <div className="space-y-0 max-h-[350px] overflow-y-auto pr-2">
          {data.event_log && data.event_log.length > 0 ? (
            data.event_log.map((event, i) => (
              <EventLogItem key={i} event={event} />
            ))
          ) : (
            <div className="text-sm text-muted-foreground text-center py-4">
              暂无日志记录
            </div>
          )}
        </div>
      </div>

      {/* Error Message */}
      {data.error_message && (
        <div className="border-t pt-4">
          <h4 className="font-medium text-red-500 mb-2 text-sm">错误信息</h4>
          <pre className="text-xs bg-red-500/10 p-3 rounded overflow-auto max-h-32">
            {data.error_message}
          </pre>
        </div>
      )}
    </div>
  )
}
