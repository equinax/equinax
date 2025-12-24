import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import {
  Database,
  RefreshCw,
  CheckCircle,
  AlertCircle,
  Clock,
  Loader2,
  Activity,
  HardDrive,
  TrendingUp,
} from 'lucide-react'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  useGetSyncStatusApiV1DataSyncStatusGet,
  useGetSyncHistoryApiV1DataSyncHistoryGet,
  useTriggerSyncApiV1DataSyncTriggerPost,
} from '@/api/generated/data-sync/data-sync'
import type { DataTableStatus, SyncHistoryItem } from '@/api/generated/schemas'

// Helper to format date
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

// Health badge component
function HealthBadge({ score }: { score: number }) {
  const color = score >= 90 ? 'text-green-500' : score >= 70 ? 'text-yellow-500' : 'text-red-500'
  const bgColor = score >= 90 ? 'bg-green-500/10' : score >= 70 ? 'bg-yellow-500/10' : 'bg-red-500/10'

  return (
    <div className={`inline-flex items-center gap-2 px-3 py-1 rounded-full ${bgColor}`}>
      {score >= 90 ? (
        <CheckCircle className={`h-4 w-4 ${color}`} />
      ) : (
        <AlertCircle className={`h-4 w-4 ${color}`} />
      )}
      <span className={`font-semibold ${color}`}>{score}%</span>
    </div>
  )
}

// Status badge component
function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    success: 'bg-green-500/10 text-green-500',
    running: 'bg-blue-500/10 text-blue-500',
    queued: 'bg-yellow-500/10 text-yellow-500',
    failed: 'bg-red-500/10 text-red-500',
    OK: 'bg-green-500/10 text-green-500',
    Empty: 'bg-yellow-500/10 text-yellow-500',
    Error: 'bg-red-500/10 text-red-500',
  }

  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${styles[status] || 'bg-gray-500/10 text-gray-500'}`}>
      {status}
    </span>
  )
}

export default function DataSyncPage() {
  // Use generated React Query hooks
  const {
    data: status,
    isLoading,
    error,
    refetch,
  } = useGetSyncStatusApiV1DataSyncStatusGet({
    query: {
      refetchInterval: 30000, // Auto-refresh every 30s
    },
  })

  const { data: history = [] } = useGetSyncHistoryApiV1DataSyncHistoryGet(
    { limit: 10 },
    {
      query: {
        refetchInterval: 30000,
      },
    }
  )

  const triggerSyncMutation = useTriggerSyncApiV1DataSyncTriggerPost()

  // Handle sync trigger
  const handleSync = () => {
    triggerSyncMutation.mutate(
      { data: { sync_type: 'daily', force: false } },
      {
        onSuccess: () => {
          // Refresh after a short delay
          setTimeout(() => refetch(), 1000)
        },
      }
    )
  }

  if (isLoading && !status) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Data Sync</h1>
          <p className="text-muted-foreground">Monitor and manage data synchronization</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isLoading}>
            <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
          <Button onClick={handleSync} disabled={triggerSyncMutation.isPending}>
            {triggerSyncMutation.isPending ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Activity className="h-4 w-4 mr-2" />
            )}
            Sync Now
          </Button>
        </div>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-500">
          Failed to load sync status
        </div>
      )}

      {/* Health Score Card */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              System Health
            </span>
            {status && <HealthBadge score={status.health_score} />}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <Progress value={status?.health_score || 0} className="h-2" />
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div>
                <p className="text-muted-foreground">Last Sync</p>
                <p className="font-medium">
                  {status?.last_sync ? formatDateTime(status.last_sync.completed_at || status.last_sync.started_at) : 'Never'}
                </p>
              </div>
              <div>
                <p className="text-muted-foreground">Next Scheduled</p>
                <p className="font-medium">{status?.next_scheduled || '-'}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Missing Dates</p>
                <p className="font-medium">{status?.missing_dates_count || 0}</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Data Tables Grid */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {status?.tables.map((table: DataTableStatus) => (
          <Card key={table.name}>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium flex items-center justify-between">
                <span className="flex items-center gap-2">
                  <HardDrive className="h-4 w-4 text-muted-foreground" />
                  {table.name}
                </span>
                <StatusBadge status={table.status} />
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{table.record_count.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground mt-1">
                {table.date_range || 'No data'}
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Sync History */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Clock className="h-5 w-5" />
            Sync History
          </CardTitle>
          <CardDescription>Recent synchronization operations</CardDescription>
        </CardHeader>
        <CardContent>
          {history.length === 0 ? (
            <p className="text-muted-foreground text-center py-8">No sync history</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Duration</TableHead>
                  <TableHead className="text-right">Records</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {history.map((item: SyncHistoryItem) => (
                  <TableRow key={item.id}>
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
                      {item.records_imported.toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {/* CLI Commands Help */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" />
            CLI Commands
          </CardTitle>
          <CardDescription>Useful command-line tools for data management</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3 font-mono text-sm">
            <div className="bg-muted/50 p-3 rounded">
              <code className="text-blue-400">python -m scripts.data_cli status</code>
              <p className="text-xs text-muted-foreground mt-1">Show comprehensive data status</p>
            </div>
            <div className="bg-muted/50 p-3 rounded">
              <code className="text-blue-400">python -m scripts.data_cli sync</code>
              <p className="text-xs text-muted-foreground mt-1">Run incremental data sync</p>
            </div>
            <div className="bg-muted/50 p-3 rounded">
              <code className="text-blue-400">python -m scripts.data_cli fix --repair</code>
              <p className="text-xs text-muted-foreground mt-1">Check and repair data issues</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
