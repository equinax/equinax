import { useState, useEffect } from 'react'
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
  Info,
  XCircle,
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
  useGetActiveSyncJobApiV1DataSyncActiveGet,
} from '@/api/generated/data-sync/data-sync'
import { useSyncSSE, type SyncStep } from '@/hooks/useSyncSSE'
import type { DataTableStatus, SyncHistoryItem, HealthDeduction } from '@/api/generated/schemas'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

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

// Health badge component with deductions tooltip
function HealthBadge({ score, deductions }: { score: number; deductions?: HealthDeduction[] }) {
  const color = score >= 90 ? 'text-green-500' : score >= 70 ? 'text-yellow-500' : 'text-red-500'
  const bgColor = score >= 90 ? 'bg-green-500/10' : score >= 70 ? 'bg-yellow-500/10' : 'bg-red-500/10'

  const badge = (
    <div className={`inline-flex items-center gap-2 px-3 py-1 rounded-full ${bgColor} cursor-help`}>
      {score >= 90 ? (
        <CheckCircle className={`h-4 w-4 ${color}`} />
      ) : (
        <AlertCircle className={`h-4 w-4 ${color}`} />
      )}
      <span className={`font-semibold ${color}`}>{score}%</span>
      {deductions && deductions.length > 0 && (
        <Info className="h-3 w-3 text-muted-foreground" />
      )}
    </div>
  )

  if (!deductions || deductions.length === 0) {
    return badge
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>{badge}</TooltipTrigger>
      <TooltipContent side="bottom" className="max-w-xs">
        <div className="space-y-1">
          <p className="font-medium text-sm">健康分数明细</p>
          <p className="text-xs text-muted-foreground">基础分数: 100</p>
          {deductions.map((d, i) => (
            <p key={i} className="text-xs text-red-400">
              {d.table}: {d.reason} ({d.points})
            </p>
          ))}
          <p className="text-xs font-medium border-t pt-1 mt-1">
            最终分数: {score}
          </p>
        </div>
      </TooltipContent>
    </Tooltip>
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
  const [currentJobId, setCurrentJobId] = useState<string | null>(null)

  // Check for active job on mount (task recovery) using generated hook
  const { data: activeJob } = useGetActiveSyncJobApiV1DataSyncActiveGet({
    query: {
      // Only run once on mount
      staleTime: Infinity,
    },
  })

  // Set currentJobId when activeJob is loaded
  useEffect(() => {
    if (activeJob && (activeJob.status === 'queued' || activeJob.status === 'running')) {
      setCurrentJobId(activeJob.id)
    }
  }, [activeJob])

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

  const { data: history = [], refetch: refetchHistory } = useGetSyncHistoryApiV1DataSyncHistoryGet(
    { limit: 10 },
    {
      query: {
        refetchInterval: 30000,
      },
    }
  )

  // SSE for real-time sync progress
  const {
    steps: syncSteps,
    currentMessage,
    overallProgress,
    isConnected: sseConnected,
    error: sseError,
  } = useSyncSSE({
    jobId: currentJobId,
    enabled: !!currentJobId,
    onJobComplete: () => {
      // Job finished, refresh data and clear job ID after a short delay
      setTimeout(() => {
        setCurrentJobId(null)
        refetch()
        refetchHistory()
      }, 1500)
    },
    onError: () => {
      // On error, also clear after delay
      setTimeout(() => {
        setCurrentJobId(null)
        refetch()
        refetchHistory()
      }, 3000)
    },
  })

  const triggerSyncMutation = useTriggerSyncApiV1DataSyncTriggerPost()

  // Handle sync trigger
  const handleSync = () => {
    triggerSyncMutation.mutate(
      { data: { sync_type: 'daily', force: false } },
      {
        onSuccess: (response) => {
          // Start polling the job
          setCurrentJobId(response.job_id)
        },
      }
    )
  }

  // Determine if sync is in progress
  const isSyncing = Boolean(
    triggerSyncMutation.isPending || currentJobId
  )

  if (isLoading && !status) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <TooltipProvider>
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
          <Button onClick={handleSync} disabled={isSyncing}>
            {isSyncing ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Activity className="h-4 w-4 mr-2" />
            )}
            {isSyncing ? 'Syncing...' : 'Sync Now'}
          </Button>
        </div>
      </div>

      {error ? (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 text-red-500">
          Failed to load sync status
        </div>
      ) : null}

      {/* Active Sync Progress Card with SSE */}
      {currentJobId && (
        <Card className="border-blue-500/50 bg-blue-500/5">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-blue-500">
              <Loader2 className="h-5 w-5 animate-spin" />
              Sync in Progress
              {sseConnected && (
                <span className="ml-auto text-xs font-normal text-green-500 flex items-center gap-1">
                  <span className="h-2 w-2 rounded-full bg-green-500 animate-pulse" />
                  Live
                </span>
              )}
            </CardTitle>
            <CardDescription>
              {currentMessage || 'Connecting to sync job...'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {/* Overall Progress */}
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-muted-foreground">Overall Progress</span>
                  <span className="font-medium">{overallProgress}%</span>
                </div>
                <Progress value={overallProgress} className="h-2" />
              </div>

              {/* Step-by-step progress */}
              {syncSteps.length > 0 && (
                <div className="space-y-2">
                  {syncSteps.map((step: SyncStep) => (
                    <div key={step.id} className="flex items-center gap-3 text-sm">
                      {step.status === 'running' && (
                        <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
                      )}
                      {step.status === 'complete' && (
                        <CheckCircle className="h-4 w-4 text-green-500" />
                      )}
                      {step.status === 'pending' && (
                        <Clock className="h-4 w-4 text-muted-foreground" />
                      )}
                      {step.status === 'error' && (
                        <XCircle className="h-4 w-4 text-red-500" />
                      )}
                      <span className={step.status === 'running' ? 'font-medium text-blue-500' : step.status === 'complete' ? 'text-muted-foreground' : ''}>
                        {step.name}
                      </span>
                    </div>
                  ))}
                </div>
              )}

              {/* SSE Error */}
              {sseError && (
                <div className="text-sm text-red-400 flex items-center gap-2">
                  <AlertCircle className="h-4 w-4" />
                  {sseError}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Health Score Card */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center justify-between">
            <span className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              System Health
            </span>
            {status && <HealthBadge score={status.health_score} deductions={status.health_deductions} />}
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
                <StatusBadge status={table.status ?? 'Unknown'} />
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
                      {(item.records_imported ?? 0).toLocaleString()}
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
    </TooltipProvider>
  )
}
