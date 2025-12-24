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
  Calendar,
} from 'lucide-react'
import {
  useGetSyncStatusApiV1DataSyncStatusGet,
  useTriggerSyncApiV1DataSyncTriggerPost,
  useGetActiveSyncJobApiV1DataSyncActiveGet,
  useAnalyzeSyncRequirementsApiV1DataSyncAnalyzeGet,
  useGetSyncJobDetailApiV1DataSyncJobJobIdDetailGet,
} from '@/api/generated/data-sync/data-sync'
import { useSyncSSE, type SyncStep, type EventLogEntry } from '@/hooks/useSyncSSE'
import type { DataTableStatus, HealthDeduction } from '@/api/generated/schemas'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { SyncHistoryPanel } from '@/components/sync'

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
    skipped: 'bg-gray-500/10 text-gray-500',
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
      staleTime: 30000,
      refetchOnMount: 'always', // Always refetch on mount for navigation recovery
    },
  })

  // Set currentJobId when activeJob is loaded
  useEffect(() => {
    if (activeJob && (activeJob.status === 'queued' || activeJob.status === 'running')) {
      setCurrentJobId(activeJob.id)
    }
  }, [activeJob])

  // Fetch job detail for recovery (includes event_log)
  const { data: jobDetail } = useGetSyncJobDetailApiV1DataSyncJobJobIdDetailGet(
    currentJobId ?? '',
    {
      query: {
        enabled: !!currentJobId,
        staleTime: 30000,
        refetchOnMount: 'always', // Always refetch on mount for navigation recovery
      },
    }
  )

  // Convert job detail event_log to EventLogEntry format
  const initialEventLog: EventLogEntry[] | undefined = jobDetail?.event_log?.map((e) => ({
    type: e.type,
    timestamp: e.timestamp,
    data: e.data as Record<string, unknown>,
  }))

  // Pre-sync analysis
  const { data: analysis, refetch: refetchAnalysis } = useAnalyzeSyncRequirementsApiV1DataSyncAnalyzeGet({
    query: {
      refetchInterval: 60000, // Refresh every minute
    },
  })

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

  // SSE for real-time sync progress
  const {
    steps: syncSteps,
    currentMessage,
    overallProgress,
    isConnected: sseConnected,
    error: sseError,
    isRecovered,
  } = useSyncSSE({
    jobId: currentJobId,
    enabled: !!currentJobId,
    initialEventLog,
    onJobComplete: () => {
      // Job finished, refresh data and clear job ID after a short delay
      setTimeout(() => {
        setCurrentJobId(null)
        refetch()
        refetchAnalysis()
      }, 1500)
    },
    onError: () => {
      // On error, also clear after delay
      setTimeout(() => {
        setCurrentJobId(null)
        refetch()
        refetchAnalysis()
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

      {/* Data Analysis Card - Shows before sync */}
      {!currentJobId && analysis && (
        <Card className={analysis.needs_sync ? 'border-amber-500/30 bg-amber-500/5' : 'border-green-500/30 bg-green-500/5'}>
          <CardContent className="py-4">
            <div className="flex items-center gap-3">
              <Calendar className={`h-5 w-5 ${analysis.needs_sync ? 'text-amber-500' : 'text-green-500'}`} />
              <div className="flex-1">
                <p className={`font-medium ${analysis.needs_sync ? 'text-amber-500' : 'text-green-500'}`}>
                  {analysis.message}
                </p>
                <p className="text-xs text-muted-foreground mt-0.5">
                  最新数据: {analysis.latest_data_date || '无'} | 今日: {analysis.today}
                </p>
              </div>
              {analysis.needs_sync && (
                <div className="text-xs text-amber-500/80 bg-amber-500/10 px-2 py-1 rounded">
                  需更新 {analysis.days_to_update} 天
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Active Sync Progress Card with SSE */}
      {currentJobId && (
        <Card className="border-blue-500/50 bg-blue-500/5">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-blue-500">
              <Loader2 className="h-5 w-5 animate-spin" />
              Sync in Progress
              <div className="ml-auto flex items-center gap-2">
                {isRecovered && (
                  <span className="text-xs font-normal text-amber-500 flex items-center gap-1">
                    <Info className="h-3 w-3" />
                    已恢复
                  </span>
                )}
                {sseConnected && (
                  <span className="text-xs font-normal text-green-500 flex items-center gap-1">
                    <span className="h-2 w-2 rounded-full bg-green-500 animate-pulse" />
                    Live
                  </span>
                )}
              </div>
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

              {/* Step-by-step progress with details */}
              {syncSteps.length > 0 && (
                <div className="space-y-2">
                  {syncSteps.map((step: SyncStep) => (
                    <div key={step.id} className="flex items-center gap-3 text-sm">
                      {step.status === 'running' && (
                        <Loader2 className="h-4 w-4 animate-spin text-blue-500 flex-shrink-0" />
                      )}
                      {step.status === 'complete' && (
                        <CheckCircle className="h-4 w-4 text-green-500 flex-shrink-0" />
                      )}
                      {step.status === 'pending' && (
                        <Clock className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                      )}
                      {step.status === 'error' && (
                        <XCircle className="h-4 w-4 text-red-500 flex-shrink-0" />
                      )}
                      <span className={step.status === 'running' ? 'font-medium text-blue-500' : step.status === 'complete' ? 'text-muted-foreground' : ''}>
                        {step.name}
                      </span>
                      {/* Show detailed info for completed steps */}
                      {step.status === 'complete' && (step.records_count !== undefined || step.duration_seconds !== undefined) && (
                        <span className="text-xs text-muted-foreground ml-auto">
                          {step.records_count !== undefined && `${step.records_count} 条`}
                          {step.duration_seconds !== undefined && ` (${step.duration_seconds}s)`}
                          {step.detail && (
                            <span className="text-green-500/70 ml-1">- {step.detail}</span>
                          )}
                        </span>
                      )}
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

      {/* Sync History - Using new panel with slide animation */}
      <SyncHistoryPanel />

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
