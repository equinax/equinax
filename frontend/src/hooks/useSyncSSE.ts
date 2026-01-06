/**
 * SSE Hook for real-time data sync updates.
 *
 * Connects to the backend SSE endpoint and handles:
 * - plan: Initial sync plan with steps
 * - progress: Step progress updates
 * - step_complete: Individual step completions
 * - job_complete: Job completion notification
 * - error: Error events
 *
 * Supports recovery from event log when reconnecting.
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'

// Failed asset from incremental sync
export interface FailedAsset {
  code: string
  name: string
  retries: number
  error: string
}

export interface SyncStep {
  id: string
  name: string
  progress: number
  status: 'pending' | 'running' | 'complete' | 'error' | 'partial'  // partial = some assets failed
  records_count?: number
  duration_seconds?: number
  detail?: string
  runningMessage?: string  // Real-time progress message for running steps
  failed_assets?: FailedAsset[]  // Assets that failed to sync
  success_count?: number
  fail_count?: number
}

export interface PlanEvent {
  type: 'plan'
  job_id: string
  steps: Array<{ id: string; name: string; progress: number }>
  message: string
  timestamp: string
}

export interface ProgressEvent {
  type: 'progress'
  job_id: string
  step: string
  progress: number
  message: string
  timestamp: string
}

export interface StepCompleteEvent {
  type: 'step_complete'
  job_id: string
  step: string
  status?: string  // 'success' | 'partial' | 'error'
  message?: string
  records_imported?: number
  records_count?: number
  duration_seconds?: number
  detail?: string
  timestamp: string
  failed_assets?: FailedAsset[]  // Assets that failed during this step
  success_count?: number
  fail_count?: number
}

export interface JobCompleteEvent {
  type: 'job_complete'
  job_id: string
  status: 'success' | 'failed'
  progress: number
  records_imported?: number
  records_classified?: number
  duration_seconds?: number
  message: string
  timestamp: string
}

export interface ErrorEvent {
  type: 'error'
  job_id: string
  message: string
  timestamp: string
}

export type SyncSSEEvent = PlanEvent | ProgressEvent | StepCompleteEvent | JobCompleteEvent | ErrorEvent

// Event log entry from API (for recovery)
export interface EventLogEntry {
  type: string
  timestamp: string
  data: Record<string, unknown>
}

export interface UseSyncSSEOptions {
  jobId: string | null
  enabled?: boolean
  initialEventLog?: EventLogEntry[]
  onJobComplete?: (event: JobCompleteEvent) => void
  onError?: (event: ErrorEvent) => void
}

export interface UseSyncSSEReturn {
  steps: SyncStep[]
  currentMessage: string | null
  overallProgress: number
  isConnected: boolean
  error: string | null
  isRecovered: boolean
}

export function useSyncSSE({
  jobId,
  enabled = true,
  initialEventLog,
  onJobComplete,
  onError,
}: UseSyncSSEOptions): UseSyncSSEReturn {
  const [steps, setSteps] = useState<SyncStep[]>([])
  const [currentMessage, setCurrentMessage] = useState<string | null>(null)
  const [overallProgress, setOverallProgress] = useState(0)
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isRecovered, setIsRecovered] = useState(false)
  const eventSourceRef = useRef<EventSource | null>(null)
  const queryClient = useQueryClient()
  const recoveredRef = useRef(false)

  // Store callbacks in refs to avoid re-triggering effect when they change
  const onJobCompleteRef = useRef(onJobComplete)
  const onErrorRef = useRef(onError)
  onJobCompleteRef.current = onJobComplete
  onErrorRef.current = onError

  // Process a single event (used for both live SSE and recovery)
  const processEvent = useCallback((data: SyncSSEEvent, isFromRecovery = false) => {
    switch (data.type) {
      case 'plan':
        // Initialize steps from plan
        setSteps(
          data.steps.map((s) => ({
            id: s.id,
            name: s.name,
            progress: s.progress,
            status: 'pending',
          }))
        )
        setCurrentMessage(data.message)
        setOverallProgress(0)
        break

      case 'progress':
        // Update current step to running with progress message
        setSteps((prev) =>
          prev.map((s) =>
            s.id === data.step
              ? { ...s, status: 'running', runningMessage: data.message }
              : s
          )
        )
        setCurrentMessage(data.message)
        setOverallProgress(data.progress)
        break

      case 'step_complete': {
        // Mark step as complete with detailed info
        const stepData = data as StepCompleteEvent
        // Determine status: partial if there are failed assets, otherwise complete
        const stepStatus = (stepData.failed_assets && stepData.failed_assets.length > 0)
          ? 'partial'
          : 'complete'
        setSteps((prev) =>
          prev.map((s) =>
            s.id === stepData.step
              ? {
                  ...s,
                  status: stepStatus,
                  records_count: stepData.records_count,
                  duration_seconds: stepData.duration_seconds,
                  detail: stepData.detail,
                  failed_assets: stepData.failed_assets,
                  success_count: stepData.success_count,
                  fail_count: stepData.fail_count,
                }
              : s
          )
        )
        if (stepData.message) {
          setCurrentMessage(stepData.message)
        }
        break
      }

      case 'job_complete':
        setOverallProgress(100)
        setCurrentMessage(data.message)
        // Mark all remaining steps as complete
        setSteps((prev) =>
          prev.map((s) =>
            s.status !== 'complete' ? { ...s, status: 'complete' } : s
          )
        )
        if (!isFromRecovery) {
          onJobCompleteRef.current?.(data)
          // Invalidate data sync queries to refresh data
          queryClient.invalidateQueries({
            queryKey: ['/api/v1/data-sync'],
          })
        }
        break

      case 'error':
        setError(data.message)
        setCurrentMessage(data.message)
        // Mark current running step as error
        setSteps((prev) =>
          prev.map((s) =>
            s.status === 'running' ? { ...s, status: 'error' } : s
          )
        )
        if (!isFromRecovery) {
          onErrorRef.current?.(data)
          // Invalidate queries
          queryClient.invalidateQueries({
            queryKey: ['/api/v1/data-sync'],
          })
        }
        break
    }
  }, [queryClient])

  // Reset state when jobId changes
  const resetState = useCallback(() => {
    setSteps([])
    setCurrentMessage(null)
    setOverallProgress(0)
    setError(null)
    setIsRecovered(false)
    recoveredRef.current = false
  }, [])

  // Recovery effect: replay events from initialEventLog
  useEffect(() => {
    if (!initialEventLog || initialEventLog.length === 0 || recoveredRef.current) {
      return
    }

    // Replay all events to restore state
    for (const entry of initialEventLog) {
      const event = {
        type: entry.type,
        ...entry.data,
        timestamp: entry.timestamp,
      } as SyncSSEEvent
      processEvent(event, true)
    }

    recoveredRef.current = true
    setIsRecovered(true)
  }, [initialEventLog, processEvent])

  // SSE connection effect
  useEffect(() => {
    if (!enabled || !jobId) {
      resetState()
      return
    }

    // Build SSE endpoint URL - use relative path to go through Vite proxy
    const baseURL = import.meta.env.VITE_API_URL || ''
    const path = `/api/v1/data-sync/job/${jobId}/events`
    const url = `${baseURL}${path}`

    const eventSource = new EventSource(url)
    eventSourceRef.current = eventSource

    eventSource.onopen = () => {
      setIsConnected(true)
      setError(null)
    }

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as SyncSSEEvent
        processEvent(data, false)

        // Close connection on terminal events
        if (data.type === 'job_complete' || data.type === 'error') {
          eventSource.close()
          setIsConnected(false)
        }
      } catch (e) {
        console.error('SSE parse error:', e)
      }
    }

    eventSource.onerror = (err) => {
      console.error('SSE connection error:', err)
      setIsConnected(false)
      // Don't set error for connection issues - EventSource will auto-reconnect
    }

    return () => {
      eventSource.close()
      eventSourceRef.current = null
      setIsConnected(false)
    }
  }, [jobId, enabled, queryClient, resetState, processEvent])

  return {
    steps,
    currentMessage,
    overallProgress,
    isConnected,
    error,
    isRecovered,
  }
}
