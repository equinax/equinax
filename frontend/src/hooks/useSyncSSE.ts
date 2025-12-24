/**
 * SSE Hook for real-time data sync updates.
 *
 * Connects to the backend SSE endpoint and handles:
 * - plan: Initial sync plan with steps
 * - progress: Step progress updates
 * - step_complete: Individual step completions
 * - job_complete: Job completion notification
 * - error: Error events
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'

export interface SyncStep {
  id: string
  name: string
  progress: number
  status: 'pending' | 'running' | 'complete' | 'error'
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
  status?: string
  message?: string
  records_imported?: number
  timestamp: string
}

export interface JobCompleteEvent {
  type: 'job_complete'
  job_id: string
  status: 'success' | 'failed'
  progress: number
  records_imported?: number
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

export interface UseSyncSSEOptions {
  jobId: string | null
  enabled?: boolean
  onJobComplete?: (event: JobCompleteEvent) => void
  onError?: (event: ErrorEvent) => void
}

export interface UseSyncSSEReturn {
  steps: SyncStep[]
  currentMessage: string | null
  overallProgress: number
  isConnected: boolean
  error: string | null
}

export function useSyncSSE({
  jobId,
  enabled = true,
  onJobComplete,
  onError,
}: UseSyncSSEOptions): UseSyncSSEReturn {
  const [steps, setSteps] = useState<SyncStep[]>([])
  const [currentMessage, setCurrentMessage] = useState<string | null>(null)
  const [overallProgress, setOverallProgress] = useState(0)
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const eventSourceRef = useRef<EventSource | null>(null)
  const queryClient = useQueryClient()

  // Reset state when jobId changes
  const resetState = useCallback(() => {
    setSteps([])
    setCurrentMessage(null)
    setOverallProgress(0)
    setError(null)
  }, [])

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
            // Update current step to running
            setSteps((prev) =>
              prev.map((s) =>
                s.id === data.step ? { ...s, status: 'running' } : s
              )
            )
            setCurrentMessage(data.message)
            setOverallProgress(data.progress)
            break

          case 'step_complete':
            // Mark step as complete
            setSteps((prev) =>
              prev.map((s) =>
                s.id === data.step ? { ...s, status: 'complete' } : s
              )
            )
            if (data.message) {
              setCurrentMessage(data.message)
            }
            break

          case 'job_complete':
            setOverallProgress(100)
            setCurrentMessage(data.message)
            // Mark all remaining steps as complete
            setSteps((prev) =>
              prev.map((s) =>
                s.status !== 'complete' ? { ...s, status: 'complete' } : s
              )
            )
            onJobComplete?.(data)
            // Invalidate data sync queries to refresh data
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/data-sync'],
            })
            // Close the connection - job is done
            eventSource.close()
            setIsConnected(false)
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
            onError?.(data)
            // Invalidate queries
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/data-sync'],
            })
            eventSource.close()
            setIsConnected(false)
            break
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
  }, [jobId, enabled, queryClient, onJobComplete, onError, resetState])

  return {
    steps,
    currentMessage,
    overallProgress,
    isConnected,
    error,
  }
}
