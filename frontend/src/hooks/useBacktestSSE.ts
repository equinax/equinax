/**
 * SSE Hook for real-time backtest updates.
 *
 * Connects to the backend SSE endpoint and handles:
 * - progress: Job progress updates
 * - result: Individual backtest completions
 * - log: Execution logs from backtrader
 * - job_complete: Job completion notification
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { getStreamBacktestEventsApiV1BacktestsJobIdEventsGetQueryKey } from '@/api/generated/backtests/backtests'

export interface LogEvent {
  type: 'log'
  job_id: string
  level: 'info' | 'warning' | 'error'
  message: string
  timestamp: string
}

export interface ProgressEvent {
  type: 'progress'
  job_id: string
  progress: number
  completed: number
  total: number
  successful: number
  failed: number
  timestamp: string
}

export interface ResultEvent {
  type: 'result'
  job_id: string
  result_id: string
  stock_code: string
  status: 'completed' | 'failed'
  total_return?: number | null
  sharpe_ratio?: number | null
  error_message?: string
  timestamp: string
}

export interface JobCompleteEvent {
  type: 'job_complete'
  job_id: string
  status: 'completed' | 'failed'
  successful?: number
  failed?: number
  total?: number
  error_message?: string
  timestamp: string
}

export type BacktestSSEEvent = LogEvent | ProgressEvent | ResultEvent | JobCompleteEvent

export interface UseBacktestSSEOptions {
  jobId: string
  enabled?: boolean
  onProgress?: (event: ProgressEvent) => void
  onResult?: (event: ResultEvent) => void
  onJobComplete?: (event: JobCompleteEvent) => void
}

export interface UseBacktestSSEReturn {
  logs: LogEvent[]
  isConnected: boolean
  lastProgress: ProgressEvent | null
  clearLogs: () => void
}

export function useBacktestSSE({
  jobId,
  enabled = true,
  onProgress,
  onResult,
  onJobComplete,
}: UseBacktestSSEOptions): UseBacktestSSEReturn {
  const [logs, setLogs] = useState<LogEvent[]>([])
  const [isConnected, setIsConnected] = useState(false)
  const [lastProgress, setLastProgress] = useState<ProgressEvent | null>(null)
  const eventSourceRef = useRef<EventSource | null>(null)
  const queryClient = useQueryClient()

  const clearLogs = useCallback(() => {
    setLogs([])
  }, [])

  useEffect(() => {
    if (!enabled || !jobId) {
      return
    }

    // Build SSE endpoint URL using same baseURL logic as axios in mutator.ts
    const baseURL = import.meta.env.VITE_API_URL || ''
    const [path] = getStreamBacktestEventsApiV1BacktestsJobIdEventsGetQueryKey(jobId)
    const url = `${baseURL}${path}`

    const eventSource = new EventSource(url)
    eventSourceRef.current = eventSource

    eventSource.onopen = () => {
      setIsConnected(true)
    }

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as BacktestSSEEvent

        switch (data.type) {
          case 'progress':
            setLastProgress(data)
            onProgress?.(data)
            // Invalidate job query to update progress in UI
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/backtests/{job_id}', jobId],
            })
            break

          case 'result':
            onResult?.(data)
            // Invalidate results query to show new result
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/backtests/{job_id}/results', jobId],
            })
            break

          case 'log':
            setLogs((prev) => {
              // Keep only the last 100 logs to prevent memory issues
              const newLogs = [...prev, data]
              if (newLogs.length > 100) {
                return newLogs.slice(-100)
              }
              return newLogs
            })
            break

          case 'job_complete':
            onJobComplete?.(data)
            // Invalidate all related queries
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/backtests/{job_id}', jobId],
            })
            queryClient.invalidateQueries({
              queryKey: ['/api/v1/backtests/{job_id}/results', jobId],
            })
            // Close the connection - job is done
            eventSource.close()
            setIsConnected(false)
            break
        }
      } catch (e) {
        console.error('SSE parse error:', e)
      }
    }

    eventSource.onerror = (error) => {
      console.error('SSE connection error:', error)
      setIsConnected(false)
      // EventSource will automatically try to reconnect
    }

    return () => {
      eventSource.close()
      eventSourceRef.current = null
      setIsConnected(false)
    }
  }, [jobId, enabled, queryClient, onProgress, onResult, onJobComplete])

  return {
    logs,
    isConnected,
    lastProgress,
    clearLogs,
  }
}
