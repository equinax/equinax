import { useState, useEffect, useRef } from 'react'
import type { ComputingStep } from '@/components/ui/computing-console'

interface StepConfig {
  id: string
  label: string
  duration: number // 预估时长 ms
}

// Dashboard 计算步骤（中文）
const DASHBOARD_STEPS: StepConfig[] = [
  { id: 'init', label: '初始化数据管道', duration: 200 },
  { id: 'regime', label: '计算市场状态指数', duration: 300 },
  { id: 'breadth', label: '分析市场宽度', duration: 250 },
  { id: 'style', label: '计算风格轮动', duration: 200 },
  { id: 'smart', label: '处理聪明钱信号', duration: 250 },
]

// Screener 计算步骤（中文）
const SCREENER_STEPS: StepConfig[] = [
  { id: 'fetch', label: '加载股票池', duration: 300 },
  { id: 'compute', label: '计算综合评分', duration: 400 },
  { id: 'labels', label: '生成量化标签', duration: 300 },
  { id: 'rank', label: '排序筛选结果', duration: 200 },
  { id: 'paginate', label: '准备数据展示', duration: 100 },
]

// Heatmap 计算步骤（中文）
const HEATMAP_STEPS: StepConfig[] = [
  { id: 'fetch', label: '加载行情数据', duration: 250 },
  { id: 'industry', label: '映射行业分类', duration: 200 },
  { id: 'aggregate', label: '聚合行业指标', duration: 300 },
  { id: 'render', label: '渲染热力图', duration: 150 },
]

// ETF Screener 计算步骤（中文）
const ETF_SCREENER_STEPS: StepConfig[] = [
  { id: 'fetch', label: '加载ETF数据', duration: 250 },
  { id: 'filter', label: '筛选代表性ETF', duration: 200 },
  { id: 'compute', label: '计算综合评分', duration: 300 },
  { id: 'labels', label: '生成标签', duration: 200 },
  { id: 'rank', label: '排序结果', duration: 150 },
]

// ETF Prediction 计算步骤（中文）
const ETF_PREDICTION_STEPS: StepConfig[] = [
  { id: 'fetch', label: '加载ETF历史数据', duration: 300 },
  { id: 'classify', label: '分类子品类', duration: 200 },
  { id: 'divergence', label: '计算背离因子', duration: 250 },
  { id: 'compression', label: '计算压缩因子', duration: 200 },
  { id: 'activation', label: '计算激活因子', duration: 200 },
  { id: 'score', label: '生成综合评分', duration: 150 },
]

/**
 * Hook to simulate computing progress based on loading state
 * Shows animated step progression while data is being fetched
 */
export function useComputingProgress(
  isLoading: boolean | undefined,
  type: 'dashboard' | 'screener' | 'heatmap' | 'etf-screener' | 'etf-prediction'
): { steps: ComputingStep[]; progress: number } {
  const baseSteps = type === 'dashboard'
    ? DASHBOARD_STEPS
    : type === 'heatmap'
      ? HEATMAP_STEPS
      : type === 'etf-screener'
        ? ETF_SCREENER_STEPS
        : type === 'etf-prediction'
          ? ETF_PREDICTION_STEPS
          : SCREENER_STEPS
  const [steps, setSteps] = useState<ComputingStep[]>(
    baseSteps.map((s) => ({ id: s.id, label: s.label, status: 'pending' as const }))
  )
  const [progress, setProgress] = useState(0)
  const intervalRef = useRef<ReturnType<typeof setInterval>>()

  useEffect(() => {
    if (isLoading) {
      // Reset to initial state
      setSteps(baseSteps.map((s) => ({ id: s.id, label: s.label, status: 'pending' as const })))
      setProgress(0)

      let elapsed = 0
      const totalDuration = baseSteps.reduce((sum, s) => sum + s.duration, 0)

      intervalRef.current = setInterval(() => {
        elapsed += 50
        const newProgress = Math.min((elapsed / totalDuration) * 100, 99)
        setProgress(newProgress)

        // Update step status based on elapsed time
        let accum = 0
        const newSteps = baseSteps.map((s) => {
          const prevAccum = accum
          accum += s.duration
          if (elapsed >= accum) {
            return { id: s.id, label: s.label, status: 'completed' as const }
          } else if (elapsed >= prevAccum) {
            return { id: s.id, label: s.label, status: 'running' as const }
          }
          return { id: s.id, label: s.label, status: 'pending' as const }
        })
        setSteps(newSteps)
      }, 50)
    } else {
      // Complete all steps
      clearInterval(intervalRef.current)
      setSteps(baseSteps.map((s) => ({ id: s.id, label: s.label, status: 'completed' as const })))
      setProgress(100)
    }

    return () => clearInterval(intervalRef.current)
  }, [isLoading, type])

  return { steps, progress }
}
