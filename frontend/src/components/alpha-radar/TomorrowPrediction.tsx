/**
 * Tomorrow Prediction Component
 *
 * Compact chip-style display for ETF subcategory predictions
 * with rich hover popups showing detailed breakdown.
 */

import { useState, useCallback } from 'react'
import { keepPreviousData } from '@tanstack/react-query'
import { format } from 'date-fns'
import { motion, AnimatePresence } from 'motion/react'
import { Target, ChevronDown, ChevronLeft, ChevronRight, TrendingUp, Activity, Zap, Settings2, RotateCcw } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ComputingConsole } from '@/components/ui/computing-console'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import {
  useGetEtfPredictionApiV1AlphaRadarEtfPredictionGet,
  usePreviewEtfPredictionApiV1AlphaRadarEtfPredictionPreviewPost,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfPredictionItem, PredictionConfigInput } from '@/api/generated/schemas'
import { cn } from '@/lib/utils'

// Default factor configuration
const DEFAULT_FACTORS: PredictionConfigInput = {
  divergence: { enabled: true, weight: 0.2 },
  rsi: { enabled: true, weight: 0.15 },
  relative_strength: { enabled: true, weight: 0.3 },
  momentum: { enabled: true, weight: 0.25 },
  activation: { enabled: true, weight: 0.1 },
}

// Factor display info
const FACTOR_INFO = {
  divergence: { label: '背离', desc: '成交量放大但价格横盘', color: 'bg-blue-500' },
  rsi: { label: '成交量', desc: '60日成交量位置，高位=活跃', color: 'bg-purple-500' },
  relative_strength: { label: '相对强度', desc: '5日涨幅表现（动量）', color: 'bg-green-500' },
  momentum: { label: '趋势', desc: '短期+长期趋势一致性', color: 'bg-orange-500' },
  activation: { label: '小盘激活', desc: '小市值ETF领先', color: 'bg-yellow-500' },
} as const

type FactorKey = keyof typeof FACTOR_INFO

// Score filter options
const SCORE_FILTERS = [
  { value: 0, label: '全部' },
  { value: 40, label: '40+' },
  { value: 60, label: '60+' },
  { value: 80, label: '80+' },
]

const ITEMS_PER_PAGE = 50

interface TomorrowPredictionProps {
  selectedDate?: Date
}

// Prediction chip with bottom border as score bar
function PredictionChip({
  item,
  rank,
}: {
  item: EtfPredictionItem
  rank: number
}) {
  const ambushScore = Number(item.ambush_score) || 0
  const divergenceScore = Number(item.divergence_score) || 0
  const compressionScore = Number(item.compression_score) || 0
  const activationScore = Number(item.activation_score) || 0
  const change5d = item.change_5d ? Number(item.change_5d) : null
  const repChange = item.rep_change ? Number(item.rep_change) : null

  // Score intensity - solid backgrounds like heatmap chips
  const getScoreStyles = (score: number) => {
    if (score >= 60) return {
      bg: 'bg-red-500 dark:bg-red-600',
      text: 'text-white',
      rank: 'text-white/90',
    }
    if (score >= 50) return {
      bg: 'bg-red-400 dark:bg-red-500',
      text: 'text-white',
      rank: 'text-white/80',
    }
    if (score >= 45) return {
      bg: 'bg-red-100 dark:bg-red-900/60',
      text: 'text-red-700 dark:text-red-300',
      rank: 'text-red-600/70 dark:text-red-400/70',
    }
    return {
      bg: 'bg-gray-100 dark:bg-gray-800',
      text: 'text-gray-700 dark:text-gray-300',
      rank: 'text-gray-500 dark:text-gray-500',
    }
  }

  const scoreStyles = getScoreStyles(ambushScore)

  // For popup display
  const getScoreColor = (score: number) => {
    if (score >= 60) return 'text-red-600 dark:text-red-400'
    if (score >= 50) return 'text-red-500 dark:text-red-400'
    if (score >= 45) return 'text-orange-600 dark:text-orange-400'
    return 'text-gray-600 dark:text-gray-400'
  }

  const getRankBadge = (r: number) => {
    if (r === 1) return 'text-yellow-600 font-black'
    if (r === 2) return 'text-gray-500 font-bold'
    if (r === 3) return 'text-amber-700 font-bold'
    return 'text-muted-foreground font-medium'
  }

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          className={cn(
            'relative flex items-center gap-1 px-1.5 pt-0.5 pb-1 rounded-t',
            'text-left transition-all hover:opacity-90 group whitespace-nowrap',
            scoreStyles.bg
          )}
        >
          {/* Rank */}
          <span className={cn('text-[10px] tabular-nums font-medium', scoreStyles.rank)}>
            {rank}
          </span>

          {/* Name */}
          <span className={cn('text-xs font-medium', scoreStyles.text)}>{item.sub_category}</span>

          {/* Score */}
          <span className={cn('text-xs font-bold tabular-nums', scoreStyles.text)}>
            {ambushScore.toFixed(0)}
          </span>

          {/* Score bar at bottom */}
          <div className="absolute bottom-0 left-0 right-0 h-[3px] bg-black/20 rounded-b overflow-hidden flex">
            <div className="bg-blue-400 h-full" style={{ width: `${divergenceScore}%` }} />
            <div className="bg-purple-400 h-full" style={{ width: `${compressionScore}%` }} />
            <div className="bg-yellow-400 h-full" style={{ width: `${activationScore}%` }} />
          </div>
        </button>
      </PopoverTrigger>

      {/* Popup with full details */}
      <PopoverContent
        side="bottom"
        align="start"
        className="w-64 p-0 bg-white dark:bg-gray-900 shadow-xl border"
      >
        {/* Header */}
        <div className="px-3 py-2 border-b bg-muted/30">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className={cn('text-sm font-bold tabular-nums', getRankBadge(rank))}>#{rank}</span>
              <span className="font-semibold">{item.sub_category}</span>
            </div>
            <span className="text-xs px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
              {item.category_label}
            </span>
          </div>
        </div>

        {/* Score Section */}
        <div className="px-3 py-2 border-b">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs text-muted-foreground">潜伏评分</span>
            <span className={cn('text-xl font-bold tabular-nums', getScoreColor(ambushScore))}>
              {ambushScore.toFixed(0)}
            </span>
          </div>
          {/* Score breakdown bar */}
          <div className="h-2 bg-muted rounded-full overflow-hidden flex">
            <div className="bg-blue-500 transition-all" style={{ width: `${divergenceScore}%` }} />
            <div className="bg-purple-500 transition-all" style={{ width: `${compressionScore}%` }} />
            <div className="bg-yellow-500 transition-all" style={{ width: `${activationScore}%` }} />
          </div>
          {/* Factor breakdown */}
          <div className="flex justify-between mt-2 text-[11px]">
            <div className="flex items-center gap-1">
              <TrendingUp className="w-3 h-3 text-blue-500" />
              <span className="text-muted-foreground">背离</span>
              <span className="font-semibold tabular-nums">{divergenceScore.toFixed(0)}</span>
            </div>
            <div className="flex items-center gap-1">
              <Activity className="w-3 h-3 text-purple-500" />
              <span className="text-muted-foreground">压缩</span>
              <span className="font-semibold tabular-nums">{compressionScore.toFixed(0)}</span>
            </div>
            <div className="flex items-center gap-1">
              <Zap className="w-3 h-3 text-yellow-500" />
              <span className="text-muted-foreground">激活</span>
              <span className="font-semibold tabular-nums">{activationScore.toFixed(0)}</span>
            </div>
          </div>
        </div>

        {/* Metrics */}
        <div className="px-3 py-2 space-y-1.5 text-xs">
          {/* 5-day change */}
          {change5d !== null && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">5日涨幅</span>
              <span className={cn('font-mono font-medium', change5d > 0 ? 'text-profit' : change5d < 0 ? 'text-loss' : '')}>
                {change5d > 0 ? '+' : ''}{change5d.toFixed(2)}%
              </span>
            </div>
          )}
          {/* Rep ETF */}
          {item.rep_name && (
            <div className="flex justify-between items-center">
              <span className="text-muted-foreground">代表ETF</span>
              <div className="text-right">
                <span className="font-medium">{item.rep_name}</span>
                {repChange !== null && (
                  <span className={cn('ml-1 font-mono text-[10px]', repChange > 0 ? 'text-profit' : repChange < 0 ? 'text-loss' : '')}>
                    {repChange > 0 ? '+' : ''}{repChange.toFixed(2)}%
                  </span>
                )}
              </div>
            </div>
          )}
          {item.rep_code && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">代码</span>
              <span className="font-mono text-muted-foreground">{item.rep_code}</span>
            </div>
          )}
        </div>

        {/* Signals */}
        {item.signals && item.signals.length > 0 && (
          <div className="px-3 py-2 border-t bg-muted/20">
            <div className="space-y-1">
              {item.signals.map((signal, idx) => {
                const iconMap = {
                  divergence: <TrendingUp className="w-3 h-3 text-blue-500" />,
                  compression: <Activity className="w-3 h-3 text-purple-500" />,
                  activation: <Zap className="w-3 h-3 text-yellow-500" />,
                }
                return (
                  <div key={idx} className="flex items-start gap-1.5 text-[11px]">
                    {iconMap[signal.type as keyof typeof iconMap]}
                    <span className="text-muted-foreground leading-tight">{signal.description}</span>
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </PopoverContent>
    </Popover>
  )
}

// Factor config panel component
function FactorConfigPanel({
  factors,
  onChange,
  onReset,
  isLoading,
}: {
  factors: PredictionConfigInput
  onChange: (key: FactorKey, enabled: boolean, weight: number) => void
  onReset: () => void
  isLoading: boolean
}) {
  const factorKeys = Object.keys(FACTOR_INFO) as FactorKey[]

  // Calculate total weight of enabled factors
  const totalWeight = factorKeys.reduce((sum, key) => {
    const factor = factors[key]
    return sum + (factor?.enabled ? (factor.weight ?? 0) : 0)
  }, 0)

  return (
    <div className="space-y-3 p-1">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground">因子权重配置</span>
        <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={onReset}>
          <RotateCcw className="h-3 w-3 mr-1" />
          重置
        </Button>
      </div>

      <div className="space-y-2.5">
        {factorKeys.map((key) => {
          const info = FACTOR_INFO[key]
          const factor = factors[key] ?? { enabled: true, weight: 0.2 }
          const normalizedWeight = totalWeight > 0 ? ((factor.weight ?? 0) / totalWeight * 100) : 0

          return (
            <div key={key} className="space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Switch
                    checked={factor.enabled}
                    onCheckedChange={(checked) => onChange(key, checked, factor.weight ?? 0.2)}
                    className="scale-75"
                  />
                  <div className={cn('w-2 h-2 rounded-full', info.color)} />
                  <span className={cn('text-xs font-medium', !factor.enabled && 'text-muted-foreground')}>
                    {info.label}
                  </span>
                </div>
                <span className="text-xs tabular-nums text-muted-foreground">
                  {factor.enabled ? `${normalizedWeight.toFixed(0)}%` : '-'}
                </span>
              </div>
              {factor.enabled && (
                <div className="flex items-center gap-2 pl-7">
                  <Slider
                    value={[(factor.weight ?? 0.2) * 100]}
                    onValueChange={(values: number[]) => onChange(key, true, values[0] / 100)}
                    min={5}
                    max={50}
                    step={5}
                    className="flex-1"
                    disabled={isLoading}
                  />
                  <span className="text-[10px] tabular-nums text-muted-foreground w-8">
                    {((factor.weight ?? 0.2) * 100).toFixed(0)}
                  </span>
                </div>
              )}
              <p className="text-[10px] text-muted-foreground pl-7">{info.desc}</p>
            </div>
          )
        })}
      </div>

      {isLoading && (
        <div className="text-xs text-center text-muted-foreground py-1">
          正在重新计算...
        </div>
      )}
    </div>
  )
}

export function TomorrowPrediction({ selectedDate }: TomorrowPredictionProps) {
  const [minScore, setMinScore] = useState(0)
  const [currentPage, setCurrentPage] = useState(0)
  const [factors, setFactors] = useState<PredictionConfigInput>(DEFAULT_FACTORS)
  const [useCustomFactors, setUseCustomFactors] = useState(false)
  const [configOpen, setConfigOpen] = useState(false)

  // Default API call
  const defaultQuery = useGetEtfPredictionApiV1AlphaRadarEtfPredictionGet(
    {
      date: selectedDate ? format(selectedDate, 'yyyy-MM-dd') : undefined,
      min_score: minScore,
    },
    {
      query: {
        placeholderData: keepPreviousData,
        enabled: !useCustomFactors,
      },
    }
  )

  // Preview API for custom factors
  const previewMutation = usePreviewEtfPredictionApiV1AlphaRadarEtfPredictionPreviewPost()

  // Handle factor change
  const handleFactorChange = useCallback((key: FactorKey, enabled: boolean, weight: number) => {
    setFactors((prev) => ({
      ...prev,
      [key]: { enabled, weight },
    }))
    setUseCustomFactors(true)
  }, [])

  // Reset to defaults
  const handleReset = useCallback(() => {
    setFactors(DEFAULT_FACTORS)
    setUseCustomFactors(false)
  }, [])

  // Trigger preview when factors change
  const handleApplyFactors = useCallback(() => {
    if (!useCustomFactors) return
    previewMutation.mutate({
      data: {
        ...factors,
        date: selectedDate ? format(selectedDate, 'yyyy-MM-dd') : undefined,
      },
    })
    setConfigOpen(false)
  }, [factors, selectedDate, useCustomFactors, previewMutation])

  // Use preview data if custom, otherwise default
  const data = useCustomFactors && previewMutation.data
    ? {
        date: previewMutation.data.date,
        predictions: previewMutation.data.predictions,
        total_subcategories: previewMutation.data.total_subcategories,
      }
    : defaultQuery.data

  const isLoading = useCustomFactors ? previewMutation.isPending : defaultQuery.isLoading
  const isFetching = useCustomFactors ? previewMutation.isPending : defaultQuery.isFetching

  const showInitialLoading = isLoading && !data
  const { steps, progress } = useComputingProgress(showInitialLoading, 'etf-prediction')
  const currentFilterLabel = SCORE_FILTERS.find((f) => f.value === minScore)?.label || '全部'

  if (showInitialLoading) {
    return (
      <Card>
        <CardHeader className="pb-2 pt-2">
          <CardTitle className="text-base flex items-center gap-2">
            <Target className="h-4 w-4 text-primary" />
            明日预测
          </CardTitle>
        </CardHeader>
        <CardContent className="pt-0 pb-3">
          <ComputingConsole title="正在计算潜伏异动评分..." steps={steps} progress={progress} />
        </CardContent>
      </Card>
    )
  }

  const predictions = data?.predictions || []
  const totalPages = Math.ceil(predictions.length / ITEMS_PER_PAGE)
  const startIdx = currentPage * ITEMS_PER_PAGE
  const endIdx = Math.min(startIdx + ITEMS_PER_PAGE, predictions.length)
  const currentItems = predictions.slice(startIdx, endIdx)

  return (
    <Card>
      <CardHeader className="pb-1.5 pt-2">
        <div className="flex items-center gap-3">
          <CardTitle className="text-base flex items-center gap-2 shrink-0">
            <Target className="h-4 w-4 text-primary" />
            明日预测
          </CardTitle>

          <div className="flex-1" />

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center gap-0.5">
              <button
                onClick={() => setCurrentPage((p) => Math.max(0, p - 1))}
                disabled={currentPage === 0}
                className="w-5 h-5 rounded flex items-center justify-center hover:bg-muted disabled:opacity-30"
              >
                <ChevronLeft className="h-3 w-3" />
              </button>
              <span className="text-[10px] text-muted-foreground tabular-nums px-1">
                {startIdx + 1}-{endIdx}/{predictions.length}
              </span>
              <button
                onClick={() => setCurrentPage((p) => Math.min(totalPages - 1, p + 1))}
                disabled={currentPage >= totalPages - 1}
                className="w-5 h-5 rounded flex items-center justify-center hover:bg-muted disabled:opacity-30"
              >
                <ChevronRight className="h-3 w-3" />
              </button>
            </div>
          )}

          {data?.date && (
            <span className="text-[10px] text-muted-foreground tabular-nums">{data.date}</span>
          )}

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" className="h-5 text-[10px] px-1.5 gap-0.5">
                {currentFilterLabel}
                <ChevronDown className="h-2.5 w-2.5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {SCORE_FILTERS.map((filter) => (
                <DropdownMenuItem
                  key={filter.value}
                  onClick={() => { setMinScore(filter.value); setCurrentPage(0) }}
                  className={cn(minScore === filter.value && 'bg-accent')}
                >
                  {filter.label}
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Factor config */}
          <Popover open={configOpen} onOpenChange={setConfigOpen}>
            <PopoverTrigger asChild>
              <Button
                variant={useCustomFactors ? 'default' : 'ghost'}
                size="sm"
                className={cn('h-5 w-5 p-0', useCustomFactors && 'bg-primary')}
              >
                <Settings2 className="h-3 w-3" />
              </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-64">
              <FactorConfigPanel
                factors={factors}
                onChange={handleFactorChange}
                onReset={handleReset}
                isLoading={previewMutation.isPending}
              />
              <div className="flex gap-2 mt-3 pt-2 border-t">
                <Button
                  size="sm"
                  variant="outline"
                  className="flex-1 h-7 text-xs"
                  onClick={() => setConfigOpen(false)}
                >
                  取消
                </Button>
                <Button
                  size="sm"
                  className="flex-1 h-7 text-xs"
                  onClick={handleApplyFactors}
                  disabled={previewMutation.isPending}
                >
                  应用
                </Button>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      </CardHeader>

      <CardContent className="pt-0 pb-2">
        <div className="relative">
          {isFetching && (
            <div className="absolute inset-0 z-10 pointer-events-none bg-background/40 backdrop-blur-[1px] rounded" />
          )}

          {predictions.length === 0 ? (
            <div className="text-center py-4 text-muted-foreground text-sm">
              暂无符合条件的预测数据
            </div>
          ) : (
            <AnimatePresence mode="wait">
              <motion.div
                key={currentPage}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.12 }}
                className="flex flex-wrap gap-x-1 gap-y-1"
              >
                {currentItems.map((item, index) => (
                  <PredictionChip
                    key={`${item.category}-${item.sub_category}`}
                    item={item}
                    rank={startIdx + index + 1}
                  />
                ))}
              </motion.div>
            </AnimatePresence>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
