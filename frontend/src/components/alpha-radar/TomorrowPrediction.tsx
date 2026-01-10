/**
 * Tomorrow Prediction Component
 *
 * Compact chip-style display for ETF subcategory predictions
 * with rich hover popups showing detailed breakdown.
 */

import { useState, useCallback, useEffect, useRef } from 'react'
import { keepPreviousData } from '@tanstack/react-query'
import { format } from 'date-fns'
import { motion, AnimatePresence } from 'motion/react'
import { Target, ChevronDown, ChevronLeft, ChevronRight, TrendingUp, Zap, Settings2, RotateCcw, BarChart3 } from 'lucide-react'
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
  divergence: { label: '背离', desc: '量升价滞，资金流入但涨幅小', color: 'bg-blue-500' },
  rsi: { label: '成交量', desc: '成交活跃度，高位=机构关注', color: 'bg-purple-500' },
  relative_strength: { label: '强度', desc: '5日涨幅动量，强者恒强', color: 'bg-green-500' },
  momentum: { label: '趋势', desc: '短期+长期趋势同向', color: 'bg-orange-500' },
  activation: { label: '小盘', desc: '小ETF领先，先知先觉信号', color: 'bg-yellow-500' },
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
  factors,
}: {
  item: EtfPredictionItem
  rank: number
  factors: PredictionConfigInput
}) {
  const ambushScore = Number(item.ambush_score) || 0
  // V2: 5 factor scores
  const divergenceScore = Number(item.divergence_score) || 0
  const rsiScore = Number(item.rsi_score) || 0
  const rsScore = Number(item.rs_score) || 0
  const momentumScore = Number(item.momentum_score) || 0
  const activationScore = Number(item.activation_score) || 0
  const change5d = item.change_5d ? Number(item.change_5d) : null
  const repChange = item.rep_change ? Number(item.rep_change) : null

  // Calculate normalized weights for enabled factors only
  const factorList = [
    { key: 'divergence', label: '背离', color: 'bg-blue-500', score: divergenceScore, config: factors.divergence },
    { key: 'rsi', label: '成交', color: 'bg-purple-500', score: rsiScore, config: factors.rsi },
    { key: 'relative_strength', label: '强度', color: 'bg-green-500', score: rsScore, config: factors.relative_strength },
    { key: 'momentum', label: '趋势', color: 'bg-orange-500', score: momentumScore, config: factors.momentum },
    { key: 'activation', label: '小盘', color: 'bg-yellow-500', score: activationScore, config: factors.activation },
  ]

  const enabledFactors = factorList.filter(f => f.config?.enabled !== false)
  const totalWeight = enabledFactors.reduce((sum, f) => sum + (f.config?.weight ?? 0.2), 0)
  const normalizedFactors = enabledFactors.map(f => ({
    ...f,
    normalizedWeight: totalWeight > 0 ? (f.config?.weight ?? 0.2) / totalWeight : 0,
  }))

  // Color gradient (gray → red)
  // Score >= 70 uses darkest red, otherwise gradient
  const getScoreColorStyle = (score: number): { bg: string; text: string } => {
    const RED_HIGH = '#c93b3b'

    // Score >= 70 uses fixed dark red
    if (score >= 70) {
      return { bg: RED_HIGH, text: '#ffffff' }
    }

    // For scores < 70, use gradient (0-70 mapped to full gradient)
    const t = Math.max(0, Math.min(70, score)) / 70

    // Gradient: gray → ivory → light red
    const gradient = [
      '#e5e5e5', // 0 - light gray (low score)
      '#f0ebe5', // ~17 - warm gray
      '#f5f3ef', // ~35 - neutral (ivory)
      '#e8a8a8', // ~52 - light red
      '#d47070', // 70 - medium red
    ]

    // Interpolate color
    const index = t * (gradient.length - 1)
    const lower = Math.floor(index)
    const upper = Math.ceil(index)
    const fraction = index - lower

    let bg: string
    if (lower === upper) {
      bg = gradient[lower]
    } else {
      // Interpolate between colors
      const c1 = gradient[lower]
      const c2 = gradient[upper]
      const r1 = parseInt(c1.slice(1, 3), 16)
      const g1 = parseInt(c1.slice(3, 5), 16)
      const b1 = parseInt(c1.slice(5, 7), 16)
      const r2 = parseInt(c2.slice(1, 3), 16)
      const g2 = parseInt(c2.slice(3, 5), 16)
      const b2 = parseInt(c2.slice(5, 7), 16)
      const r = Math.round(r1 + (r2 - r1) * fraction)
      const g = Math.round(g1 + (g2 - g1) * fraction)
      const b = Math.round(b1 + (b2 - b1) * fraction)
      bg = `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`
    }

    // Text color based on background luminance
    const r = parseInt(bg.slice(1, 3), 16)
    const g = parseInt(bg.slice(3, 5), 16)
    const b = parseInt(bg.slice(5, 7), 16)
    const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    const text = luminance > 0.6 ? '#1f2937' : '#ffffff'

    return { bg, text }
  }

  const colorStyle = getScoreColorStyle(ambushScore)

  // For popup display - text color only
  const getScoreTextColor = (score: number) => {
    if (score >= 65) return 'text-red-600 dark:text-red-400'
    if (score >= 50) return 'text-red-500 dark:text-red-400'
    if (score >= 35) return 'text-gray-600 dark:text-gray-400'
    return 'text-green-600 dark:text-green-400'
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
            'relative flex items-center gap-1 px-1.5 py-0.5 rounded',
            'text-left transition-all hover:scale-105 hover:shadow-md whitespace-nowrap'
          )}
          style={{ backgroundColor: colorStyle.bg, color: colorStyle.text }}
        >
          {/* Rank */}
          <span className="text-[10px] tabular-nums font-medium opacity-80">
            {rank}
          </span>

          {/* Name */}
          <span className="text-xs font-medium">{item.sub_category}</span>

          {/* Score */}
          <span className="text-xs font-bold tabular-nums">
            {ambushScore.toFixed(0)}
          </span>
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
            <span className="text-xs text-muted-foreground">综合评分</span>
            <span className={cn('text-xl font-bold tabular-nums', getScoreTextColor(ambushScore))}>
              {ambushScore.toFixed(0)}
              <span className="text-xs font-normal text-muted-foreground ml-0.5">/100</span>
            </span>
          </div>
          {/* Score breakdown bar - shows enabled factors only */}
          <div className="h-2 bg-muted rounded-full overflow-hidden flex">
            {normalizedFactors.map((f) => (
              <div key={f.key} className={cn(f.color, 'transition-all')} style={{ width: `${f.score}%` }} />
            ))}
          </div>
          {/* Weight indicators - only enabled factors */}
          <div className="flex mt-1 text-[9px] text-muted-foreground/60">
            {normalizedFactors.map((f) => (
              <span key={f.key} style={{ width: `${f.normalizedWeight * 100}%` }}>
                {(f.normalizedWeight * 100).toFixed(0)}%
              </span>
            ))}
          </div>
          {/* Factor breakdown - enabled factors with score > 0 in one row */}
          <div className="flex flex-wrap gap-x-2 gap-y-0.5 mt-1.5 text-[10px]">
            {normalizedFactors
              .filter((f) => f.score > 0)
              .map((f) => (
                <div key={f.key} className="flex items-center gap-0.5">
                  <div className={cn('w-1.5 h-1.5 rounded-full', f.color)} />
                  <span className="text-muted-foreground">{f.label}</span>
                  <span className="font-semibold tabular-nums">{f.score.toFixed(0)}</span>
                </div>
              ))}
          </div>
        </div>

        {/* Metrics */}
        <div className="px-3 py-2 space-y-1.5 text-xs">
          {/* 3-day change (change_5d field actually contains 3-day data in V2) */}
          {change5d !== null && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">近期涨幅</span>
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
            <div className="text-[10px] text-muted-foreground mb-1">信号解读</div>
            <div className="space-y-1">
              {item.signals.map((signal, idx) => {
                const iconMap = {
                  divergence: <TrendingUp className="w-3 h-3 text-blue-500 shrink-0" />,
                  compression: <BarChart3 className="w-3 h-3 text-purple-500 shrink-0" />,
                  activation: <Zap className="w-3 h-3 text-yellow-500 shrink-0" />,
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
  // Applied factors (used for actual queries)
  const [appliedFactors, setAppliedFactors] = useState<PredictionConfigInput>(DEFAULT_FACTORS)
  // Pending factors (edited in config panel, not yet applied)
  const [pendingFactors, setPendingFactors] = useState<PredictionConfigInput>(DEFAULT_FACTORS)
  const [useCustomFactors, setUseCustomFactors] = useState(false)
  const [configOpen, setConfigOpen] = useState(false)

  // Track previous date to detect changes
  const prevDateRef = useRef(selectedDate)

  // Preview API for custom factors
  const previewMutation = usePreviewEtfPredictionApiV1AlphaRadarEtfPredictionPreviewPost()

  // When date changes, re-trigger preview if using custom factors
  useEffect(() => {
    if (prevDateRef.current !== selectedDate) {
      setCurrentPage(0)
      prevDateRef.current = selectedDate
      // If using custom factors, re-fetch with new date
      if (useCustomFactors) {
        previewMutation.mutate({
          data: {
            ...appliedFactors,
            date: selectedDate ? format(selectedDate, 'yyyy-MM-dd') : undefined,
          },
        })
      }
    }
  }, [selectedDate, useCustomFactors, appliedFactors, previewMutation])

  // Sync pending factors when opening config panel
  useEffect(() => {
    if (configOpen) {
      setPendingFactors(appliedFactors)
    }
  }, [configOpen, appliedFactors])

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

  // Handle factor change in config panel (only updates pending state)
  const handleFactorChange = useCallback((key: FactorKey, enabled: boolean, weight: number) => {
    setPendingFactors((prev) => ({
      ...prev,
      [key]: { enabled, weight },
    }))
  }, [])

  // Reset pending factors to defaults (doesn't apply until clicking Apply)
  const handleReset = useCallback(() => {
    setPendingFactors(DEFAULT_FACTORS)
  }, [])

  // Apply pending factors and trigger API
  const handleApplyFactors = useCallback(() => {
    const isDefault = JSON.stringify(pendingFactors) === JSON.stringify(DEFAULT_FACTORS)
    setAppliedFactors(pendingFactors)
    setUseCustomFactors(!isDefault)
    if (!isDefault) {
      previewMutation.mutate({
        data: {
          ...pendingFactors,
          date: selectedDate ? format(selectedDate, 'yyyy-MM-dd') : undefined,
        },
      })
    }
    setConfigOpen(false)
  }, [pendingFactors, selectedDate, previewMutation])

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
            <PopoverContent side="left" align="start" sideOffset={8} className="w-64">
              <FactorConfigPanel
                factors={pendingFactors}
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
                    factors={appliedFactors}
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
