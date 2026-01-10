/**
 * ETF Category Heatmap Component
 *
 * Displays ETF performance grouped by category:
 * - Top movers row: Top 10 ETFs by daily change (bubble-up mechanism)
 * - 6 category rows:
 *   - broad (宽基): 沪深300, 中证500, 科创50
 *   - sector (行业): 银行, 证券, 医药
 *   - theme (赛道): AI, 芯片, 机器人
 *   - cross_border (跨境): 纳指, 标普, 恒科
 *   - commodity (商品): 黄金, 白银, 原油
 *   - bond (债券): 国债, 城投, 信用债
 */

import { useNavigate, Link } from 'react-router-dom'
import { keepPreviousData } from '@tanstack/react-query'
import { format } from 'date-fns'
import { motion } from 'motion/react'
import { Flame, ArrowRight } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ComputingConsole } from '@/components/ui/computing-console'
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from '@/components/ui/tooltip'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import { useGetEtfHeatmapApiV1AlphaRadarEtfHeatmapGet } from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfHeatmapItem } from '@/api/generated/schemas'
import { cn } from '@/lib/utils'

// Category label mapping for top movers
const CATEGORY_LABELS: Record<string, string> = {
  broad: '宽基',
  sector: '行业',
  theme: '赛道',
  cross_border: '跨境',
  commodity: '商品',
  bond: '债券',
}

interface EtfCategoryHeatmapProps {
  selectedDate?: Date
  onItemClick?: (code: string) => void
}

// Color mapping based on change percentage
function getChangeColor(changePct: number | null | undefined): string {
  if (changePct === null || changePct === undefined) return 'bg-gray-100 dark:bg-gray-800'

  const pct = Number(changePct)
  if (pct >= 5) return 'bg-red-600 text-white'
  if (pct >= 3) return 'bg-red-500 text-white'
  if (pct >= 1) return 'bg-red-400 text-white'
  if (pct > 0) return 'bg-red-200 dark:bg-red-900/50 text-red-700 dark:text-red-300'
  if (pct === 0) return 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400'
  if (pct > -1) return 'bg-green-200 dark:bg-green-900/50 text-green-700 dark:text-green-300'
  if (pct > -3) return 'bg-green-400 text-white'
  if (pct > -5) return 'bg-green-500 text-white'
  return 'bg-green-600 text-white'
}

// Format change percentage
function formatChangePct(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  return num >= 0 ? `+${num.toFixed(2)}%` : `${num.toFixed(2)}%`
}

// Format amount in Chinese units
function formatAmount(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (num >= 1e8) return `${(num / 1e8).toFixed(2)}亿`
  if (num >= 1e4) return `${(num / 1e4).toFixed(0)}万`
  return num.toFixed(0)
}

// Single ETF cell component
function EtfCell({
  item,
  onClick,
  index,
}: {
  item: EtfHeatmapItem
  onClick: () => void
  index: number
}) {
  const changePct = item.change_pct ? Number(item.change_pct) : null

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <motion.button
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.2, delay: index * 0.02 }}
          onClick={onClick}
          className={cn(
            'shrink-0 px-2 py-1 rounded text-xs font-medium',
            'transition-all hover:scale-105 hover:shadow-md',
            'cursor-pointer whitespace-nowrap',
            getChangeColor(changePct)
          )}
        >
          <span className="mr-1">{item.name}</span>
          <span className="font-mono">{formatChangePct(changePct)}</span>
        </motion.button>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="text-xs">
        <div className="space-y-1">
          <div className="font-medium">{item.full_name}</div>
          <div className="text-muted-foreground">代码: {item.code}</div>
          <div className="text-muted-foreground">成交额: {formatAmount(item.amount ? Number(item.amount) : null)}</div>
        </div>
      </TooltipContent>
    </Tooltip>
  )
}

// Top mover cell with category badge
function TopMoverCell({
  item,
  onClick,
  index,
}: {
  item: EtfHeatmapItem
  onClick: () => void
  index: number
}) {
  const changePct = item.change_pct ? Number(item.change_pct) : null
  const categoryLabel = item.category ? CATEGORY_LABELS[item.category] || item.category : ''

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <motion.button
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.2, delay: index * 0.02 }}
          onClick={onClick}
          className={cn(
            'shrink-0 px-2 py-1.5 rounded text-xs font-medium',
            'transition-all hover:scale-105 hover:shadow-md',
            'cursor-pointer whitespace-nowrap',
            'ring-1 ring-orange-300 dark:ring-orange-700',
            getChangeColor(changePct)
          )}
        >
          <div className="flex flex-col items-center gap-0.5">
            <span className="font-medium">{item.name}</span>
            <span className="font-mono text-[10px]">{formatChangePct(changePct)}</span>
          </div>
        </motion.button>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="text-xs">
        <div className="space-y-1">
          <div className="font-medium">{item.full_name}</div>
          <div className="text-muted-foreground">代码: {item.code}</div>
          <div className="text-muted-foreground">品类: {categoryLabel}</div>
          <div className="text-muted-foreground">成交额: {formatAmount(item.amount ? Number(item.amount) : null)}</div>
        </div>
      </TooltipContent>
    </Tooltip>
  )
}

export function EtfCategoryHeatmap({
  selectedDate,
  onItemClick,
}: EtfCategoryHeatmapProps) {
  const navigate = useNavigate()

  // Fetch ETF heatmap data
  const { data, isLoading, isFetching } = useGetEtfHeatmapApiV1AlphaRadarEtfHeatmapGet(
    {
      date: selectedDate ? format(selectedDate, 'yyyy-MM-dd') : undefined,
    },
    {
      query: {
        placeholderData: keepPreviousData,
      },
    }
  )

  // Loading progress
  const showInitialLoading = isLoading && !data
  const { steps, progress } = useComputingProgress(showInitialLoading, 'etf-screener')

  // Handle ETF click
  const handleItemClick = (code: string) => {
    if (onItemClick) {
      onItemClick(code)
    } else {
      const params = new URLSearchParams()
      if (selectedDate) {
        params.set('date', format(selectedDate, 'yyyy-MM-dd'))
      }
      params.set('from', 'alpha-radar')
      const queryString = params.toString()
      navigate(`/universe/${code}${queryString ? `?${queryString}` : ''}`)
    }
  }

  // Initial loading state
  if (showInitialLoading) {
    return (
      <Card>
        <CardHeader className="pb-3 pt-3">
          <CardTitle className="text-lg">ETF 概览</CardTitle>
        </CardHeader>
        <CardContent className="pt-0">
          <ComputingConsole
            title="正在加载ETF数据..."
            steps={steps}
            progress={progress}
          />
        </CardContent>
      </Card>
    )
  }

  // Filter out empty categories
  const categories = data?.categories?.filter(cat => cat.items.length > 0) || []

  if (categories.length === 0) {
    return null
  }

  return (
    <Card>
      <CardHeader className="pb-3 pt-3">
        <div className="flex items-center gap-4">
          <CardTitle className="text-lg shrink-0">ETF 概览</CardTitle>
          <div className="flex-1" />
          {data?.date && (
            <span className="text-xs text-muted-foreground font-mono">
              {data.date}
            </span>
          )}
          <Link
            to="/etf-rotation"
            className="text-xs text-muted-foreground hover:text-foreground transition-colors flex items-center gap-1"
          >
            轮动详情
            <ArrowRight className="h-3 w-3" />
          </Link>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        <div className="relative">
          {/* Loading overlay */}
          {isFetching && (
            <div className="absolute inset-0 z-10 pointer-events-none overflow-hidden rounded-lg">
              <div className="absolute inset-0 bg-background/40 backdrop-blur-[1px]" />
              <div
                className="absolute inset-0"
                style={{
                  background: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.2) 50%, transparent 100%)',
                  backgroundSize: '200% 100%',
                  animation: 'shimmer 1.2s ease-in-out infinite',
                }}
              />
              <style>{`
                @keyframes shimmer {
                  0% { background-position: 200% 0; }
                  100% { background-position: -200% 0; }
                }
              `}</style>
            </div>
          )}

          {/* Top Movers Row (Bubble-up mechanism) */}
          <TooltipProvider delayDuration={200}>
            {data?.top_movers && data.top_movers.length > 0 && (
              <div className="mb-3 pb-3 border-b border-dashed border-orange-200 dark:border-orange-800">
                <div className="flex items-center gap-2">
                  {/* Label with flame icon */}
                  <div className="w-10 shrink-0 flex items-center gap-0.5">
                    <Flame className="h-3.5 w-3.5 text-orange-500" />
                    <span className="text-xs font-medium text-orange-600 dark:text-orange-400">飙升</span>
                  </div>

                  {/* Top mover cards */}
                  <div className="flex-1 flex gap-2 overflow-x-auto scrollbar-thin scrollbar-thumb-muted scrollbar-track-transparent pb-1">
                    {data.top_movers.map((item, index) => (
                      <TopMoverCell
                        key={item.code}
                        item={item}
                        index={index}
                        onClick={() => handleItemClick(item.code)}
                      />
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Category rows */}
            <div className="space-y-2">
              {categories.map((category) => (
                <div key={category.category} className="flex items-center gap-2">
                  {/* Category label */}
                  <div className="w-10 shrink-0 text-xs font-medium text-muted-foreground">
                    {category.label}
                  </div>

                  {/* ETF cards - horizontal scroll */}
                  <div className="flex-1 flex gap-1.5 overflow-x-auto scrollbar-thin scrollbar-thumb-muted scrollbar-track-transparent pb-1">
                    {category.items.map((item, index) => (
                      <EtfCell
                        key={item.code}
                        item={item}
                        index={index}
                        onClick={() => handleItemClick(item.code)}
                      />
                    ))}
                  </div>

                  {/* Category average */}
                  {category.avg_change_pct !== null && category.avg_change_pct !== undefined && (
                    <div className={cn(
                      'shrink-0 text-xs font-mono px-2 py-0.5 rounded',
                      Number(category.avg_change_pct) > 0
                        ? 'text-profit bg-red-50 dark:bg-red-900/20'
                        : Number(category.avg_change_pct) < 0
                          ? 'text-loss bg-green-50 dark:bg-green-900/20'
                          : 'text-muted-foreground'
                    )}>
                      {formatChangePct(Number(category.avg_change_pct))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </TooltipProvider>
        </div>
      </CardContent>
    </Card>
  )
}
