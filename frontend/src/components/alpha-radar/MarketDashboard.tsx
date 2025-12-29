import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { DashboardResponse } from '@/api/generated/schemas'

interface MarketDashboardProps {
  data?: DashboardResponse
  isLoading?: boolean
}

// Regime color mapping
function getRegimeStyle(regime: string) {
  switch (regime) {
    case 'BULL':
      return {
        color: 'text-green-600 dark:text-green-400',
        bg: 'bg-green-100 dark:bg-green-900/30',
        label: '牛市',
      }
    case 'BEAR':
      return {
        color: 'text-red-600 dark:text-red-400',
        bg: 'bg-red-100 dark:bg-red-900/30',
        label: '熊市',
      }
    default:
      return {
        color: 'text-amber-600 dark:text-amber-400',
        bg: 'bg-amber-100 dark:bg-amber-900/30',
        label: '震荡',
      }
  }
}

// Score to color
function getScoreColor(score: number) {
  if (score > 30) return 'text-green-600 dark:text-green-400'
  if (score < -30) return 'text-red-600 dark:text-red-400'
  return 'text-amber-600 dark:text-amber-400'
}

export function MarketDashboard({ data, isLoading }: MarketDashboardProps) {
  if (isLoading) {
    return (
      <Card className="p-3">
        <div className="flex items-center gap-6">
          <Skeleton className="h-6 w-24" />
          <Skeleton className="h-6 w-32" />
          <Skeleton className="h-6 w-28" />
          <Skeleton className="h-6 w-36" />
        </div>
      </Card>
    )
  }

  if (!data) return null

  const { market_state, market_breadth, style_rotation, smart_money } = data
  const regimeStyle = getRegimeStyle(market_state.regime)
  const regimeScore = Number(market_state.regime_score)

  return (
    <Card className="px-4 py-2">
      <div className="flex items-center gap-4 text-sm flex-wrap">
        {/* Market State */}
        <div className="flex items-center gap-2">
          <Badge className={cn('text-xs', regimeStyle.bg, regimeStyle.color)}>
            {regimeStyle.label}
          </Badge>
          <span className={cn('font-bold font-mono', getScoreColor(regimeScore))}>
            {regimeScore > 0 ? '+' : ''}{regimeScore.toFixed(0)}
          </span>
        </div>

        <div className="h-4 w-px bg-border" />

        {/* Market Breadth */}
        <div className="flex items-center gap-3">
          <span className="text-profit flex items-center gap-0.5 font-mono">
            <TrendingUp className="h-3.5 w-3.5" />
            {market_breadth.up_count}
          </span>
          <span className="text-loss flex items-center gap-0.5 font-mono">
            <TrendingDown className="h-3.5 w-3.5" />
            {market_breadth.down_count}
          </span>
          <span className="text-muted-foreground font-mono">
            平{market_breadth.flat_count}
          </span>
          <span className="text-xs text-muted-foreground">
            比<span className="font-mono ml-0.5">{Number(market_breadth.up_down_ratio).toFixed(2)}</span>
          </span>
        </div>

        <div className="h-4 w-px bg-border" />

        {/* Limit up/down */}
        <div className="flex items-center gap-2 text-xs">
          <span className="text-profit font-mono">涨停{market_breadth.limit_up_count}</span>
          <span className="text-loss font-mono">跌停{market_breadth.limit_down_count}</span>
        </div>

        <div className="h-4 w-px bg-border" />

        {/* Style Rotation - simplified */}
        <div className="flex items-center gap-2 text-xs">
          <span className="text-muted-foreground">
            {style_rotation.dominant_style === 'large_value' ? '大盘价值' :
              style_rotation.dominant_style === 'small_growth' ? '小盘成长' : '风格均衡'}
          </span>
          <span className="font-mono">
            {Number(style_rotation.large_value_strength).toFixed(0)}:{Number(style_rotation.small_growth_strength).toFixed(0)}
          </span>
        </div>

        <div className="h-4 w-px bg-border" />

        {/* Smart Money */}
        <div className="flex items-center gap-3">
          <Badge
            variant="outline"
            className={cn(
              'text-xs',
              smart_money.money_flow_proxy === 'inflow' && 'border-green-500 text-green-600',
              smart_money.money_flow_proxy === 'outflow' && 'border-red-500 text-red-600',
              smart_money.money_flow_proxy === 'neutral' && 'border-amber-500 text-amber-600'
            )}
          >
            {smart_money.money_flow_proxy === 'inflow' ? '流入' :
              smart_money.money_flow_proxy === 'outflow' ? '流出' : '平衡'}
          </Badge>
          <span className="text-xs text-muted-foreground">
            量比<span className="font-mono ml-0.5">{Number(smart_money.market_avg_volume_ratio).toFixed(2)}</span>
          </span>
          <span className="text-xs">
            <span className="text-profit font-mono">吸{smart_money.accumulation_signal_count}</span>
            <span className="text-muted-foreground mx-1">/</span>
            <span className="text-loss font-mono">派{smart_money.distribution_signal_count}</span>
          </span>
        </div>
      </div>
    </Card>
  )
}
