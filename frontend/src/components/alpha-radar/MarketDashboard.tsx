import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { Progress } from '@/components/ui/progress'
import {
  TrendingUp,
  TrendingDown,
  Activity,
  ArrowRightLeft,
  Wallet,
} from 'lucide-react'
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
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i}>
            <CardContent className="p-4">
              <Skeleton className="h-4 w-20 mb-3" />
              <Skeleton className="h-8 w-28 mb-2" />
              <Skeleton className="h-4 w-full" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  if (!data) return null

  const { market_state, market_breadth, style_rotation, smart_money } = data
  const regimeStyle = getRegimeStyle(market_state.regime)
  const regimeScore = Number(market_state.regime_score)

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
      {/* Market State Card */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs text-muted-foreground font-medium">市场状态</p>
            <div className={cn('h-8 w-8 rounded-full flex items-center justify-center', regimeStyle.bg)}>
              <Activity className={cn('h-4 w-4', regimeStyle.color)} />
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Badge className={cn('text-sm', regimeStyle.bg, regimeStyle.color)}>
                {regimeStyle.label}
              </Badge>
              <span className={cn('text-lg font-bold', getScoreColor(regimeScore))}>
                {regimeScore > 0 ? '+' : ''}{regimeScore.toFixed(0)}
              </span>
            </div>
            <p className="text-xs text-muted-foreground">
              {market_state.regime_description}
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Market Breadth Card */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs text-muted-foreground font-medium">涨跌分布</p>
            <div className="h-8 w-8 rounded-full bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
              <TrendingUp className="h-4 w-4 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-4 text-sm">
              <span className="text-profit flex items-center gap-1">
                <TrendingUp className="h-3.5 w-3.5" />
                {market_breadth.up_count}
              </span>
              <span className="text-loss flex items-center gap-1">
                <TrendingDown className="h-3.5 w-3.5" />
                {market_breadth.down_count}
              </span>
              <span className="text-muted-foreground">
                平 {market_breadth.flat_count}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Progress
                value={(market_breadth.up_count / (market_breadth.up_count + market_breadth.down_count + market_breadth.flat_count)) * 100}
                className="h-1.5 flex-1"
              />
              <span className="text-xs font-mono">
                {Number(market_breadth.up_down_ratio).toFixed(2)}
              </span>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <span className="text-profit">涨停 {market_breadth.limit_up_count}</span>
              <span className="text-loss">跌停 {market_breadth.limit_down_count}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Style Rotation Card */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs text-muted-foreground font-medium">风格轮动</p>
            <div className="h-8 w-8 rounded-full bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
              <ArrowRightLeft className="h-4 w-4 text-purple-600 dark:text-purple-400" />
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs">
              <span>大盘价值</span>
              <span className="font-mono">{Number(style_rotation.large_value_strength).toFixed(0)}</span>
            </div>
            <Progress
              value={Number(style_rotation.large_value_strength)}
              className="h-1.5"
            />
            <div className="flex items-center justify-between text-xs">
              <span>小盘成长</span>
              <span className="font-mono">{Number(style_rotation.small_growth_strength).toFixed(0)}</span>
            </div>
            <Progress
              value={Number(style_rotation.small_growth_strength)}
              className="h-1.5"
            />
            <p className="text-xs text-muted-foreground">
              当前主导: {style_rotation.dominant_style === 'large_value' ? '大盘价值' :
                style_rotation.dominant_style === 'small_growth' ? '小盘成长' : '均衡'}
            </p>
          </div>
        </CardContent>
      </Card>

      {/* Smart Money Card */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs text-muted-foreground font-medium">资金流向</p>
            <div className="h-8 w-8 rounded-full bg-emerald-100 dark:bg-emerald-900/30 flex items-center justify-center">
              <Wallet className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Badge
                variant="outline"
                className={cn(
                  smart_money.money_flow_proxy === 'inflow' && 'border-green-500 text-green-600',
                  smart_money.money_flow_proxy === 'outflow' && 'border-red-500 text-red-600',
                  smart_money.money_flow_proxy === 'neutral' && 'border-amber-500 text-amber-600'
                )}
              >
                {smart_money.money_flow_proxy === 'inflow' ? '资金流入' :
                  smart_money.money_flow_proxy === 'outflow' ? '资金流出' : '资金平衡'}
              </Badge>
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>
                <span className="text-muted-foreground">量比</span>
                <p className="font-mono">{Number(smart_money.market_avg_volume_ratio).toFixed(2)}x</p>
              </div>
              <div>
                <span className="text-muted-foreground">高换手</span>
                <p className="font-mono">{smart_money.high_turnover_count}</p>
              </div>
              <div>
                <span className="text-muted-foreground text-profit">吸筹信号</span>
                <p className="font-mono text-profit">{smart_money.accumulation_signal_count}</p>
              </div>
              <div>
                <span className="text-muted-foreground text-loss">派发信号</span>
                <p className="font-mono text-loss">{smart_money.distribution_signal_count}</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
