import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { ComputingConsole } from '@/components/ui/computing-console'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { TrendingUp, TrendingDown } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import type { DashboardResponse } from '@/api/generated/schemas'

interface MarketDashboardProps {
  data?: DashboardResponse
  isLoading?: boolean
}

// Regime color mapping (A股惯例: 涨红跌绿)
function getRegimeStyle(regime: string) {
  switch (regime) {
    case 'BULL':
      return {
        color: 'text-profit',  // 红色 - 牛市
        bg: 'bg-profit/10 hover:bg-profit/20',
        label: '牛市',
      }
    case 'BEAR':
      return {
        color: 'text-loss',    // 绿色 - 熊市
        bg: 'bg-loss/10 hover:bg-loss/20',
        label: '熊市',
      }
    default:
      return {
        color: 'text-amber-600 dark:text-amber-400',
        bg: 'bg-amber-100 hover:bg-amber-200 dark:bg-amber-900/30 dark:hover:bg-amber-900/50',
        label: '震荡',
      }
  }
}

// Score to color (A股惯例: 涨红跌绿)
function getScoreColor(score: number) {
  if (score > 30) return 'text-profit'   // 红色 - 牛市
  if (score < -30) return 'text-loss'    // 绿色 - 熊市
  return 'text-amber-600 dark:text-amber-400'
}

export function MarketDashboard({ data, isLoading }: MarketDashboardProps) {
  const { steps, progress } = useComputingProgress(isLoading, 'dashboard')

  if (isLoading) {
    return (
      <Card className="px-4 py-3">
        <ComputingConsole
          title="正在计算市场数据..."
          steps={steps}
          progress={progress}
        />
      </Card>
    )
  }

  if (!data) return null

  const { market_state, market_breadth, style_rotation, smart_money } = data
  const regimeStyle = getRegimeStyle(market_state.regime)
  const regimeScore = Number(market_state.regime_score)

  return (
    <TooltipProvider delayDuration={200}>
      <Card className="px-4 py-2">
        <div className="flex items-center gap-4 text-sm flex-wrap">
          {/* Market State */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-2 cursor-help">
                <Badge className={cn('text-xs', regimeStyle.bg, regimeStyle.color)}>
                  {regimeStyle.label}
                </Badge>
                <span className={cn('font-bold font-mono', getScoreColor(regimeScore))}>
                  {regimeScore > 0 ? '+' : ''}{regimeScore.toFixed(0)}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="max-w-[280px] bg-popover text-popover-foreground border shadow-md">
              <p className="font-medium mb-1">市场状态指数</p>
              <p className="text-xs text-muted-foreground">
                综合20日累计收益(50%)、MA20偏离度(30%)、MA60偏离度(20%)计算。
                ≥30为牛市，≤-30为熊市，其余为震荡。范围-100~+100。
              </p>
            </TooltipContent>
          </Tooltip>

          <div className="h-4 w-px bg-border" />

          {/* Market Breadth */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-3 cursor-help">
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
            </TooltipTrigger>
            <TooltipContent side="bottom" className="max-w-[280px] bg-popover text-popover-foreground border shadow-md">
              <p className="font-medium mb-1">市场宽度</p>
              <p className="text-xs text-muted-foreground">
                统计当日上涨、下跌、平盘股票数量。涨跌比=上涨数/下跌数，
                &gt;1表示多数股票上涨，&lt;1表示多数股票下跌。
              </p>
            </TooltipContent>
          </Tooltip>

          <div className="h-4 w-px bg-border" />

          {/* Limit up/down */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-2 text-xs cursor-help">
                <span className="text-profit font-mono">涨停{market_breadth.limit_up_count}</span>
                <span className="text-loss font-mono">跌停{market_breadth.limit_down_count}</span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="max-w-[280px] bg-popover text-popover-foreground border shadow-md">
              <p className="font-medium mb-1">涨跌停统计</p>
              <p className="text-xs text-muted-foreground">
                当日涨停(≥9.9%)和跌停(≤-9.9%)股票数量。
                涨停多反映市场情绪亢奋，跌停多反映恐慌。
              </p>
            </TooltipContent>
          </Tooltip>

          <div className="h-4 w-px bg-border" />

          {/* Style Rotation - simplified */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-2 text-xs cursor-help">
                <span className="text-muted-foreground">
                  {style_rotation.dominant_style === 'large_value' ? '大盘价值' :
                    style_rotation.dominant_style === 'small_growth' ? '小盘成长' : '风格均衡'}
                </span>
                <span className="font-mono">
                  {Number(style_rotation.large_value_strength).toFixed(0)}:{Number(style_rotation.small_growth_strength).toFixed(0)}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="max-w-[280px] bg-popover text-popover-foreground border shadow-md">
              <p className="font-medium mb-1">风格轮动</p>
              <p className="text-xs text-muted-foreground">
                对比大盘价值股(上证50)与小盘成长股(中证1000)的相对强弱。
                数值为双方强度，差距&gt;10则显示主导风格。
              </p>
            </TooltipContent>
          </Tooltip>

          <div className="h-4 w-px bg-border" />

          {/* Smart Money */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-3 cursor-help">
                <Badge
                  className={cn(
                    'text-xs border-0',
                    smart_money.money_flow_proxy === 'inflow' && 'bg-profit/10 text-profit hover:bg-profit/20',
                    smart_money.money_flow_proxy === 'outflow' && 'bg-loss/10 text-loss hover:bg-loss/20',
                    smart_money.money_flow_proxy === 'neutral' && 'bg-amber-100 text-amber-600 hover:bg-amber-200 dark:bg-amber-900/30 dark:text-amber-400 dark:hover:bg-amber-900/50'
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
            </TooltipTrigger>
            <TooltipContent side="bottom" className="max-w-[320px] bg-popover text-popover-foreground border shadow-md">
              <p className="font-medium mb-1">聪明钱指标</p>
              <p className="text-xs text-muted-foreground mb-1">
                <span className="font-medium text-foreground">流入/流出:</span> 基于量价背离判断资金流向。
                放量下跌=吸筹(流入)，放量上涨=派发(流出)。
              </p>
              <p className="text-xs text-muted-foreground mb-1">
                <span className="font-medium text-foreground">量比:</span> 当日成交量/5日均量，&gt;1表示放量。
              </p>
              <p className="text-xs text-muted-foreground">
                <span className="font-medium text-foreground">吸/派:</span> 全市场吸筹和派发信号的股票数量。
              </p>
            </TooltipContent>
          </Tooltip>
        </div>
      </Card>
    </TooltipProvider>
  )
}
