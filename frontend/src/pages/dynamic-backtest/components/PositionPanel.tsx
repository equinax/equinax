import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'
import { TrendingUp, TrendingDown } from 'lucide-react'

interface PositionPanelProps {
  variant?: 'card' | 'embedded'
}

export function PositionPanel({ variant = 'card' }: PositionPanelProps) {
  const { positions, currentCash, initialCapital, stocks } = useDynamicBacktestStore()
  
  const positionList = Array.from(positions.values())
  const totalPositionValue = positionList.reduce((sum, p) => sum + p.currentValue, 0)
  const totalEquity = currentCash + totalPositionValue
  const totalPnL = totalEquity - initialCapital
  const totalPnLPercent = (totalPnL / initialCapital) * 100
  
  const header = (
    <div className="flex items-center justify-between gap-2">
      <CardTitle className="text-base">持仓与资金</CardTitle>
      <div className="flex items-center gap-3 text-xs">
        <span className="text-muted-foreground">可用</span>
        <span className="font-mono font-medium">¥{currentCash.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>
        <span className="text-muted-foreground">持仓</span>
        <span className="font-mono font-medium">¥{totalPositionValue.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>
      </div>
    </div>
  )

  const content = (
    <>
      {/* 总资产 */}
      <div className="p-2 rounded-md border">
        <div className="flex items-center justify-between">
          <span className="text-sm text-muted-foreground">总资产</span>
          <span className="font-mono font-semibold">
            ¥{totalEquity.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}
          </span>
        </div>
        <div className="flex items-center justify-between mt-1">
          <span className="text-sm text-muted-foreground">总盈亏</span>
          <span className={`font-mono font-medium flex items-center gap-1 ${
            totalPnL >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
          }`}>
            {totalPnL >= 0 ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
            {totalPnL >= 0 ? '+' : ''}¥{totalPnL.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}
            <span className="text-xs">({totalPnLPercent >= 0 ? '+' : ''}{totalPnLPercent.toFixed(2)}%)</span>
          </span>
        </div>
      </div>

      {/* 持仓列表 */}
      {positionList.length > 0 ? (
        <div className="space-y-1.5">
          <p className="text-xs text-muted-foreground font-medium">当前持仓</p>
          {positionList.map(pos => {
            const stock = stocks.get(pos.stockCode)
            return (
              <div
                key={pos.stockCode}
                className="p-1.5 rounded-md bg-muted/30 text-sm"
              >
                <div className="flex items-center justify-between">
                  <span className="font-mono">{pos.stockCode}</span>
                  <span className={`font-mono ${
                    pos.unrealizedPnL >= 0 
                      ? 'text-green-600 dark:text-green-400' 
                      : 'text-red-600 dark:text-red-400'
                  }`}>
                    {pos.unrealizedPnL >= 0 ? '+' : ''}{(pos.unrealizedPnLPercent * 100).toFixed(2)}%
                  </span>
                </div>
                <div className="flex items-center justify-between mt-1 text-xs text-muted-foreground">
                  <span>{stock?.name}</span>
                  <span>{pos.totalShares}股 / ¥{pos.currentValue.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span>
                </div>
                <div className="flex items-center justify-between mt-0.5 text-xs text-muted-foreground">
                  <span>成本: ¥{pos.avgCost.toFixed(2)}</span>
                  <span>现价: ¥{pos.currentPrice.toFixed(2)}</span>
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <p className="text-sm text-muted-foreground text-center py-2">
          暂无持仓
        </p>
      )}
    </>
  )

  if (variant === 'embedded') {
    return (
      <div className="space-y-2">
        {header}
        {content}
      </div>
    )
  }

  return (
    <Card>
      <CardHeader className="p-3 pb-2">
        {header}
      </CardHeader>
      <CardContent className="p-3 pt-0 space-y-2">
        {content}
      </CardContent>
    </Card>
  )
}
