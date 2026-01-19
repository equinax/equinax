import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Trash2 } from 'lucide-react'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'

interface TradeListProps {
  variant?: 'card' | 'embedded'
}

export function TradeList({ variant = 'card' }: TradeListProps) {
  const { trades, removeTrade, stocks } = useDynamicBacktestStore()
  
  // 显示所有股票的交易，按时间顺序
  const allTrades = [...trades].sort((a, b) => a.date.localeCompare(b.date))

  const header = (
    <div className="flex items-center justify-between">
      <CardTitle className="text-base">交易记录</CardTitle>
      {allTrades.length > 0 && (
        <span className="text-sm text-muted-foreground">
          共 {allTrades.length} 笔
        </span>
      )}
    </div>
  )

  const content = allTrades.length === 0 ? (
    <p className="text-sm text-muted-foreground text-center py-4">
      暂无交易记录
    </p>
  ) : (
    <div className="space-y-1.5">
      {allTrades.map(trade => {
        const stock = stocks.get(trade.stockCode)
        return (
          <div
            key={trade.id}
            className="flex items-center justify-between px-0 py-0.5 rounded-sm bg-muted/20 text-sm"
          >
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span
                  className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                    trade.type === 'BUY'
                      ? 'bg-green-500/20 text-green-600 dark:text-green-400'
                      : 'bg-red-500/20 text-red-600 dark:text-red-400'
                  }`}
                >
                  {trade.type === 'BUY' ? '买入' : '卖出'}
                </span>
                <span className="font-mono text-xs">{trade.date}</span>
                <span className="text-xs text-muted-foreground">
                  {stock?.name || trade.stockCode}
                </span>
              </div>
              <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
                <span>{trade.executedShares}</span>
                <span>¥{trade.price.toFixed(2)}</span>
                <span className={trade.type === 'BUY' ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}>
                  ¥{trade.executedAmount.toFixed(0)}
                </span>
              </div>
            </div>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 w-7 p-0 text-muted-foreground hover:text-destructive"
              onClick={() => removeTrade(trade.id)}
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
          </div>
        )
      })}
    </div>
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
      <CardContent className="p-3 pt-0">
        {content}
      </CardContent>
    </Card>
  )
}
