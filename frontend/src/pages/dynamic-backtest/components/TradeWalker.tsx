import { useMemo } from 'react'
import { ChevronLeft, ChevronRight, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useDynamicBacktestStore, Trade } from '@/lib/dynamic-backtest'
import { cn } from '@/lib/utils'

interface TradeWalkerProps {
  sortedTrades: Trade[]
  currentIndex: number
  onNavigate: (direction: 'prev' | 'next') => void
}

export function TradeWalker({ sortedTrades, currentIndex, onNavigate }: TradeWalkerProps) {
  const store = useDynamicBacktestStore()
  
  const currentTrade = currentIndex >= 0 ? sortedTrades[currentIndex] : null
  const stock = currentTrade ? store.stocks.get(currentTrade.stockCode) : null
  
  // 计算配对交易信息
  const pairInfo = useMemo(() => {
    if (!currentTrade?.pairId) return null
    
    const pairTrade = sortedTrades.find(
      t => t.pairId === currentTrade.pairId && t.id !== currentTrade.id
    )
    if (!pairTrade) return null
    
    const buyTrade = currentTrade.type === 'BUY' ? currentTrade : pairTrade
    const sellTrade = currentTrade.type === 'SELL' ? currentTrade : pairTrade
    
    const holdingDays = Math.ceil(
      (new Date(sellTrade.date).getTime() - new Date(buyTrade.date).getTime()) / (1000 * 60 * 60 * 24)
    )
    
    const profit = sellTrade.executedAmount - sellTrade.totalCost - buyTrade.totalCost
    const profitPct = (profit / buyTrade.totalCost) * 100
    
    return {
      pairTrade,
      holdingDays,
      profit,
      profitPct,
      isProfit: profit > 0,
    }
  }, [currentTrade, sortedTrades])
  
  if (sortedTrades.length === 0) {
    return null
  }
  
  return (
    <div className="fixed top-2 left-1/2 -translate-x-1/2 z-50 flex items-center gap-2 px-3 py-2 bg-background/95 backdrop-blur border rounded-lg shadow-lg text-sm">
      {/* 导航按钮 */}
      <div className="flex items-center gap-1">
        <Button
          variant="ghost"
          size="sm"
          className="h-6 w-6 p-0"
          onClick={() => onNavigate('prev')}
          disabled={currentIndex <= 0}
          title="上一个 (← 或 K)"
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        
        <span className="text-xs text-muted-foreground w-12 text-center font-mono">
          {currentIndex >= 0 ? `${currentIndex + 1}/${sortedTrades.length}` : `-/${sortedTrades.length}`}
        </span>
        
        <Button
          variant="ghost"
          size="sm"
          className="h-6 w-6 p-0"
          onClick={() => onNavigate('next')}
          disabled={currentIndex >= sortedTrades.length - 1}
          title="下一个 (→ 或 J)"
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
      
      {/* 当前交易信息 */}
      {currentTrade && stock && (
        <>
          <div className="w-px h-4 bg-border" />
          
          <div className="flex items-center gap-2 flex-1 min-w-0">
            {/* 股票信息 */}
            <span className="font-mono text-xs">{currentTrade.stockCode.replace(/^(sh\.|sz\.)/, '')}</span>
            <span className="text-muted-foreground text-xs truncate">{stock.name}</span>
            
            {/* 交易信息 */}
            <span className="text-xs text-muted-foreground">{currentTrade.date}</span>
            <span className={cn(
              'text-xs font-medium px-1.5 py-0.5 rounded',
              currentTrade.type === 'BUY' 
                ? 'bg-green-500/20 text-green-600 dark:text-green-400' 
                : 'bg-red-500/20 text-red-600 dark:text-red-400'
            )}>
              {currentTrade.type === 'BUY' ? '买入' : '卖出'}
            </span>
            <span className="text-xs">
              {currentTrade.executedShares}股 × ¥{currentTrade.price.toFixed(2)}
            </span>
            
            {/* 配对交易信息 */}
            {pairInfo && (
              <>
                <span className="text-muted-foreground">|</span>
                <span className="text-xs text-muted-foreground">
                  持有 {pairInfo.holdingDays} 天
                </span>
                <span className={cn(
                  'text-xs font-medium',
                  pairInfo.isProfit ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
                )}>
                  {pairInfo.isProfit ? '+' : ''}{pairInfo.profit.toFixed(0)}
                  ({pairInfo.isProfit ? '+' : ''}{pairInfo.profitPct.toFixed(1)}%)
                </span>
              </>
            )}
          </div>
          
          {/* 清除选择 */}
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => store.setHighlightedTrade(null)}
            title="取消选中 (Esc)"
          >
            <X className="h-3 w-3" />
          </Button>
        </>
      )}
      
      {/* 未选中时显示提示 */}
      {!currentTrade && (
        <span className="text-xs text-muted-foreground">
          按 ← → 键浏览交易
        </span>
      )}
    </div>
  )
}
