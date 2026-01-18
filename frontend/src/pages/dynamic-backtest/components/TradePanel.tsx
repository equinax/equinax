import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useDynamicBacktestStore, TradeMode, getKlineOnDate } from '@/lib/dynamic-backtest'

export function TradePanel() {
  const store = useDynamicBacktestStore()
  const [tradeType, setTradeType] = useState<'BUY' | 'SELL'>('BUY')
  const [tradeMode, setTradeMode] = useState<TradeMode>('amount')
  const [tradeDate, setTradeDate] = useState('')
  const [inputValue, setInputValue] = useState('')
  const [error, setError] = useState<string | null>(null)
  
  const selectedStock = store.selectedStockCode ? store.stocks.get(store.selectedStockCode) : null
  
  // 获取选中日期的价格
  const datePrice = selectedStock && tradeDate
    ? getKlineOnDate(selectedStock.kline, tradeDate)?.close
    : null
  
  const handleSubmit = () => {
    setError(null)
    
    if (!store.selectedStockCode || !selectedStock) {
      setError('请先选择股票')
      return
    }
    
    if (!tradeDate) {
      setError('请选择交易日期')
      return
    }
    
    if (!inputValue || Number(inputValue) <= 0) {
      setError('请输入有效的交易数值')
      return
    }
    
    const kline = getKlineOnDate(selectedStock.kline, tradeDate)
    if (!kline) {
      setError('该日期无交易数据，请选择交易日')
      return
    }
    
    const success = store.addTrade({
      stockCode: store.selectedStockCode,
      type: tradeType,
      date: tradeDate,
      price: kline.close,
      mode: tradeMode,
      inputValue: Number(inputValue),
    })
    
    if (success) {
      setInputValue('')
      setTradeDate('')
    } else {
      setError(tradeType === 'BUY' ? '资金不足或数量太小' : '持仓不足')
    }
  }
  
  if (!selectedStock) {
    return null
  }
  
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">交易操作</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* 买入/卖出切换 */}
        <div className="flex rounded-md border p-0.5 bg-muted/30">
          <button
            type="button"
            onClick={() => setTradeType('BUY')}
            className={`flex-1 py-1.5 text-sm rounded font-medium transition-colors ${
              tradeType === 'BUY'
                ? 'bg-green-500/20 text-green-600 dark:text-green-400'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            买入
          </button>
          <button
            type="button"
            onClick={() => setTradeType('SELL')}
            className={`flex-1 py-1.5 text-sm rounded font-medium transition-colors ${
              tradeType === 'SELL'
                ? 'bg-red-500/20 text-red-600 dark:text-red-400'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            卖出
          </button>
        </div>
        
        {/* 交易日期 */}
        <div className="space-y-1.5">
          <label className="text-sm font-medium">交易日期</label>
          <Input
            type="date"
            value={tradeDate}
            onChange={(e) => setTradeDate(e.target.value)}
            min={store.startDate}
            max={store.endDate}
          />
          {datePrice && (
            <p className="text-xs text-muted-foreground">
              收盘价: ¥{datePrice.toFixed(2)}
            </p>
          )}
        </div>
        
        {/* 交易方式 */}
        <div className="space-y-1.5">
          <label className="text-sm font-medium">交易方式</label>
          <div className="flex gap-1">
            <Button
              variant={tradeMode === 'amount' ? 'default' : 'outline'}
              size="sm"
              className="flex-1 h-8"
              onClick={() => setTradeMode('amount')}
            >
              金额
            </Button>
            <Button
              variant={tradeMode === 'percent' ? 'default' : 'outline'}
              size="sm"
              className="flex-1 h-8"
              onClick={() => setTradeMode('percent')}
            >
              {tradeType === 'BUY' ? '资金比例' : '持仓比例'}
            </Button>
            <Button
              variant={tradeMode === 'shares' ? 'default' : 'outline'}
              size="sm"
              className="flex-1 h-8"
              onClick={() => setTradeMode('shares')}
            >
              股数
            </Button>
          </div>
        </div>
        
        {/* 数值输入 */}
        <div className="space-y-1.5">
          <label className="text-sm font-medium">
            {tradeMode === 'amount' && '金额 (元)'}
            {tradeMode === 'percent' && '比例 (%)'}
            {tradeMode === 'shares' && '股数'}
          </label>
          <Input
            type="number"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            placeholder={
              tradeMode === 'amount' ? '如：10000'
                : tradeMode === 'percent' ? '如：50'
                : '如：100'
            }
            step={tradeMode === 'shares' ? 100 : 1}
          />
          {tradeMode === 'shares' && (
            <p className="text-xs text-muted-foreground">
              A股需为100的整数倍
            </p>
          )}
        </div>
        
        {/* 错误提示 */}
        {error && (
          <p className="text-sm text-destructive">{error}</p>
        )}
        
        {/* 提交按钮 */}
        <Button
          className="w-full"
          variant={tradeType === 'BUY' ? 'default' : 'destructive'}
          onClick={handleSubmit}
        >
          {tradeType === 'BUY' ? '确认买入' : '确认卖出'}
        </Button>
        
        {/* 快捷操作 */}
        {tradeType === 'SELL' && (
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              className="flex-1"
              onClick={() => {
                setTradeMode('percent')
                setInputValue('50')
              }}
            >
              半仓
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="flex-1"
              onClick={() => {
                setTradeMode('percent')
                setInputValue('100')
              }}
            >
              清仓
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
