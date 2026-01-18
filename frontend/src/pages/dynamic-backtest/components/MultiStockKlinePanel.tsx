import { useEffect, useRef, useState, useCallback } from 'react'
import { createChart, ColorType, IChartApi, CandlestickData, Time, SeriesMarker, SeriesMarkerPosition, SeriesMarkerShape, LogicalRange } from 'lightweight-charts'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useTheme } from '@/components/theme-provider'
import { getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore, getKlineOnDate, StockData } from '@/lib/dynamic-backtest'

interface MiniKlineChartProps {
  stock: StockData
  height?: number
  onVisibleRangeChange?: (range: LogicalRange | null) => void
  syncedRange?: LogicalRange | null
}

function MiniKlineChart({ stock, height = 180, onVisibleRangeChange, syncedRange }: MiniKlineChartProps) {
  const store = useDynamicBacktestStore()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  
  const [tradePopup, setTradePopup] = useState<{
    open: boolean
    date: string
    price: number
    x: number
    y: number
  } | null>(null)
  const [tradeType, setTradeType] = useState<'BUY' | 'SELL'>('BUY')
  const [tradeShares, setTradeShares] = useState('')
  
  const trades = store.trades.filter(t => t.stockCode === stock.code)
  
  // 处理交易提交
  const handleSubmitTrade = useCallback(() => {
    if (!tradePopup || !tradeShares) return
    
    const shares = Number(tradeShares)
    if (shares <= 0) return
    
    const success = store.addTrade({
      stockCode: stock.code,
      type: tradeType,
      date: tradePopup.date,
      price: tradePopup.price,
      mode: 'shares',
      inputValue: shares,
    })
    
    if (success) {
      setTradePopup(null)
      setTradeShares('')
    }
  }, [tradePopup, tradeShares, tradeType, stock.code, store])
  
  useEffect(() => {
    if (!chartContainerRef.current || stock.kline.length === 0) return
    
    if (chartRef.current) {
      chartRef.current.remove()
      chartRef.current = null
    }
    
    const chartColors = getChartThemeColors(isDark)
    const marketColors = getMarketColors()
    
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: chartColors.text,
      },
      grid: {
        vertLines: { color: chartColors.grid },
        horzLines: { color: chartColors.grid },
      },
      width: chartContainerRef.current.clientWidth,
      height,
      rightPriceScale: {
        borderColor: chartColors.border,
        scaleMargins: { top: 0.1, bottom: 0.15 },
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        minBarSpacing: 0.5,
      },
      crosshair: {
        mode: 1,
      },
      handleScale: {
        axisPressedMouseMove: {
          time: true,
          price: true,
        },
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
    })
    
    chartRef.current = chart
    
    // K线系列
    const candlestickSeries = chart.addCandlestickSeries({
      upColor: marketColors.profit,
      downColor: marketColors.loss,
      borderUpColor: marketColors.profit,
      borderDownColor: marketColors.loss,
      wickUpColor: marketColors.profit,
      wickDownColor: marketColors.loss,
    })
    
    const chartData: CandlestickData<Time>[] = stock.kline.map(d => ({
      time: d.date as Time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    candlestickSeries.setData(chartData)
    
    // 添加交易标记
    const markers: SeriesMarker<Time>[] = trades.map(trade => ({
      time: trade.date as Time,
      position: (trade.type === 'BUY' ? 'belowBar' : 'aboveBar') as SeriesMarkerPosition,
      color: trade.type === 'BUY' ? marketColors.profit : marketColors.loss,
      shape: (trade.type === 'BUY' ? 'arrowUp' : 'arrowDown') as SeriesMarkerShape,
      text: `${trade.type === 'BUY' ? 'B' : 'S'}${trade.executedShares}`,
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    candlestickSeries.setMarkers(markers)
    
    // 成交量
    const volumeSeries = chart.addHistogramSeries({
      color: chartColors.text,
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    })
    
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.9, bottom: 0 },
    })
    
    const volumeData = stock.kline.map(d => ({
      time: d.date as Time,
      value: d.volume,
      color: d.close >= d.open ? marketColors.profit + '40' : marketColors.loss + '40',
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    volumeSeries.setData(volumeData)
    
    chart.timeScale().fitContent()
    
    // 监听时间范围变化
    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && onVisibleRangeChange) {
        // 限制不能超出数据范围
        const dataLength = chartData.length
        const clampedFrom = Math.max(0, range.from)
        const clampedTo = Math.min(dataLength - 1, range.to)
        
        if (clampedFrom !== range.from || clampedTo !== range.to) {
          chart.timeScale().setVisibleLogicalRange({
            from: clampedFrom,
            to: clampedTo,
          })
        } else {
          onVisibleRangeChange(range)
        }
      }
    })
    
    // 点击事件 - 弹出交易菜单
    chart.subscribeClick((param) => {
      if (param.time && param.point) {
        const dateStr = param.time as string
        const kline = getKlineOnDate(stock.kline, dateStr)
        if (kline) {
          setTradePopup({
            open: true,
            date: dateStr,
            price: kline.close,
            x: param.point.x,
            y: param.point.y,
          })
          setTradeType('BUY')
          setTradeShares('')
        }
      }
    })
    
    // 调整大小
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        })
      }
    }
    
    window.addEventListener('resize', handleResize)
    
    return () => {
      window.removeEventListener('resize', handleResize)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [stock, trades, isDark, height, onVisibleRangeChange])
  
  // 同步缩放范围
  useEffect(() => {
    if (chartRef.current && syncedRange) {
      const currentRange = chartRef.current.timeScale().getVisibleLogicalRange()
      if (currentRange && 
          (Math.abs(currentRange.from - syncedRange.from) > 0.5 || 
           Math.abs(currentRange.to - syncedRange.to) > 0.5)) {
        chartRef.current.timeScale().setVisibleLogicalRange(syncedRange)
      }
    }
  }, [syncedRange])
  
  return (
    <div className="relative">
      <div className="flex items-center justify-between px-2 py-1 bg-muted/30 rounded-t border-x border-t text-sm">
        <div className="flex items-center gap-2">
          <span className="font-mono font-medium">{stock.code}</span>
          <span className="text-muted-foreground">{stock.name}</span>
        </div>
        <button
          onClick={() => store.removeStock(stock.code)}
          className="text-muted-foreground hover:text-destructive p-1"
        >
          <X className="h-3 w-3" />
        </button>
      </div>
      <div ref={chartContainerRef} className="border-x border-b rounded-b" />
      
      {/* 交易弹出菜单 */}
      {tradePopup && (
        <div 
          className="absolute z-50 bg-background border rounded-lg shadow-lg p-3 w-48"
          style={{ 
            left: Math.min(tradePopup.x, (chartContainerRef.current?.clientWidth || 300) - 200),
            top: Math.min(tradePopup.y, height - 180),
          }}
        >
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">{tradePopup.date}</span>
              <button 
                onClick={() => setTradePopup(null)}
                className="text-muted-foreground hover:text-foreground"
              >
                <X className="h-3 w-3" />
              </button>
            </div>
            <p className="text-xs text-muted-foreground">
              收盘价: ¥{tradePopup.price.toFixed(2)}
            </p>
            
            {/* 买卖切换 */}
            <div className="flex rounded-md border p-0.5 bg-muted/30">
              <button
                type="button"
                onClick={() => setTradeType('BUY')}
                className={`flex-1 py-1 text-xs rounded font-medium transition-colors ${
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
                className={`flex-1 py-1 text-xs rounded font-medium transition-colors ${
                  tradeType === 'SELL'
                    ? 'bg-red-500/20 text-red-600 dark:text-red-400'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                卖出
              </button>
            </div>
            
            {/* 股数输入 */}
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">股数</label>
              <Input
                type="number"
                value={tradeShares}
                onChange={(e) => setTradeShares(e.target.value)}
                placeholder="如: 100"
                step={100}
                className="h-8 text-sm"
                autoFocus
              />
            </div>
            
            {/* 快捷按钮 */}
            <div className="flex gap-1">
              {[100, 500, 1000].map(n => (
                <Button
                  key={n}
                  variant="outline"
                  size="sm"
                  className="flex-1 h-6 text-xs"
                  onClick={() => setTradeShares(String(n))}
                >
                  {n}
                </Button>
              ))}
            </div>
            
            {/* 确认按钮 */}
            <Button
              className="w-full h-8"
              variant={tradeType === 'BUY' ? 'default' : 'destructive'}
              onClick={handleSubmitTrade}
              disabled={!tradeShares || Number(tradeShares) <= 0}
            >
              确认{tradeType === 'BUY' ? '买入' : '卖出'}
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}

interface MultiStockKlinePanelProps {
  syncedRange?: LogicalRange | null
}

export function MultiStockKlinePanel({ syncedRange: externalSyncedRange }: MultiStockKlinePanelProps) {
  const { stocks } = useDynamicBacktestStore()
  const [internalSyncedRange, setInternalSyncedRange] = useState<LogicalRange | null>(null)
  
  // 使用外部同步范围（来自权益曲线）或内部同步范围
  const effectiveSyncedRange = externalSyncedRange || internalSyncedRange
  
  const stockList = Array.from(stocks.values())
  
  const handleRangeChange = useCallback((range: LogicalRange | null) => {
    setInternalSyncedRange(range)
  }, [])
  
  if (stockList.length === 0) {
    return (
      <div className="flex h-[200px] items-center justify-center bg-muted/30 rounded-lg border">
        <p className="text-muted-foreground">添加股票查看K线</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-2">
      {stockList.map((stock, index) => (
        <MiniKlineChart
          key={stock.code}
          stock={stock}
          height={Math.max(160, 220 - stockList.length * 15)}
          onVisibleRangeChange={index === 0 ? handleRangeChange : undefined}
          syncedRange={index === 0 ? externalSyncedRange : effectiveSyncedRange}
        />
      ))}
    </div>
  )
}
