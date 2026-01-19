import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import { createChart, ColorType, IChartApi, CandlestickData, Time, SeriesMarker, SeriesMarkerPosition, SeriesMarkerShape, WhitespaceData } from 'lightweight-charts'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useTheme } from '@/components/theme-provider'
import { getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore, getKlineOnDate, StockData, roundToLot, chartSyncManager } from '@/lib/dynamic-backtest'

interface MiniKlineChartProps {
  stock: StockData
  height?: number
  sharedDates: string[]
}

function MiniKlineChart({ stock, height = 180, sharedDates }: MiniKlineChartProps) {
  const store = useDynamicBacktestStore()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const chartIdRef = useRef(`kline-${stock.code}`)
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
  const [tradeError, setTradeError] = useState<string | null>(null)
  
  const trades = store.trades.filter(t => t.stockCode === stock.code)
  
  // 计算选中日期的可用现金和持仓状态
  const stateAtDate = useMemo(() => {
    if (!tradePopup) return null
    return store.getStateAtDate(tradePopup.date)
  }, [tradePopup, store])
  
  const availableCashAtDate = stateAtDate?.availableCash ?? 0
  const positionAtDate = stateAtDate?.positions.get(stock.code)
  
  // 计算仓位快捷选项
  const calculatePositionShares = useCallback((ratio: number) => {
    if (!tradePopup) return 0
    if (tradeType === 'BUY') {
      // 买入：用选中日期的可用资金计算
      const amount = availableCashAtDate * ratio
      return roundToLot(amount / tradePopup.price)
    } else {
      // 卖出：用选中日期的持仓计算
      if (!positionAtDate) return 0
      return roundToLot(positionAtDate.totalShares * ratio)
    }
  }, [tradePopup, tradeType, availableCashAtDate, positionAtDate])
  
  // 处理交易提交
  const handleSubmitTrade = useCallback(() => {
    if (!tradePopup || !tradeShares) return
    
    const shares = Number(tradeShares)
    if (shares <= 0) return
    
    setTradeError(null)
    
    const result = store.addTrade({
      stockCode: stock.code,
      type: tradeType,
      date: tradePopup.date,
      price: tradePopup.price,
      mode: 'shares',
      inputValue: shares,
    })
    
    if (result.success) {
      setTradePopup(null)
      setTradeShares('')
      setTradeError(null)
    } else {
      setTradeError(result.error || '交易失败')
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
        minimumWidth: 80,
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        minBarSpacing: 0.5,
        visible: false,
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
      minimumWidth: 80,
    })
    
    const volumeData = stock.kline.map(d => ({
      time: d.date as Time,
      value: d.volume,
      color: d.close >= d.open ? marketColors.profit + '40' : marketColors.loss + '40',
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    volumeSeries.setData(volumeData)

    // 添加隐藏时间轴系列，确保时间对齐（缺数据也保留时间轴）
    if (sharedDates.length > 0) {
      const timeAxisSeries = chart.addLineSeries({
        color: 'transparent',
        lineWidth: 1,
        lastValueVisible: false,
        priceLineVisible: false,
        crosshairMarkerVisible: false,
      })
      const timeAxisData: WhitespaceData<Time>[] = sharedDates.map(date => ({
        time: date as Time,
      }))
      timeAxisSeries.setData(timeAxisData)
    }
    
    chart.timeScale().fitContent()
    
    // 注册到同步管理器
    chartSyncManager.register(chartIdRef.current, chart)
    
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
    
    const chartId = chartIdRef.current
    return () => {
      window.removeEventListener('resize', handleResize)
      chartSyncManager.unregister(chartId)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [stock, trades, isDark, height, sharedDates])
  
  return (
    <div className="relative">
      <div ref={chartContainerRef} className="border-x border-b" />
      <div className="absolute left-0 top-0 z-10 flex items-center gap-2 bg-[#d1b2ad]/35 px-2 py-1 text-sm text-foreground">
        <button
          onClick={() => store.removeStock(stock.code)}
          className="text-muted-foreground hover:text-destructive"
          aria-label={`移除${stock.code}`}
        >
          <X className="h-3 w-3" />
        </button>
        <span className="font-mono font-medium">{stock.code}</span>
        <span className="text-muted-foreground">{stock.name}</span>
      </div>
      
      {/* 交易弹出菜单 */}
      {tradePopup && (
        <div 
          className="absolute z-50 bg-background border rounded-lg shadow-lg p-2 w-52"
          style={{ 
            left: Math.min(tradePopup.x, (chartContainerRef.current?.clientWidth || 300) - 220),
            top: Math.min(tradePopup.y, height - 200),
          }}
        >
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">{tradePopup.date}</span>
              <button 
                onClick={() => {
                  setTradePopup(null)
                  setTradeError(null)
                }}
                className="text-muted-foreground hover:text-foreground"
              >
                <X className="h-3 w-3" />
              </button>
            </div>
            <div className="text-xs text-muted-foreground space-y-0.5">
              <p>收盘价: ¥{tradePopup.price.toFixed(2)}</p>
              <p>可用资金: <span className="font-mono">¥{availableCashAtDate.toLocaleString('zh-CN', { maximumFractionDigits: 0 })}</span></p>
              {positionAtDate && (
                <p>持仓: <span className="font-mono">{positionAtDate.totalShares}</span> 股</p>
              )}
            </div>
            
            {/* 买卖切换 */}
            <div className="flex rounded-md border p-0.5 bg-muted/30">
              <button
                type="button"
                onClick={() => {
                  setTradeType('BUY')
                  setTradeError(null)
                }}
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
                onClick={() => {
                  setTradeType('SELL')
                  setTradeError(null)
                }}
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
            
            {/* 快捷仓位按钮 */}
            <div className="flex gap-1">
              {[
                { label: '1/4仓', ratio: 0.25 },
                { label: '半仓', ratio: 0.5 },
                { label: '全仓', ratio: 1 },
              ].map(({ label, ratio }) => {
                const shares = calculatePositionShares(ratio)
                return (
                  <Button
                    key={label}
                    variant="outline"
                    size="sm"
                    className="flex-1 h-6 text-xs"
                    onClick={() => setTradeShares(String(shares))}
                    disabled={shares === 0}
                    title={`${shares}股`}
                  >
                    {label}
                  </Button>
                )
              })}
            </div>
            
            {/* 错误提示 */}
            {tradeError && (
              <p className="text-xs text-destructive">{tradeError}</p>
            )}
            
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

export function MultiStockKlinePanel() {
  const { stocks } = useDynamicBacktestStore()
  
  const sharedDates = useMemo(() => {
    const dateSet = new Set<string>()
    stocks.forEach(stock => {
      stock.kline.forEach(k => {
        dateSet.add(k.date)
      })
    })
    return Array.from(dateSet).sort()
  }, [stocks])
  
  const stockList = Array.from(stocks.values())
  const chartHeight = Math.round(Math.max(160, 220 - stockList.length * 15) * 0.75)
  
  if (stockList.length === 0) {
    return (
      <div className="flex h-[200px] items-center justify-center bg-muted/30 border">
        <p className="text-muted-foreground">添加股票查看K线</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-0">
      {stockList.map((stock, index) => (
        <div
          key={stock.code}
          className={index === 0 ? '' : '-mt-px'}
        >
          <MiniKlineChart
            stock={stock}
            height={chartHeight}
            sharedDates={sharedDates}
          />
        </div>
      ))}
    </div>
  )
}
