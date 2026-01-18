import { useEffect, useRef, useState } from 'react'
import { createChart, ColorType, IChartApi, CandlestickData, Time, SeriesMarker, SeriesMarkerPosition, SeriesMarkerShape } from 'lightweight-charts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { useTheme } from '@/components/theme-provider'
import { getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'

type TimeRange = '1m' | '3m' | '6m' | '1y' | 'all'

const timeRanges: { value: TimeRange; label: string; days: number }[] = [
  { value: '1m', label: '1月', days: 30 },
  { value: '3m', label: '3月', days: 90 },
  { value: '6m', label: '6月', days: 180 },
  { value: '1y', label: '1年', days: 365 },
  { value: 'all', label: '全部', days: -1 },
]

export function StockKlinePanel() {
  const { selectedStockCode, stocks, trades } = useDynamicBacktestStore()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const [timeRange, setTimeRange] = useState<TimeRange>('all')
  const [selectedDate, setSelectedDate] = useState<string | null>(null)
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  
  const stock = selectedStockCode ? stocks.get(selectedStockCode) : null
  const stockTrades = trades.filter(t => t.stockCode === selectedStockCode)
  
  // 过滤数据
  const filteredData = (() => {
    if (!stock || stock.kline.length === 0) return []
    
    const range = timeRanges.find(r => r.value === timeRange)
    if (!range || range.days === -1) return stock.kline
    
    const sortedDates = stock.kline.map(d => d.date).sort((a, b) => b.localeCompare(a))
    if (sortedDates.length === 0) return stock.kline
    const latestDate = new Date(sortedDates[0])
    
    const cutoffDate = new Date(latestDate)
    cutoffDate.setDate(cutoffDate.getDate() - range.days)
    const cutoffStr = cutoffDate.toISOString().split('T')[0]
    
    return stock.kline.filter(d => d.date >= cutoffStr)
  })()
  
  useEffect(() => {
    if (!chartContainerRef.current || filteredData.length === 0) return
    
    // 清理现有图表
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
      height: 350,
      rightPriceScale: {
        borderColor: chartColors.border,
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        mode: 1,
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
    
    const chartData: CandlestickData<Time>[] = filteredData.map(d => ({
      time: d.date as Time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    candlestickSeries.setData(chartData)
    
    // 添加交易标记
    const markers: SeriesMarker<Time>[] = stockTrades.map(trade => ({
      time: trade.date as Time,
      position: (trade.type === 'BUY' ? 'belowBar' : 'aboveBar') as SeriesMarkerPosition,
      color: trade.type === 'BUY' ? marketColors.profit : marketColors.loss,
      shape: (trade.type === 'BUY' ? 'arrowUp' : 'arrowDown') as SeriesMarkerShape,
      text: `${trade.type === 'BUY' ? 'B' : 'S'} ${trade.executedShares}股`,
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    candlestickSeries.setMarkers(markers)
    
    // 成交量
    const volumeSeries = chart.addHistogramSeries({
      color: chartColors.text,
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    })
    
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    })
    
    const volumeData = filteredData.map(d => ({
      time: d.date as Time,
      value: d.volume,
      color: d.close >= d.open ? marketColors.profit + '60' : marketColors.loss + '60',
    })).sort((a, b) => (a.time as string).localeCompare(b.time as string))
    
    volumeSeries.setData(volumeData)
    
    chart.timeScale().fitContent()
    
    // 点击选择日期
    chart.subscribeClick((param) => {
      if (param.time) {
        setSelectedDate(param.time as string)
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
  }, [filteredData, stockTrades, isDark])
  
  if (!stock) {
    return (
      <Card>
        <CardContent className="flex h-[400px] items-center justify-center">
          <p className="text-muted-foreground">选择股票查看K线</p>
        </CardContent>
      </Card>
    )
  }
  
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">
            {stock.code} - {stock.name}
          </CardTitle>
          <div className="flex gap-1">
            {timeRanges.map(range => (
              <Button
                key={range.value}
                variant={timeRange === range.value ? 'default' : 'outline'}
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={() => setTimeRange(range.value)}
              >
                {range.label}
              </Button>
            ))}
          </div>
        </div>
        {selectedDate && (
          <p className="text-sm text-muted-foreground">
            选中日期: {selectedDate}
          </p>
        )}
      </CardHeader>
      <CardContent className="pb-4">
        <div ref={chartContainerRef} className="rounded-lg border" />
      </CardContent>
    </Card>
  )
}
