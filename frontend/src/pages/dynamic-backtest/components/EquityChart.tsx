import { useEffect, useRef } from 'react'
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore, chartSyncManager } from '@/lib/dynamic-backtest'

interface EquityChartProps {
  height?: number
}

export function EquityChart({ height = 180 }: EquityChartProps) {
  const { equityCurve, benchmark, metrics } = useDynamicBacktestStore()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const chartIdRef = useRef('equity-curve')
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  
  useEffect(() => {
    if (!chartContainerRef.current) return
    
    // 清理现有图表
    if (chartRef.current) {
      chartSyncManager.unregister(chartIdRef.current)
      chartRef.current.remove()
      chartRef.current = null
    }
    
    if (equityCurve.length === 0) return
    
    const chartColors = getChartThemeColors(isDark)
    
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
        scaleMargins: { top: 0.1, bottom: 0.1 },
        minimumWidth: 80,
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
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
    
    // 权益曲线（归一化为收益率）
    const equitySeries = chart.addLineSeries({
      color: chartColors.equity,
      lineWidth: 2,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => (price * 100).toFixed(2) + '%',
      },
    })
    
    const equityData: LineData<Time>[] = equityCurve.map(e => ({
      time: e.date as Time,
      value: e.cumulativeReturn,
    }))
    
    equitySeries.setData(equityData)
    
    // 基准曲线
    if (benchmark && benchmark.kline.length > 0) {
      const benchmarkSeries = chart.addLineSeries({
        color: '#888888',
        lineWidth: 1,
        lineStyle: 2, // dashed
        priceFormat: {
          type: 'custom',
          formatter: (price: number) => (price * 100).toFixed(2) + '%',
        },
      })
      
      // 计算基准归一化收益
      const startDate = equityCurve[0]?.date
      const endDate = equityCurve[equityCurve.length - 1]?.date
      const filtered = benchmark.kline.filter(k => k.date >= startDate && k.date <= endDate)
      
      if (filtered.length > 0) {
        const basePrice = filtered[0].close
        const benchmarkData: LineData<Time>[] = filtered.map(k => ({
          time: k.date as Time,
          value: (k.close - basePrice) / basePrice,
        }))
        benchmarkSeries.setData(benchmarkData)
      }
    }
    
    chart.timeScale().fitContent()
    
    // 注册到同步管理器
    chartSyncManager.register(chartIdRef.current, chart)
    
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
      chartSyncManager.unregister(chartIdRef.current)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [equityCurve, benchmark, isDark, height])
  
  const metricsItems = metrics ? [
    { label: '总收益', value: metrics.totalReturn, format: 'percent', colorize: true },
    { label: '年化', value: metrics.annualReturn, format: 'percent', colorize: true },
    { label: '回撤', value: -metrics.maxDrawdown, format: 'percent', colorize: true },
    { label: '夏普', value: metrics.sharpeRatio, format: 'number', colorize: false },
    { label: '胜率', value: metrics.winRate, format: 'percent', colorize: false },
    { label: '超额', value: metrics.excessReturn, format: 'percent', colorize: true },
  ] : []
  
  const formatMetricValue = (value: number, format: string) => {
    if (format === 'percent') {
      return (value * 100).toFixed(2) + '%'
    }
    return value.toFixed(2)
  }
  
  const getMetricColor = (value: number, colorize: boolean) => {
    if (!colorize) return 'text-foreground'
    if (value > 0) return 'text-green-600 dark:text-green-400'
    if (value < 0) return 'text-red-600 dark:text-red-400'
    return 'text-foreground'
  }
  
  if (equityCurve.length === 0) {
    return (
      <div className="relative">
        <div className="flex items-center justify-between px-2 py-0.5 bg-muted/30 border-x border-t text-sm">
          <div className="flex items-center gap-2">
            <span className="font-medium">权益曲线</span>
          </div>
        </div>
        <div 
          className="flex items-center justify-center bg-muted/30 border-x border-b"
          style={{ height }}
        >
          <p className="text-muted-foreground text-sm">添加股票并执行交易后显示</p>
        </div>
      </div>
    )
  }
  
  return (
    <div className="relative">
      <div className="flex items-center justify-between gap-3 px-2 py-0.5 bg-muted/30 border-x border-t text-sm">
        <div className="flex items-center gap-2 flex-shrink-0">
          <span className="font-medium">权益曲线</span>
        </div>
        {metricsItems.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            {metricsItems.map(item => (
              <div key={item.label} className="flex items-center gap-1">
                <span>{item.label}</span>
                <span className={`font-mono font-medium ${getMetricColor(item.value, item.colorize)}`}>
                  {item.value > 0 && item.colorize ? '+' : ''}
                  {formatMetricValue(item.value, item.format)}
                </span>
              </div>
            ))}
          </div>
        )}
        <div className="flex items-center gap-3 text-xs flex-shrink-0">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-0.5 bg-blue-500" />
            <span className="text-muted-foreground">策略</span>
          </div>
          {benchmark && (
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-0.5 bg-gray-500 opacity-60" />
              <span className="text-muted-foreground">{benchmark.name}</span>
            </div>
          )}
        </div>
      </div>
      <div ref={chartContainerRef} className="border-x border-b" />
    </div>
  )
}
