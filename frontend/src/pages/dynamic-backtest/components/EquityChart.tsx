import { useEffect, useRef } from 'react'
import { createChart, ColorType, IChartApi, LineData, Time, LogicalRange } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'

interface EquityChartProps {
  height?: number
  onVisibleRangeChange?: (range: LogicalRange | null) => void
  syncedRange?: LogicalRange | null
}

export function EquityChart({ height = 180, onVisibleRangeChange, syncedRange }: EquityChartProps) {
  const { equityCurve, benchmark } = useDynamicBacktestStore()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  
  useEffect(() => {
    if (!chartContainerRef.current) return
    
    // 清理现有图表
    if (chartRef.current) {
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
    
    // 监听时间范围变化
    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (range && onVisibleRangeChange) {
        const dataLength = equityData.length
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
  }, [equityCurve, benchmark, isDark, height, onVisibleRangeChange])
  
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
  
  if (equityCurve.length === 0) {
    return (
      <div className="relative">
        <div className="flex items-center justify-between px-2 py-1 bg-muted/30 rounded-t border-x border-t text-sm">
          <div className="flex items-center gap-2">
            <span className="font-medium">权益曲线</span>
          </div>
        </div>
        <div 
          className="flex items-center justify-center bg-muted/30 border-x border-b rounded-b"
          style={{ height }}
        >
          <p className="text-muted-foreground text-sm">添加股票并执行交易后显示</p>
        </div>
      </div>
    )
  }
  
  return (
    <div className="relative">
      <div className="flex items-center justify-between px-2 py-1 bg-muted/30 rounded-t border-x border-t text-sm">
        <div className="flex items-center gap-2">
          <span className="font-medium">权益曲线</span>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-0.5 bg-blue-500 rounded" />
            <span className="text-muted-foreground">策略</span>
          </div>
          {benchmark && (
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-0.5 bg-gray-500 rounded opacity-60" />
              <span className="text-muted-foreground">{benchmark.name}</span>
            </div>
          )}
        </div>
      </div>
      <div ref={chartContainerRef} className="border-x border-b rounded-b" />
    </div>
  )
}
