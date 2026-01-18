import { useEffect, useRef } from 'react'
import { createChart, ColorType, IChartApi, LineData, Time } from 'lightweight-charts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useTheme } from '@/components/theme-provider'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useDynamicBacktestStore } from '@/lib/dynamic-backtest'

export function EquityChart() {
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
      height: 250,
      rightPriceScale: {
        borderColor: chartColors.border,
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
      },
      crosshair: {
        mode: 1,
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
  }, [equityCurve, benchmark, isDark])
  
  if (equityCurve.length === 0) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">权益曲线</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-[250px] items-center justify-center bg-muted/30 rounded-lg">
            <p className="text-muted-foreground">添加股票并执行交易后显示权益曲线</p>
          </div>
        </CardContent>
      </Card>
    )
  }
  
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">权益曲线</CardTitle>
          <div className="flex items-center gap-4 text-sm">
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-0.5 bg-blue-500 rounded" />
              <span className="text-muted-foreground">策略</span>
            </div>
            {benchmark && (
              <div className="flex items-center gap-1.5">
                <div className="w-3 h-0.5 bg-gray-500 rounded" style={{ borderStyle: 'dashed' }} />
                <span className="text-muted-foreground">{benchmark.name}</span>
              </div>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="pb-4">
        <div ref={chartContainerRef} className="rounded-lg border" />
      </CardContent>
    </Card>
  )
}
