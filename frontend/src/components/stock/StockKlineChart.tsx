import { useEffect, useRef, useState } from 'react'
import { createChart, ColorType, IChartApi, CandlestickData, Time } from 'lightweight-charts'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import { useTheme } from '@/components/theme-provider'

interface KLineDataPoint {
  date?: unknown
  open?: unknown
  high?: unknown
  low?: unknown
  close?: unknown
  volume?: unknown
}

interface StockKlineChartProps {
  data: KLineDataPoint[]
  className?: string
}

type TimeRange = '1w' | '1m' | '3m' | '6m' | '1y' | 'all'

const timeRanges: { value: TimeRange; label: string; days: number }[] = [
  { value: '1w', label: '1周', days: 7 },
  { value: '1m', label: '1月', days: 30 },
  { value: '3m', label: '3月', days: 90 },
  { value: '6m', label: '6月', days: 180 },
  { value: '1y', label: '1年', days: 365 },
  { value: 'all', label: '全部', days: -1 },
]

export function StockKlineChart({ data, className }: StockKlineChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const [timeRange, setTimeRange] = useState<TimeRange>('3m')
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  // Filter data based on time range
  const filteredData = (() => {
    if (!data || data.length === 0) return []

    const range = timeRanges.find(r => r.value === timeRange)
    if (!range || range.days === -1) return data

    // Find the latest date in data (not current date, since data may be historical)
    const sortedDates = data.map(d => String(d.date || '')).filter(Boolean).sort((a, b) => b.localeCompare(a))
    if (sortedDates.length === 0) return data
    const latestDate = new Date(sortedDates[0])

    const cutoffDate = new Date(latestDate)
    cutoffDate.setDate(cutoffDate.getDate() - range.days)
    const cutoffStr = cutoffDate.toISOString().split('T')[0]

    return data.filter(d => String(d.date || '') >= cutoffStr)
  })()

  useEffect(() => {
    if (!chartContainerRef.current || filteredData.length === 0) return

    // Clean up existing chart
    if (chartRef.current) {
      chartRef.current.remove()
      chartRef.current = null
    }

    // 获取主题颜色
    const chartColors = getChartThemeColors(isDark)
    const marketColors = getMarketColors()
    const profitColor = marketColors.profit
    const lossColor = marketColors.loss

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
      height: 400,
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

    // Add candlestick series
    const candlestickSeries = chart.addCandlestickSeries({
      upColor: profitColor,
      downColor: lossColor,
      borderUpColor: profitColor,
      borderDownColor: lossColor,
      wickUpColor: profitColor,
      wickDownColor: lossColor,
    })

    // Convert data to chart format
    const chartData: CandlestickData<Time>[] = filteredData
      .filter(d => d.open != null && d.high != null && d.low != null && d.close != null && d.date != null)
      .map(d => ({
        time: String(d.date) as Time,
        open: Number(d.open),
        high: Number(d.high),
        low: Number(d.low),
        close: Number(d.close),
      }))
      .sort((a, b) => (a.time as string).localeCompare(b.time as string))

    candlestickSeries.setData(chartData)

    // Add volume series
    const volumeSeries = chart.addHistogramSeries({
      color: chartColors.text,
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '',
    })

    volumeSeries.priceScale().applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    })

    const volumeData = filteredData
      .filter(d => d.volume != null && d.close != null && d.open != null && d.date != null)
      .map(d => ({
        time: String(d.date) as Time,
        value: Number(d.volume),
        color: Number(d.close) >= Number(d.open)
          ? profitColor + '80'  // 50% opacity
          : lossColor + '80',
      }))
      .sort((a, b) => (a.time as string).localeCompare(b.time as string))

    volumeSeries.setData(volumeData)

    // Fit content
    chart.timeScale().fitContent()

    // Handle resize
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
  }, [filteredData, isDark])

  if (!data || data.length === 0) {
    return (
      <div className={cn('flex h-[400px] items-center justify-center rounded-lg border border-dashed bg-muted/50', className)}>
        <p className="text-muted-foreground">暂无K线数据</p>
      </div>
    )
  }

  return (
    <div className={cn('space-y-4', className)}>
      {/* Time range selector */}
      <div className="flex gap-1">
        {timeRanges.map(range => (
          <Button
            key={range.value}
            variant={timeRange === range.value ? 'default' : 'outline'}
            size="sm"
            onClick={() => setTimeRange(range.value)}
          >
            {range.label}
          </Button>
        ))}
      </div>

      {/* Chart container */}
      <div ref={chartContainerRef} className="rounded-lg border" />
    </div>
  )
}
