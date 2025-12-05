import { useEffect, useRef } from 'react'
import { createChart, IChartApi, ISeriesApi, LineStyle, ColorType, LineData, Time } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import type { EquityCurvePoint } from '@/types/backtest'

const STOCK_COLORS = [
  '#3b82f6',  // 蓝色
  '#22c55e',  // 绿色
  '#f97316',  // 橙色
  '#a855f7',  // 紫色
  '#ef4444',  // 红色
  '#06b6d4',  // 青色
  '#78716c',  // 棕色
  '#64748b',  // 灰蓝色
]

interface MultiEquityCurveChartProps {
  data: Record<string, EquityCurvePoint[]> | null | undefined
  height?: number
}

export function MultiEquityCurveChart({ data, height = 400 }: MultiEquityCurveChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesMapRef = useRef<Map<string, ISeriesApi<'Line'>>>(new Map())
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const stockCodes = data ? Object.keys(data) : []

  useEffect(() => {
    if (!chartContainerRef.current) return

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: isDark ? '#a1a1aa' : '#71717a',
      },
      grid: {
        vertLines: { color: isDark ? '#27272a' : '#e4e4e7' },
        horzLines: { color: isDark ? '#27272a' : '#e4e4e7' },
      },
      crosshair: {
        mode: 1,
        vertLine: {
          color: isDark ? '#52525b' : '#a1a1aa',
          width: 1,
          style: LineStyle.Dashed,
        },
        horzLine: {
          color: isDark ? '#52525b' : '#a1a1aa',
          width: 1,
          style: LineStyle.Dashed,
        },
      },
      timeScale: {
        borderColor: isDark ? '#27272a' : '#e4e4e7',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: isDark ? '#27272a' : '#e4e4e7',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      width: chartContainerRef.current.clientWidth,
      height: height,
    })

    chartRef.current = chart

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
      seriesMapRef.current.clear()
      chart.remove()
    }
  }, [isDark, height])

  // Update data
  useEffect(() => {
    if (!chartRef.current || !data) return

    const chart = chartRef.current
    const existingSeries = seriesMapRef.current

    // Remove series for stocks that no longer exist
    existingSeries.forEach((series, stockCode) => {
      if (!data[stockCode]) {
        chart.removeSeries(series)
        existingSeries.delete(stockCode)
      }
    })

    // Add or update series for each stock
    Object.entries(data).forEach(([stockCode, points], index) => {
      if (!Array.isArray(points)) return

      const color = STOCK_COLORS[index % STOCK_COLORS.length]

      let series = existingSeries.get(stockCode)
      if (!series) {
        series = chart.addLineSeries({
          color,
          lineWidth: 2,
          priceFormat: {
            type: 'custom',
            formatter: (price: number) => '¥' + price.toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 }),
          },
          title: stockCode,
        })
        existingSeries.set(stockCode, series)
      }

      const chartData: LineData<Time>[] = points
        .filter((point): point is EquityCurvePoint =>
          point !== null &&
          typeof point === 'object' &&
          'date' in point &&
          'value' in point
        )
        .map((point) => ({
          time: point.date as Time,
          value: Number(point.value),
        }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      if (chartData.length > 0) {
        series.setData(chartData)
      }
    })

    chart.timeScale().fitContent()
  }, [data])

  if (!data || Object.keys(data).length === 0) {
    return (
      <div
        className="flex items-center justify-center bg-muted/30 rounded-lg"
        style={{ height }}
      >
        <p className="text-muted-foreground">暂无权益曲线数据</p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="rounded-lg border bg-card">
        <div ref={chartContainerRef} />
      </div>
      {/* Legend */}
      <div className="flex flex-wrap gap-4 justify-center">
        {stockCodes.map((stockCode, index) => (
          <div key={stockCode} className="flex items-center gap-2">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: STOCK_COLORS[index % STOCK_COLORS.length] }}
            />
            <span className="text-sm text-muted-foreground">{stockCode}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
