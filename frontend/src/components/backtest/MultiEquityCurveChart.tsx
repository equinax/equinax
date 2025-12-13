import { useEffect, useRef, useMemo } from 'react'
import { createChart, IChartApi, ISeriesApi, LineStyle, ColorType, LineData, Time, SeriesMarker } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getChartPalette, getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

interface MultiEquityCurveChartProps {
  data: Record<string, EquityCurvePoint[]> | null | undefined
  trades?: Record<string, TradeRecord[]> | null
  height?: number
}

export function MultiEquityCurveChart({ data, trades, height = 400 }: MultiEquityCurveChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesMapRef = useRef<Map<string, ISeriesApi<'Line'>>>(new Map())
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const stockCodes = data ? Object.keys(data) : []

  // 获取主题感知的调色板
  const chartPalette = useMemo(() => getChartPalette(isDark), [isDark])

  useEffect(() => {
    if (!chartContainerRef.current) return

    // 获取图表主题颜色
    const chartColors = getChartThemeColors(isDark)

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: chartColors.text,
      },
      grid: {
        vertLines: { color: chartColors.grid },
        horzLines: { color: chartColors.grid },
      },
      crosshair: {
        mode: 1,
        vertLine: {
          color: chartColors.crosshair,
          width: 1,
          style: LineStyle.Dashed,
        },
        horzLine: {
          color: chartColors.crosshair,
          width: 1,
          style: LineStyle.Dashed,
        },
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: chartColors.border,
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

    const seriesMap = seriesMapRef.current
    return () => {
      window.removeEventListener('resize', handleResize)
      seriesMap.clear()
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

    // 获取主题调色板
    const palette = getChartPalette(isDark)

    // Add or update series for each stock
    Object.entries(data).forEach(([stockCode, points], index) => {
      if (!Array.isArray(points)) return

      const color = palette[index % palette.length]

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
  }, [data, isDark])

  // Update trade markers
  useEffect(() => {
    if (!chartRef.current || !trades) return

    const colors = getMarketColors()
    const existingSeries = seriesMapRef.current

    // Add markers to each series
    existingSeries.forEach((series, stockCode) => {
      const stockTrades = trades[stockCode]
      if (!stockTrades || !Array.isArray(stockTrades) || stockTrades.length === 0) {
        series.setMarkers([])
        return
      }

      const markers: SeriesMarker<Time>[] = stockTrades.flatMap((trade) => {
        const entryDate = trade.entry_date || trade.open_datetime?.split(' ')[0]
        const exitDate = trade.exit_date || trade.close_datetime?.split(' ')[0]
        const entryPrice = trade.entry_price ?? trade.open_price
        const exitPrice = trade.exit_price ?? trade.close_price

        const result: SeriesMarker<Time>[] = []

        // Entry marker (buy)
        if (entryDate) {
          result.push({
            time: entryDate as Time,
            position: 'belowBar',
            color: colors.profit,
            shape: 'arrowUp',
            text: `B @${entryPrice?.toFixed(2) ?? ''}`,
          })
        }

        // Exit marker (sell)
        if (exitDate) {
          result.push({
            time: exitDate as Time,
            position: 'aboveBar',
            color: colors.loss,
            shape: 'arrowDown',
            text: `S @${exitPrice?.toFixed(2) ?? ''}`,
          })
        }

        return result
      }).sort((a, b) => (a.time as string).localeCompare(b.time as string))

      series.setMarkers(markers)
    })
  }, [trades])

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
              style={{ backgroundColor: chartPalette[index % chartPalette.length] }}
            />
            <span className="text-sm text-muted-foreground">{stockCode}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
