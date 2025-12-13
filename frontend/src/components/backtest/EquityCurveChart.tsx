import { useEffect, useRef } from 'react'
import { createChart, IChartApi, ISeriesApi, LineStyle, ColorType, AreaData, Time, SeriesMarker } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

interface EquityCurveChartProps {
  data: EquityCurvePoint[] | { [key: string]: unknown }[] | null | undefined
  trades?: TradeRecord[] | null
  height?: number
}

export function EquityCurveChart({ data, trades, height = 400 }: EquityCurveChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Area'> | null>(null)
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  useEffect(() => {
    if (!chartContainerRef.current) return

    // 获取主题颜色
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

    // Add area series - 使用主题色
    const series = chart.addAreaSeries({
      lineColor: chartColors.equity,
      topColor: chartColors.equityFill,
      bottomColor: 'transparent',
      lineWidth: 2,
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => '¥' + price.toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 }),
      },
    })

    seriesRef.current = series

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
      chart.remove()
    }
  }, [isDark, height])

  // Update data
  useEffect(() => {
    if (!seriesRef.current || !data || !Array.isArray(data)) return

    const chartData: AreaData<Time>[] = data
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
      seriesRef.current.setData(chartData)
      chartRef.current?.timeScale().fitContent()
    }
  }, [data])

  // Update trade markers
  useEffect(() => {
    if (!seriesRef.current || !trades || !Array.isArray(trades) || trades.length === 0) {
      // Clear markers if no trades
      seriesRef.current?.setMarkers([])
      return
    }

    const colors = getMarketColors()

    const markers: SeriesMarker<Time>[] = trades.flatMap((trade) => {
      // Support both frontend and backend field names
      const entryDate = trade.entry_date || trade.open_datetime?.split(' ')[0]
      const exitDate = trade.exit_date || trade.close_datetime?.split(' ')[0]
      const entryPrice = trade.entry_price ?? trade.open_price
      const exitPrice = trade.exit_price ?? trade.close_price

      const result: SeriesMarker<Time>[] = []

      // Entry marker (buy) - use profit color (red for CN, green for US)
      if (entryDate) {
        result.push({
          time: entryDate as Time,
          position: 'belowBar',
          color: colors.profit,
          shape: 'arrowUp',
          text: `B @${entryPrice?.toFixed(2) ?? ''}`,
        })
      }

      // Exit marker (sell) - use loss color (green for CN, red for US)
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

    seriesRef.current.setMarkers(markers)
  }, [trades])

  if (!data || !Array.isArray(data) || data.length === 0) {
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
    <div className="rounded-lg border bg-card">
      <div ref={chartContainerRef} />
    </div>
  )
}
