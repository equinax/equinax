/**
 * Professional stock chart component with:
 * - Time range selector (1M, 3M, 6M, 1Y, 3Y, 5Y, ALL)
 * - Technical indicators (MA, MACD, RSI, Volume)
 * - A-stock color convention (red=up, green=down)
 * - Date format: YYYY-MM-DD
 * - Theme-aware styling
 */

import { useEffect, useRef, useState, useMemo } from 'react'
import {
  createChart,
  IChartApi,
  ISeriesApi,
  LineStyle,
  ColorType,
  Time,
  CandlestickData,
  LineData,
  HistogramData,
  Coordinate,
  ISeriesPrimitive,
  SeriesType,
} from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getMarketColorsForTheme } from '@/lib/market-colors'
import { getChartThemeColors, INDICATOR_COLORS } from '@/lib/chart-theme'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { calcMA, calcMACD, calcRSI } from '@/lib/indicators'

interface StockChartProps {
  code: string
  height?: number
  /** Optional end date (yyyy-MM-dd format). If provided, chart shows data ending at this date. */
  endDate?: string
}

type TimeRange = '1M' | '3M' | '6M' | '1Y' | '3Y' | '5Y' | 'ALL'

interface IndicatorState {
  ma5: boolean
  ma10: boolean
  ma20: boolean
  ma60: boolean
  volume: boolean
  macd: boolean
  rsi: boolean
}

// Time range configuration
const TIME_RANGES: { key: TimeRange; label: string }[] = [
  { key: '1M', label: '1月' },
  { key: '3M', label: '3月' },
  { key: '6M', label: '6月' },
  { key: '1Y', label: '1年' },
  { key: '3Y', label: '3年' },
  { key: '5Y', label: '5年' },
  { key: 'ALL', label: '全部' },
]

// Trading days per time range (approximate)
const TRADING_DAYS_MAP: Record<TimeRange, number> = {
  '1M': 22,
  '3M': 66,
  '6M': 132,
  '1Y': 250,
  '3Y': 750,
  '5Y': 1000,
  'ALL': Infinity,
}

// Calculate visible logical range based on time range and reference date
function getVisibleRange(
  range: TimeRange,
  dataLength: number,
  referenceDate?: string,
  sortedDates?: string[]
): { from: number; to: number } {
  // Default: show all data
  if (range === 'ALL' || dataLength === 0) {
    return { from: 0, to: dataLength - 1 }
  }

  // Find reference point index (either referenceDate or last data point)
  let refIndex = dataLength - 1
  if (referenceDate && sortedDates) {
    const idx = sortedDates.indexOf(referenceDate)
    if (idx >= 0) refIndex = idx
  }

  const visibleDays = TRADING_DAYS_MAP[range]

  // If reference date is provided, center the view on it
  if (referenceDate && sortedDates) {
    const halfDays = Math.floor(visibleDays / 2)
    const from = Math.max(0, refIndex - halfDays)
    const to = Math.min(dataLength - 1, refIndex + halfDays)
    return { from, to }
  }

  // Otherwise, show most recent data
  const from = Math.max(0, dataLength - visibleDays)
  const to = dataLength - 1
  return { from, to }
}

// Vertical line primitive renderer
class VertLinePaneRenderer {
  _x: Coordinate | null = null
  _color: string

  constructor(x: Coordinate | null, color: string) {
    this._x = x
    this._color = color
  }

  draw(target: { useBitmapCoordinateSpace: (fn: (scope: { context: CanvasRenderingContext2D; bitmapSize: { width: number; height: number }; horizontalPixelRatio: number }) => void) => void }) {
    target.useBitmapCoordinateSpace(scope => {
      if (this._x === null) return
      const ctx = scope.context
      const x = Math.round(this._x * scope.horizontalPixelRatio)

      ctx.save()
      ctx.strokeStyle = this._color
      ctx.lineWidth = 1 * scope.horizontalPixelRatio
      ctx.setLineDash([4 * scope.horizontalPixelRatio, 4 * scope.horizontalPixelRatio])
      ctx.beginPath()
      ctx.moveTo(x + 0.5, 0)
      ctx.lineTo(x + 0.5, scope.bitmapSize.height)
      ctx.stroke()
      ctx.restore()
    })
  }
}

// Vertical line primitive pane view
class VertLinePaneView {
  _source: VertLine
  _x: Coordinate | null = null

  constructor(source: VertLine) {
    this._source = source
  }

  update() {
    const timeScale = this._source._chart.timeScale()
    this._x = timeScale.timeToCoordinate(this._source._time)
  }

  renderer() {
    return new VertLinePaneRenderer(this._x, this._source._color)
  }
}

// Vertical line primitive - draws a dashed vertical line at specified time
class VertLine implements ISeriesPrimitive<Time> {
  _chart: IChartApi
  _series: ISeriesApi<SeriesType>
  _time: Time
  _color: string
  _paneViews: VertLinePaneView[]

  constructor(chart: IChartApi, series: ISeriesApi<SeriesType>, time: Time, color: string = '#1E40AF') {
    this._chart = chart
    this._series = series
    this._time = time
    this._color = color
    this._paneViews = [new VertLinePaneView(this)]
  }

  updateAllViews() {
    this._paneViews.forEach(pw => pw.update())
  }

  paneViews() {
    return this._paneViews
  }
}

export function StockChart({ code, height = 500, endDate }: StockChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApiRef = useRef<IChartApi | null>(null)

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const colors = getMarketColorsForTheme(isDark)
  const chartColors = getChartThemeColors(isDark)

  // State
  const [timeRange, setTimeRange] = useState<TimeRange>('6M')
  const [indicators, setIndicators] = useState<IndicatorState>({
    ma5: true,
    ma10: true,
    ma20: true,
    ma60: false,
    volume: true,
    macd: false,
    rsi: false,
  })

  // Fetch all K-line data once (time range buttons control zoom, not data fetching)
  const { data: klineData, isLoading } = useGetKlineApiV1StocksCodeKlineGet(
    code,
    { limit: 1000 },
    {
      query: {
        enabled: !!code,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Calculate indicators
  const calculatedIndicators = useMemo(() => {
    if (!klineData?.data || klineData.data.length === 0) return null

    const sortedData = [...klineData.data]
      .sort((a, b) => a.date.localeCompare(b.date))
      .map(d => ({ date: d.date, close: Number(d.close) || 0 }))

    return {
      ma5: calcMA(sortedData, 5),
      ma10: calcMA(sortedData, 10),
      ma20: calcMA(sortedData, 20),
      ma60: calcMA(sortedData, 60),
      macd: calcMACD(sortedData),
      rsi: calcRSI(sortedData, 14),
    }
  }, [klineData])

  // Toggle indicator
  const toggleIndicator = (key: keyof IndicatorState) => {
    setIndicators(prev => ({ ...prev, [key]: !prev[key] }))
  }

  // Toggle all MA
  const toggleAllMA = () => {
    const allOn = indicators.ma5 && indicators.ma10 && indicators.ma20 && indicators.ma60
    setIndicators(prev => ({
      ...prev,
      ma5: !allOn,
      ma10: !allOn,
      ma20: !allOn,
      ma60: !allOn,
    }))
  }

  // Count enabled sub-charts
  const subChartCount = [indicators.volume, indicators.macd, indicators.rsi].filter(Boolean).length

  // Create/update chart
  useEffect(() => {
    if (!chartRef.current || !klineData?.data) return

    // Clean up existing chart
    if (chartApiRef.current) {
      try {
        chartApiRef.current.remove()
      } catch {
        // Chart already disposed
      }
      chartApiRef.current = null
    }

    const chart = createChart(chartRef.current, {
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
          width: 1 as const,
          style: LineStyle.Dashed,
        },
        horzLine: {
          color: chartColors.crosshair,
          width: 1 as const,
          style: LineStyle.Dashed,
        },
      },
      localization: {
        dateFormat: 'yyyy-MM-dd',
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: false,
        fixLeftEdge: true,
        fixRightEdge: true,
        rightOffset: 5,
        minBarSpacing: 3,
      },
      rightPriceScale: {
        visible: true,
        borderColor: chartColors.border,
        scaleMargins: { top: 0.05, bottom: subChartCount > 0 ? 0.35 : 0.05 },
      },
      height,
      width: chartRef.current.clientWidth,
    })
    chartApiRef.current = chart

    // Sort K-line data
    const sortedKline = [...klineData.data].sort((a, b) => a.date.localeCompare(b.date))

    // Add candlestick series (main chart)
    const candleSeries = chart.addCandlestickSeries({
      upColor: colors.profit,
      downColor: colors.loss,
      borderUpColor: colors.profit,
      borderDownColor: colors.loss,
      wickUpColor: colors.profit,
      wickDownColor: colors.loss,
      priceScaleId: 'right',
    })

    const candleData: CandlestickData<Time>[] = sortedKline.map(d => ({
      time: d.date as Time,
      open: Number(d.open) || 0,
      high: Number(d.high) || 0,
      low: Number(d.low) || 0,
      close: Number(d.close) || 0,
    }))
    candleSeries.setData(candleData)

    // Add vertical line primitive for reference date
    if (endDate) {
      const dateExists = sortedKline.some(d => d.date === endDate)
      if (dateExists) {
        const vertLine = new VertLine(chart, candleSeries, endDate as Time, '#1E40AF')
        candleSeries.attachPrimitive(vertLine)
      }
    }

    // MA line series storage
    const lineSeries: { [key: string]: ISeriesApi<'Line'> } = {}

    // Add MA lines
    const maKeys = ['ma5', 'ma10', 'ma20', 'ma60'] as const
    maKeys.forEach(key => {
      if (indicators[key] && calculatedIndicators) {
        lineSeries[key] = chart.addLineSeries({
          color: INDICATOR_COLORS[key],
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          priceScaleId: 'right',
        })
      }
    })

    // Set MA data
    if (calculatedIndicators) {
      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      if (lineSeries.ma5) lineSeries.ma5.setData(mapToLineData(calculatedIndicators.ma5))
      if (lineSeries.ma10) lineSeries.ma10.setData(mapToLineData(calculatedIndicators.ma10))
      if (lineSeries.ma20) lineSeries.ma20.setData(mapToLineData(calculatedIndicators.ma20))
      if (lineSeries.ma60) lineSeries.ma60.setData(mapToLineData(calculatedIndicators.ma60))
    }

    // Sub-chart index tracker
    let subChartIndex = 0
    const getSubChartMargins = () => {
      const totalSubCharts = subChartCount
      if (totalSubCharts === 0) return { top: 0.7, bottom: 0 }

      // Calculate margin based on position
      const baseTop = 0.65
      const perChart = 0.35 / totalSubCharts
      const top = baseTop + subChartIndex * perChart
      const bottom = 1 - top - perChart
      subChartIndex++
      return { top, bottom: Math.max(0, bottom) }
    }

    // Add Volume sub-chart
    if (indicators.volume) {
      const volumeSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: 'volume',
      })

      chart.priceScale('volume').applyOptions({
        visible: false,
        scaleMargins: getSubChartMargins(),
      })

      const volumeData: HistogramData<Time>[] = sortedKline.map(d => {
        const change = (Number(d.close) || 0) - (Number(d.open) || 0)
        return {
          time: d.date as Time,
          value: Number(d.volume) || 0,
          color: change >= 0 ? `${colors.profit}80` : `${colors.loss}80`,
        }
      })
      volumeSeries.setData(volumeData)
    }

    // Add MACD sub-chart
    if (indicators.macd && calculatedIndicators) {
      const macdScaleId = 'macd'
      const margins = getSubChartMargins()

      const macdHistSeries = chart.addHistogramSeries({
        priceScaleId: macdScaleId,
      })

      const macdDifSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.macdDif,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: macdScaleId,
      })

      const macdDeaSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.macdDea,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: macdScaleId,
      })

      chart.priceScale(macdScaleId).applyOptions({
        visible: false,
        scaleMargins: margins,
      })

      const { macd } = calculatedIndicators
      const histData: HistogramData<Time>[] = Array.from(macd.hist.entries())
        .map(([date, value]) => ({
          time: date as Time,
          value,
          color: value >= 0 ? `${colors.profit}80` : `${colors.loss}80`,
        }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      macdHistSeries.setData(histData)
      macdDifSeries.setData(mapToLineData(macd.dif))
      macdDeaSeries.setData(mapToLineData(macd.dea))
    }

    // Add RSI sub-chart
    if (indicators.rsi && calculatedIndicators) {
      const rsiScaleId = 'rsi'
      const margins = getSubChartMargins()

      const rsiSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.rsi,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: true,
        priceScaleId: rsiScaleId,
      })

      chart.priceScale(rsiScaleId).applyOptions({
        visible: false,
        scaleMargins: margins,
      })

      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      rsiSeries.setData(mapToLineData(calculatedIndicators.rsi))
    }

    // Set initial visible range based on timeRange and endDate
    const sortedDates = sortedKline.map(d => d.date)
    const { from, to } = getVisibleRange(timeRange, sortedKline.length, endDate, sortedDates)
    if (sortedKline.length > 0) {
      chart.timeScale().setVisibleLogicalRange({ from, to })
    }

    // Resize handler
    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const width = entry.contentRect.width
        if (chartApiRef.current) {
          try {
            chartApiRef.current.applyOptions({ width })
          } catch {
            // Chart might be disposed
          }
        }
      }
    })

    if (containerRef.current) {
      resizeObserver.observe(containerRef.current)
    }

    return () => {
      resizeObserver.disconnect()
      try {
        chart.remove()
      } catch {
        // Chart already disposed
      }
    }
  }, [isDark, height, klineData, calculatedIndicators, indicators, colors, chartColors, subChartCount, timeRange, endDate])

  return (
    <div className="space-y-2" ref={containerRef}>
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-4 px-4 pt-2">
        {/* Time range selector */}
        <div className="flex items-center gap-1">
          {TIME_RANGES.map(({ key, label }) => (
            <Button
              key={key}
              variant={timeRange === key ? 'default' : 'outline'}
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={() => setTimeRange(key)}
            >
              {label}
            </Button>
          ))}
        </div>

        {/* Separator */}
        <div className="w-px h-5 bg-border" />

        {/* MA toggles */}
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">MA:</span>
          <Button
            variant={indicators.ma5 && indicators.ma10 && indicators.ma20 && indicators.ma60 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={toggleAllMA}
          >
            All
          </Button>
          <Button
            variant={indicators.ma5 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma5')}
          >
            5
          </Button>
          <Button
            variant={indicators.ma10 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma10')}
          >
            10
          </Button>
          <Button
            variant={indicators.ma20 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma20')}
          >
            20
          </Button>
          <Button
            variant={indicators.ma60 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma60')}
          >
            60
          </Button>
        </div>

        {/* Sub-chart toggles */}
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">副图:</span>
          <Button
            variant={indicators.volume ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('volume')}
          >
            成交量
          </Button>
          <Button
            variant={indicators.macd ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('macd')}
          >
            MACD
          </Button>
          <Button
            variant={indicators.rsi ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('rsi')}
          >
            RSI
          </Button>
        </div>
      </div>

      {/* Chart container */}
      <div className="border-t">
        {isLoading ? (
          <Skeleton className="w-full" style={{ height }} />
        ) : (
          <div ref={chartRef} style={{ height }} />
        )}

        {/* Legend */}
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground px-4 py-2 border-t">
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: colors.profit }} />
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: colors.loss }} />
            K线
          </span>
          {indicators.ma5 && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ma5 }} />
              MA5
            </span>
          )}
          {indicators.ma10 && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ma10 }} />
              MA10
            </span>
          )}
          {indicators.ma20 && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ma20 }} />
              MA20
            </span>
          )}
          {indicators.ma60 && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ma60 }} />
              MA60
            </span>
          )}
          {indicators.volume && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-2 rounded-sm" style={{ backgroundColor: INDICATOR_COLORS.volume }} />
              成交量
            </span>
          )}
          {indicators.macd && (
            <>
              <span className="flex items-center gap-1">
                <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.macdDif }} />
                DIF
              </span>
              <span className="flex items-center gap-1">
                <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.macdDea }} />
                DEA
              </span>
            </>
          )}
          {indicators.rsi && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.rsi }} />
              RSI(14)
            </span>
          )}
        </div>
      </div>
    </div>
  )
}
