/**
 * Professional stock chart component with:
 * - Time range selector (1M, 3M, 6M, 1Y, 3Y, 5Y, ALL)
 * - Technical indicators (MA, MACD, RSI, Volume)
 * - A-stock color convention (red=up, green=down)
 * - Date format: YYYY-MM-DD
 * - Theme-aware styling
 * - Hover tooltip showing detailed data for each date
 */

import { useEffect, useRef, useState, useMemo, useCallback } from 'react'
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
  MouseEventParams,
  LogicalRange,
} from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getMarketColorsForTheme } from '@/lib/market-colors'
import { getChartThemeColors, INDICATOR_COLORS } from '@/lib/chart-theme'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'
import {
  useGetKlineApiV1StocksCodeKlineGet,
  useFetchMissingKlineApiV1StocksCodeKlineFetchMissingPost,
} from '@/api/generated/stocks/stocks'
import type { KLineData } from '@/api/generated/schemas'
import { calcMA, calcMACD, calcRSI } from '@/lib/indicators'

export interface HoverData {
  date: string
  open: number
  high: number
  low: number
  close: number
  preclose: number
  volume: number
  amount: number
  turn: number | null
  change_pct: number
}

export interface PriceLine {
  price: number
  color: string
  label: string
  lineStyle?: 'solid' | 'dashed'
}

export interface VerticalMarker {
  date: string    // YYYY-MM-DD format
  color: string
  label?: string  // optional label (not rendered on chart, for reference)
}

interface StockChartProps {
  code: string
  height?: number | string
  endDate?: string
  onHoverData?: (data: HoverData | null) => void
  onLoadingChange?: (isLoading: boolean) => void
  priceLines?: PriceLine[]
  verticalMarkers?: VerticalMarker[]
  onChartReady?: (chartApi: IChartApi) => void
  minimal?: boolean
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
  sortedDates?: string[],
  markerDates?: string[]
): { from: number; to: number } {
  if (range === 'ALL' || dataLength === 0) {
    return { from: 0, to: dataLength - 1 }
  }

  let refIndex = dataLength - 1
  if (referenceDate && sortedDates) {
    const idx = sortedDates.indexOf(referenceDate)
    if (idx >= 0) refIndex = idx
  }

  const visibleDays = TRADING_DAYS_MAP[range]
  let from: number
  let to: number

  if (referenceDate && sortedDates) {
    const halfDays = Math.floor(visibleDays / 2)
    from = Math.max(0, refIndex - halfDays)
    to = Math.min(dataLength - 1, refIndex + halfDays)
  } else {
    from = Math.max(0, dataLength - visibleDays)
    to = dataLength - 1
  }

  if (markerDates && sortedDates) {
    for (const md of markerDates) {
      const idx = sortedDates.indexOf(md)
      if (idx >= 0 && idx > to) {
        to = idx + 5
      }
    }
    to = Math.min(to, dataLength - 1)
  }

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

export function StockChart({ code, height = 500, endDate, onHoverData, onLoadingChange, priceLines, verticalMarkers, onChartReady, minimal = false }: StockChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApiRef = useRef<IChartApi | null>(null)

  const onHoverDataRef = useRef(onHoverData)
  onHoverDataRef.current = onHoverData
  const onChartReadyRef = useRef(onChartReady)
  onChartReadyRef.current = onChartReady
  const priceLinesRef = useRef(priceLines)
  priceLinesRef.current = priceLines
  const verticalMarkersRef = useRef(verticalMarkers)
  verticalMarkersRef.current = verticalMarkers

  const priceLinesKey = priceLines?.map(p => `${p.price}:${p.color}`).join('|') ?? ''
  const verticalMarkersKey = verticalMarkers?.map(m => m.date).join('|') ?? ''
  
  const seriesRefs = useRef<{
    candle: ISeriesApi<'Candlestick'> | null
    volume: ISeriesApi<'Histogram'> | null
    ma5: ISeriesApi<'Line'> | null
    ma10: ISeriesApi<'Line'> | null
    ma20: ISeriesApi<'Line'> | null
    ma60: ISeriesApi<'Line'> | null
    macdHist: ISeriesApi<'Histogram'> | null
    macdDif: ISeriesApi<'Line'> | null
    macdDea: ISeriesApi<'Line'> | null
    rsi: ISeriesApi<'Line'> | null
  }>({
    candle: null,
    volume: null,
    ma5: null,
    ma10: null,
    ma20: null,
    ma60: null,
    macdHist: null,
    macdDif: null,
    macdDea: null,
    rsi: null,
  })

  const [chartHeight, setChartHeight] = useState<number>(typeof height === 'number' ? height : 500)
  const [chartVersion, setChartVersion] = useState(0)
  const prevChartVersionRef = useRef(0)
  const savedRangeRef = useRef<LogicalRange | null>(null)

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const colors = useMemo(() => getMarketColorsForTheme(isDark), [isDark])
  const chartColors = useMemo(() => getChartThemeColors(isDark), [isDark])

  const [timeRange, setTimeRange] = useState<TimeRange>('6M')
  const [indicators, setIndicators] = useState<IndicatorState>(() => minimal ? {
    ma5: false,
    ma10: false,
    ma20: false,
    ma60: false,
    volume: true,
    macd: false,
    rsi: false,
  } : {
    ma5: true,
    ma10: true,
    ma20: true,
    ma60: false,
    volume: true,
    macd: false,
    rsi: false,
  })

  const [extraKlineData, setExtraKlineData] = useState<KLineData[]>([])
  const isFetchingRef = useRef(false)
  const oldestDateRef = useRef<string | null>(null)
  const prevDataLengthRef = useRef(0)
  const mergedKlineDataRef = useRef<KLineData[]>([])
  const activeVertLinesRef = useRef<VertLine[]>([])
  const activePriceLinesRef = useRef<ReturnType<ISeriesApi<'Candlestick'>['createPriceLine']>[]>([])

  useEffect(() => {
    setExtraKlineData([])
    oldestDateRef.current = null
    prevDataLengthRef.current = 0
    savedRangeRef.current = null
  }, [code])

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

  const fetchMissingMutation = useFetchMissingKlineApiV1StocksCodeKlineFetchMissingPost()

  useEffect(() => {
    onLoadingChange?.(fetchMissingMutation.isPending)
  }, [fetchMissingMutation.isPending, onLoadingChange])

  const mergedKlineData = useMemo(() => {
    if (!klineData?.data) return []
    const allData = [...klineData.data, ...extraKlineData]
    const uniqueMap = new Map<string, KLineData>()
    allData.forEach(d => uniqueMap.set(d.date, d))
    return Array.from(uniqueMap.values()).sort((a, b) => a.date.localeCompare(b.date))
  }, [klineData, extraKlineData])

  useEffect(() => {
    mergedKlineDataRef.current = mergedKlineData
  }, [mergedKlineData])

  const calculatedIndicators = useMemo(() => {
    if (mergedKlineData.length === 0) return null

    const sortedData = mergedKlineData.map(d => ({ date: d.date, close: Number(d.close) || 0 }))

    return {
      ma5: calcMA(sortedData, 5),
      ma10: calcMA(sortedData, 10),
      ma20: calcMA(sortedData, 20),
      ma60: calcMA(sortedData, 60),
      macd: calcMACD(sortedData),
      rsi: calcRSI(sortedData, 14),
    }
  }, [mergedKlineData])

  const toggleIndicator = (key: keyof IndicatorState) => {
    setIndicators(prev => ({ ...prev, [key]: !prev[key] }))
  }

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

  // Stabilize mutation ref to avoid re-creating chart on every render
  const fetchMissingMutateRef = useRef(fetchMissingMutation.mutateAsync)
  useEffect(() => {
    fetchMissingMutateRef.current = fetchMissingMutation.mutateAsync
  }, [fetchMissingMutation.mutateAsync])

  const fetchOlderData = useCallback(async (beforeDate: string) => {
    if (isFetchingRef.current || !code) return
    
    isFetchingRef.current = true
    
    const endDate = new Date(beforeDate)
    endDate.setDate(endDate.getDate() - 1)
    const startDate = new Date(endDate)
    startDate.setFullYear(startDate.getFullYear() - 1)
    
    const startStr = startDate.toISOString().split('T')[0]
    const endStr = endDate.toISOString().split('T')[0]
    
    try {
      const result = await fetchMissingMutateRef.current({
        code,
        data: { start_date: startStr, end_date: endStr },
      })
      
      if (result.data && result.data.length > 0) {
        setExtraKlineData(prev => {
          const allData = [...prev, ...result.data]
          const uniqueMap = new Map<string, KLineData>()
          allData.forEach(d => uniqueMap.set(d.date, d))
          return Array.from(uniqueMap.values())
        })
      }
    } catch (error) {
      console.error('Failed to fetch older data:', error)
    } finally {
      isFetchingRef.current = false
    }
  }, [code])

  const subChartCount = [indicators.volume, indicators.macd, indicators.rsi].filter(Boolean).length

  useEffect(() => {
    if (!chartRef.current) return

    if (chartApiRef.current) {
      try {
        savedRangeRef.current = chartApiRef.current.timeScale().getVisibleLogicalRange()
        chartApiRef.current.remove()
      } catch {
        // Chart already disposed
      }
      chartApiRef.current = null
      activeVertLinesRef.current = []
      Object.keys(seriesRefs.current).forEach(key => {
        seriesRefs.current[key as keyof typeof seriesRefs.current] = null
      })
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
        visible: !minimal,
        fixLeftEdge: false,
        fixRightEdge: false,
        rightOffset: 5,
        minBarSpacing: 1,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
      rightPriceScale: {
        visible: true,
        borderColor: chartColors.border,
        scaleMargins: { top: 0.05, bottom: minimal ? 0.15 : (subChartCount > 0 ? 0.35 : 0.05) },
      },
      height: chartHeight,
      width: chartRef.current.clientWidth,
    })
    chartApiRef.current = chart
    onChartReadyRef.current?.(chart)

    seriesRefs.current.candle = chart.addCandlestickSeries({
      upColor: colors.profit,
      downColor: colors.loss,
      borderUpColor: colors.profit,
      borderDownColor: colors.loss,
      wickUpColor: colors.profit,
      wickDownColor: colors.loss,
      priceScaleId: 'right',
    })

    const maKeys = ['ma5', 'ma10', 'ma20', 'ma60'] as const
    maKeys.forEach(key => {
      if (indicators[key]) {
        seriesRefs.current[key] = chart.addLineSeries({
          color: INDICATOR_COLORS[key],
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          priceScaleId: 'right',
        })
      }
    })

    let subChartIndex = 0
    const getSubChartMargins = () => {
      const totalSubCharts = subChartCount
      if (totalSubCharts === 0) return { top: 0.7, bottom: 0 }

      const baseTop = 0.65
      const perChart = 0.35 / totalSubCharts
      const top = baseTop + subChartIndex * perChart
      const bottom = 1 - top - perChart
      subChartIndex++
      return { top, bottom: Math.max(0, bottom) }
    }

    if (indicators.volume) {
      seriesRefs.current.volume = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: 'volume',
      })

      chart.priceScale('volume').applyOptions({
        visible: false,
        scaleMargins: getSubChartMargins(),
      })
    }

    if (indicators.macd) {
      const macdScaleId = 'macd'
      const margins = getSubChartMargins()

      seriesRefs.current.macdHist = chart.addHistogramSeries({
        priceScaleId: macdScaleId,
      })

      seriesRefs.current.macdDif = chart.addLineSeries({
        color: INDICATOR_COLORS.macdDif,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: macdScaleId,
      })

      seriesRefs.current.macdDea = chart.addLineSeries({
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
    }

    if (indicators.rsi) {
      const rsiScaleId = 'rsi'
      const margins = getSubChartMargins()

      seriesRefs.current.rsi = chart.addLineSeries({
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
    }

    setChartVersion(v => v + 1)

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

    let hideTooltipTimer: NodeJS.Timeout | null = null;

    const handleMouseMove = (param: MouseEventParams<Time>) => {
      try {
        const currentData = mergedKlineDataRef.current
        if (!param.time || currentData.length === 0) {
          onHoverDataRef.current?.(null)
          return
        }

        if (hideTooltipTimer) {
          clearTimeout(hideTooltipTimer)
          hideTooltipTimer = null
        }

        const timeStr = String(param.time)
        const dataPoint = currentData.find(d => d.date === timeStr)

        if (dataPoint) {
          const open = Number(dataPoint.open) || 0
          const close = Number(dataPoint.close) || 0
          const preclose = Number(dataPoint.preclose) || 0
          const change_pct = preclose !== 0 ? ((close - preclose) / preclose) * 100 : 0

          onHoverDataRef.current?.({
            date: dataPoint.date,
            open,
            high: Number(dataPoint.high) || 0,
            low: Number(dataPoint.low) || 0,
            close,
            preclose,
            volume: Number(dataPoint.volume) || 0,
            amount: Number(dataPoint.amount) || 0,
            turn: dataPoint.turn != null ? Number(dataPoint.turn) : null,
            change_pct: parseFloat(change_pct.toFixed(2)),
          })
        } else {
          onHoverDataRef.current?.(null)
        }
      } catch (error) {
        console.error('Error in handleMouseMove:', error)
        onHoverDataRef.current?.(null)
      }
    }

    const handleMouseLeave = () => {
      hideTooltipTimer = setTimeout(() => {
        onHoverDataRef.current?.(null)
      }, 100)
    }

    const handleVisibleRangeChange = (logicalRange: LogicalRange | null) => {
      if (!logicalRange || isFetchingRef.current) return
      
      const FETCH_THRESHOLD = 10
      if (logicalRange.from < FETCH_THRESHOLD && oldestDateRef.current) {
        fetchOlderData(oldestDateRef.current)
      }
    }

    chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleRangeChange)
    chart.subscribeCrosshairMove(handleMouseMove);

    const chartContainer = chartRef.current;
    if (chartContainer) {
      chartContainer.addEventListener('mouseleave', handleMouseLeave, { capture: true });
      const handleMouseEnter = () => {
        if (hideTooltipTimer) {
          clearTimeout(hideTooltipTimer);
          hideTooltipTimer = null;
        }
      };
      chartContainer.addEventListener('mouseenter', handleMouseEnter);

      return () => {
        if (hideTooltipTimer) clearTimeout(hideTooltipTimer);
        try {
          resizeObserver.disconnect();
          chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleRangeChange);
          chart.unsubscribeCrosshairMove(handleMouseMove);
          if (chartContainer) {
            chartContainer.removeEventListener('mouseleave', handleMouseLeave, { capture: true });
            chartContainer.removeEventListener('mouseenter', handleMouseEnter);
          }
          chart.remove();
        } catch (error) {
          console.error('Error cleaning up chart:', error);
        }
      };
    }
  }, [isDark, chartHeight, indicators, colors, chartColors, subChartCount, fetchOlderData])

  useEffect(() => {
    if (!chartApiRef.current || mergedKlineData.length === 0) return

    const chart = chartApiRef.current
    const sortedKline = mergedKlineData

    if (sortedKline.length > 0) {
      oldestDateRef.current = sortedKline[0].date
    }

    if (seriesRefs.current.candle) {
      const candleData: CandlestickData<Time>[] = sortedKline.map(d => ({
        time: d.date as Time,
        open: Number(d.open) || 0,
        high: Number(d.high) || 0,
        low: Number(d.low) || 0,
        close: Number(d.close) || 0,
      }))
      seriesRefs.current.candle.setData(candleData)
    }

    if (calculatedIndicators) {
      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      if (seriesRefs.current.ma5) seriesRefs.current.ma5.setData(mapToLineData(calculatedIndicators.ma5))
      if (seriesRefs.current.ma10) seriesRefs.current.ma10.setData(mapToLineData(calculatedIndicators.ma10))
      if (seriesRefs.current.ma20) seriesRefs.current.ma20.setData(mapToLineData(calculatedIndicators.ma20))
      if (seriesRefs.current.ma60) seriesRefs.current.ma60.setData(mapToLineData(calculatedIndicators.ma60))
      
      if (seriesRefs.current.macdHist && seriesRefs.current.macdDif && seriesRefs.current.macdDea) {
        const { macd } = calculatedIndicators
        const histData: HistogramData<Time>[] = Array.from(macd.hist.entries())
          .map(([date, value]) => ({
            time: date as Time,
            value,
            color: value >= 0 ? `${colors.profit}80` : `${colors.loss}80`,
          }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))
        
        seriesRefs.current.macdHist.setData(histData)
        seriesRefs.current.macdDif.setData(mapToLineData(macd.dif))
        seriesRefs.current.macdDea.setData(mapToLineData(macd.dea))
      }

      if (seriesRefs.current.rsi) {
        seriesRefs.current.rsi.setData(mapToLineData(calculatedIndicators.rsi))
      }
    }

    if (seriesRefs.current.volume) {
      const volumeData: HistogramData<Time>[] = sortedKline.map(d => {
        const change = (Number(d.close) || 0) - (Number(d.open) || 0)
        return {
          time: d.date as Time,
          value: Number(d.volume) || 0,
          color: change >= 0 ? `${colors.profit}80` : `${colors.loss}80`,
        }
      })
      seriesRefs.current.volume.setData(volumeData)
    }

    const prevLen = prevDataLengthRef.current
    const currLen = sortedKline.length
    const shift = currLen - prevLen
    const isChartRecreated = chartVersion !== prevChartVersionRef.current

    const markerDates = verticalMarkersRef.current?.map(m => m.date)

    if (isChartRecreated) {
      if (savedRangeRef.current) {
        chart.timeScale().setVisibleLogicalRange(savedRangeRef.current)
      } else {
        const sortedDates = sortedKline.map(d => d.date)
        const { from, to } = getVisibleRange(timeRange, currLen, endDate, sortedDates, markerDates)
        chart.timeScale().setVisibleLogicalRange({ from, to })
      }
      prevChartVersionRef.current = chartVersion
    } else if (prevLen === 0) {
      const sortedDates = sortedKline.map(d => d.date)
      const { from, to } = getVisibleRange(timeRange, currLen, endDate, sortedDates, markerDates)
      chart.timeScale().setVisibleLogicalRange({ from, to })
    } else if (shift > 0) {
      if (!minimal) {
        const currentRange = chart.timeScale().getVisibleLogicalRange()
        if (currentRange) {
          chart.timeScale().setVisibleLogicalRange({
            from: currentRange.from + shift,
            to: currentRange.to + shift,
          })
        }
      }
    }

    prevDataLengthRef.current = currLen

  }, [mergedKlineData, calculatedIndicators, chartVersion, colors])

  useEffect(() => {
    if (minimal) return
    if (!chartApiRef.current || mergedKlineData.length === 0) return
    
    const sortedDates = mergedKlineData.map(d => d.date)
    const markerDates = verticalMarkersRef.current?.map(m => m.date)
    const { from, to } = getVisibleRange(timeRange, mergedKlineData.length, endDate, sortedDates, markerDates)
    chartApiRef.current.timeScale().setVisibleLogicalRange({ from, to })
  }, [timeRange, endDate])

  useEffect(() => {
    if (!chartApiRef.current || !seriesRefs.current.candle) return

    for (const vl of activeVertLinesRef.current) {
      try {
        seriesRefs.current.candle.detachPrimitive(vl)
      } catch (e) {
        console.warn('Failed to detach primitive', e)
      }
    }
    activeVertLinesRef.current = []

    const newVertLines: VertLine[] = []

    if (endDate) {
      const dateExists = mergedKlineData.some(d => d.date === endDate)
      if (dateExists) {
        const vertLine = new VertLine(chartApiRef.current, seriesRefs.current.candle, endDate as Time, '#1E40AF')
        seriesRefs.current.candle.attachPrimitive(vertLine)
        newVertLines.push(vertLine)
      }
    }

    if (verticalMarkersRef.current) {
      for (const marker of verticalMarkersRef.current) {
        const dateExists = mergedKlineDataRef.current.some(d => d.date === marker.date)
        if (dateExists) {
          const vertLine = new VertLine(chartApiRef.current!, seriesRefs.current.candle!, marker.date as Time, marker.color)
          seriesRefs.current.candle!.attachPrimitive(vertLine)
          newVertLines.push(vertLine)
        }
      }
    }

    activeVertLinesRef.current = newVertLines
  }, [endDate, verticalMarkersKey, chartVersion, mergedKlineData.length])

  useEffect(() => {
    if (!seriesRefs.current.candle) return

    for (const pl of activePriceLinesRef.current) {
      try {
        seriesRefs.current.candle.removePriceLine(pl)
      } catch {
        // noop
      }
    }
    activePriceLinesRef.current = []

    if (!priceLinesRef.current || priceLinesRef.current.length === 0) return

    const newPriceLines: typeof activePriceLinesRef.current = []
    for (const pl of priceLinesRef.current) {
      const priceLine = seriesRefs.current.candle.createPriceLine({
        price: pl.price,
        color: pl.color,
        lineWidth: 1,
        lineStyle: pl.lineStyle === 'solid' ? LineStyle.Solid : LineStyle.Dashed,
        lineVisible: true,
        axisLabelVisible: true,
        title: pl.label,
      })
      newPriceLines.push(priceLine)
    }
    activePriceLinesRef.current = newPriceLines
  }, [priceLinesKey, chartVersion, mergedKlineData.length])

  const isFlexHeight = typeof height === 'string'

  useEffect(() => {
    if (!isFlexHeight || !chartRef.current) return
    
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const newHeight = entry.contentRect.height
        if (newHeight > 0 && newHeight !== chartHeight) {
          setChartHeight(newHeight)
        }
      }
    })
    
    observer.observe(chartRef.current)
    return () => observer.disconnect()
  }, [isFlexHeight, chartHeight])

  return (
    <div className={cn("flex flex-col", isFlexHeight ? "h-full" : "")} ref={containerRef}>
      {/* Toolbar */}
      {!minimal && (
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
      )}

      {/* Chart container */}
      <div className={cn(minimal ? "" : "border-t", isFlexHeight ? "flex-1 min-h-0" : "")}>
        {isLoading ? (
          <Skeleton className="w-full h-full" style={isFlexHeight ? undefined : { height }} />
        ) : (
          <div 
            ref={chartRef} 
            className={isFlexHeight ? "h-full" : ""} 
            style={{ 
              touchAction: 'pan-y',
              ...(isFlexHeight ? {} : { height }) 
            }}
          >
          </div>
        )}
      </div>

      {/* Legend */}
      {!minimal && (
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground px-4 py-2 border-t shrink-0">
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
      )}
    </div>
  )
}


