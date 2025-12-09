import { useEffect, useRef, useState, useMemo } from 'react'
import {
  createChart,
  IChartApi,
  ISeriesApi,
  LineStyle,
  ColorType,
  Time,
  SeriesMarker,
  CandlestickData,
  LineData,
  HistogramData,
} from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getMarketColors } from '@/lib/market-colors'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { calcMA, calcEMA, calcBOLL, calcMACD, calcRSI } from '@/lib/indicators'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

interface EquityCurveWithIndicatorsProps {
  stockCode: string
  equityCurve: EquityCurvePoint[] | null | undefined
  trades?: TradeRecord[] | null
  height?: number
}

// Indicator toggle configuration
interface IndicatorState {
  // Main chart - MA
  ma5: boolean
  ma10: boolean
  ma20: boolean
  ma60: boolean
  // Main chart - EMA
  ema12: boolean
  ema26: boolean
  // Main chart - Bollinger
  boll: boolean
  // Sub charts
  volume: boolean
  macd: boolean
  rsi: boolean
}

// Marker display mode
type MarkerMode = 'none' | 'arrow' | 'arrow_price'

// Indicator color configuration
const INDICATOR_COLORS = {
  ma5: '#f59e0b',     // amber
  ma10: '#3b82f6',    // blue
  ma20: '#a855f7',    // purple
  ma60: '#22c55e',    // green
  ema12: '#06b6d4',   // cyan
  ema26: '#f97316',   // orange
  bollUpper: '#94a3b8', // slate (dashed)
  bollMiddle: '#64748b', // slate
  bollLower: '#94a3b8', // slate (dashed)
  equity: '#ec4899',  // pink
  rsi6: '#f59e0b',    // amber
  rsi12: '#3b82f6',   // blue
  rsi24: '#a855f7',   // purple
  macdDif: '#3b82f6', // blue
  macdDea: '#f97316', // orange
}

// Indicator toggle button config
const INDICATOR_GROUPS = [
  {
    name: '均线',
    items: [
      { key: 'ma5' as const, label: 'MA5' },
      { key: 'ma10' as const, label: 'MA10' },
      { key: 'ma20' as const, label: 'MA20' },
      { key: 'ma60' as const, label: 'MA60' },
    ],
  },
  {
    name: 'EMA',
    items: [
      { key: 'ema12' as const, label: 'EMA12' },
      { key: 'ema26' as const, label: 'EMA26' },
    ],
  },
  {
    name: '其他',
    items: [
      { key: 'boll' as const, label: 'BOLL' },
      { key: 'volume' as const, label: '成交量' },
      { key: 'macd' as const, label: 'MACD' },
      { key: 'rsi' as const, label: 'RSI' },
    ],
  },
]

export function EquityCurveWithIndicators({
  stockCode,
  equityCurve,
  trades,
  height = 500,
}: EquityCurveWithIndicatorsProps) {
  const mainChartRef = useRef<HTMLDivElement>(null)
  const volumeChartRef = useRef<HTMLDivElement>(null)
  const macdChartRef = useRef<HTMLDivElement>(null)
  const rsiChartRef = useRef<HTMLDivElement>(null)

  const mainChartApiRef = useRef<IChartApi | null>(null)
  const volumeChartApiRef = useRef<IChartApi | null>(null)
  const macdChartApiRef = useRef<IChartApi | null>(null)
  const rsiChartApiRef = useRef<IChartApi | null>(null)

  // Version counter to trigger sync re-setup when any chart is recreated
  const [chartVersion, setChartVersion] = useState(0)

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const colors = getMarketColors()

  // Indicator toggle state
  const [indicators, setIndicators] = useState<IndicatorState>({
    ma5: true,
    ma10: true,
    ma20: true,
    ma60: false,
    ema12: false,
    ema26: false,
    boll: false,
    volume: true,
    macd: false,
    rsi: false,
  })

  // Marker display mode
  const [markerMode, setMarkerMode] = useState<MarkerMode>('arrow_price')

  // Cycle marker mode
  const cycleMarkerMode = () => {
    setMarkerMode(prev => {
      if (prev === 'arrow_price') return 'arrow'
      if (prev === 'arrow') return 'none'
      return 'arrow_price'
    })
  }

  // Marker mode display text
  const markerModeText = {
    none: '信号:隐藏',
    arrow: '信号:箭头',
    arrow_price: '信号:详细',
  }

  // Extract date range from equity curve
  const dateRange = useMemo(() => {
    if (!equityCurve || equityCurve.length === 0) return null
    const dates = equityCurve.map(p => p.date).sort()
    return {
      startDate: dates[0],
      endDate: dates[dates.length - 1],
    }
  }, [equityCurve])

  // Fetch K-line data
  const { data: klineData, isLoading: klineLoading } = useGetKlineApiV1StocksCodeKlineGet(
    stockCode,
    dateRange ? { start_date: dateRange.startDate, end_date: dateRange.endDate } : undefined,
    {
      query: {
        enabled: !!stockCode && !!dateRange,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Calculate indicators from kline data
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
      ema12: calcEMA(sortedData, 12),
      ema26: calcEMA(sortedData, 26),
      boll: calcBOLL(sortedData),
      macd: calcMACD(sortedData),
      rsi6: calcRSI(sortedData, 6),
      rsi12: calcRSI(sortedData, 12),
      rsi24: calcRSI(sortedData, 24),
    }
  }, [klineData])

  const isLoading = klineLoading

  // Toggle indicator
  const toggleIndicator = (key: keyof IndicatorState) => {
    setIndicators(prev => ({ ...prev, [key]: !prev[key] }))
  }

  // Chart options factory
  const getChartOptions = (chartHeight: number, showTimeScale = true) => ({
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
        width: 1 as const,
        style: LineStyle.Dashed,
      },
      horzLine: {
        color: isDark ? '#52525b' : '#a1a1aa',
        width: 1 as const,
        style: LineStyle.Dashed,
      },
    },
    timeScale: {
      visible: showTimeScale,
      borderColor: isDark ? '#27272a' : '#e4e4e7',
      timeVisible: true,
      secondsVisible: false,
    },
    rightPriceScale: {
      borderColor: isDark ? '#27272a' : '#e4e4e7',
      scaleMargins: { top: 0.1, bottom: 0.1 },
      minimumWidth: 80, // Fixed width for alignment
    },
    leftPriceScale: {
      visible: true,
      borderColor: isDark ? '#27272a' : '#e4e4e7',
      minimumWidth: 80, // Fixed width for alignment
    },
    height: chartHeight,
  })

  // Calculate dynamic heights
  const subChartHeight = 100
  const activeSubCharts = [indicators.volume, indicators.macd, indicators.rsi].filter(Boolean).length
  const mainChartHeight = height - activeSubCharts * subChartHeight

  // Create/update main chart
  useEffect(() => {
    if (!mainChartRef.current) return

    // Clean up existing chart
    if (mainChartApiRef.current) {
      try {
        mainChartApiRef.current.remove()
      } catch {
        // Chart already disposed
      }
      mainChartApiRef.current = null
    }

    const chart = createChart(mainChartRef.current, {
      ...getChartOptions(mainChartHeight),
      width: mainChartRef.current.clientWidth,
    })
    mainChartApiRef.current = chart

    // Add candlestick series
    const candleSeries = chart.addCandlestickSeries({
      upColor: colors.profit,
      downColor: colors.loss,
      borderUpColor: colors.profit,
      borderDownColor: colors.loss,
      wickUpColor: colors.profit,
      wickDownColor: colors.loss,
    })

    // Add equity curve on left scale
    const equitySeries = chart.addAreaSeries({
      lineColor: INDICATOR_COLORS.equity,
      topColor: `${INDICATOR_COLORS.equity}40`,
      bottomColor: `${INDICATOR_COLORS.equity}00`,
      lineWidth: 2,
      priceScaleId: 'left',
      priceFormat: {
        type: 'custom',
        formatter: (price: number) => '¥' + price.toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 }),
      },
    })

    // Configure left price scale for equity
    chart.priceScale('left').applyOptions({
      scaleMargins: { top: 0.1, bottom: 0.1 },
    })

    // Series refs for updates
    const seriesRefs = {
      candle: candleSeries,
      equity: equitySeries,
      ma5: null as ISeriesApi<'Line'> | null,
      ma10: null as ISeriesApi<'Line'> | null,
      ma20: null as ISeriesApi<'Line'> | null,
      ma60: null as ISeriesApi<'Line'> | null,
      ema12: null as ISeriesApi<'Line'> | null,
      ema26: null as ISeriesApi<'Line'> | null,
      bollUpper: null as ISeriesApi<'Line'> | null,
      bollMiddle: null as ISeriesApi<'Line'> | null,
      bollLower: null as ISeriesApi<'Line'> | null,
    }

    // Add MA lines
    if (indicators.ma5) {
      seriesRefs.ma5 = chart.addLineSeries({
        color: INDICATOR_COLORS.ma5,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }
    if (indicators.ma10) {
      seriesRefs.ma10 = chart.addLineSeries({
        color: INDICATOR_COLORS.ma10,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }
    if (indicators.ma20) {
      seriesRefs.ma20 = chart.addLineSeries({
        color: INDICATOR_COLORS.ma20,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }
    if (indicators.ma60) {
      seriesRefs.ma60 = chart.addLineSeries({
        color: INDICATOR_COLORS.ma60,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }

    // Add EMA lines
    if (indicators.ema12) {
      seriesRefs.ema12 = chart.addLineSeries({
        color: INDICATOR_COLORS.ema12,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }
    if (indicators.ema26) {
      seriesRefs.ema26 = chart.addLineSeries({
        color: INDICATOR_COLORS.ema26,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }

    // Add Bollinger Bands
    if (indicators.boll) {
      seriesRefs.bollUpper = chart.addLineSeries({
        color: INDICATOR_COLORS.bollUpper,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      seriesRefs.bollMiddle = chart.addLineSeries({
        color: INDICATOR_COLORS.bollMiddle,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      })
      seriesRefs.bollLower = chart.addLineSeries({
        color: INDICATOR_COLORS.bollLower,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
      })
    }

    // Set K-line data
    if (klineData?.data) {
      const candleData: CandlestickData<Time>[] = klineData.data.map(d => ({
        time: d.date as Time,
        open: Number(d.open) || 0,
        high: Number(d.high) || 0,
        low: Number(d.low) || 0,
        close: Number(d.close) || 0,
      })).sort((a, b) => (a.time as string).localeCompare(b.time as string))

      candleSeries.setData(candleData)
    }

    // Set equity curve data
    if (equityCurve && equityCurve.length > 0) {
      const equityData = equityCurve
        .map(p => ({
          time: p.date as Time,
          value: Number(p.value),
        }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      equitySeries.setData(equityData)

      // Add trade markers based on markerMode
      if (trades && trades.length > 0 && markerMode !== 'none') {
        const markers: SeriesMarker<Time>[] = trades.flatMap((trade) => {
          const entryDate = trade.entry_date || trade.open_datetime?.split(' ')[0]
          const exitDate = trade.exit_date || trade.close_datetime?.split(' ')[0]
          const entryPrice = trade.entry_price ?? trade.open_price
          const exitPrice = trade.exit_price ?? trade.close_price

          const result: SeriesMarker<Time>[] = []

          if (entryDate) {
            result.push({
              time: entryDate as Time,
              position: 'belowBar',
              color: colors.profit,
              shape: 'arrowUp',
              text: markerMode === 'arrow_price' ? `B @${entryPrice?.toFixed(2) ?? ''}` : '',
            })
          }

          if (exitDate) {
            result.push({
              time: exitDate as Time,
              position: 'aboveBar',
              color: colors.loss,
              shape: 'arrowDown',
              text: markerMode === 'arrow_price' ? `S @${exitPrice?.toFixed(2) ?? ''}` : '',
            })
          }

          return result
        }).sort((a, b) => (a.time as string).localeCompare(b.time as string))

        equitySeries.setMarkers(markers)
      } else {
        equitySeries.setMarkers([])
      }
    }

    // Set indicator data from frontend calculation
    if (calculatedIndicators) {
      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      if (seriesRefs.ma5) seriesRefs.ma5.setData(mapToLineData(calculatedIndicators.ma5))
      if (seriesRefs.ma10) seriesRefs.ma10.setData(mapToLineData(calculatedIndicators.ma10))
      if (seriesRefs.ma20) seriesRefs.ma20.setData(mapToLineData(calculatedIndicators.ma20))
      if (seriesRefs.ma60) seriesRefs.ma60.setData(mapToLineData(calculatedIndicators.ma60))
      if (seriesRefs.ema12) seriesRefs.ema12.setData(mapToLineData(calculatedIndicators.ema12))
      if (seriesRefs.ema26) seriesRefs.ema26.setData(mapToLineData(calculatedIndicators.ema26))
      if (seriesRefs.bollUpper) seriesRefs.bollUpper.setData(mapToLineData(calculatedIndicators.boll.upper))
      if (seriesRefs.bollMiddle) seriesRefs.bollMiddle.setData(mapToLineData(calculatedIndicators.boll.middle))
      if (seriesRefs.bollLower) seriesRefs.bollLower.setData(mapToLineData(calculatedIndicators.boll.lower))
    }

    chart.timeScale().fitContent()

    // Trigger sync re-setup
    setChartVersion(v => v + 1)

    // Resize handler
    const handleResize = () => {
      if (mainChartRef.current && mainChartApiRef.current) {
        mainChartApiRef.current.applyOptions({
          width: mainChartRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      try {
        chart.remove()
      } catch {
        // Chart already disposed
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, mainChartHeight, klineData, calculatedIndicators, equityCurve, trades, indicators, colors, markerMode])

  // Create/update volume chart
  useEffect(() => {
    if (!volumeChartRef.current || !indicators.volume) {
      if (volumeChartApiRef.current) {
        try {
          volumeChartApiRef.current.remove()
        } catch {
          // Chart already disposed
        }
        volumeChartApiRef.current = null
      }
      return
    }

    const chart = createChart(volumeChartRef.current, {
      ...getChartOptions(subChartHeight, false), // Hide time scale on sub-chart
      width: volumeChartRef.current.clientWidth,
    })
    volumeChartApiRef.current = chart

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '',
    })

    chart.priceScale('').applyOptions({
      scaleMargins: { top: 0.1, bottom: 0 },
    })

    if (klineData?.data) {
      const volumeData: HistogramData<Time>[] = klineData.data.map(d => {
        const change = (Number(d.close) || 0) - (Number(d.open) || 0)
        return {
          time: d.date as Time,
          value: Number(d.volume) || 0,
          color: change >= 0 ? colors.profit : colors.loss,
        }
      }).sort((a, b) => (a.time as string).localeCompare(b.time as string))

      volumeSeries.setData(volumeData)
    }

    chart.timeScale().fitContent()

    // Trigger sync re-setup
    setChartVersion(v => v + 1)

    const handleResize = () => {
      if (volumeChartRef.current && volumeChartApiRef.current) {
        volumeChartApiRef.current.applyOptions({
          width: volumeChartRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      try {
        chart.remove()
      } catch {
        // Chart already disposed
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, indicators.volume, klineData, colors])

  // Create/update MACD chart
  useEffect(() => {
    if (!macdChartRef.current || !indicators.macd) {
      if (macdChartApiRef.current) {
        try {
          macdChartApiRef.current.remove()
        } catch {
          // Chart already disposed
        }
        macdChartApiRef.current = null
      }
      return
    }

    const chart = createChart(macdChartRef.current, {
      ...getChartOptions(subChartHeight, false), // Hide time scale on sub-chart
      width: macdChartRef.current.clientWidth,
    })
    macdChartApiRef.current = chart

    const macdHistSeries = chart.addHistogramSeries({
      priceScaleId: '',
    })

    const difSeries = chart.addLineSeries({
      color: INDICATOR_COLORS.macdDif,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      priceScaleId: '',
    })

    const deaSeries = chart.addLineSeries({
      color: INDICATOR_COLORS.macdDea,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      priceScaleId: '',
    })

    chart.priceScale('').applyOptions({
      scaleMargins: { top: 0.1, bottom: 0.1 },
    })

    if (calculatedIndicators) {
      const { macd } = calculatedIndicators

      const histData: HistogramData<Time>[] = Array.from(macd.hist.entries())
        .map(([date, value]) => ({
          time: date as Time,
          value,
          color: value >= 0 ? colors.profit : colors.loss,
        }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      const difData: LineData<Time>[] = Array.from(macd.dif.entries())
        .map(([date, value]) => ({ time: date as Time, value }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      const deaData: LineData<Time>[] = Array.from(macd.dea.entries())
        .map(([date, value]) => ({ time: date as Time, value }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      macdHistSeries.setData(histData)
      difSeries.setData(difData)
      deaSeries.setData(deaData)
    }

    chart.timeScale().fitContent()

    // Trigger sync re-setup
    setChartVersion(v => v + 1)

    const handleResize = () => {
      if (macdChartRef.current && macdChartApiRef.current) {
        macdChartApiRef.current.applyOptions({
          width: macdChartRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      try {
        chart.remove()
      } catch {
        // Chart already disposed
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, indicators.macd, calculatedIndicators, colors])

  // Create/update RSI chart
  useEffect(() => {
    if (!rsiChartRef.current || !indicators.rsi) {
      if (rsiChartApiRef.current) {
        try {
          rsiChartApiRef.current.remove()
        } catch {
          // Chart already disposed
        }
        rsiChartApiRef.current = null
      }
      return
    }

    const chart = createChart(rsiChartRef.current, {
      ...getChartOptions(subChartHeight, false), // Hide time scale on sub-chart
      width: rsiChartRef.current.clientWidth,
    })
    rsiChartApiRef.current = chart

    const rsi6Series = chart.addLineSeries({
      color: INDICATOR_COLORS.rsi6,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    const rsi12Series = chart.addLineSeries({
      color: INDICATOR_COLORS.rsi12,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    const rsi24Series = chart.addLineSeries({
      color: INDICATOR_COLORS.rsi24,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    chart.priceScale('right').applyOptions({
      scaleMargins: { top: 0.1, bottom: 0.1 },
    })

    if (calculatedIndicators) {
      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      rsi6Series.setData(mapToLineData(calculatedIndicators.rsi6))
      rsi12Series.setData(mapToLineData(calculatedIndicators.rsi12))
      rsi24Series.setData(mapToLineData(calculatedIndicators.rsi24))
    }

    chart.timeScale().fitContent()

    // Trigger sync re-setup
    setChartVersion(v => v + 1)

    const handleResize = () => {
      if (rsiChartRef.current && rsiChartApiRef.current) {
        rsiChartApiRef.current.applyOptions({
          width: rsiChartRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      try {
        chart.remove()
      } catch {
        // Chart already disposed
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, indicators.rsi, calculatedIndicators])

  // Centralized time scale sync across all charts
  useEffect(() => {
    // Use requestAnimationFrame to ensure charts are fully initialized
    const timeoutId = requestAnimationFrame(() => {
      const charts = [
        mainChartApiRef.current,
        volumeChartApiRef.current,
        macdChartApiRef.current,
        rsiChartApiRef.current,
      ].filter((c): c is IChartApi => c !== null)

      if (charts.length < 2) return

      let isSyncing = false

      const syncToAll = (sourceChart: IChartApi) => {
        if (isSyncing) return
        const range = sourceChart.timeScale().getVisibleLogicalRange()
        if (!range) return

        isSyncing = true
        charts.forEach(chart => {
          if (chart !== sourceChart) {
            try {
              chart.timeScale().setVisibleLogicalRange(range)
            } catch {
              // Chart might be disposed
            }
          }
        })
        isSyncing = false
      }

      charts.forEach(chart => {
        try {
          chart.timeScale().subscribeVisibleLogicalRangeChange(() => syncToAll(chart))
        } catch {
          // Chart might be disposed
        }
      })
    })

    return () => {
      cancelAnimationFrame(timeoutId)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chartVersion]) // Re-run whenever any chart is recreated

  if (!equityCurve || equityCurve.length === 0) {
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
      {/* Indicator toggles */}
      <div className="flex flex-wrap gap-4">
        {INDICATOR_GROUPS.map(group => (
          <div key={group.name} className="flex items-center gap-1">
            <span className="text-xs text-muted-foreground mr-1">{group.name}:</span>
            {group.items.map(item => (
              <Button
                key={item.key}
                variant={indicators[item.key] ? 'default' : 'outline'}
                size="sm"
                className="h-6 px-2 text-xs"
                onClick={() => toggleIndicator(item.key)}
              >
                {item.label}
              </Button>
            ))}
          </div>
        ))}
        {/* Marker mode toggle - single cycling button */}
        <Button
          variant="outline"
          size="sm"
          className="h-6 px-2 text-xs"
          onClick={cycleMarkerMode}
        >
          {markerModeText[markerMode]}
        </Button>
      </div>

      {/* Charts container */}
      <div className="rounded-lg border bg-card">
        {isLoading ? (
          <Skeleton className="w-full" style={{ height }} />
        ) : (
          <>
            {/* Main chart */}
            <div ref={mainChartRef} style={{ height: mainChartHeight }} />

            {/* Volume sub-chart */}
            {indicators.volume && (
              <div ref={volumeChartRef} className="border-t" style={{ height: subChartHeight }} />
            )}

            {/* MACD sub-chart */}
            {indicators.macd && (
              <div ref={macdChartRef} className="border-t" style={{ height: subChartHeight }} />
            )}

            {/* RSI sub-chart */}
            {indicators.rsi && (
              <div ref={rsiChartRef} className="border-t" style={{ height: subChartHeight }} />
            )}
          </>
        )}
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <span className="flex items-center gap-1">
          <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.equity }} />
          权益曲线
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
        {indicators.ema12 && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ema12 }} />
            EMA12
          </span>
        )}
        {indicators.ema26 && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.ema26 }} />
            EMA26
          </span>
        )}
        {indicators.boll && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-0.5 rounded border border-dashed" style={{ borderColor: INDICATOR_COLORS.bollMiddle }} />
            BOLL
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
          <>
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.rsi6 }} />
              RSI6
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.rsi12 }} />
              RSI12
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.rsi24 }} />
              RSI24
            </span>
          </>
        )}
      </div>
    </div>
  )
}
