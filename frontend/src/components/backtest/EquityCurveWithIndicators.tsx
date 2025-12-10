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
import { cn } from '@/lib/utils'

interface EquityCurveWithIndicatorsProps {
  stockCode: string
  equityCurve: EquityCurvePoint[] | null | undefined
  trades?: TradeRecord[] | null
  height?: number
}

// Indicator toggle configuration
interface IndicatorState {
  // Price display
  candle: boolean
  closeLine: boolean
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
  // Overlays
  volume: boolean
  macd: boolean
  rsi: boolean
  // Equity
  equity: boolean
}

// Y-axis display mode - which data's scale to show on right axis
type YAxisMode = 'price' | 'volume' | 'macd' | 'rsi' | 'equity'

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
  closeLine: '#6366f1', // indigo
  rsi: '#8b5cf6',     // violet
  macdDif: '#3b82f6', // blue
  macdDea: '#f97316', // orange
  volume: '#64748b',  // slate
}

export function EquityCurveWithIndicators({
  stockCode,
  equityCurve,
  trades,
  height = 500,
}: EquityCurveWithIndicatorsProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<HTMLDivElement>(null)
  const chartApiRef = useRef<IChartApi | null>(null)

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const colors = getMarketColors()

  // Indicator toggle state
  const [indicators, setIndicators] = useState<IndicatorState>({
    candle: true,
    closeLine: false,
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
    equity: true,
  })

  // Y-axis mode
  const [yAxisMode, setYAxisMode] = useState<YAxisMode>('price')

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
      rsi: calcRSI(sortedData, 14),
    }
  }, [klineData])

  const isLoading = klineLoading

  // Toggle indicator
  const toggleIndicator = (key: keyof IndicatorState) => {
    setIndicators(prev => ({ ...prev, [key]: !prev[key] }))
  }

  // Create/update chart
  useEffect(() => {
    if (!chartRef.current) return

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
        borderColor: isDark ? '#27272a' : '#e4e4e7',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        visible: true,
        borderColor: isDark ? '#27272a' : '#e4e4e7',
        scaleMargins: { top: 0.05, bottom: 0.05 },
      },
      leftPriceScale: {
        visible: false,
      },
      height,
      width: chartRef.current.clientWidth,
    })
    chartApiRef.current = chart

    // Determine which price scale to use for right axis based on yAxisMode
    const getRightScaleId = () => {
      switch (yAxisMode) {
        case 'price': return 'right'
        case 'volume': return 'volume'
        case 'macd': return 'macd'
        case 'rsi': return 'rsi'
        case 'equity': return 'equity'
        default: return 'right'
      }
    }

    // Series refs
    let candleSeries: ISeriesApi<'Candlestick'> | null = null
    let closeLineSeries: ISeriesApi<'Line'> | null = null
    let equitySeries: ISeriesApi<'Line'> | null = null
    let volumeSeries: ISeriesApi<'Histogram'> | null = null
    let macdHistSeries: ISeriesApi<'Histogram'> | null = null
    let macdDifSeries: ISeriesApi<'Line'> | null = null
    let macdDeaSeries: ISeriesApi<'Line'> | null = null
    let rsiSeries: ISeriesApi<'Line'> | null = null

    const lineSeries: { [key: string]: ISeriesApi<'Line'> } = {}

    // Add candlestick series (price scale)
    if (indicators.candle) {
      candleSeries = chart.addCandlestickSeries({
        upColor: colors.profit,
        downColor: colors.loss,
        borderUpColor: colors.profit,
        borderDownColor: colors.loss,
        wickUpColor: colors.profit,
        wickDownColor: colors.loss,
        priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
      })

      if (yAxisMode !== 'price') {
        chart.priceScale('price').applyOptions({
          visible: false,
          scaleMargins: { top: 0.05, bottom: 0.25 },
        })
      }
    }

    // Add close line series
    if (indicators.closeLine) {
      closeLineSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.closeLine,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: true,
        priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
      })

      if (yAxisMode !== 'price') {
        chart.priceScale('price').applyOptions({
          visible: false,
          scaleMargins: { top: 0.05, bottom: 0.25 },
        })
      }
    }

    // Add MA lines
    const maKeys = ['ma5', 'ma10', 'ma20', 'ma60'] as const
    maKeys.forEach(key => {
      if (indicators[key]) {
        lineSeries[key] = chart.addLineSeries({
          color: INDICATOR_COLORS[key],
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
        })
      }
    })

    // Add EMA lines
    const emaKeys = ['ema12', 'ema26'] as const
    emaKeys.forEach(key => {
      if (indicators[key]) {
        lineSeries[key] = chart.addLineSeries({
          color: INDICATOR_COLORS[key],
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
        })
      }
    })

    // Add Bollinger Bands
    if (indicators.boll) {
      lineSeries.bollUpper = chart.addLineSeries({
        color: INDICATOR_COLORS.bollUpper,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
      })
      lineSeries.bollMiddle = chart.addLineSeries({
        color: INDICATOR_COLORS.bollMiddle,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
      })
      lineSeries.bollLower = chart.addLineSeries({
        color: INDICATOR_COLORS.bollLower,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: yAxisMode === 'price' ? 'right' : 'price',
      })
    }

    // Add equity curve
    if (indicators.equity) {
      equitySeries = chart.addLineSeries({
        color: INDICATOR_COLORS.equity,
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: true,
        priceScaleId: yAxisMode === 'equity' ? 'right' : 'equity',
        priceFormat: {
          type: 'custom',
          formatter: (price: number) => '¥' + price.toLocaleString('zh-CN', { minimumFractionDigits: 0, maximumFractionDigits: 0 }),
        },
      })

      if (yAxisMode !== 'equity') {
        chart.priceScale('equity').applyOptions({
          visible: false,
          scaleMargins: { top: 0.05, bottom: 0.25 },
        })
      }
    }

    // Add volume (overlay)
    if (indicators.volume) {
      volumeSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: yAxisMode === 'volume' ? 'right' : 'volume',
      })

      chart.priceScale(yAxisMode === 'volume' ? 'right' : 'volume').applyOptions({
        visible: yAxisMode === 'volume',
        scaleMargins: { top: 0.7, bottom: 0 },
      })
    }

    // Add MACD (overlay)
    if (indicators.macd) {
      const macdScaleId = yAxisMode === 'macd' ? 'right' : 'macd'

      macdHistSeries = chart.addHistogramSeries({
        priceScaleId: macdScaleId,
      })

      macdDifSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.macdDif,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: macdScaleId,
      })

      macdDeaSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.macdDea,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceScaleId: macdScaleId,
      })

      chart.priceScale(macdScaleId).applyOptions({
        visible: yAxisMode === 'macd',
        scaleMargins: { top: 0.7, bottom: 0 },
      })
    }

    // Add RSI (overlay)
    if (indicators.rsi) {
      const rsiScaleId = yAxisMode === 'rsi' ? 'right' : 'rsi'

      rsiSeries = chart.addLineSeries({
        color: INDICATOR_COLORS.rsi,
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: true,
        priceScaleId: rsiScaleId,
      })

      chart.priceScale(rsiScaleId).applyOptions({
        visible: yAxisMode === 'rsi',
        scaleMargins: { top: 0.7, bottom: 0 },
      })
    }

    // Set K-line data
    if (klineData?.data) {
      const sortedKline = [...klineData.data].sort((a, b) => a.date.localeCompare(b.date))

      if (candleSeries) {
        const candleData: CandlestickData<Time>[] = sortedKline.map(d => ({
          time: d.date as Time,
          open: Number(d.open) || 0,
          high: Number(d.high) || 0,
          low: Number(d.low) || 0,
          close: Number(d.close) || 0,
        }))
        candleSeries.setData(candleData)
      }

      if (closeLineSeries) {
        const closeData: LineData<Time>[] = sortedKline.map(d => ({
          time: d.date as Time,
          value: Number(d.close) || 0,
        }))
        closeLineSeries.setData(closeData)
      }

      if (volumeSeries) {
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
    }

    // Set equity curve data
    if (equitySeries && equityCurve && equityCurve.length > 0) {
      const equityData = equityCurve
        .map(p => ({
          time: p.date as Time,
          value: Number(p.value),
        }))
        .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      equitySeries.setData(equityData)

      // Add trade markers
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
      }
    }

    // Set indicator data
    if (calculatedIndicators) {
      const mapToLineData = (dataMap: Map<string, number>): LineData<Time>[] =>
        Array.from(dataMap.entries())
          .map(([date, value]) => ({ time: date as Time, value }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

      // MA
      if (lineSeries.ma5) lineSeries.ma5.setData(mapToLineData(calculatedIndicators.ma5))
      if (lineSeries.ma10) lineSeries.ma10.setData(mapToLineData(calculatedIndicators.ma10))
      if (lineSeries.ma20) lineSeries.ma20.setData(mapToLineData(calculatedIndicators.ma20))
      if (lineSeries.ma60) lineSeries.ma60.setData(mapToLineData(calculatedIndicators.ma60))

      // EMA
      if (lineSeries.ema12) lineSeries.ema12.setData(mapToLineData(calculatedIndicators.ema12))
      if (lineSeries.ema26) lineSeries.ema26.setData(mapToLineData(calculatedIndicators.ema26))

      // BOLL
      if (lineSeries.bollUpper) lineSeries.bollUpper.setData(mapToLineData(calculatedIndicators.boll.upper))
      if (lineSeries.bollMiddle) lineSeries.bollMiddle.setData(mapToLineData(calculatedIndicators.boll.middle))
      if (lineSeries.bollLower) lineSeries.bollLower.setData(mapToLineData(calculatedIndicators.boll.lower))

      // MACD
      if (macdHistSeries && macdDifSeries && macdDeaSeries) {
        const { macd } = calculatedIndicators

        const histData: HistogramData<Time>[] = Array.from(macd.hist.entries())
          .map(([date, value]) => ({
            time: date as Time,
            value,
            color: value >= 0 ? `${colors.profit}80` : `${colors.loss}80`,
          }))
          .sort((a, b) => (a.time as string).localeCompare(b.time as string))

        macdHistSeries.setData(histData)
        macdDifSeries.setData(mapToLineData(macd.dif))
        macdDeaSeries.setData(mapToLineData(macd.dea))
      }

      // RSI
      if (rsiSeries) {
        rsiSeries.setData(mapToLineData(calculatedIndicators.rsi))
      }
    }

    chart.timeScale().fitContent()

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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDark, height, klineData, calculatedIndicators, equityCurve, trades, indicators, colors, markerMode, yAxisMode])

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

  // Y-axis mode options
  const yAxisOptions: { value: YAxisMode; label: string }[] = [
    { value: 'price', label: '价格' },
    { value: 'equity', label: '权益' },
    { value: 'volume', label: '成交量' },
    { value: 'macd', label: 'MACD' },
    { value: 'rsi', label: 'RSI' },
  ]

  return (
    <div className="space-y-2">
      {/* Indicator toggles - row 1: Price display */}
      <div className="flex flex-wrap gap-4 items-center">
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">价格:</span>
          <Button
            variant={indicators.candle ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('candle')}
          >
            蜡烛图
          </Button>
          <Button
            variant={indicators.closeLine ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('closeLine')}
          >
            收盘价
          </Button>
        </div>

        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">均线:</span>
          <Button
            variant={indicators.ma5 && indicators.ma10 && indicators.ma20 && indicators.ma60 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => {
              const allOn = indicators.ma5 && indicators.ma10 && indicators.ma20 && indicators.ma60
              setIndicators(prev => ({
                ...prev,
                ma5: !allOn,
                ma10: !allOn,
                ma20: !allOn,
                ma60: !allOn,
              }))
            }}
          >
            全部
          </Button>
          <Button
            variant={indicators.ma5 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma5')}
          >
            MA5
          </Button>
          <Button
            variant={indicators.ma10 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma10')}
          >
            MA10
          </Button>
          <Button
            variant={indicators.ma20 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma20')}
          >
            MA20
          </Button>
          <Button
            variant={indicators.ma60 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ma60')}
          >
            MA60
          </Button>
        </div>

        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">EMA:</span>
          <Button
            variant={indicators.ema12 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ema12')}
          >
            EMA12
          </Button>
          <Button
            variant={indicators.ema26 ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('ema26')}
          >
            EMA26
          </Button>
        </div>

        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">其他:</span>
          <Button
            variant={indicators.boll ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('boll')}
          >
            BOLL
          </Button>
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

        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground mr-1">回测:</span>
          <Button
            variant={indicators.equity ? 'default' : 'outline'}
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={() => toggleIndicator('equity')}
          >
            权益曲线
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={cycleMarkerMode}
          >
            {markerModeText[markerMode]}
          </Button>
        </div>

        {/* Y-axis selector */}
        <div className="flex items-center gap-1 ml-auto">
          <span className="text-xs text-muted-foreground mr-1">Y轴:</span>
          {yAxisOptions.map(opt => (
            <Button
              key={opt.value}
              variant={yAxisMode === opt.value ? 'default' : 'outline'}
              size="sm"
              className="h-6 px-2 text-xs"
              onClick={() => setYAxisMode(opt.value)}
              disabled={
                (opt.value === 'volume' && !indicators.volume) ||
                (opt.value === 'macd' && !indicators.macd) ||
                (opt.value === 'rsi' && !indicators.rsi) ||
                (opt.value === 'equity' && !indicators.equity) ||
                (opt.value === 'price' && !indicators.candle && !indicators.closeLine)
              }
            >
              {opt.label}
            </Button>
          ))}
        </div>
      </div>

      {/* Chart container */}
      <div ref={containerRef} className="rounded-lg border bg-card">
        {isLoading ? (
          <Skeleton className="w-full" style={{ height }} />
        ) : (
          <div ref={chartRef} style={{ height }} />
        )}

        {/* Legend */}
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground px-3 py-2 border-t">
          {indicators.candle && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: colors.profit }} />
              <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: colors.loss }} />
              K线
            </span>
          )}
          {indicators.closeLine && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.closeLine }} />
              收盘价
            </span>
          )}
          {indicators.equity && (
            <span className="flex items-center gap-1">
              <span className="w-3 h-0.5 rounded" style={{ backgroundColor: INDICATOR_COLORS.equity }} />
              权益曲线
            </span>
          )}
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
