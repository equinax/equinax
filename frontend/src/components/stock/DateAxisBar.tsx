import { useEffect, useRef } from 'react'
import {
  createChart,
  IChartApi,
  ISeriesApi,
  ColorType,
  Time,
  LineData,
  SeriesType,
} from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getChartThemeColors } from '@/lib/chart-theme'

interface DateAxisBarProps {
  sharedDates: string[]
  endDate?: string
  onChartReady?: (chart: IChartApi, series: ISeriesApi<SeriesType>) => void
}

const DATE_AXIS_HEIGHT = 30
/** Must match StockChart's PRICE_SCALE_MIN_WIDTH for pixel-perfect x-axis alignment */
export const PRICE_SCALE_MIN_WIDTH = 80

export function DateAxisBar({ sharedDates, endDate, onChartReady }: DateAxisBarProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const onChartReadyRef = useRef(onChartReady)
  onChartReadyRef.current = onChartReady

  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const chartColors = getChartThemeColors(isDark)

  useEffect(() => {
    if (!containerRef.current || sharedDates.length === 0) return

    if (chartRef.current) {
      chartRef.current.remove()
      chartRef.current = null
    }

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: chartColors.text,
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { visible: false },
      },
      width: containerRef.current.clientWidth,
      height: DATE_AXIS_HEIGHT,
      rightPriceScale: {
        visible: true,
        borderVisible: false,
        minimumWidth: PRICE_SCALE_MIN_WIDTH,
      },
      leftPriceScale: {
        visible: false,
      },
      timeScale: {
        borderColor: chartColors.border,
        timeVisible: false,
        visible: true,
        fixLeftEdge: false,
        fixRightEdge: false,
        rightOffset: 5,
        minBarSpacing: 1,
        borderVisible: true,
      },
      crosshair: {
        mode: 0,
        vertLine: {
          visible: true,
          labelVisible: true,
          style: 2,
          width: 1,
          color: 'rgba(150, 150, 150, 0.5)',
        },
        horzLine: { visible: false, labelVisible: false },
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
      localization: {
        dateFormat: 'yyyy-MM-dd',
      },
    })

    chartRef.current = chart

    const series = chart.addLineSeries({
      color: 'transparent',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
      crosshairMarkerVisible: false,
      priceScaleId: 'right',
      priceFormat: {
        type: 'custom',
        formatter: () => '',
      },
    })

    const lineData: LineData<Time>[] = sharedDates.map(date => ({
      time: date as Time,
      value: 0,
    }))
    series.setData(lineData)

    if (endDate) {
      const sortedDates = sharedDates
      const refIdx = sortedDates.indexOf(endDate)
      if (refIdx >= 0) {
        const halfDays = 66
        const from = Math.max(0, refIdx - halfDays)
        const to = Math.min(sortedDates.length - 1, refIdx + halfDays)
        chart.timeScale().setVisibleLogicalRange({ from, to })
      } else {
        chart.timeScale().fitContent()
      }
    } else {
      chart.timeScale().fitContent()
    }

    onChartReadyRef.current?.(chart, series)

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (chartRef.current) {
          try {
            chartRef.current.applyOptions({ width: entry.contentRect.width })
          } catch {
          }
        }
      }
    })

    if (containerRef.current) {
      resizeObserver.observe(containerRef.current)
    }

    return () => {
      resizeObserver.disconnect()
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [sharedDates, endDate, isDark, chartColors.text, chartColors.border])

  if (sharedDates.length === 0) return null

  return (
    <div
      ref={containerRef}
      className="border-x border-t bg-background"
      style={{ height: DATE_AXIS_HEIGHT }}
    />
  )
}
