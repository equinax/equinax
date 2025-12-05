import { useEffect, useRef, useState } from 'react'
import { createChart, IChartApi, ISeriesApi, LineStyle, ColorType, AreaData, Time } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import type { EquityCurvePoint } from '@/types/backtest'

interface EquityCurveChartProps {
  data: EquityCurvePoint[] | { [key: string]: unknown }[] | null | undefined
  height?: number
}

export function EquityCurveChart({ data, height = 400 }: EquityCurveChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<'Area'> | null>(null)
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

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

    // Add area series
    const series = chart.addAreaSeries({
      lineColor: '#3b82f6',
      topColor: 'rgba(59, 130, 246, 0.4)',
      bottomColor: 'rgba(59, 130, 246, 0.0)',
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
