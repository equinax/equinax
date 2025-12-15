import { useEffect, useRef, useMemo, useCallback, useState } from 'react'
import { createChart, IChartApi, ISeriesApi, LineStyle, ColorType, LineData, Time, SeriesMarker } from 'lightweight-charts'
import { useTheme } from '@/components/theme-provider'
import { getChartPalette, getMarketColors } from '@/lib/market-colors'
import { getChartThemeColors } from '@/lib/chart-theme'
import { cn } from '@/lib/utils'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

interface MultiEquityCurveChartProps {
  data: Record<string, EquityCurvePoint[]> | null | undefined
  trades?: Record<string, TradeRecord[]> | null
  height?: number
  /** 所有可选股票代码（用于显示完整图例） */
  allStockCodes?: string[]
  /** 隐藏的股票代码集合 */
  hiddenStocks?: Set<string>
  /** 股票显示/隐藏切换回调 */
  onToggleStock?: (stockCode: string) => void
}

export function MultiEquityCurveChart({
  data,
  trades,
  height = 400,
  allStockCodes,
  hiddenStocks,
  onToggleStock,
}: MultiEquityCurveChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesMapRef = useRef<Map<string, ISeriesApi<'Line'>>>(new Map())
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  // 使用 allStockCodes 如果提供，否则从 data 获取
  const stockCodes = allStockCodes ?? (data ? Object.keys(data) : [])

  // 获取主题感知的调色板
  const chartPalette = useMemo(() => getChartPalette(isDark), [isDark])

  // 灰色调色板用于隐藏的股票
  const grayColor = isDark ? '#52525b' : '#a1a1aa'

  // 拖拽选择状态
  const [isDragging, setIsDragging] = useState(false)
  const [dragAction, setDragAction] = useState<'show' | 'hide' | null>(null)

  // 全局 mouseup 监听（防止在图例外松开鼠标）
  useEffect(() => {
    const handleMouseUp = () => {
      setIsDragging(false)
      setDragAction(null)
    }
    window.addEventListener('mouseup', handleMouseUp)
    return () => window.removeEventListener('mouseup', handleMouseUp)
  }, [])

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

    // Remove series for stocks that no longer exist in data
    existingSeries.forEach((series, stockCode) => {
      if (!data[stockCode]) {
        chart.removeSeries(series)
        existingSeries.delete(stockCode)
      }
    })

    // 获取主题调色板
    const palette = getChartPalette(isDark)

    // Add or update series for each stock
    // 关键：使用 stockCodes (allStockCodes) 来获取正确的颜色索引
    Object.entries(data).forEach(([stockCode, points]) => {
      if (!Array.isArray(points)) return

      // 使用 stockCode 在 allStockCodes 中的索引来获取颜色，保持颜色一致性
      const colorIndex = stockCodes.indexOf(stockCode)
      const color = palette[(colorIndex >= 0 ? colorIndex : 0) % palette.length]

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
      } else {
        // 更新已有 series 的颜色（主题切换时）
        series.applyOptions({ color })
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
  }, [data, isDark, stockCodes])

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

  // 获取股票在所有股票列表中的索引（用于保持颜色一致性）
  const getStockIndex = useCallback((stockCode: string) => {
    return stockCodes.indexOf(stockCode)
  }, [stockCodes])

  const hasData = data && Object.keys(data).length > 0

  // 图例组件（提取为变量以便复用）
  const legendElement = stockCodes.length > 0 && (
    <div className="flex flex-wrap gap-2 justify-center max-h-32 overflow-y-auto p-2">
      {stockCodes.map((stockCode) => {
        const index = getStockIndex(stockCode)
        const isHidden = hiddenStocks?.has(stockCode) ?? false
        const color = chartPalette[index % chartPalette.length]

        return (
          <button
            key={stockCode}
            type="button"
            className={cn(
              'flex items-center gap-1.5 px-2 py-1 rounded-md text-xs transition-all select-none',
              'hover:bg-muted/80',
              onToggleStock ? 'cursor-pointer' : 'cursor-default',
              isHidden ? 'opacity-50' : 'opacity-100'
            )}
            disabled={!onToggleStock}
            onMouseDown={(e) => {
              if (!onToggleStock) return
              e.preventDefault()
              setIsDragging(true)
              // 根据当前状态决定拖拽动作
              const action = isHidden ? 'show' : 'hide'
              setDragAction(action)
              onToggleStock(stockCode)
            }}
            onMouseEnter={() => {
              if (!isDragging || !dragAction || !onToggleStock) return
              // 只有当状态与拖拽动作不一致时才切换
              if ((dragAction === 'show' && isHidden) ||
                  (dragAction === 'hide' && !isHidden)) {
                onToggleStock(stockCode)
              }
            }}
          >
            <div
              className="w-2.5 h-2.5 rounded-full transition-colors"
              style={{ backgroundColor: isHidden ? grayColor : color }}
            />
            <span className={cn(
              'transition-colors',
              isHidden ? 'text-muted-foreground line-through' : 'text-foreground'
            )}>
              {stockCode}
            </span>
          </button>
        )
      })}
    </div>
  )

  return (
    <div className="space-y-3">
      {/* 图表区域 - 容器始终存在以保持 chart 实例 */}
      <div className="rounded-lg border bg-card relative" style={{ minHeight: height }}>
        <div ref={chartContainerRef} />
        {/* 无数据时显示提示覆盖层 */}
        {!hasData && (
          <div
            className="absolute inset-0 flex items-center justify-center bg-muted/30 rounded-lg"
          >
            <p className="text-muted-foreground">
              {stockCodes.length > 0 ? '点击图例选择要显示的股票' : '暂无权益曲线数据'}
            </p>
          </div>
        )}
      </div>
      {/* 图例始终显示 */}
      {legendElement}
    </div>
  )
}
