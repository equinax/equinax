import { useMemo, useCallback, useState, useEffect } from 'react'
import ReactECharts from 'echarts-for-react'
import { useTheme } from '@/components/theme-provider'
import { getChartPalette } from '@/lib/market-colors'
import { cn } from '@/lib/utils'
import type { MonthlyReturns } from '@/types/backtest'

interface MultiMonthlyReturnsChartProps {
  data: Record<string, MonthlyReturns> | null | undefined
  height?: number
  /** 所有可选股票代码（用于显示完整图例） */
  allStockCodes?: string[]
  /** 隐藏的股票代码集合 */
  hiddenStocks?: Set<string>
  /** 股票显示/隐藏切换回调 */
  onToggleStock?: (stockCode: string) => void
}

export function MultiMonthlyReturnsChart({
  data,
  height = 350,
  allStockCodes,
  hiddenStocks,
  onToggleStock,
}: MultiMonthlyReturnsChartProps) {
  const { theme } = useTheme()

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  const grayColor = isDark ? '#52525b' : '#a1a1aa'

  // 使用 allStockCodes 如果提供，否则从 data 获取
  const legendStockCodes = allStockCodes ?? (data ? Object.keys(data) : [])

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

  // 获取主题感知的调色板
  const chartPalette = useMemo(() => getChartPalette(isDark), [isDark])

  const { stockCodes, months, seriesData } = useMemo(() => {
    if (!data || typeof data !== 'object') {
      return { stockCodes: [], months: [], seriesData: [] }
    }

    const codes = Object.keys(data)

    // Collect all months across all stocks
    const monthSet = new Set<string>()
    Object.values(data).forEach(monthlyData => {
      if (monthlyData && typeof monthlyData === 'object') {
        Object.keys(monthlyData).forEach(month => monthSet.add(month))
      }
    })
    const sortedMonths = Array.from(monthSet).sort()

    // Build series data for each stock
    // 关键：使用 legendStockCodes 中的索引来获取颜色，保持颜色一致性
    const series = codes.map((stockCode) => {
      const monthlyData = data[stockCode] || {}
      // 使用 stockCode 在 legendStockCodes 中的索引
      const colorIndex = legendStockCodes.indexOf(stockCode)
      return {
        name: stockCode,
        type: 'bar' as const,
        data: sortedMonths.map(month => {
          const value = monthlyData[month]
          return typeof value === 'number' ? value : 0
        }),
        itemStyle: {
          color: chartPalette[(colorIndex >= 0 ? colorIndex : 0) % chartPalette.length],
        },
      }
    })

    return { stockCodes: codes, months: sortedMonths, seriesData: series }
  }, [data, legendStockCodes, chartPalette])

  const option = useMemo(() => ({
    tooltip: {
      trigger: 'axis' as const,
      axisPointer: { type: 'shadow' as const },
      formatter: (params: { seriesName: string; value: number; color: string; name: string }[]) => {
        const month = params[0]?.name || ''
        const items = params
          .map(p => `<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:${p.color};"></span>${p.seriesName}: ${(p.value * 100).toFixed(2)}%`)
          .join('<br/>')
        return `${month}<br/>${items}`
      },
    },
    // 禁用内置 legend，使用自定义图例
    legend: {
      show: false,
    },
    grid: {
      top: 20,
      left: 60,
      right: 20,
      bottom: 60,
    },
    xAxis: {
      type: 'category' as const,
      data: months,
      axisLabel: {
        rotate: 45,
        color: isDark ? '#a1a1aa' : '#71717a',
        fontSize: 10,
      },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    yAxis: {
      type: 'value' as const,
      axisLabel: {
        formatter: (value: number) => `${(value * 100).toFixed(0)}%`,
        color: isDark ? '#a1a1aa' : '#71717a',
      },
      axisLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
      splitLine: { lineStyle: { color: isDark ? '#27272a' : '#e4e4e7' } },
    },
    series: seriesData,
  }), [stockCodes, months, seriesData, isDark])

  // 获取股票在所有股票列表中的索引（用于保持颜色一致性）
  const getStockIndex = useCallback((stockCode: string) => {
    return legendStockCodes.indexOf(stockCode)
  }, [legendStockCodes])

  const hasData = data && Object.keys(data).length > 0

  // 图例组件（提取为变量以便复用）
  const legendElement = legendStockCodes.length > 0 && (
    <div className="flex flex-wrap gap-2 justify-center max-h-32 overflow-y-auto p-2">
      {legendStockCodes.map((stockCode) => {
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
      {/* 图表区域 */}
      {hasData ? (
        <div className="rounded-lg border bg-card p-4">
          <ReactECharts
            option={option}
            style={{ height }}
            notMerge={true}
          />
        </div>
      ) : (
        <div
          className="flex items-center justify-center bg-muted/30 rounded-lg"
          style={{ height }}
        >
          <p className="text-muted-foreground">
            {legendStockCodes.length > 0 ? '点击图例选择要显示的股票' : '暂无月度收益数据'}
          </p>
        </div>
      )}
      {/* 图例始终显示 */}
      {legendElement}
    </div>
  )
}
