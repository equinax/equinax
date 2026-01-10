/**
 * ETF Matrix Tooltip Component
 *
 * Rich tooltip showing cell details:
 * - Sub-category name and parent category
 * - Date
 * - Change percentage
 * - Trading amount
 * - Representative ETF info
 * - Dynamically loaded ETF list sorted by change %
 */

import { useState, useEffect, useRef } from 'react'
import { createPortal } from 'react-dom'
import { getEtfSubcategoryListApiV1AlphaRadarEtfSubcategoryListGet } from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfSubcategoryItem } from '@/api/generated/schemas'
import type { EtfTooltipData } from './types'

interface EtfMatrixTooltipProps {
  data: EtfTooltipData
}

/**
 * Format large numbers in Chinese units
 */
function formatAmount(value: number | null): string {
  if (value === null) return '-'
  const absValue = Math.abs(value)
  if (absValue >= 1e8) {
    return `${(value / 1e8).toFixed(2)}亿`
  }
  if (absValue >= 1e4) {
    return `${(value / 1e4).toFixed(0)}万`
  }
  return value.toFixed(0)
}

/**
 * Format percentage
 */
function formatPercent(value: number | null): string {
  if (value === null) return '-'
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${value.toFixed(2)}%`
}

export function EtfMatrixTooltip({ data }: EtfMatrixTooltipProps) {
  const {
    subCategory,
    category,
    categoryLabel,
    date,
    changePct,
    amount,
    repName,
    repCode,
    mouseX,
    mouseY,
  } = data

  // State for ETF list
  const [etfList, setEtfList] = useState<EtfSubcategoryItem[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const debounceRef = useRef<NodeJS.Timeout | null>(null)
  const lastRequestRef = useRef<string>('')

  // Load ETF list with debounce
  useEffect(() => {
    const requestKey = `${category}-${subCategory}-${date}`

    // Skip if same request
    if (requestKey === lastRequestRef.current) return

    // Clear previous timer
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }

    // Reset state
    setEtfList([])
    setIsLoading(true)

    // Debounce API call (300ms)
    debounceRef.current = setTimeout(async () => {
      try {
        const result = await getEtfSubcategoryListApiV1AlphaRadarEtfSubcategoryListGet({
          category,
          sub_category: subCategory,
          date,
        })
        lastRequestRef.current = requestKey
        setEtfList(result.etfs || [])
      } catch (error) {
        console.error('Failed to load ETF list:', error)
        setEtfList([])
      } finally {
        setIsLoading(false)
      }
    }, 300)

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current)
      }
    }
  }, [category, subCategory, date])

  // Position tooltip to avoid going off-screen
  const tooltipStyle: React.CSSProperties = {
    left: mouseX + 12,
    top: mouseY + 12,
    maxWidth: 280,
  }

  // Adjust if too close to right edge
  if (mouseX > window.innerWidth - 300) {
    tooltipStyle.left = mouseX - 240
  }

  // Adjust if too close to bottom edge (account for ETF list height)
  const estimatedHeight = 180 + Math.min(etfList.length, 8) * 20
  if (mouseY > window.innerHeight - estimatedHeight) {
    tooltipStyle.top = mouseY - estimatedHeight + 40
  }

  return createPortal(
    <div
      className="fixed z-50 bg-white/90 backdrop-blur-sm text-gray-900 rounded-lg p-3 shadow-lg text-sm pointer-events-none border border-gray-200"
      style={tooltipStyle}
    >
      {/* Header */}
      <div className="flex justify-between items-center gap-4 mb-2">
        <span className="text-gray-500">{date}</span>
        <div className="text-right">
          <span className="font-bold">{subCategory}</span>
          <span className="text-gray-400 text-xs ml-1">({categoryLabel})</span>
        </div>
      </div>

      {/* Metrics */}
      <div className="border-t border-gray-200 pt-2 space-y-1">
        {/* Change */}
        <div className="flex justify-between">
          <span className="text-gray-500">涨跌幅</span>
          <span
            className={
              changePct !== null && changePct > 0
                ? 'text-red-500 font-mono font-medium'
                : changePct !== null && changePct < 0
                  ? 'text-green-500 font-mono font-medium'
                  : 'font-mono'
            }
          >
            {formatPercent(changePct)}
          </span>
        </div>

        {/* Trading Amount */}
        {amount !== null && (
          <div className="flex justify-between">
            <span className="text-gray-500">成交额</span>
            <span className="font-mono">{formatAmount(amount)}</span>
          </div>
        )}

        {/* Representative ETF */}
        {repName && (
          <div className="flex justify-between items-start">
            <span className="text-gray-500">代表ETF</span>
            <div className="text-right">
              <div className="text-gray-700">{repName}</div>
              {repCode && (
                <div className="text-gray-400 text-xs font-mono">{repCode}</div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ETF List Section */}
      <div className="border-t border-gray-200 mt-2 pt-2">
        <div className="text-gray-500 text-xs mb-1">成分ETF (按涨幅排序)</div>
        {isLoading ? (
          <div className="text-gray-400 text-xs py-1">加载中...</div>
        ) : etfList.length === 0 ? (
          <div className="text-gray-400 text-xs py-1">暂无数据</div>
        ) : (
          <div className="max-h-40 overflow-y-auto space-y-0.5">
            {etfList.slice(0, 10).map((etf) => (
              <div key={etf.code} className="flex justify-between items-center text-xs">
                <div className="flex items-center gap-1 truncate flex-1 min-w-0">
                  <span className="text-gray-600 truncate">{etf.name}</span>
                  <span className="text-gray-400 font-mono text-[10px] flex-shrink-0">{etf.code}</span>
                </div>
                <span
                  className={`font-mono ml-2 flex-shrink-0 ${
                    etf.change_pct && Number(etf.change_pct) > 0
                      ? 'text-red-500'
                      : etf.change_pct && Number(etf.change_pct) < 0
                        ? 'text-green-500'
                        : 'text-gray-500'
                  }`}
                >
                  {etf.change_pct !== null
                    ? `${Number(etf.change_pct) > 0 ? '+' : ''}${Number(etf.change_pct).toFixed(2)}%`
                    : '-'}
                </span>
              </div>
            ))}
            {etfList.length > 10 && (
              <div className="text-gray-400 text-xs pt-1">
                ...共 {etfList.length} 只
              </div>
            )}
          </div>
        )}
      </div>
    </div>,
    document.body
  )
}
