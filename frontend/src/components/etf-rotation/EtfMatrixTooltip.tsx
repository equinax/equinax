/**
 * ETF Matrix Tooltip Component
 *
 * Rich tooltip showing cell details:
 * - Sub-category name and parent category
 * - Date
 * - Change percentage
 * - Trading amount
 * - Representative ETF info
 */

import { createPortal } from 'react-dom'
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
    categoryLabel,
    date,
    changePct,
    amount,
    repName,
    repCode,
    mouseX,
    mouseY,
  } = data

  // Position tooltip to avoid going off-screen
  const tooltipStyle: React.CSSProperties = {
    left: mouseX + 12,
    top: mouseY + 12,
    maxWidth: 240,
  }

  // Adjust if too close to right edge
  if (mouseX > window.innerWidth - 260) {
    tooltipStyle.left = mouseX - 200
  }

  // Adjust if too close to bottom edge
  if (mouseY > window.innerHeight - 180) {
    tooltipStyle.top = mouseY - 140
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
    </div>,
    document.body
  )
}
