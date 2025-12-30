/**
 * Matrix Tooltip Component
 *
 * Rich tooltip showing cell details:
 * - Date and industry
 * - Change percentage
 * - Money flow (if available)
 * - Top stock info
 * - Algorithm signals
 */

import { createPortal } from 'react-dom'
import type { TooltipData } from './types'

interface MatrixTooltipProps {
  data: TooltipData
}

/**
 * Format large numbers (money flow) in Chinese units
 */
function formatAmount(value: number): string {
  const absValue = Math.abs(value)
  if (absValue >= 1e8) {
    return `${(value / 1e8).toFixed(2)}亿`
  }
  if (absValue >= 1e4) {
    return `${(value / 1e4).toFixed(2)}万`
  }
  return value.toFixed(0)
}

/**
 * Format percentage
 */
function formatPercent(value: number): string {
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${value.toFixed(2)}%`
}

export function MatrixTooltip({ data }: MatrixTooltipProps) {
  const {
    industry,
    date,
    change_pct,
    money_flow,
    main_strength,
    top_stock,
    signals,
    mouseX,
    mouseY,
  } = data

  // Position tooltip to avoid going off-screen
  const tooltipStyle: React.CSSProperties = {
    left: mouseX + 12,
    top: mouseY + 12,
    maxWidth: 280,
  }

  // Adjust if too close to right edge
  if (mouseX > window.innerWidth - 300) {
    tooltipStyle.left = mouseX - 200
  }

  // Adjust if too close to bottom edge
  if (mouseY > window.innerHeight - 200) {
    tooltipStyle.top = mouseY - 150
  }

  return createPortal(
    <div
      className="fixed z-50 bg-gray-900/95 text-white rounded-lg p-3 shadow-xl text-sm pointer-events-none"
      style={tooltipStyle}
    >
      {/* Header */}
      <div className="flex justify-between items-center gap-4 mb-2">
        <span className="text-gray-400">{date}</span>
        <span className="font-bold">{industry}</span>
      </div>

      {/* Metrics */}
      <div className="border-t border-gray-700 pt-2 space-y-1">
        {/* Change */}
        <div className="flex justify-between">
          <span className="text-gray-400">涨跌幅</span>
          <span
            className={
              change_pct > 0
                ? 'text-red-400 font-mono'
                : change_pct < 0
                  ? 'text-green-400 font-mono'
                  : 'font-mono'
            }
          >
            {formatPercent(change_pct)}
          </span>
        </div>

        {/* Money Flow */}
        {money_flow !== null && (
          <div className="flex justify-between">
            <span className="text-gray-400">成交额</span>
            <span className="font-mono">{formatAmount(money_flow)}</span>
          </div>
        )}

        {/* Main Strength */}
        {main_strength !== null && (
          <div className="flex justify-between">
            <span className="text-gray-400">主力强度</span>
            <span className="font-mono">{main_strength.toFixed(1)}</span>
          </div>
        )}

        {/* Top Stock */}
        {top_stock && (
          <div className="flex justify-between">
            <span className="text-gray-400">龙头股</span>
            <span>
              {top_stock.name}{' '}
              <span
                className={
                  Number(top_stock.change_pct) > 0
                    ? 'text-red-400 font-mono'
                    : 'text-green-400 font-mono'
                }
              >
                {formatPercent(Number(top_stock.change_pct))}
              </span>
            </span>
          </div>
        )}
      </div>

      {/* Signals */}
      {signals.length > 0 && (
        <div className="border-t border-gray-700 pt-2 mt-2 text-xs text-yellow-400">
          {signals.map((s, i) => (
            <span key={i} className="mr-2">
              {s.label}
            </span>
          ))}
        </div>
      )}
    </div>,
    document.body
  )
}
