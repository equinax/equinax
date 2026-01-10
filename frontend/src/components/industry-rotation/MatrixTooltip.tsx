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

import { useRef, useLayoutEffect, useState } from 'react'
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
    volume_baseline,
    limit_up_count,
    limit_up_stocks,
    dragon_stock,
  } = data

  const tooltipRef = useRef<HTMLDivElement>(null)
  const [position, setPosition] = useState<{ left: number; top: number } | null>(null)

  // Measure tooltip and adjust position after render
  useLayoutEffect(() => {
    if (!tooltipRef.current) return

    const rect = tooltipRef.current.getBoundingClientRect()
    const tooltipWidth = rect.width
    const tooltipHeight = rect.height
    const padding = 12

    let left = mouseX + padding
    let top = mouseY + padding

    // Adjust if too close to right edge
    if (left + tooltipWidth > window.innerWidth - padding) {
      left = mouseX - tooltipWidth - padding
    }

    // Adjust if too close to bottom edge
    if (top + tooltipHeight > window.innerHeight - padding) {
      top = mouseY - tooltipHeight - padding
    }

    // Ensure not off-screen on left/top
    left = Math.max(padding, left)
    top = Math.max(padding, top)

    setPosition({ left, top })
  }, [mouseX, mouseY])

  // Initial position (will be adjusted after measurement)
  const tooltipStyle: React.CSSProperties = {
    left: position?.left ?? mouseX + 12,
    top: position?.top ?? mouseY + 12,
    maxWidth: 280,
    visibility: position ? 'visible' : 'hidden', // Hide until positioned
  }

  return createPortal(
    <div
      ref={tooltipRef}
      className="fixed z-50 bg-white/60 backdrop-blur-[2px] text-gray-900 rounded p-3 shadow-lg text-sm pointer-events-none border border-gray-300"
      style={tooltipStyle}
    >
      {/* Header */}
      <div className="flex justify-between items-center gap-4 mb-2">
        <span className="text-gray-500">{date}</span>
        <span className="font-bold">{industry}</span>
      </div>

      {/* Metrics */}
      <div className="border-t border-gray-200 pt-2 space-y-1">
        {/* Change */}
        <div className="flex justify-between">
          <span className="text-gray-500">涨跌幅</span>
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
            <span className="text-gray-500">成交额</span>
            <span className="font-mono">{formatAmount(money_flow)}</span>
          </div>
        )}

        {/* Volume Baseline (for weighted volume) */}
        {volume_baseline !== null && volume_baseline !== undefined && (
          <div className="flex justify-between">
            <span className="text-gray-500">基准成交</span>
            <span className="font-mono text-gray-600">{formatAmount(volume_baseline * 1e8)}</span>
          </div>
        )}

        {/* Main Strength */}
        {main_strength !== null && (
          <div className="flex justify-between">
            <span className="text-gray-500">主力强度</span>
            <span className="font-mono">{main_strength.toFixed(1)}</span>
          </div>
        )}

        {/* Top Stock */}
        {top_stock && (
          <div className="flex justify-between">
            <span className="text-gray-500">涨幅最大</span>
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

        {/* Dragon Stock (龙头战法筛选) */}
        {dragon_stock && (
          <div className="flex justify-between">
            <span className="text-yellow-400">龙头战法</span>
            <span>
              <span className="text-yellow-300 font-medium">{dragon_stock.name}</span>
              <span className="text-red-400 font-mono ml-1">
                {formatPercent(Number(dragon_stock.change_pct))}
              </span>
            </span>
          </div>
        )}
      </div>

      {/* Limit-up stocks section */}
      {limit_up_count !== undefined && limit_up_count > 0 && (
        <div className="border-t border-gray-200 pt-2 mt-2">
          <div className="flex justify-between mb-1">
            <span className="text-gray-500">涨停家数</span>
            <span className="font-mono text-orange-400 font-bold">{limit_up_count}</span>
          </div>
          {/* Limit-up stocks list (show all) */}
          {limit_up_stocks && limit_up_stocks.length > 0 && (
            <div className="text-xs space-y-0.5 mt-1 max-h-48 overflow-y-auto">
              {limit_up_stocks.map((stock) => (
                <div key={stock.code} className="flex justify-between text-gray-600">
                  <span>{stock.name}</span>
                  <span className="text-red-400 font-mono">{formatPercent(Number(stock.change_pct))}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Signals */}
      {signals.length > 0 && (
        <div className="border-t border-gray-200 pt-2 mt-2 text-xs text-yellow-400">
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
