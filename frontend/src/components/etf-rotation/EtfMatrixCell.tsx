/**
 * ETF Matrix Cell Component
 *
 * Individual cell in the ETF rotation matrix:
 * - Shows change percentage with color gradient
 * - Hover to show tooltip
 */

import { memo } from 'react'

interface EtfMatrixCellProps {
  x: number
  y: number
  width: number
  height: number
  changePct: number | null
  showText?: boolean
  onHover: (event: React.MouseEvent) => void
  onLeave: () => void
  /** Highlight cell with neutral border (for prediction intersection) */
  highlight?: boolean
}

/**
 * HSB (HSV) to Hex color conversion
 */
function hsbToHex(h: number, s: number, b: number): string {
  s /= 100
  b /= 100
  const k = (n: number) => (n + h / 60) % 6
  const f = (n: number) => b * (1 - s * Math.max(0, Math.min(k(n), 4 - k(n), 1)))
  const toHex = (x: number) => Math.round(x * 255).toString(16).padStart(2, '0')
  return `#${toHex(f(5))}${toHex(f(3))}${toHex(f(1))}`
}

/**
 * Continuous diverging color scale: red for positive, green for negative
 */
function getDivergingColor(value: number, maxMagnitude: number): string {
  const clamped = Math.max(-maxMagnitude, Math.min(maxMagnitude, value))
  const magnitude = Math.abs(clamped) / maxMagnitude

  // Hue: 0 = red (positive), 145 = green (negative)
  const hue = value >= 0 ? 0 : 145

  // Saturation: 0% at center -> 85% at max
  const saturation = magnitude * 85

  // Brightness: 100% at center -> 85% at max
  const brightness = 100 - magnitude * 15

  return hsbToHex(hue, saturation, brightness)
}

/**
 * Get color for change percentage
 * Red for positive, green for negative, white at 0
 */
function getChangeColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  return getDivergingColor(value, 8) // ±8% as max intensity
}

/**
 * Get text color based on background brightness (lower contrast for subtlety)
 */
function getTextColor(bgColor: string): string {
  // Parse hex color
  const hex = bgColor.replace('#', '')
  const r = parseInt(hex.slice(0, 2), 16) / 255
  const g = parseInt(hex.slice(2, 4), 16) / 255
  const b = parseInt(hex.slice(4, 6), 16) / 255

  // Calculate relative luminance
  const luminance =
    0.2126 * (r <= 0.03928 ? r / 12.92 : Math.pow((r + 0.055) / 1.055, 2.4)) +
    0.7152 * (g <= 0.03928 ? g / 12.92 : Math.pow((g + 0.055) / 1.055, 2.4)) +
    0.0722 * (b <= 0.03928 ? b / 12.92 : Math.pow((b + 0.055) / 1.055, 2.4))

  // Use muted colors for lower contrast
  return luminance > 0.5 ? '#777777' : 'rgba(255,255,255,0.85)'
}

/**
 * Format percentage for display (compact: just absolute value, no sign or %)
 */
function formatPercent(value: number | null): string {
  if (value === null) return '-'
  return Math.abs(value).toFixed(1)
}

export const EtfMatrixCell = memo(function EtfMatrixCell({
  x,
  y,
  width,
  height,
  changePct,
  showText = true,
  onHover,
  onLeave,
  highlight = false,
}: EtfMatrixCellProps) {
  const bgColor = getChangeColor(changePct)
  const textColor = getTextColor(bgColor)
  const displayText = formatPercent(changePct)

  // Calculate font size based on width (smaller for narrow cells)
  const fontSize = width < 20 ? 5 : width < 28 ? 6 : width < 36 ? 7 : 8

  // Highlight border: neutral blue color for prediction intersection
  const strokeColor = highlight ? '#3b82f6' : '#e5e5e5'
  const strokeWidth = highlight ? 1.5 : 0.5

  return (
    <g
      onMouseEnter={onHover}
      onMouseLeave={onLeave}
      style={{ cursor: 'pointer' }}
    >
      {/* Background */}
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={bgColor}
        stroke={strokeColor}
        strokeWidth={strokeWidth}
      />
      {/* Text - only show when showText is true */}
      {showText && (
        <text
          x={x + width / 2}
          y={y + height / 2}
          textAnchor="middle"
          dominantBaseline="central"
          fill={textColor}
          fontSize={fontSize}
          fontFamily="ui-monospace, monospace"
        >
          {displayText}
        </text>
      )}
    </g>
  )
})
