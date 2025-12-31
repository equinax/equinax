/**
 * Matrix Cell Component
 *
 * Individual cell in the rotation matrix:
 * - Vertical split based on selected metrics
 * - Each section has its own color gradient
 * - Each section shows the metric value
 */

import { memo } from 'react'
import { motion } from 'motion/react'

// Metric keys
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

// Highlight range for color filtering
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

interface MatrixCellProps {
  x: number
  y: number
  width: number
  height: number
  visibleMetrics: MetricKey[]
  changePct: number
  volume: number | null
  flow: number | null
  momentum: number | null
  highlightRange?: HighlightRange | null
  onHover: (event: React.MouseEvent) => void
  onLeave: () => void
}

/**
 * HSB (HSV) to Hex color conversion
 * HSB provides more vibrant colors than HSL
 * @param h - Hue (0-360)
 * @param s - Saturation (0-100)
 * @param b - Brightness (0-100)
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
 * Uses HSB for more vibrant colors
 * @param value - The value to map
 * @param maxMagnitude - The maximum expected magnitude (for scaling)
 */
function getDivergingColor(value: number, maxMagnitude: number): string {
  // Clamp to reasonable range
  const clamped = Math.max(-maxMagnitude, Math.min(maxMagnitude, value))
  // Normalize magnitude to 0-1
  const magnitude = Math.abs(clamped) / maxMagnitude

  // Hue: 0 = red (positive), 145 = green (negative)
  const hue = value >= 0 ? 0 : 145

  // Saturation: 0% at center → 85% at max (vibrant colors)
  const saturation = magnitude * 85

  // Brightness: 100% at center → 85% at max
  const brightness = 100 - magnitude * 15

  return hsbToHex(hue, saturation, brightness)
}

/**
 * Continuous sequential color scale (for single-direction metrics)
 * Uses HSB for more vibrant colors
 * @param value - Normalized value from 0 to 1
 * @param hue - Base hue for the color
 */
function getSequentialColor(normalizedValue: number, hue: number): string {
  // Clamp to 0-1
  const t = Math.max(0, Math.min(1, normalizedValue))

  // Saturation: 5% at min → 80% at max
  const saturation = 5 + t * 75

  // Brightness: 100% at min → 70% at max (darker for higher values)
  const brightness = 100 - t * 30

  return hsbToHex(hue, saturation, brightness)
}

/**
 * Get color for change percentage (continuous red/green)
 * Red for positive, green for negative, white at 0
 */
function getChangeColor(value: number): string {
  return getDivergingColor(value, 8) // ±8% as max intensity
}

/**
 * Get color for volume (continuous blue gradient)
 * Uses log scale for better distribution
 */
function getVolumeColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  const yi = value / 100000000 // Convert to 亿
  // Log scale: 10亿 → 0.3, 100亿 → 0.6, 500亿 → 0.85, 1000亿 → 1.0
  const logValue = Math.log10(Math.max(yi, 1)) / Math.log10(1000)
  return getSequentialColor(Math.min(logValue, 1), 210) // Blue hue
}

/**
 * Get color for flow (continuous amber/yellow gradient)
 * Higher flow = deeper amber
 */
function getFlowColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  // Normalize: typical range is 15-50, map to 0-1
  const normalized = (value - 10) / 45 // 10→0, 55→1
  return getSequentialColor(normalized, 35) // Amber hue
}

/**
 * Get color for momentum (continuous purple gradient)
 * Higher momentum = deeper purple
 */
function getMomentumColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  // Normalize: typical range is 15-50, map to 0-1
  const normalized = (value - 10) / 45 // 10→0, 55→1
  return getSequentialColor(normalized, 280) // Purple hue
}

/**
 * Get text color based on background brightness
 * Uses relative luminance calculation for accessibility
 */
function getTextColor(bgColor: string): string {
  // Parse hex color
  const hex = bgColor.replace('#', '')
  const r = parseInt(hex.substring(0, 2), 16) / 255
  const g = parseInt(hex.substring(2, 4), 16) / 255
  const b = parseInt(hex.substring(4, 6), 16) / 255

  // Calculate relative luminance (WCAG formula)
  const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b

  // Use white text on dark backgrounds, dark text on light backgrounds
  return luminance < 0.5 ? '#ffffff' : '#374151'
}

/**
 * Format change percentage
 */
function formatChange(value: number): string {
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${value.toFixed(1)}%`
}

/**
 * Format volume in 亿
 */
function formatVolume(value: number | null): string {
  if (value === null) return '-'
  const yi = value / 100000000
  if (yi >= 100) return `${Math.round(yi)}亿`
  if (yi >= 10) return `${yi.toFixed(0)}亿`
  return `${yi.toFixed(1)}亿`
}

/**
 * Format flow/momentum
 */
function formatFlow(value: number | null): string {
  if (value === null) return '-'
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${value.toFixed(0)}`
}

// Metric config for getting color and formatting
const METRIC_CONFIG: Record<MetricKey, {
  getColor: (value: number | null) => string
  format: (value: number | null) => string
}> = {
  change: {
    getColor: (v) => getChangeColor(v ?? 0),
    format: (v) => formatChange(v ?? 0),
  },
  volume: {
    getColor: getVolumeColor,
    format: formatVolume,
  },
  flow: {
    getColor: getFlowColor,
    format: formatFlow,
  },
  momentum: {
    getColor: getMomentumColor,
    format: formatFlow,
  },
}

export const MatrixCell = memo(function MatrixCell({
  x,
  y,
  width,
  height,
  visibleMetrics,
  changePct,
  volume,
  flow,
  momentum,
  highlightRange,
  onHover,
  onLeave,
}: MatrixCellProps) {
  // Get metric values
  const metricValues: Record<MetricKey, number | null> = {
    change: changePct,
    volume,
    flow,
    momentum,
  }

  // Calculate stripe width for each metric
  const stripeWidth = width / visibleMetrics.length
  const fontSize = visibleMetrics.length > 2 ? 6 : visibleMetrics.length > 1 ? 7 : 8

  // Calculate if this cell should be highlighted based on highlightRange
  const isHighlighted = !highlightRange || (() => {
    const value = metricValues[highlightRange.metric]
    if (value === null) return false
    return value >= highlightRange.min && value <= highlightRange.max
  })()

  // Opacity: 1 if highlighted or no filter, 0.15 if dimmed
  const cellOpacity = isHighlighted ? 1 : 0.15

  return (
    <motion.g
      initial={{ x }}
      animate={{ x, opacity: cellOpacity }}
      transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1], opacity: { duration: 0.15 } }}
      onMouseEnter={onHover}
      onMouseLeave={onLeave}
      style={{ cursor: 'pointer' }}
    >
      {/* Render each metric as a vertical stripe */}
      {visibleMetrics.map((metric, idx) => {
        const value = metricValues[metric]
        const config = METRIC_CONFIG[metric]
        const bgColor = config.getColor(value)
        const textColor = getTextColor(bgColor)
        const text = config.format(value)
        const stripeX = idx * stripeWidth

        return (
          <g key={metric}>
            {/* Background stripe */}
            <motion.rect
              x={stripeX}
              y={y}
              width={stripeWidth}
              height={height}
              animate={{ fill: bgColor }}
              transition={{ fill: { duration: 0.3 } }}
            />

            {/* Text */}
            <text
              x={stripeX + stripeWidth / 2}
              y={y + height / 2}
              textAnchor="middle"
              dominantBaseline="middle"
              fontSize={fontSize}
              fontFamily="ui-monospace, monospace"
              fill={textColor}
              style={{ pointerEvents: 'none' }}
            >
              {text}
            </text>
          </g>
        )
      })}
    </motion.g>
  )
})
