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
  onHover: (event: React.MouseEvent) => void
  onLeave: () => void
}

/**
 * Get color for change percentage (red = gain, green = loss)
 * Using lower saturation colors from sector-colors.ts
 */
function getChangeColor(value: number): string {
  if (value > 5) return '#c93b3b'  // 胭脂红 (extreme profit)
  if (value > 3) return '#d47070'  // light red
  if (value > 1) return '#e8a8a8'  // very light red
  if (value > 0) return '#f0c8c8'  // pale red
  if (value > -1) return '#f5f3ef' // ivory white (neutral)
  if (value > -3) return '#c8e8d8' // pale green
  if (value > -5) return '#8bc9a5' // very light green
  return '#4ca87a'                 // 翡翠绿 (extreme loss)
}

/**
 * Get color for volume (blue gradient)
 * Using muted colors from sector-colors.ts amount gradient
 */
function getVolumeColor(value: number | null): string {
  if (value === null) return '#f5f3ef'
  const yi = value / 100000000
  if (yi > 500) return '#1a6ba8' // deep blue
  if (yi > 300) return '#2989c9' // darker blue
  if (yi > 150) return '#4aa5de' // standard blue
  if (yi > 80) return '#7ebfe8'  // medium blue
  if (yi > 40) return '#b3d9f2'  // light blue
  return '#e8f4fc'              // very light blue
}

/**
 * Get color for flow (amber/orange gradient)
 * Using muted warm colors, data range typically +15 to +50
 */
function getFlowColor(value: number | null): string {
  if (value === null) return '#f5f3ef'
  if (value > 45) return '#a85d20' // deep amber
  if (value > 40) return '#c47a30' // dark amber
  if (value > 35) return '#d99545' // standard amber
  if (value > 30) return '#e8b060' // medium amber
  if (value > 25) return '#f0c888' // light amber
  if (value > 20) return '#f5ddb0' // very light amber
  return '#faf0d8'                // pale amber
}

/**
 * Get color for momentum (purple gradient)
 * Using muted colors from sector-colors.ts main_strength gradient
 */
function getMomentumColor(value: number | null): string {
  if (value === null) return '#f5f3ef'
  if (value > 45) return '#5e1a98' // deep purple
  if (value > 40) return '#7a2eb0' // dark purple
  if (value > 35) return '#964dc8' // standard purple
  if (value > 30) return '#b580d8' // medium purple
  if (value > 25) return '#d4b3e8' // light purple
  if (value > 20) return '#e8d0f0' // very light purple
  return '#f3e8f8'                // pale purple
}

/**
 * Get text color based on background brightness
 */
function getTextColor(bgColor: string): string {
  // Dark colors need white text
  const darkColors = [
    '#c93b3b', '#d47070', '#4ca87a',                         // red/green (muted)
    '#1a6ba8', '#2989c9', '#4aa5de',                         // blue (muted)
    '#a85d20', '#c47a30', '#d99545',                         // amber (muted)
    '#5e1a98', '#7a2eb0', '#964dc8',                         // purple (muted)
  ]
  return darkColors.includes(bgColor) ? '#ffffff' : '#374151'
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
  const fontSize = visibleMetrics.length > 2 ? 7 : visibleMetrics.length > 1 ? 8 : 9

  return (
    <motion.g
      initial={{ x }}
      animate={{ x }}
      transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
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
