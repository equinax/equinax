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
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum' | 'limit_up' | 'dragon'

// 3-state metric
type MetricState = 'off' | 'raw' | 'weighted'

// Highlight range for color filtering
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

// Dragon stock data
interface DragonStockData {
  code: string
  name: string
  change_pct: number
}

interface MatrixCellProps {
  x: number
  y: number
  width: number
  height: number
  visibleMetrics: MetricKey[]
  metricStates: Record<MetricKey, MetricState>
  changePct: number
  volume: number | null
  flow: number | null
  momentum: number | null
  // New metrics
  limitUpCount: number | null
  dragonStock: DragonStockData | null
  // Weighted calculation inputs
  marketChange?: number // For weighted change (超额收益)
  volumeBaseline?: number | null // Industry's 120-day avg volume (亿)
  highlightRange?: HighlightRange | null
  // 连板路径高亮
  isPathHighlighted?: boolean // 此单元格是否在连板路径中
  hasPathHighlight?: boolean  // 是否有任何路径高亮激活
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
 * Actual range is typically 30-55, so we use 25-55 for better contrast
 */
function getFlowColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  // Normalize: actual range is 25-55, map to 0-1 for better contrast
  const normalized = (value - 25) / 30 // 25→0, 55→1
  return getSequentialColor(Math.max(0, normalized), 35) // Amber hue
}

/**
 * Get color for momentum (continuous purple gradient)
 * Higher momentum = deeper purple
 * Actual range is typically 30-55, so we use 25-55 for better contrast
 */
function getMomentumColor(value: number | null): string {
  if (value === null) return '#f5f5f5'
  // Normalize: actual range is 25-55, map to 0-1 for better contrast
  const normalized = (value - 25) / 30 // 25→0, 55→1
  return getSequentialColor(Math.max(0, normalized), 280) // Purple hue
}

/**
 * Get color for limit-up count (fire gradient: gray → orange → red)
 * 0: light gray, 10+: deep red
 */
function getLimitUpColor(count: number | null): string {
  if (count === null || count === 0) return '#f5f5f5'
  // Normalize: 0 → 0, 10 → 1 (10 stocks = max intensity)
  const t = Math.min(count / 10, 1)
  // Fire gradient: orange (25°) to red (0°)
  const hue = 25 - t * 25
  const saturation = 40 + t * 50  // 40% → 90%
  const brightness = 100 - t * 30  // 100% → 70%
  return hsbToHex(hue, saturation, brightness)
}

/**
 * Get color for dragon stock (based on change percentage)
 * ST stocks (change < 8%): Light lavender (浅紫色)
 * Main board (8% <= change < 15%): Light gold (浅金色) - 重点关注
 * STAR/ChiNext (change >= 15%): Light pink (浅粉色) - 不太显眼
 */
function getDragonColor(changePct: number | null): string {
  if (changePct === null) return '#f5f5f5'  // No dragon = light gray

  // Distinguish board type by change percentage
  // ST limit-up: ~4.9-5%, Main board: ~9.9-10%, STAR/ChiNext: ~19.8-20%
  if (changePct >= 15) {
    // STAR/ChiNext: Light pink (浅粉色) - less prominent, can't trade these
    const t = Math.min((changePct - 15) / 5, 1)  // 15% → 0, 20% → 1
    const saturation = 25 + t * 15  // 25% → 40% (light)
    const brightness = 95 - t * 5   // 95% → 90%
    return hsbToHex(350, saturation, brightness)  // Pink hue (350°)
  } else if (changePct >= 8) {
    // Main board: Light gold gradient (龙头=浅金色) - 重点关注
    const t = Math.min((changePct - 8) / 7, 1)  // 8% → 0, 15% → 1
    const saturation = 45 + t * 20  // 45% → 65%
    const brightness = 95 - t * 10  // 95% → 85%
    return hsbToHex(48, saturation, brightness)  // Gold hue (48°)
  } else {
    // ST stocks: Light lavender (浅紫色) - distinctive from gold/pink
    const t = Math.min(changePct / 8, 1)  // 0% → 0, 8% → 1
    const saturation = 30 + t * 20  // 30% → 50%
    const brightness = 95 - t * 5   // 95% → 90%
    return hsbToHex(270, saturation, brightness)  // Lavender hue (270°)
  }
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
 * Get text color specifically for dragon cells
 * All dragon backgrounds are now light colors, use dark text for contrast
 */
function getDragonTextColor(bgColor: string): string {
  // Parse hex color
  const hex = bgColor.replace('#', '')
  const r = parseInt(hex.substring(0, 2), 16) / 255
  const g = parseInt(hex.substring(2, 4), 16) / 255
  const b = parseInt(hex.substring(4, 6), 16) / 255

  // Detect color type by RGB ratios
  // Pink: high R, low G, medium B (hue ~350)
  // Gold: high R, high G, low B (hue ~48)
  // Lavender: medium R, low G, high B (hue ~270)

  if (b > g && b > r * 0.7) {
    return '#581c87'  // Purple-800 for lavender backgrounds
  }
  if (r > g * 1.1 && r > b * 1.5) {
    return '#9f1239'  // Rose-800 for pink backgrounds
  }
  return '#78350f'    // Amber-900 for gold backgrounds
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

/**
 * Format limit-up count (empty if 0)
 */
function formatLimitUp(count: number | null): string {
  if (count === null || count === 0) return ''
  return count.toString()
}

/**
 * Format dragon stock name (truncate to 4 chars)
 */
function formatDragonName(name: string | null): string {
  if (!name) return '-'  // No qualifying dragon
  return name.length > 4 ? name.slice(0, 4) : name
}

/**
 * Format weighted volume as percentage deviation from baseline
 */
function formatWeightedVolume(value: number | null, baseline: number | null): string {
  if (value === null || baseline === null || baseline === 0) return '-'
  const yi = value / 100000000 // Convert to 亿
  const deviation = ((yi - baseline) / baseline) * 100
  const prefix = deviation > 0 ? '+' : ''
  return `${prefix}${deviation.toFixed(0)}%`
}

/**
 * Get color for weighted volume (deviation from baseline)
 * Uses blue-based sequential scale: lighter for below average, darker for above
 */
function getWeightedVolumeColor(value: number | null, baseline: number | null): string {
  if (value === null || baseline === null || baseline === 0) return '#f5f5f5'
  const yi = value / 100000000 // Convert to 亿
  const deviation = ((yi - baseline) / baseline) * 100

  // Map deviation to 0-1 range: -50% → 0.1, 0% → 0.5, +50% → 0.9
  // This gives good color differentiation for typical ±50% deviations
  const normalized = 0.5 + (deviation / 100) // -100% → -0.5, 0% → 0.5, +100% → 1.5
  const clamped = Math.max(0.05, Math.min(1, normalized))
  return getSequentialColor(clamped, 210) // Blue hue
}

// Metric config for getting color and formatting
// Note: limit_up and dragon need special handling with extra data
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
  limit_up: {
    getColor: getLimitUpColor,
    format: formatLimitUp,
  },
  dragon: {
    // Dragon needs special handling - uses dragonStock.change_pct for color
    // and dragonStock.name for text
    getColor: getDragonColor,
    format: () => '-',  // Placeholder - actual format done in render
  },
}

export const MatrixCell = memo(function MatrixCell({
  x,
  y,
  width,
  height,
  visibleMetrics,
  metricStates,
  changePct,
  volume,
  flow,
  momentum,
  limitUpCount,
  dragonStock,
  marketChange,
  volumeBaseline,
  highlightRange,
  isPathHighlighted = false,
  hasPathHighlight = false,
  onHover,
  onLeave,
}: MatrixCellProps) {
  // Calculate weighted values when in weighted mode
  const weightedChange = metricStates.change === 'weighted' && marketChange !== undefined
    ? changePct - marketChange
    : changePct

  // Get metric values (raw or weighted based on state)
  const metricValues: Record<MetricKey, number | null> = {
    change: metricStates.change === 'weighted' ? weightedChange : changePct,
    volume,
    flow,
    momentum,
    limit_up: limitUpCount,
    dragon: dragonStock?.change_pct ?? null,
  }

  // Get metric values for range comparison
  // When in weighted mode, compare against weighted values (used by ColorRangeBar filter)
  const getComparisonValue = (metric: MetricKey): number | null => {
    // Change: use weighted value when in weighted mode
    if (metric === 'change') {
      if (metricStates.change === 'weighted') {
        return weightedChange // Already calculated: changePct - marketChange
      }
      return changePct
    }

    // Volume: use deviation % when in weighted mode, raw 亿 when raw
    if (metric === 'volume') {
      if (metricStates.volume === 'weighted') {
        if (volume === null || volumeBaseline == null || volumeBaseline === 0) return null
        const yi = volume / 100000000 // Convert to 亿
        const baseline = Number(volumeBaseline)
        return ((yi - baseline) / baseline) * 100 // Deviation %
      }
      // Raw mode: convert to 亿 for comparison
      return volume !== null ? volume / 100000000 : null
    }

    // Limit up: raw count
    if (metric === 'limit_up') {
      return limitUpCount
    }

    // Dragon: use change_pct for range comparison
    if (metric === 'dragon') {
      return dragonStock?.change_pct ?? null
    }

    // Flow and momentum: use raw values
    const value = metricValues[metric]
    return value
  }

  // Calculate stripe width for each metric
  const stripeWidth = width / visibleMetrics.length
  const fontSize = visibleMetrics.length > 2 ? 6 : visibleMetrics.length > 1 ? 7 : 8

  // Calculate if this cell should be highlighted based on highlightRange
  const isHighlighted = !highlightRange || (() => {
    const value = getComparisonValue(highlightRange.metric)
    if (value === null) return false
    return value >= highlightRange.min && value <= highlightRange.max
  })()

  // 连板路径高亮优先级：路径高亮 > 范围高亮
  // 当有路径高亮激活时，非路径单元格变暗（但不要太暗）
  const pathDimmed = hasPathHighlight && !isPathHighlighted

  // Opacity: 考虑范围高亮和路径高亮两种情况
  // 路径变暗用 0.5，保持可读性
  const cellOpacity = pathDimmed ? 0.5 : (isHighlighted ? 1 : 0.15)

  // Show border when cell is highlighted with an active filter (helps visibility for near-white cells)
  const showHighlightBorder = highlightRange && isHighlighted

  // 连板路径高亮边框（金色发光效果）
  const showPathBorder = isPathHighlighted

  // Get border color based on metric theme
  const getBorderColor = (): string => {
    if (!highlightRange) return '#666'
    switch (highlightRange.metric) {
      case 'change':
        // Red for positive, green for negative based on the cell's change value
        return changePct >= 0 ? '#c93b3b' : '#2d8a2d'
      case 'volume':
        return '#2989c9' // Blue
      case 'flow':
        return '#c47a30' // Amber
      case 'momentum':
        return '#7a2eb0' // Purple
      case 'limit_up':
        return '#ff6b35' // Fire orange
      case 'dragon':
        return '#fbbf24' // Gold
      default:
        return '#666'
    }
  }

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
        const state = metricStates[metric]
        const stripeX = idx * stripeWidth

        // Use weighted color/format for volume when in weighted mode
        // Dragon needs special handling for color (based on change) and text (stock name)
        let bgColor: string
        let text: string
        if (metric === 'volume' && state === 'weighted') {
          bgColor = getWeightedVolumeColor(volume, volumeBaseline ?? null)
          text = formatWeightedVolume(volume, volumeBaseline ?? null)
        } else if (metric === 'dragon') {
          // Dragon: color based on change_pct, text is stock name
          // Uses special text color for better readability on gold/red backgrounds
          bgColor = getDragonColor(dragonStock?.change_pct ?? null)
          text = formatDragonName(dragonStock?.name ?? null)
          const dragonTextColor = getDragonTextColor(bgColor)

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

              {/* Text with special dragon color */}
              <text
                x={stripeX + stripeWidth / 2}
                y={y + height / 2}
                textAnchor="middle"
                dominantBaseline="middle"
                fontSize={fontSize}
                fontFamily="ui-monospace, monospace"
                fill={dragonTextColor}
                style={{ pointerEvents: 'none' }}
              >
                {text}
              </text>
            </g>
          )
        } else {
          bgColor = config.getColor(value)
          text = config.format(value)
        }

        const textColor = getTextColor(bgColor)

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

      {/* Highlight border for visibility when filtering near-white values */}
      {showHighlightBorder && (
        <rect
          x={0}
          y={y}
          width={width}
          height={height}
          fill="none"
          stroke={getBorderColor()}
          strokeWidth={1.5}
          style={{ pointerEvents: 'none' }}
        />
      )}

      {/* 连板路径高亮边框 - 金色发光效果 */}
      {showPathBorder && (
        <>
          {/* 外发光效果 */}
          <rect
            x={-1}
            y={y - 1}
            width={width + 2}
            height={height + 2}
            fill="none"
            stroke="#fbbf24"
            strokeWidth={3}
            opacity={0.5}
            style={{ pointerEvents: 'none' }}
          />
          {/* 内边框 */}
          <rect
            x={0}
            y={y}
            width={width}
            height={height}
            fill="none"
            stroke="#f59e0b"
            strokeWidth={2}
            style={{ pointerEvents: 'none' }}
          />
        </>
      )}
    </motion.g>
  )
})
