/**
 * Color Range Bar Component
 *
 * Horizontal gradient bar that shows the color scale for the current metric.
 * Hovering over a position highlights cells in that value range.
 */

import { useCallback, useRef, useState } from 'react'

// Metric keys matching page component
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

interface ColorRangeBarProps {
  metric: MetricKey
  isWeighted: boolean  // Whether metric is in weighted state
  onHoverRange: (range: { min: number; max: number } | null) => void
}

// Metric config type
interface MetricConfig {
  min: number
  max: number
  hue: (t: number) => number  // t: 0 to 1, returns hue
  label: (value: number) => string
}

// Raw metric configuration for range and colors
const METRIC_CONFIG: Record<MetricKey, MetricConfig> = {
  change: {
    min: -8,
    max: 8,
    // Green (145) at min, Red (0) at max, with white in middle
    hue: (t) => t < 0.5 ? 145 : 0,
    label: (v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`,
  },
  volume: {
    min: 10,
    max: 500,  // In 亿 units
    hue: () => 210,  // Blue
    label: (v) => `${v.toFixed(0)}亿`,
  },
  flow: {
    min: 25,
    max: 55,
    hue: () => 35,  // Amber
    label: (v) => v.toFixed(0),
  },
  momentum: {
    min: 25,
    max: 55,
    hue: () => 280,  // Purple
    label: (v) => v.toFixed(0),
  },
}

// Weighted metric configuration (only for metrics that support weighted mode)
const METRIC_CONFIG_WEIGHTED: Partial<Record<MetricKey, MetricConfig>> = {
  change: {
    min: -6,    // 超额收益 typical range ±6%
    max: 6,
    hue: (t) => t < 0.5 ? 145 : 0,  // Same diverging colors
    label: (v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`,
  },
  volume: {
    min: -50,   // Deviation from baseline ±50%
    max: 50,
    hue: () => 210,  // Blue (same as raw)
    label: (v) => `${v > 0 ? '+' : ''}${v.toFixed(0)}%`,
  },
}

/**
 * HSB to Hex conversion (copied from MatrixCell for consistency)
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
 * Generate gradient color for a normalized position (0-1)
 * @param metric - The metric type
 * @param t - Position from 0 to 1
 * @param colorMode - 'diverging' (white at center), 'sequential' (light to dark), 'weighted-volume' (white at low, dark at high)
 * @param hue - Base hue for the color
 */
function getGradientColor(metric: MetricKey, t: number, colorMode: 'diverging' | 'sequential' | 'weighted-volume', hue: number): string {
  if (colorMode === 'diverging') {
    // Diverging: color1 at 0, white at 0.5, color2 at 1
    const magnitude = Math.abs(t - 0.5) * 2  // 0 at center, 1 at edges
    // For change: green (145) at low end, red (0) at high end
    const effectiveHue = metric === 'change' ? (t < 0.5 ? 145 : 0) : hue
    const saturation = magnitude * 85
    const brightness = 100 - magnitude * 15
    return hsbToHex(effectiveHue, saturation, brightness)
  } else if (colorMode === 'weighted-volume') {
    // Weighted volume: white at -50% (t=0), dark blue at +50% (t=1)
    // This is a sequential scale from white to dark blue
    const saturation = 5 + t * 75  // 5% to 80%
    const brightness = 100 - t * 30  // 100% to 70%
    return hsbToHex(hue, saturation, brightness)
  } else {
    // Sequential: light at 0, dark at 1
    const saturation = 5 + t * 75
    const brightness = 100 - t * 30
    return hsbToHex(hue, saturation, brightness)
  }
}

// Bar dimensions
const BAR_WIDTH = 160
const BAR_HEIGHT = 14
const HOVER_RANGE_PCT = 0.1  // 10% of total range

export function ColorRangeBar({ metric, isWeighted, onHoverRange }: ColorRangeBarProps) {
  const barRef = useRef<SVGRectElement>(null)
  const [hoverPosition, setHoverPosition] = useState<number | null>(null)
  // Use weighted config when available and in weighted mode
  const config = isWeighted && METRIC_CONFIG_WEIGHTED[metric]
    ? METRIC_CONFIG_WEIGHTED[metric]
    : METRIC_CONFIG[metric]

  // Calculate value from mouse position
  const handleMouseMove = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
    const rect = barRef.current?.getBoundingClientRect()
    if (!rect) return

    const x = e.clientX - rect.left
    const t = Math.max(0, Math.min(1, x / BAR_WIDTH))
    setHoverPosition(t)

    // Calculate value range (±5% of total range around hover point)
    const totalRange = config.max - config.min
    const halfWindow = totalRange * HOVER_RANGE_PCT / 2
    const value = config.min + t * totalRange

    onHoverRange({
      min: value - halfWindow,
      max: value + halfWindow,
    })
  }, [config, onHoverRange])

  const handleMouseLeave = useCallback(() => {
    setHoverPosition(null)
    onHoverRange(null)
  }, [onHoverRange])

  // Determine color mode
  // Change uses diverging (green-white-red), weighted volume uses sequential (white-blue)
  const colorMode: 'diverging' | 'sequential' | 'weighted-volume' =
    metric === 'change' ? 'diverging' :
    (isWeighted && metric === 'volume') ? 'weighted-volume' :
    'sequential'
  const hue = config.hue(0.5) // Get base hue for this metric

  // Generate gradient stops
  const gradientStops = Array.from({ length: 20 }, (_, i) => {
    const t = i / 19
    return { offset: `${t * 100}%`, color: getGradientColor(metric, t, colorMode, hue) }
  })

  // Calculate hover indicator position and label
  const hoverValue = hoverPosition !== null
    ? config.min + hoverPosition * (config.max - config.min)
    : null

  // Unique gradient id for each metric/weighted combination
  const gradientId = `gradient-${metric}-${isWeighted ? 'weighted' : 'raw'}`

  return (
    <svg
      width={BAR_WIDTH}
      height={BAR_HEIGHT + 4}
      className="cursor-crosshair"
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      <defs>
        <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
          {gradientStops.map((stop, i) => (
            <stop key={i} offset={stop.offset} stopColor={stop.color} />
          ))}
        </linearGradient>
      </defs>

      {/* Gradient bar */}
      <rect
        ref={barRef}
        x={0}
        y={2}
        width={BAR_WIDTH}
        height={BAR_HEIGHT}
        rx={3}
        fill={`url(#${gradientId})`}
        className="stroke-border"
        strokeWidth={0.5}
      />

      {/* Hover indicator */}
      {hoverPosition !== null && (
        <>
          {/* Vertical line at hover position */}
          <line
            x1={hoverPosition * BAR_WIDTH}
            y1={0}
            x2={hoverPosition * BAR_WIDTH}
            y2={BAR_HEIGHT + 4}
            stroke="currentColor"
            strokeWidth={1.5}
            className="text-foreground"
          />
          {/* Value label */}
          <text
            x={hoverPosition * BAR_WIDTH}
            y={-2}
            textAnchor="middle"
            fontSize={8}
            className="fill-foreground font-mono"
          >
            {config.label(hoverValue!)}
          </text>
        </>
      )}
    </svg>
  )
}
