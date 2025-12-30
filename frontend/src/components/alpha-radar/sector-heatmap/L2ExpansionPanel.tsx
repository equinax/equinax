/**
 * L2 Expansion Panel Component
 *
 * Renders L2 industries as vertical bars below L1:
 * - All L2 extend DOWN from the L1 baseline
 * - Animated expand from top, collapse to top
 */

import { memo, useId } from 'react'
import { motion } from 'motion/react'
import type { ProcessedL1Item, ProcessedL2Item, ChartDimensions } from './types'
import { DEFAULT_DIMENSIONS } from './types'

interface L2ExpansionPanelProps {
  segment: ProcessedL1Item
  baselineY: number  // Y position of L1 top edge
  l1Height: number
  maxL2Height: number  // Maximum L2 height for clip rect
  dimensions?: Partial<ChartDimensions>
  onL2Hover: (l2: ProcessedL2Item | null) => void
  onL2Click: (l1Name: string, l2Name: string) => void
}

interface L2BarProps {
  item: ProcessedL2Item
  x: number
  width: number
  y: number
  onHover: (hovering: boolean) => void
  onClick: () => void
}

const L2Bar = memo(function L2Bar({
  item,
  x,
  width,
  y,
  onHover,
  onClick,
}: L2BarProps) {
  const { name, color, textColor, height, metricLabel } = item

  // Text shadow for contrast
  const textShadow =
    textColor === 'light'
      ? '0 1px 2px rgba(0,0,0,0.5)'
      : '0 1px 1px rgba(255,255,255,0.6)'

  // Determine label visibility based on height
  const showLabel = height >= 24
  const showMetricValue = height >= 32

  return (
    <g
      style={{ cursor: 'pointer', outline: 'none' }}
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
      onClick={onClick}
    >
      {/* Bar background */}
      <motion.rect
        x={x}
        y={y}
        width={width}
        height={height}
        rx={2}
        animate={{ fill: color }}
        transition={{ fill: { duration: 0.3 } }}
      />

      {/* Border for visibility */}
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill="none"
        stroke={textColor === 'light' ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.1)'}
        strokeWidth={0.5}
        rx={2}
      />

      {/* L2 name */}
      {showLabel && (
        <text
          x={x + width / 2}
          y={y + (showMetricValue ? height / 2 - 5 : height / 2)}
          textAnchor="middle"
          dominantBaseline="middle"
          fill={textColor === 'light' ? '#ffffff' : '#1f2937'}
          fontSize={10}
          fontWeight={500}
          style={{ textShadow, pointerEvents: 'none' }}
        >
          {name.length > 6 ? name.slice(0, 5) + '...' : name}
        </text>
      )}

      {/* Metric value */}
      {showMetricValue && (
        <text
          x={x + width / 2}
          y={y + height / 2 + 8}
          textAnchor="middle"
          dominantBaseline="middle"
          fill={textColor === 'light' ? 'rgba(255,255,255,0.85)' : 'rgba(0,0,0,0.65)'}
          fontSize={9}
          fontFamily="monospace"
          style={{ textShadow, pointerEvents: 'none' }}
        >
          {metricLabel}
        </text>
      )}

      {/* Tooltip for small bars */}
      {!showLabel && <title>{`${name}: ${metricLabel}`}</title>}
    </g>
  )
})

export const L2ExpansionPanel = memo(function L2ExpansionPanel({
  segment,
  baselineY,
  l1Height,
  maxL2Height,
  dimensions: customDims,
  onL2Hover,
  onL2Click,
}: L2ExpansionPanelProps) {
  const dims = { ...DEFAULT_DIMENSIONS, ...customDims }
  const { losers } = segment.children
  const clipId = useId()

  // L1 bottom edge - where L2 starts
  const l1BottomY = baselineY + l1Height

  // Calculate bar positions
  let currentY = l1BottomY
  const barsWithPositions = losers.map((l2) => {
    const y = currentY
    currentY += l2.height + dims.L2_GAP
    return { ...l2, y }
  })

  return (
    <g key={`l2-panel-${segment.name}`}>
      {/* Animated clip path - reveals content from top to bottom */}
      <defs>
        <clipPath id={clipId}>
          <motion.rect
            x={segment.x - 1}
            y={l1BottomY}
            width={segment.width + 2}
            initial={{ height: 0 }}
            animate={{ height: maxL2Height + 20 }}
            exit={{ height: 0 }}
            transition={{ duration: 0.3, ease: [0.4, 0, 0.2, 1] }}
          />
        </clipPath>
      </defs>

      {/* L2 bars clipped by animated rect */}
      <g clipPath={`url(#${clipId})`}>
        {barsWithPositions.map((l2) => (
          <L2Bar
            key={l2.name}
            item={l2}
            x={segment.x}
            width={segment.width}
            y={l2.y}
            onHover={(hovering) => onL2Hover(hovering ? l2 : null)}
            onClick={() => onL2Click(segment.name, l2.name)}
          />
        ))}
      </g>
    </g>
  )
})
