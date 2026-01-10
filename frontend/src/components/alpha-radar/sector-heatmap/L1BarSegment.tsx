/**
 * L1 Bar Segment Component
 *
 * Renders a single L1 industry as a horizontal bar segment
 * with high-contrast labels and hover/click interactions
 */

import { memo } from 'react'
import { motion } from 'motion/react'
import type { ProcessedL1Item } from './types'

interface L1BarSegmentProps {
  segment: ProcessedL1Item
  height: number
  isHovered: boolean
  onClick: () => void
  onHover: (hovering: boolean, event?: React.MouseEvent) => void
}

export const L1BarSegment = memo(function L1BarSegment({
  segment,
  height,
  isHovered,
  onClick,
  onHover,
}: L1BarSegmentProps) {
  const { name, color, textColor, x, width, metricLabel } = segment

  // Determine if we have enough space for labels
  const showFullLabel = width > 60
  const showAnyLabel = width > 30
  const showMetricValue = width > 50

  // Text shadow for contrast
  const textShadow =
    textColor === 'light'
      ? '0 1px 3px rgba(0,0,0,0.6), 0 0 2px rgba(0,0,0,0.3)'
      : '0 1px 2px rgba(255,255,255,0.8)'

  return (
    <motion.g
      animate={{ x }}
      transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
      onClick={onClick}
      onMouseEnter={(e) => onHover(true, e)}
      onMouseLeave={() => onHover(false)}
      style={{ cursor: 'pointer', outline: 'none' }}
      role="button"
      tabIndex={0}
      aria-label={`${name}: ${metricLabel}`}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault()
          onClick()
        }
      }}
    >
      {/* Background rect - add 0.5px overlap to prevent sub-pixel gaps */}
      <motion.rect
        height={height}
        animate={{
          width: width + 0.5,
          fill: color,
          opacity: isHovered ? 1 : 0.95,
          filter: isHovered ? 'brightness(1.08)' : 'brightness(1)',
        }}
        transition={{
          width: { duration: 0.4, ease: [0.4, 0, 0.2, 1] },
          fill: { duration: 0.3 },
          opacity: { duration: 0.15 },
          filter: { duration: 0.15 },
        }}
      />

      {/* Industry name - horizontal for wide, vertical for narrow */}
      {showAnyLabel ? (
        <text
          x={width / 2}
          y={showMetricValue ? height / 2 - 6 : height / 2}
          textAnchor="middle"
          dominantBaseline="middle"
          fill={textColor === 'light' ? '#ffffff' : '#1f2937'}
          fontSize={showFullLabel ? 12 : 10}
          fontWeight={600}
          style={{ textShadow, pointerEvents: 'none' }}
        >
          {showFullLabel ? name : name.slice(0, 2)}
        </text>
      ) : (
        // Vertical text for very narrow segments
        <text
          x={width / 2}
          y={height / 2}
          textAnchor="middle"
          dominantBaseline="middle"
          fill={textColor === 'light' ? '#ffffff' : '#1f2937'}
          fontSize={9}
          fontWeight={500}
          style={{ textShadow, pointerEvents: 'none' }}
          transform={`rotate(-90, ${width / 2}, ${height / 2})`}
        >
          {name.slice(0, 4)}
        </text>
      )}

      {/* Metric value */}
      {showMetricValue && (
        <text
          x={width / 2}
          y={height / 2 + 10}
          textAnchor="middle"
          dominantBaseline="middle"
          fill={textColor === 'light' ? 'rgba(255,255,255,0.9)' : 'rgba(0,0,0,0.7)'}
          fontSize={10}
          fontFamily="monospace"
          style={{ textShadow, pointerEvents: 'none' }}
        >
          {metricLabel}
        </text>
      )}

      {/* Tooltip for narrow segments */}
      {!showAnyLabel && <title>{`${name}: ${metricLabel}`}</title>}
    </motion.g>
  )
})
