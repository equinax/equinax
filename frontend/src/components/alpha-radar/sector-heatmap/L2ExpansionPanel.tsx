/**
 * L2 Expansion Panel Component
 *
 * Renders L2 industries as vertical bars:
 * - Gainers extend UP from the L1 baseline
 * - Losers extend DOWN from the L1 baseline
 * - Animated expand/collapse with staggered children
 */

import { memo } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import type { ProcessedL1Item, ProcessedL2Item, ChartDimensions } from './types'
import { DEFAULT_DIMENSIONS } from './types'

interface L2ExpansionPanelProps {
  segment: ProcessedL1Item
  baselineY: number  // Y position of L1 top edge
  l1Height: number
  dimensions?: Partial<ChartDimensions>
  onL2Hover: (l2: ProcessedL2Item | null) => void
  onL2Click: (l1Name: string, l2Name: string) => void
}

interface L2BarProps {
  item: ProcessedL2Item
  x: number
  width: number
  direction: 'up' | 'down'
  baselineY: number
  cumulativeOffset: number
  index: number
  onHover: (hovering: boolean) => void
  onClick: () => void
}

const L2Bar = memo(function L2Bar({
  item,
  x,
  width,
  direction,
  baselineY,
  cumulativeOffset,
  index,
  onHover,
  onClick,
}: L2BarProps) {
  const { name, changePct, color, textColor, height } = item

  // Calculate Y position
  // UP: bars grow upward from baseline (y decreases)
  // DOWN: bars grow downward from baseline (y increases)
  const y = direction === 'up'
    ? baselineY - cumulativeOffset - height
    : baselineY + cumulativeOffset

  // Text shadow for contrast
  const textShadow =
    textColor === 'light'
      ? '0 1px 2px rgba(0,0,0,0.5)'
      : '0 1px 1px rgba(255,255,255,0.6)'

  // Format change percentage
  const changeStr = `${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`

  // Determine label visibility based on height
  const showLabel = height >= 24
  const showChange = height >= 32

  return (
    <motion.g
      initial={{
        opacity: 0,
        y: direction === 'up' ? 10 : -10,
      }}
      animate={{
        opacity: 1,
        y: 0,
      }}
      exit={{
        opacity: 0,
        y: direction === 'up' ? 10 : -10,
      }}
      transition={{
        duration: 0.2,
        delay: index * 0.03,
      }}
      style={{ cursor: 'pointer' }}
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
      onClick={onClick}
    >
      {/* Bar background */}
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={color}
        rx={2}
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
          y={y + (showChange ? height / 2 - 5 : height / 2)}
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

      {/* Change percentage */}
      {showChange && (
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
          {changeStr}
        </text>
      )}

      {/* Tooltip for small bars */}
      {!showLabel && <title>{`${name}: ${changeStr}`}</title>}
    </motion.g>
  )
})

export const L2ExpansionPanel = memo(function L2ExpansionPanel({
  segment,
  baselineY,
  l1Height,
  dimensions: customDims,
  onL2Hover,
  onL2Click,
}: L2ExpansionPanelProps) {
  const dims = { ...DEFAULT_DIMENSIONS, ...customDims }
  const { gainers, losers } = segment.children

  // Calculate cumulative offsets for stacking
  // Gainers stack upward from L1 top
  // Losers stack downward from L1 bottom
  let gainerOffset = 0
  let loserOffset = l1Height

  return (
    <AnimatePresence>
      <motion.g
        key={`l2-panel-${segment.name}`}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
      >
        {/* Gainers - extend UP */}
        {gainers.map((l2, idx) => {
          const currentOffset = gainerOffset
          gainerOffset += l2.height + dims.L2_GAP

          return (
            <L2Bar
              key={l2.name}
              item={l2}
              x={segment.x}
              width={segment.width}
              direction="up"
              baselineY={baselineY}
              cumulativeOffset={currentOffset}
              index={idx}
              onHover={(hovering) => onL2Hover(hovering ? l2 : null)}
              onClick={() => onL2Click(segment.name, l2.name)}
            />
          )
        })}

        {/* Losers - extend DOWN */}
        {losers.map((l2, idx) => {
          const currentOffset = loserOffset
          loserOffset += l2.height + dims.L2_GAP

          return (
            <L2Bar
              key={l2.name}
              item={l2}
              x={segment.x}
              width={segment.width}
              direction="down"
              baselineY={baselineY}
              cumulativeOffset={currentOffset}
              index={idx + gainers.length}
              onHover={(hovering) => onL2Hover(hovering ? l2 : null)}
              onClick={() => onL2Click(segment.name, l2.name)}
            />
          )
        })}
      </motion.g>
    </AnimatePresence>
  )
})
