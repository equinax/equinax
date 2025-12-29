/**
 * Diverging Bar Chart Component
 *
 * Main container for the bidirectional sector heatmap:
 * - L1 horizontal bar with click-to-expand ALL L2
 * - L2 vertical expansion (gainers up, losers down)
 * - Responsive width with ResizeObserver
 */

import { memo, useState, useRef, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import type { SectorL1Item, SectorMetric } from '@/api/generated/schemas'
import { useSectorData } from './hooks/useSectorData'
import { useChartLayout } from './hooks/useChartLayout'
import { L1BarSegment } from './L1BarSegment'
import { L2ExpansionPanel } from './L2ExpansionPanel'
import { SectorTooltip } from './SectorTooltip'
import type { ProcessedL1Item, ProcessedL2Item, ChartDimensions } from './types'
import { DEFAULT_DIMENSIONS } from './types'

interface DivergingBarChartProps {
  sectors: SectorL1Item[] | undefined
  metric: SectorMetric
  isDark: boolean
  minValue: number
  maxValue: number
  dimensions?: Partial<ChartDimensions>
  onL2Click?: (l1Name: string, l2Name: string) => void
}

interface HoverState {
  segment: ProcessedL1Item | ProcessedL2Item | null
  type: 'l1' | 'l2'
  parentName?: string
  mouseX: number
  mouseY: number
}

export const DivergingBarChart = memo(function DivergingBarChart({
  sectors,
  metric,
  isDark,
  minValue,
  maxValue,
  dimensions: customDims,
  onL2Click,
}: DivergingBarChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [containerWidth, setContainerWidth] = useState(0)
  const [isExpanded, setIsExpanded] = useState(false)
  const [hoveredL1, setHoveredL1] = useState<string | null>(null)
  const [hoverState, setHoverState] = useState<HoverState>({
    segment: null,
    type: 'l1',
    mouseX: 0,
    mouseY: 0,
  })

  const dims = { ...DEFAULT_DIMENSIONS, ...customDims }

  // Auto-collapse L2 when metric changes
  useEffect(() => {
    setIsExpanded(false)
  }, [metric])

  // Process sector data
  const processedData = useSectorData({
    sectors,
    metric,
    isDark,
    minValue,
    maxValue,
  })

  // Calculate layout
  const { segments, svgHeight, l1BaselineY } = useChartLayout({
    data: processedData,
    containerWidth,
    isExpanded,
    dimensions: customDims,
  })

  // Responsive width detection
  useEffect(() => {
    if (!containerRef.current) return

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        setContainerWidth(entry.contentRect.width)
      }
    })

    observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [])

  // Handle click - toggle global expansion
  const handleClick = useCallback(() => {
    setIsExpanded((prev) => !prev)
  }, [])

  // Handle L1 hover
  const handleL1Hover = useCallback(
    (segment: ProcessedL1Item | null, event?: React.MouseEvent) => {
      setHoveredL1(segment?.name ?? null)
      if (segment && event) {
        setHoverState({
          segment,
          type: 'l1',
          mouseX: event.clientX,
          mouseY: event.clientY,
        })
      } else {
        setHoverState((prev) => ({ ...prev, segment: null }))
      }
    },
    []
  )

  // Handle L2 click
  const handleL2Click = useCallback(
    (l1Name: string, l2Name: string) => {
      onL2Click?.(l1Name, l2Name)
    },
    [onL2Click]
  )

  // Track mouse position for tooltip
  const handleMouseMove = useCallback((event: React.MouseEvent) => {
    setHoverState((prev) => {
      if (prev.segment) {
        return { ...prev, mouseX: event.clientX, mouseY: event.clientY }
      }
      return prev
    })
  }, [])

  if (!sectors || sectors.length === 0) {
    return (
      <div className="flex items-center justify-center h-32 text-muted-foreground">
        暂无数据
      </div>
    )
  }

  return (
    <motion.div
      ref={containerRef}
      className="w-full relative overflow-hidden"
      onMouseMove={handleMouseMove}
      animate={{ height: svgHeight }}
      transition={{ duration: 0.3, ease: 'easeInOut' }}
    >
      <svg
        width="100%"
        height={svgHeight}
        className="overflow-visible"
        role="img"
        aria-label="行业热力图"
      >
        {/* L1 Bar Row - animated position */}
        <motion.g
          animate={{ y: l1BaselineY }}
          transition={{ duration: 0.3, ease: 'easeInOut' }}
        >
          {segments.map((segment) => (
            <L1BarSegment
              key={segment.name}
              segment={segment}
              height={dims.L1_BAR_HEIGHT}
              isHovered={hoveredL1 === segment.name}
              onClick={handleClick}
              onHover={(hovering, e) => {
                if (hovering && e) {
                  handleL1Hover(segment, e)
                } else {
                  handleL1Hover(null)
                }
              }}
            />
          ))}
        </motion.g>

        {/* L2 Expansion Panels for ALL segments */}
        <AnimatePresence>
          {isExpanded &&
            segments.map((segment) => (
              <L2ExpansionPanel
                key={`l2-${segment.name}`}
                segment={segment}
                baselineY={l1BaselineY}
                l1Height={dims.L1_BAR_HEIGHT}
                dimensions={customDims}
                onL2Hover={(l2) => {
                  if (l2) {
                    setHoverState((prev) => ({
                      ...prev,
                      segment: l2,
                      type: 'l2',
                      parentName: segment.name,
                    }))
                  } else {
                    setHoverState((prev) => ({ ...prev, segment: null }))
                  }
                }}
                onL2Click={(l1Name, l2Name) => handleL2Click(l1Name, l2Name)}
              />
            ))}
        </AnimatePresence>
      </svg>

      {/* Tooltip */}
      <SectorTooltip
        segment={hoverState.segment}
        type={hoverState.type}
        parentName={hoverState.parentName}
        mouseX={hoverState.mouseX}
        mouseY={hoverState.mouseY}
        metric={metric}
      />
    </motion.div>
  )
})
