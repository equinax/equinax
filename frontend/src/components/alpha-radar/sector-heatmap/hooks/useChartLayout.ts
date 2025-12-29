/**
 * Layout calculation hook for Bidirectional Diverging Bar Chart
 *
 * Calculates:
 * - L1 horizontal positions and widths
 * - L2 vertical positions and heights
 * - SVG total dimensions
 */

import { useMemo } from 'react'
import type { ProcessedL1Item, ChartDimensions } from '../types'
import { DEFAULT_DIMENSIONS } from '../types'

interface UseChartLayoutOptions {
  data: ProcessedL1Item[]
  containerWidth: number
  isExpanded: boolean
  dimensions?: Partial<ChartDimensions>
}

interface ChartLayout {
  segments: ProcessedL1Item[]
  svgHeight: number
  l1BaselineY: number  // Y position of L1 row
  maxGainerHeight: number  // Max height of gainers (for SVG padding)
  maxLoserHeight: number   // Max height of losers (for SVG padding)
}

export function useChartLayout({
  data,
  containerWidth,
  isExpanded,
  dimensions: customDimensions,
}: UseChartLayoutOptions): ChartLayout {
  const dims = { ...DEFAULT_DIMENSIONS, ...customDimensions }

  return useMemo(() => {
    if (data.length === 0 || containerWidth === 0) {
      return {
        segments: [],
        svgHeight: dims.L1_BAR_HEIGHT,
        l1BaselineY: 0,
        maxGainerHeight: 0,
        maxLoserHeight: 0,
      }
    }

    // Calculate L1 widths first, then normalize to fit container
    const rawWidths = data.map((item) =>
      Math.max(item.proportion * containerWidth, dims.MIN_SEGMENT_WIDTH)
    )
    const totalRawWidth = rawWidths.reduce((sum, w) => sum + w, 0)
    const scale = totalRawWidth > containerWidth ? containerWidth / totalRawWidth : 1

    // Calculate L1 positions with normalized widths
    let currentX = 0
    const segments = data.map((item, index) => {
      const width = rawWidths[index] * scale
      const result = {
        ...item,
        x: currentX,
        width,
      }
      currentX += width

      // Calculate L2 heights for all sectors when expanded
      if (isExpanded) {
        const { gainers, losers } = item.children

        // Calculate heights for gainers
        const totalGainerProportion = gainers.reduce((sum, g) => sum + g.proportion, 0)
        let gainerY = 0
        result.children.gainers = gainers.map((g) => {
          // Normalize proportion within gainers
          const normalizedProportion = totalGainerProportion > 0
            ? g.proportion / totalGainerProportion
            : 1 / gainers.length
          const height = Math.max(
            normalizedProportion * (dims.L2_MAX_HEIGHT / 2),
            dims.L2_MIN_BAR_HEIGHT
          )
          const y = gainerY
          gainerY += height + dims.L2_GAP
          return { ...g, height, y }
        })

        // Calculate heights for losers
        const totalLoserProportion = losers.reduce((sum, l) => sum + l.proportion, 0)
        let loserY = 0
        result.children.losers = losers.map((l) => {
          const normalizedProportion = totalLoserProportion > 0
            ? l.proportion / totalLoserProportion
            : 1 / losers.length
          const height = Math.max(
            normalizedProportion * (dims.L2_MAX_HEIGHT / 2),
            dims.L2_MIN_BAR_HEIGHT
          )
          const y = loserY
          loserY += height + dims.L2_GAP
          return { ...l, height, y }
        })
      }

      return result
    })

    // Calculate max heights across ALL sectors when expanded
    let maxGainerHeight = 0
    let maxLoserHeight = 0

    if (isExpanded) {
      // Find the maximum gainer and loser heights across all sectors
      segments.forEach((segment) => {
        const { gainers, losers } = segment.children
        const gainerHeight = gainers.reduce(
          (sum, g) => sum + g.height + dims.L2_GAP,
          0
        )
        const loserHeight = losers.reduce(
          (sum, l) => sum + l.height + dims.L2_GAP,
          0
        )
        maxGainerHeight = Math.max(maxGainerHeight, gainerHeight)
        maxLoserHeight = Math.max(maxLoserHeight, loserHeight)
      })
    }

    // SVG height = L1 height + space for gainers above + space for losers below
    const svgHeight = dims.L1_BAR_HEIGHT + maxGainerHeight + maxLoserHeight + 16 // padding

    // L1 baseline Y = space for gainers above
    const l1BaselineY = maxGainerHeight + 8

    return {
      segments,
      svgHeight,
      l1BaselineY,
      maxGainerHeight,
      maxLoserHeight,
    }
  }, [data, containerWidth, isExpanded, dims])
}
