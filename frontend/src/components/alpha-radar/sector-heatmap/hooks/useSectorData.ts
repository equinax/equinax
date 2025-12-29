/**
 * Data transformation hook for Bidirectional Diverging Bar Chart
 *
 * Transforms API response into processed data:
 * - Sorts L1 by change (most gain left, most loss right)
 * - Splits L2 into gainers (up) and losers (down)
 * - Calculates proportions for sizing
 */

import { useMemo } from 'react'
import type { SectorL1Item, SectorMetric } from '@/api/generated/schemas'
import { getValueColor, getContrastTextColor } from '@/lib/sector-colors'
import type { ProcessedL1Item, ProcessedL2Item } from '../types'

interface UseSectorDataOptions {
  sectors: SectorL1Item[] | undefined
  metric: SectorMetric
  isDark: boolean
  minValue: number
  maxValue: number
}

export function useSectorData({
  sectors,
  metric,
  isDark,
  minValue,
  maxValue,
}: UseSectorDataOptions): ProcessedL1Item[] {
  return useMemo(() => {
    if (!sectors || sectors.length === 0) return []

    // Calculate total amount for proportions
    const totalAmount = sectors.reduce(
      (sum, s) => sum + Number(s.total_amount || 0),
      0
    )

    if (totalAmount === 0) return []

    // Sort L1 by change: most gain on LEFT, most loss on RIGHT
    const sortedL1 = [...sectors].sort(
      (a, b) => Number(b.avg_change_pct || 0) - Number(a.avg_change_pct || 0)
    )

    // Process each L1 sector
    return sortedL1.map((sector) => {
      const changePct = Number(sector.avg_change_pct || 0)
      const amount = Number(sector.total_amount || 0)
      const proportion = amount / totalAmount

      // Get color based on metric value
      const value = Number(sector.value || 0)
      const color = getValueColor(value, minValue, maxValue, metric, isDark)
      const textColor = getContrastTextColor(color)

      // Process L2 children
      const children = sector.children || []
      const l1TotalAmount = children.reduce(
        (sum, c) => sum + Number(c.total_amount || 0),
        0
      )

      // Split into gainers and losers
      const processedChildren = children.map((child) => {
        const childChangePct = Number(child.avg_change_pct || 0)
        const childAmount = Number(child.total_amount || 0)
        const childProportion = l1TotalAmount > 0 ? childAmount / l1TotalAmount : 0
        const childValue = Number(child.value || 0)
        const childColor = getValueColor(childValue, minValue, maxValue, metric, isDark)
        const childTextColor = getContrastTextColor(childColor)

        return {
          name: child.name,
          changePct: childChangePct,
          amount: childAmount,
          proportion: childProportion,
          color: childColor,
          textColor: childTextColor,
          stockCount: child.stock_count,
          upCount: child.up_count,
          downCount: child.down_count,
          // Layout will be calculated later
          height: 0,
          y: 0,
        } as ProcessedL2Item
      })

      // Split and sort
      // Gainers: least gain first (closest to baseline), most gain last (at top)
      const gainers = processedChildren
        .filter((c) => c.changePct >= 0)
        .sort((a, b) => a.changePct - b.changePct)

      // Losers: least loss first (closest to baseline), most loss last (at bottom)
      const losers = processedChildren
        .filter((c) => c.changePct < 0)
        .sort((a, b) => b.changePct - a.changePct)

      return {
        name: sector.name,
        changePct,
        amount,
        proportion,
        color,
        textColor,
        stockCount: sector.stock_count,
        upCount: sector.up_count,
        downCount: sector.down_count,
        // Layout will be calculated by useChartLayout
        x: 0,
        width: 0,
        children: {
          gainers,
          losers,
        },
      } as ProcessedL1Item
    })
  }, [sectors, metric, isDark, minValue, maxValue])
}
