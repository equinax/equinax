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

// Format metric value for display
function formatMetricLabel(value: number, metric: SectorMetric): string {
  switch (metric) {
    case 'change':
      return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
    case 'amount':
      // Convert to 亿
      const yi = value / 100000000
      return yi >= 1 ? `${yi.toFixed(1)}亿` : `${(value / 10000).toFixed(0)}万`
    case 'main_strength':
      return value.toFixed(2)
    case 'score':
      return value.toFixed(1)
    default:
      return value.toFixed(2)
  }
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

    // Sort L1 by metric value: largest on LEFT
    const sortedL1 = [...sectors].sort(
      (a, b) => Number(b.value || 0) - Number(a.value || 0)
    )

    // Process each L1 sector
    return sortedL1.map((sector) => {
      const changePct = Number(sector.avg_change_pct || 0)
      const amount = Number(sector.total_amount || 0)
      const proportion = amount / totalAmount

      // Get the metric value for display
      const metricValue = Number(sector.value || 0)
      const metricLabel = formatMetricLabel(metricValue, metric)

      // Get color based on metric value
      const color = getValueColor(metricValue, minValue, maxValue, metric, isDark)
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
        const childMetricValue = Number(child.value || 0)
        const childMetricLabel = formatMetricLabel(childMetricValue, metric)
        const childColor = getValueColor(childMetricValue, minValue, maxValue, metric, isDark)
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
          metricValue: childMetricValue,
          metricLabel: childMetricLabel,
          // Layout will be calculated later
          height: 0,
          y: 0,
        } as ProcessedL2Item
      })

      // Unified sorting: all L2 sorted by metricValue from large to small
      // Display below L1 bar (in losers array for rendering)
      const sortedChildren = processedChildren.sort(
        (a, b) => b.metricValue - a.metricValue
      )

      // Put all in losers array (displays below L1 bar)
      const gainers: ProcessedL2Item[] = []
      const losers = sortedChildren

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
        metricValue,
        metricLabel,
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
