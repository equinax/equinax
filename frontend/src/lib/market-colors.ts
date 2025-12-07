/**
 * Market color configuration for different regions.
 *
 * - China (A-shares): Red = up/profit, Green = down/loss
 * - US/International: Green = up/profit, Red = down/loss
 */

export type MarketRegion = 'CN' | 'US'

export interface MarketColors {
  /** Color for profit/gain/up movement */
  profit: string
  /** Color for loss/down movement */
  loss: string
  /** Tailwind class for profit text */
  profitClass: string
  /** Tailwind class for loss text */
  lossClass: string
  /** Color gradient for heatmap (loss to profit) */
  heatmapGradient: string[]
}

const COLORS: Record<MarketRegion, MarketColors> = {
  // China A-shares: Red up, Green down
  CN: {
    profit: '#ef4444', // red
    loss: '#22c55e', // green
    profitClass: 'text-red-500',
    lossClass: 'text-green-500',
    heatmapGradient: ['#22c55e', '#86efac', '#ffffff', '#fca5a5', '#ef4444'],
  },
  // US/International: Green up, Red down
  US: {
    profit: '#22c55e', // green
    loss: '#ef4444', // red
    profitClass: 'text-green-500',
    lossClass: 'text-red-500',
    heatmapGradient: ['#ef4444', '#fca5a5', '#ffffff', '#86efac', '#22c55e'],
  },
}

// Default market region - hardcoded to CN (China A-shares)
// Can be extended to support user settings in the future
let currentRegion: MarketRegion = 'CN'

/**
 * Get current market colors based on configured region
 */
export function getMarketColors(): MarketColors {
  return COLORS[currentRegion]
}

/**
 * Get the current market region
 */
export function getMarketRegion(): MarketRegion {
  return currentRegion
}

/**
 * Set the market region (for runtime switching)
 */
export function setMarketRegion(region: MarketRegion): void {
  currentRegion = region
}

/**
 * Get color based on value (positive = profit, negative = loss)
 */
export function getValueColor(value: number): string {
  const colors = getMarketColors()
  return value >= 0 ? colors.profit : colors.loss
}

/**
 * Get Tailwind class based on value
 */
export function getValueColorClass(value: number): string {
  const colors = getMarketColors()
  return value >= 0 ? colors.profitClass : colors.lossClass
}

/**
 * Chart color palette for multi-series charts
 * These are neutral colors not related to profit/loss
 */
export const CHART_PALETTE = [
  '#3b82f6', // blue
  '#f97316', // orange
  '#8b5cf6', // purple
  '#06b6d4', // cyan
  '#ec4899', // pink
  '#84cc16', // lime
  '#f59e0b', // amber
  '#6366f1', // indigo
] as const
