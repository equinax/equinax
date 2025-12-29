/**
 * Type definitions for Bidirectional Diverging Bar Chart
 */

export interface ProcessedL2Item {
  name: string
  changePct: number
  amount: number
  proportion: number  // Share of parent L1's amount
  color: string
  textColor: 'light' | 'dark'
  stockCount: number
  upCount: number
  downCount: number
  // Computed layout
  height: number
  y: number
}

export interface ProcessedL1Item {
  name: string
  changePct: number
  amount: number
  proportion: number  // Share of total market amount
  color: string
  textColor: 'light' | 'dark'
  stockCount: number
  upCount: number
  downCount: number
  // Computed layout
  x: number
  width: number
  // Children split by direction
  children: {
    gainers: ProcessedL2Item[]  // Sorted: most gain at top
    losers: ProcessedL2Item[]   // Sorted: most loss at bottom
  }
}

export interface ChartDimensions {
  L1_BAR_HEIGHT: number
  L2_MAX_HEIGHT: number
  L2_MIN_BAR_HEIGHT: number
  L2_GAP: number
  LABEL_PADDING: number
  MIN_SEGMENT_WIDTH: number
}

export const DEFAULT_DIMENSIONS: ChartDimensions = {
  L1_BAR_HEIGHT: 48,
  L2_MAX_HEIGHT: 300,
  L2_MIN_BAR_HEIGHT: 24,
  L2_GAP: 2,
  LABEL_PADDING: 8,
  MIN_SEGMENT_WIDTH: 40,
}

export interface HoveredSegment {
  type: 'l1' | 'l2'
  l1Name: string
  l2Name?: string
  rect: DOMRect
}
