/**
 * ETF Rotation Matrix Types
 */

export type {
  EtfRotationFlatResponse,
  EtfRotationColumn,
  EtfFlatDayCell
} from '@/api/generated/schemas'

export interface EtfTooltipData {
  subCategory: string
  category: string
  categoryLabel: string
  date: string
  changePct: number | null
  amount: number | null
  repCode: string | null
  repName: string | null
  mouseX: number
  mouseY: number
}

// Category display colors (for grouped headers)
export const CATEGORY_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  broad: { bg: '#faf8f3', text: '#8b7355', border: '#e8e4d9' },      // 宽基 - Warm beige
  sector: { bg: '#f3f8fa', text: '#4a6b7c', border: '#d9e4e8' },     // 行业 - Cool blue
  theme: { bg: '#f8f3fa', text: '#6b4a7c', border: '#e4d9e8' },      // 赛道 - Light purple
  cross_border: { bg: '#faf3f8', text: '#7c4a6b', border: '#e8d9e4' }, // 跨境 - Light pink
  commodity: { bg: '#f3faf3', text: '#4a7c4a', border: '#d9e8d9' },  // 商品 - Light green
  bond: { bg: '#fafaf3', text: '#7c7c4a', border: '#e8e8d9' },       // 债券 - Light yellow
}
