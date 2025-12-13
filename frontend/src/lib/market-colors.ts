/**
 * Market color configuration for different regions.
 * ═══════════════════════════════════════════════════════════════
 * 设计理念：与主题（冰夜永黑/极昼白芒）完美协调
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

/**
 * 主题感知的市场颜色
 * ═══════════════════════════════════════════════════════════════
 *
 * 极昼白芒 (Light):
 * - 涨红: 胭脂红 #c93b3b - 庄重、沉稳，在象牙白背景上有力量感
 * - 跌绿: 翡翠绿 #288a5b - 深邃、稳重，不刺眼
 *
 * 冰夜永黑 (Dark):
 * - 涨红: 珊瑚红 #f07575 - 温暖明亮，在冰川深黑上醒目
 * - 跌绿: 极光绿 #4cc38a - 清新明亮，如北极极光
 */
const COLORS_LIGHT: Record<MarketRegion, MarketColors> = {
  // China A-shares: Red up, Green down (Light Theme)
  CN: {
    profit: '#c93b3b',  // 胭脂红 - 庄重沉稳
    loss: '#288a5b',    // 翡翠绿 - 深邃有力
    profitClass: 'text-profit',
    lossClass: 'text-loss',
    heatmapGradient: ['#288a5b', '#6ab894', '#f5f3ef', '#e5a3a3', '#c93b3b'],
  },
  // US/International: Green up, Red down (Light Theme)
  US: {
    profit: '#288a5b',
    loss: '#c93b3b',
    profitClass: 'text-loss',
    lossClass: 'text-profit',
    heatmapGradient: ['#c93b3b', '#e5a3a3', '#f5f3ef', '#6ab894', '#288a5b'],
  },
}

const COLORS_DARK: Record<MarketRegion, MarketColors> = {
  // China A-shares: Red up, Green down (Dark Theme)
  CN: {
    profit: '#f07575',  // 珊瑚红 - 温暖明亮
    loss: '#4cc38a',    // 极光绿 - 清新如极光
    profitClass: 'text-profit',
    lossClass: 'text-loss',
    heatmapGradient: ['#4cc38a', '#3a9469', '#1a2236', '#b85c5c', '#f07575'],
  },
  // US/International: Green up, Red down (Dark Theme)
  US: {
    profit: '#4cc38a',
    loss: '#f07575',
    profitClass: 'text-loss',
    lossClass: 'text-profit',
    heatmapGradient: ['#f07575', '#b85c5c', '#1a2236', '#3a9469', '#4cc38a'],
  },
}

// Default market region - hardcoded to CN (China A-shares)
let currentRegion: MarketRegion = 'CN'

// 缓存当前主题状态
let cachedIsDark: boolean | null = null

/**
 * 检测当前是否为暗色主题
 */
function isDarkTheme(): boolean {
  if (typeof window === 'undefined') return false
  return document.documentElement.classList.contains('dark')
}

/**
 * Get current market colors based on configured region and theme
 */
export function getMarketColors(): MarketColors {
  const isDark = isDarkTheme()
  cachedIsDark = isDark
  return isDark ? COLORS_DARK[currentRegion] : COLORS_LIGHT[currentRegion]
}

/**
 * Get market colors for specific theme
 */
export function getMarketColorsForTheme(isDark: boolean): MarketColors {
  return isDark ? COLORS_DARK[currentRegion] : COLORS_LIGHT[currentRegion]
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
 * ═══════════════════════════════════════════════════════════════
 * 设计原则：
 * - 足够多的颜色支持大量股票对比
 * - 颜色之间有明显区分度
 * - 在亮色和暗色主题下都有好的可见性
 * - 避免红绿（保留给涨跌）
 */
export const CHART_PALETTE = [
  // 第一梯队 - 高辨识度主色
  '#3b82f6', // blue-500 深海蓝
  '#f97316', // orange-500 橙色
  '#8b5cf6', // violet-500 紫罗兰
  '#06b6d4', // cyan-500 青色
  '#ec4899', // pink-500 玫红
  '#f59e0b', // amber-500 琥珀
  '#6366f1', // indigo-500 靛蓝
  '#14b8a6', // teal-500 蓝绿

  // 第二梯队 - 变体色
  '#0ea5e9', // sky-500 天蓝
  '#a855f7', // purple-500 紫色
  '#f43f5e', // rose-500 玫瑰
  '#84cc16', // lime-500 青柠
  '#d946ef', // fuchsia-500 品红
  '#eab308', // yellow-500 金黄
  '#0891b2', // cyan-600 深青
  '#7c3aed', // violet-600 深紫

  // 第三梯队 - 扩展色（更多股票时使用）
  '#2563eb', // blue-600
  '#ea580c', // orange-600
  '#c026d3', // fuchsia-600
  '#0d9488', // teal-600
  '#db2777', // pink-600
  '#ca8a04', // yellow-600
  '#4f46e5', // indigo-600
  '#059669', // emerald-600

  // 第四梯队 - 深色变体
  '#1d4ed8', // blue-700
  '#c2410c', // orange-700
  '#9333ea', // purple-600
  '#0f766e', // teal-700
  '#be185d', // pink-700
  '#a16207', // yellow-700
  '#4338ca', // indigo-700
  '#047857', // emerald-700

  // 第五梯队 - 更多扩展
  '#1e40af', // blue-800
  '#9a3412', // orange-800
  '#7e22ce', // purple-700
  '#115e59', // teal-800
  '#9d174d', // pink-800
  '#854d0e', // yellow-800
  '#3730a3', // indigo-800
  '#065f46', // emerald-800
] as const

/**
 * 获取主题感知的图表调色板
 * 暗色主题下使用稍亮的颜色以保证可见性
 */
export function getChartPalette(isDark: boolean): readonly string[] {
  if (isDark) {
    // 暗色主题 - 使用更亮的颜色
    return [
      '#60a5fa', // blue-400
      '#fb923c', // orange-400
      '#a78bfa', // violet-400
      '#22d3ee', // cyan-400
      '#f472b6', // pink-400
      '#fbbf24', // amber-400
      '#818cf8', // indigo-400
      '#2dd4bf', // teal-400
      '#38bdf8', // sky-400
      '#c084fc', // purple-400
      '#fb7185', // rose-400
      '#a3e635', // lime-400
      '#e879f9', // fuchsia-400
      '#facc15', // yellow-400
      '#67e8f9', // cyan-300
      '#a5b4fc', // indigo-300
      '#93c5fd', // blue-300
      '#fdba74', // orange-300
      '#f0abfc', // fuchsia-300
      '#5eead4', // teal-300
      '#f9a8d4', // pink-300
      '#fde047', // yellow-300
      '#a5b4fc', // indigo-300
      '#6ee7b7', // emerald-300
      '#7dd3fc', // sky-300
      '#fed7aa', // orange-200
      '#ddd6fe', // violet-200
      '#99f6e4', // teal-200
      '#fbcfe8', // pink-200
      '#fef08a', // yellow-200
      '#c7d2fe', // indigo-200
      '#a7f3d0', // emerald-200
    ] as const
  }
  return CHART_PALETTE
}
