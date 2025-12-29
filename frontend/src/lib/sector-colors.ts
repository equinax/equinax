/**
 * Sector heatmap color utilities for Alpha Radar.
 * ═══════════════════════════════════════════════════════════════
 *
 * A股惯例：涨红跌绿 (Red = up/profit, Green = down/loss)
 *
 * 设计原则：
 * - 涨跌幅以0为中心，使用红绿渐变
 * - 其他指标（成交额、主力强度、评分）使用蓝色渐变
 * - 支持明暗主题自适应
 */

import type { SectorMetric } from '@/api/generated/schemas'

/**
 * Heatmap gradient colors for different themes
 */
const HEATMAP_GRADIENTS = {
  // Change metric: Green → White → Red (A-share convention)
  change: {
    light: [
      '#288a5b', // 翡翠绿 (extreme loss)
      '#4ca87a', // light green
      '#8bc9a5', // very light green
      '#f5f3ef', // ivory white (neutral)
      '#e8a8a8', // very light red
      '#d47070', // light red
      '#c93b3b', // 胭脂红 (extreme profit)
    ],
    dark: [
      '#4cc38a', // 极光绿 (extreme loss)
      '#3a9469', // muted green
      '#2a6b4d', // darker green
      '#1a2236', // background dark
      '#7a4545', // darker red
      '#b85c5c', // muted red
      '#f07575', // 珊瑚红 (extreme profit)
    ],
  },
  // Amount/Volume metric: Light blue → Deep blue
  amount: {
    light: [
      '#e8f4fc', // very light blue
      '#b3d9f2', // light blue
      '#7ebfe8', // medium blue
      '#4aa5de', // standard blue
      '#2989c9', // deeper blue
      '#1a6ba8', // dark blue
      '#0d4d87', // deep blue
    ],
    dark: [
      '#1a2530', // very dark blue-gray
      '#1f3445', // dark blue
      '#254560', // medium dark
      '#3060a0', // standard blue
      '#4080c0', // brighter blue
      '#60a0e0', // light blue
      '#90c0f0', // very light blue
    ],
  },
  // Main strength: Purple gradient
  main_strength: {
    light: [
      '#f3e8f8', // very light purple
      '#d4b3e8', // light purple
      '#b580d8', // medium purple
      '#964dc8', // standard purple
      '#7a2eb0', // deeper purple
      '#5e1a98', // dark purple
      '#420a80', // deep purple
    ],
    dark: [
      '#1a1825', // very dark purple
      '#2a2040', // dark purple
      '#3a3060', // medium dark
      '#5540a0', // standard purple
      '#7560c0', // brighter purple
      '#9580e0', // light purple
      '#b5a0f0', // very light purple
    ],
  },
  // Score: Teal gradient (same as panorama theme)
  score: {
    light: [
      '#e8f8f5', // very light teal
      '#b3e8dc', // light teal
      '#80d8c4', // medium teal
      '#4dc8ac', // standard teal
      '#2db090', // deeper teal
      '#1a9878', // dark teal
      '#0a8060', // deep teal
    ],
    dark: [
      '#1a2520', // very dark teal
      '#1f3530', // dark teal
      '#254540', // medium dark
      '#306050', // standard teal
      '#408070', // brighter teal
      '#60a090', // light teal
      '#80c0b0', // very light teal
    ],
  },
}

/**
 * Get heatmap gradient colors based on metric and theme
 */
export function getHeatmapGradient(
  metric: SectorMetric | string,
  isDark: boolean
): string[] {
  const key = metric as keyof typeof HEATMAP_GRADIENTS
  const gradients = HEATMAP_GRADIENTS[key] || HEATMAP_GRADIENTS.change
  return isDark ? gradients.dark : gradients.light
}

/**
 * Calculate color for a value based on min/max range
 *
 * For 'change' metric: Centers around 0
 * For other metrics: Linear interpolation from min to max
 */
export function getValueColor(
  value: number,
  min: number,
  max: number,
  metric: SectorMetric | string,
  isDark: boolean
): string {
  const gradient = getHeatmapGradient(metric, isDark)

  if (metric === 'change') {
    // For change metric, center around 0
    // ±5% maps to full intensity
    const normalizedValue = Math.max(-5, Math.min(5, value)) / 5
    const t = (normalizedValue + 1) / 2 // Convert to 0-1 range
    return interpolateGradient(gradient, t)
  }

  // For other metrics, linear interpolation
  if (max === min) return gradient[Math.floor(gradient.length / 2)]
  const t = (value - min) / (max - min)
  return interpolateGradient(gradient, t)
}

/**
 * Interpolate within a gradient array
 */
function interpolateGradient(gradient: string[], t: number): string {
  const clampedT = Math.max(0, Math.min(1, t))
  const index = clampedT * (gradient.length - 1)
  const lower = Math.floor(index)
  const upper = Math.ceil(index)

  if (lower === upper) return gradient[lower]

  const fraction = index - lower
  return interpolateColor(gradient[lower], gradient[upper], fraction)
}

/**
 * Interpolate between two hex colors
 */
function interpolateColor(color1: string, color2: string, t: number): string {
  const r1 = parseInt(color1.slice(1, 3), 16)
  const g1 = parseInt(color1.slice(3, 5), 16)
  const b1 = parseInt(color1.slice(5, 7), 16)

  const r2 = parseInt(color2.slice(1, 3), 16)
  const g2 = parseInt(color2.slice(3, 5), 16)
  const b2 = parseInt(color2.slice(5, 7), 16)

  const r = Math.round(r1 + (r2 - r1) * t)
  const g = Math.round(g1 + (g2 - g1) * t)
  const b = Math.round(b1 + (b2 - b1) * t)

  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`
}

/**
 * Format value for tooltip display based on metric
 */
export function formatMetricValue(
  value: number | null | undefined,
  metric: SectorMetric | string
): string {
  if (value === null || value === undefined) return '-'

  switch (metric) {
    case 'change':
      const sign = value >= 0 ? '+' : ''
      return `${sign}${value.toFixed(2)}%`
    case 'amount':
      // Convert to 亿 (hundred million)
      const yi = value / 100000000
      if (yi >= 1) return `${yi.toFixed(2)}亿`
      // Convert to 万 (ten thousand)
      const wan = value / 10000
      return `${wan.toFixed(0)}万`
    case 'main_strength':
    case 'score':
      return value.toFixed(1)
    default:
      return value.toFixed(2)
  }
}

/**
 * Get metric label in Chinese
 */
export function getMetricLabel(metric: SectorMetric | string): string {
  switch (metric) {
    case 'change':
      return '涨跌幅'
    case 'amount':
      return '成交额'
    case 'main_strength':
      return '主力强度'
    case 'score':
      return '综合评分'
    default:
      return metric
  }
}

/**
 * Get text color for contrasting with background
 * Returns 'light' or 'dark' based on background luminance
 */
export function getContrastTextColor(bgColor: string): 'light' | 'dark' {
  const r = parseInt(bgColor.slice(1, 3), 16)
  const g = parseInt(bgColor.slice(3, 5), 16)
  const b = parseInt(bgColor.slice(5, 7), 16)

  // Calculate relative luminance
  const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255

  return luminance > 0.5 ? 'dark' : 'light'
}
