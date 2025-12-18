/**
 * 图表主题颜色配置
 * ═══════════════════════════════════════════════════════════════
 * 为 lightweight-charts 提供与主题一致的颜色配置
 * 由于该库不支持 CSS 变量，这里提供 HEX 颜色值
 */

export interface ChartThemeColors {
  // 文字颜色
  text: string
  textSecondary: string
  // 网格和边框
  grid: string
  border: string
  crosshair: string
  // 权益曲线
  equity: string
  equityFill: string
  // 收盘价线
  closeLine: string
}

/**
 * 获取当前主题的图表颜色
 */
export function getChartThemeColors(isDark: boolean): ChartThemeColors {
  if (isDark) {
    // 冰夜永黑 - Ice Night Eternal
    return {
      text: '#e2e8f0',           // 银霜白
      textSecondary: '#7a8ba3',  // 冰川灰
      grid: '#1a2236',           // 冰川深蓝
      border: '#1e2942',         // 深邃边框
      crosshair: '#4a5568',      // 冷灰十字线
      equity: '#60a5fa',         // 极光蓝
      equityFill: 'rgba(96, 165, 250, 0.15)',
      closeLine: '#818cf8',      // 靛蓝色
    }
  } else {
    // 极昼白芒 - Polar Day Radiance
    return {
      text: '#1a2744',           // 深墨蓝
      textSecondary: '#5a6a7f',  // 墨灰
      grid: '#e8e4dc',           // 暖灰网格
      border: '#ddd8ce',         // 象牙边框
      crosshair: '#9ca3af',      // 中灰十字线
      equity: '#3b82f6',         // 靛蓝
      equityFill: 'rgba(59, 130, 246, 0.12)',
      closeLine: '#6366f1',      // 紫靛色
    }
  }
}

/**
 * 指标颜色配置 - 在两种主题下保持一致的辨识度
 */
export const INDICATOR_COLORS = {
  // 均线系列 - 暖色系渐变
  ma5: '#f59e0b',      // 琥珀 - 最短周期最亮
  ma10: '#3b82f6',     // 靛蓝
  ma20: '#a855f7',     // 紫罗兰
  ma60: '#22c55e',     // 翠绿 - 长周期更沉稳

  // EMA - 青橙对比
  ema12: '#06b6d4',    // 青色
  ema26: '#f97316',    // 橙色

  // 布林带 - 灰色系，不喧宾夺主
  bollUpper: '#94a3b8',
  bollMiddle: '#64748b',
  bollLower: '#94a3b8',

  // 技术指标
  rsi: '#8b5cf6',      // 紫罗兰
  macdDif: '#3b82f6',  // 靛蓝
  macdDea: '#f97316',  // 橙色

  // 成交量
  volume: '#64748b',

  // 收盘价线和权益曲线
  closeLine: '#818cf8', // 靛蓝色
  equity: '#60a5fa',    // 极光蓝
} as const
