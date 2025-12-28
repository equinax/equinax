/**
 * Color system for Universe Cockpit
 *
 * Provides consistent styling for classification badges and indicators.
 */

// Size category colors - purple to gray gradient (large to small)
export const SIZE_COLORS: Record<string, string> = {
  MEGA: 'bg-purple-100 text-purple-800 dark:bg-purple-900/50 dark:text-purple-200',
  LARGE: 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200',
  MID: 'bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-200',
  SMALL: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/50 dark:text-yellow-200',
  MICRO: 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400',
}

// Size category labels in Chinese
export const SIZE_LABELS: Record<string, string> = {
  MEGA: '超大盘',
  LARGE: '大盘',
  MID: '中盘',
  SMALL: '小盘',
  MICRO: '微盘',
}

// Volatility category colors - red (high) to cyan (low)
export const VOL_COLORS: Record<string, string> = {
  HIGH: 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200',
  NORMAL: 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300',
  LOW: 'bg-cyan-100 text-cyan-800 dark:bg-cyan-900/50 dark:text-cyan-200',
}

export const VOL_LABELS: Record<string, string> = {
  HIGH: '高波动',
  NORMAL: '正常',
  LOW: '低波动',
}

// Value/Growth style colors - amber (value) to emerald (growth)
export const VALUE_COLORS: Record<string, string> = {
  VALUE: 'bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-200',
  NEUTRAL: 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300',
  GROWTH: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-200',
}

export const VALUE_LABELS: Record<string, string> = {
  VALUE: '价值',
  NEUTRAL: '均衡',
  GROWTH: '成长',
}

// Turnover activity colors
export const TURNOVER_COLORS: Record<string, string> = {
  HOT: 'bg-orange-100 text-orange-800 dark:bg-orange-900/50 dark:text-orange-200',
  NORMAL: 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300',
  DEAD: 'bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-500',
}

export const TURNOVER_LABELS: Record<string, string> = {
  HOT: '热门',
  NORMAL: '正常',
  DEAD: '冷门',
}

// Board type colors - distinct colors for each board
export const BOARD_COLORS: Record<string, string> = {
  MAIN: 'bg-blue-600 text-white',
  GEM: 'bg-orange-500 text-white',
  STAR: 'bg-purple-600 text-white',
  BSE: 'bg-rose-500 text-white',
}

export const BOARD_LABELS: Record<string, string> = {
  MAIN: '主',
  GEM: '创',
  STAR: '科',
  BSE: '北',
}

// Asset type colors
export const ASSET_TYPE_COLORS: Record<string, string> = {
  stock: 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200',
  etf: 'bg-violet-100 text-violet-800 dark:bg-violet-900/50 dark:text-violet-200',
  STOCK: 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200',
  ETF: 'bg-violet-100 text-violet-800 dark:bg-violet-900/50 dark:text-violet-200',
}

// Market regime colors
export const REGIME_COLORS: Record<string, string> = {
  BULL: 'bg-green-500 text-white',
  BEAR: 'bg-red-500 text-white',
  RANGE: 'bg-yellow-500 text-white',
}

export const REGIME_LABELS: Record<string, string> = {
  BULL: '牛市',
  BEAR: '熊市',
  RANGE: '震荡',
}

// Exchange colors
export const EXCHANGE_COLORS: Record<string, string> = {
  sh: 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200',
  sz: 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200',
  SH: 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200',
  SZ: 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200',
}

// Helper functions
export function getSizeColor(size: string | null | undefined): string {
  if (!size) return SIZE_COLORS.MID
  return SIZE_COLORS[size.toUpperCase()] || SIZE_COLORS.MID
}

export function getSizeLabel(size: string | null | undefined): string {
  if (!size) return '-'
  return SIZE_LABELS[size.toUpperCase()] || size
}

export function getVolColor(vol: string | null | undefined): string {
  if (!vol) return VOL_COLORS.NORMAL
  return VOL_COLORS[vol.toUpperCase()] || VOL_COLORS.NORMAL
}

export function getVolLabel(vol: string | null | undefined): string {
  if (!vol) return '-'
  return VOL_LABELS[vol.toUpperCase()] || vol
}

export function getValueColor(value: string | null | undefined): string {
  if (!value) return VALUE_COLORS.NEUTRAL
  return VALUE_COLORS[value.toUpperCase()] || VALUE_COLORS.NEUTRAL
}

export function getValueLabel(value: string | null | undefined): string {
  if (!value) return '-'
  return VALUE_LABELS[value.toUpperCase()] || value
}

export function getTurnoverColor(turnover: string | null | undefined): string {
  if (!turnover) return TURNOVER_COLORS.NORMAL
  return TURNOVER_COLORS[turnover.toUpperCase()] || TURNOVER_COLORS.NORMAL
}

export function getTurnoverLabel(turnover: string | null | undefined): string {
  if (!turnover) return '-'
  return TURNOVER_LABELS[turnover.toUpperCase()] || turnover
}

export function getBoardColor(board: string | null | undefined): string {
  if (!board) return 'bg-gray-200 text-gray-700'
  return BOARD_COLORS[board.toUpperCase()] || 'bg-gray-200 text-gray-700'
}

export function getBoardLabel(board: string | null | undefined): string {
  if (!board) return '-'
  return BOARD_LABELS[board.toUpperCase()] || board
}

export function getRegimeColor(regime: string | null | undefined): string {
  if (!regime) return 'bg-gray-200 text-gray-700'
  return REGIME_COLORS[regime.toUpperCase()] || 'bg-gray-200 text-gray-700'
}

export function getRegimeLabel(regime: string | null | undefined): string {
  if (!regime) return '-'
  return REGIME_LABELS[regime.toUpperCase()] || regime
}

export function getExchangeColor(exchange: string | null | undefined): string {
  if (!exchange) return 'bg-gray-100 text-gray-700'
  return EXCHANGE_COLORS[exchange.toUpperCase()] || 'bg-gray-100 text-gray-700'
}

// Price change color based on value
export function getPriceChangeColor(change: number | string | null | undefined): string {
  if (change === null || change === undefined) return 'text-muted-foreground'
  const num = typeof change === 'string' ? parseFloat(change) : change
  if (isNaN(num)) return 'text-muted-foreground'
  if (num > 0) return 'text-profit'
  if (num < 0) return 'text-loss'
  return 'text-muted-foreground'
}

// Format price change with sign
export function formatPriceChange(change: number | string | null | undefined): string {
  if (change === null || change === undefined) return '-'
  const num = typeof change === 'string' ? parseFloat(change) : change
  if (isNaN(num)) return '-'
  const sign = num >= 0 ? '+' : ''
  return `${sign}${num.toFixed(2)}%`
}

// Format market cap - input is already in 亿元 units
export function formatMarketCap(cap: number | string | null | undefined): string {
  if (cap === null || cap === undefined) return '-'
  const num = typeof cap === 'string' ? parseFloat(cap) : cap
  if (isNaN(num)) return '-'
  // cap is already in 亿元 units from API
  if (num >= 10000) {
    return `${(num / 10000).toFixed(2)}万亿`
  }
  if (num >= 1000) {
    return `${(num / 1000).toFixed(1)}千亿`
  }
  if (num >= 1) {
    return `${num.toFixed(0)}亿`
  }
  return `${(num * 10000).toFixed(0)}万`
}

// Format number with locale
export function formatNumber(num: number | string | null | undefined, decimals = 2): string {
  if (num === null || num === undefined) return '-'
  const val = typeof num === 'string' ? parseFloat(num) : num
  if (isNaN(val)) return '-'
  return val.toLocaleString('zh-CN', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

// Format price with currency
export function formatPrice(price: number | string | null | undefined): string {
  if (price === null || price === undefined) return '-'
  const num = typeof price === 'string' ? parseFloat(price) : price
  if (isNaN(num)) return '-'
  return `¥${num.toFixed(2)}`
}

// Format turnover rate as percentage
export function formatTurnover(turn: number | string | null | undefined): string {
  if (turn === null || turn === undefined) return '-'
  const num = typeof turn === 'string' ? parseFloat(turn) : turn
  if (isNaN(num)) return '-'
  return `${num.toFixed(2)}%`
}

// Format PE/PB ratio
export function formatRatio(ratio: number | string | null | undefined): string {
  if (ratio === null || ratio === undefined) return '-'
  const num = typeof ratio === 'string' ? parseFloat(ratio) : ratio
  if (isNaN(num)) return '-'
  if (num < 0) return '亏损'
  return num.toFixed(2)
}
