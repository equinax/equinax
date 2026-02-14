export const QUANT_LABEL_CN: Record<string, string> = {
  main_accumulation: '主力吸筹',
  undervalued: '低估值',
  oversold: '超卖',
  high_volatility: '高波动',
  breakout: '突破',
  volume_surge: '放量',
}

/** Format market value (input in 亿元 from backend) */
export const formatMv = (val: string | number | null | undefined): string => {
  if (val == null) return '—'
  const num = parseFloat(String(val))
  if (num >= 10000) return `${(num / 10000).toFixed(0)}万亿`
  if (num >= 1) return `${num.toFixed(0)}亿`
  return `${(num * 10000).toFixed(0)}万`
}

/** Format volume (input in raw shares) */
export const formatVol = (val: string | number | null | undefined): string => {
  if (val == null) return '—'
  const num = parseFloat(String(val))
  if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(0)}万`
  return `${num.toFixed(0)}`
}
