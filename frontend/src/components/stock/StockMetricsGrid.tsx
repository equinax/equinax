import { cn } from '@/lib/utils'

// Accept any API response type - we'll handle conversion internally
interface StockMetricsGridProps {
  kline?: {
    date?: unknown
    open?: unknown
    high?: unknown
    low?: unknown
    close?: unknown
    volume?: unknown
    amount?: unknown
    pct_chg?: unknown
    turn?: unknown
  }
  fundamentals?: unknown
  className?: string
}

function formatPrice(value: unknown): string {
  if (value == null) return '-'
  return Number(value).toFixed(2)
}

function formatVolume(value: unknown): string {
  if (value == null) return '-'
  const num = Number(value)
  if (num >= 100000000) {
    return `${(num / 100000000).toFixed(2)}亿`
  }
  if (num >= 10000) {
    return `${(num / 10000).toFixed(2)}万`
  }
  return num.toLocaleString()
}

function formatAmount(value: unknown): string {
  if (value == null) return '-'
  const num = Number(value)
  if (num >= 100000000) {
    return `${(num / 100000000).toFixed(2)}亿`
  }
  if (num >= 10000) {
    return `${(num / 10000).toFixed(2)}万`
  }
  return `${num.toFixed(2)}`
}

function formatRatio(value: unknown): string {
  if (value == null) return '-'
  return Number(value).toFixed(2)
}

export function StockMetricsGrid({ kline, fundamentals, className }: StockMetricsGridProps) {
  // Extract fundamentals data safely
  const fund = (fundamentals && typeof fundamentals === 'object') ? fundamentals as Record<string, unknown> : {}

  // Calculate amplitude (振幅)
  const high = Number(kline?.high)
  const low = Number(kline?.low)
  const close = Number(kline?.close)
  const pctChg = Number(kline?.pct_chg) || 0
  const amplitude = high && low && close
    ? ((high - low) / close) * 100
    : undefined

  const metrics = [
    // Row 1: Price metrics
    { label: '开盘价', value: formatPrice(kline?.open) },
    { label: '最高价', value: formatPrice(kline?.high), highlight: 'high' },
    { label: '最低价', value: formatPrice(kline?.low), highlight: 'low' },
    { label: '昨收价', value: close && pctChg !== undefined
      ? formatPrice(close / (1 + pctChg / 100))
      : '-'
    },
    { label: '振幅', value: amplitude != null ? `${amplitude.toFixed(2)}%` : '-' },

    // Row 2: Volume and fundamental metrics
    { label: '成交量', value: formatVolume(kline?.volume) },
    { label: '成交额', value: formatAmount(kline?.amount) },
    { label: '换手率', value: kline?.turn != null ? `${Number(kline.turn).toFixed(2)}%` : '-' },
    { label: 'PE(TTM)', value: formatRatio(fund.pe_ttm) },
    { label: 'PB(MRQ)', value: formatRatio(fund.pb_mrq) },
  ]

  return (
    <div className={cn('grid grid-cols-5 gap-px bg-border rounded-lg overflow-hidden', className)}>
      {metrics.map((metric, index) => (
        <div
          key={index}
          className={cn(
            'bg-card p-3 text-center',
            metric.highlight === 'high' && 'text-profit',
            metric.highlight === 'low' && 'text-loss'
          )}
        >
          <p className="text-xs text-muted-foreground mb-1">{metric.label}</p>
          <p className={cn(
            'text-sm font-medium',
            metric.highlight === 'high' && 'text-profit',
            metric.highlight === 'low' && 'text-loss'
          )}>
            {metric.value}
          </p>
        </div>
      ))}
    </div>
  )
}
