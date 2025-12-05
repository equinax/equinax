import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'

interface TechnicalIndicators {
  ma_5?: number
  ma_10?: number
  ma_20?: number
  ma_60?: number
  ema_12?: number
  ema_26?: number
  macd_dif?: number
  macd_dea?: number
  macd_hist?: number
  rsi_6?: number
  rsi_12?: number
  rsi_24?: number
  kdj_k?: number
  kdj_d?: number
  kdj_j?: number
  boll_upper?: number
  boll_middle?: number
  boll_lower?: number
}

interface StockIndicatorsProps {
  data?: TechnicalIndicators
  currentPrice?: number
  className?: string
}

function formatValue(value: number | undefined | null, decimals = 2): string {
  if (value == null) return '-'
  return value.toFixed(decimals)
}

function getMaSignal(ma: number | undefined, price: number | undefined): 'bullish' | 'bearish' | 'neutral' {
  if (ma == null || price == null) return 'neutral'
  if (price > ma * 1.01) return 'bullish'
  if (price < ma * 0.99) return 'bearish'
  return 'neutral'
}

function getRsiSignal(rsi: number | undefined): 'overbought' | 'oversold' | 'neutral' {
  if (rsi == null) return 'neutral'
  if (rsi > 70) return 'overbought'
  if (rsi < 30) return 'oversold'
  return 'neutral'
}

interface IndicatorRowProps {
  label: string
  value: string
  signal?: 'bullish' | 'bearish' | 'overbought' | 'oversold' | 'neutral'
}

function IndicatorRow({ label, value, signal }: IndicatorRowProps) {
  return (
    <div className="flex justify-between items-center py-1">
      <span className="text-muted-foreground text-sm">{label}</span>
      <span className={cn(
        'font-mono text-sm',
        signal === 'bullish' && 'text-profit',
        signal === 'bearish' && 'text-loss',
        signal === 'overbought' && 'text-loss',
        signal === 'oversold' && 'text-profit',
      )}>
        {value}
      </span>
    </div>
  )
}

export function StockIndicators({ data, currentPrice, className }: StockIndicatorsProps) {
  if (!data) {
    return (
      <div className={cn('flex h-[200px] items-center justify-center rounded-lg border border-dashed bg-muted/50', className)}>
        <p className="text-muted-foreground">暂无技术指标数据</p>
      </div>
    )
  }

  return (
    <div className={cn('grid gap-4 md:grid-cols-2 lg:grid-cols-4', className)}>
      {/* Moving Averages */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">移动平均线</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1">
          <IndicatorRow
            label="MA5"
            value={formatValue(data.ma_5)}
            signal={getMaSignal(data.ma_5, currentPrice)}
          />
          <IndicatorRow
            label="MA10"
            value={formatValue(data.ma_10)}
            signal={getMaSignal(data.ma_10, currentPrice)}
          />
          <IndicatorRow
            label="MA20"
            value={formatValue(data.ma_20)}
            signal={getMaSignal(data.ma_20, currentPrice)}
          />
          <IndicatorRow
            label="MA60"
            value={formatValue(data.ma_60)}
            signal={getMaSignal(data.ma_60, currentPrice)}
          />
        </CardContent>
      </Card>

      {/* MACD */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">MACD</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1">
          <IndicatorRow
            label="DIF"
            value={formatValue(data.macd_dif, 4)}
            signal={data.macd_dif != null ? (data.macd_dif > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <IndicatorRow
            label="DEA"
            value={formatValue(data.macd_dea, 4)}
            signal={data.macd_dea != null ? (data.macd_dea > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <IndicatorRow
            label="MACD柱"
            value={formatValue(data.macd_hist, 4)}
            signal={data.macd_hist != null ? (data.macd_hist > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {data.macd_dif != null && data.macd_dea != null && (
              data.macd_dif > data.macd_dea
                ? <span className="text-profit">金叉信号</span>
                : <span className="text-loss">死叉信号</span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* RSI */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">RSI 相对强弱</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1">
          <IndicatorRow
            label="RSI(6)"
            value={formatValue(data.rsi_6, 1)}
            signal={getRsiSignal(data.rsi_6)}
          />
          <IndicatorRow
            label="RSI(12)"
            value={formatValue(data.rsi_12, 1)}
            signal={getRsiSignal(data.rsi_12)}
          />
          <IndicatorRow
            label="RSI(24)"
            value={formatValue(data.rsi_24, 1)}
            signal={getRsiSignal(data.rsi_24)}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {data.rsi_12 != null && (
              data.rsi_12 > 70
                ? <span className="text-loss">超买区域 ({'>'}70)</span>
                : data.rsi_12 < 30
                  ? <span className="text-profit">超卖区域 ({'<'}30)</span>
                  : <span>中性区域 (30-70)</span>
            )}
          </div>
        </CardContent>
      </Card>

      {/* KDJ */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium">KDJ 随机指标</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1">
          <IndicatorRow
            label="K"
            value={formatValue(data.kdj_k, 1)}
          />
          <IndicatorRow
            label="D"
            value={formatValue(data.kdj_d, 1)}
          />
          <IndicatorRow
            label="J"
            value={formatValue(data.kdj_j, 1)}
            signal={data.kdj_j != null ? (data.kdj_j > 100 ? 'overbought' : data.kdj_j < 0 ? 'oversold' : 'neutral') : 'neutral'}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {data.kdj_k != null && data.kdj_d != null && (
              data.kdj_k > data.kdj_d
                ? <span className="text-profit">K线在D线上方</span>
                : <span className="text-loss">K线在D线下方</span>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
