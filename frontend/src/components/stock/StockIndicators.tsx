import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { cn } from '@/lib/utils'

// Accept API response type (array of indicators or single object)
interface StockIndicatorsProps {
  data?: unknown
  currentPrice?: unknown
  className?: string
}

// Helper to safely get number from unknown
function getNum(value: unknown): number | undefined {
  if (value == null) return undefined
  const num = Number(value)
  return isNaN(num) ? undefined : num
}

function formatValue(value: unknown, decimals = 2): string {
  const num = getNum(value)
  if (num == null) return '-'
  return num.toFixed(decimals)
}

function getMaSignal(ma: unknown, price: unknown): 'bullish' | 'bearish' | 'neutral' {
  const maNum = getNum(ma)
  const priceNum = getNum(price)
  if (maNum == null || priceNum == null) return 'neutral'
  if (priceNum > maNum * 1.01) return 'bullish'
  if (priceNum < maNum * 0.99) return 'bearish'
  return 'neutral'
}

function getRsiSignal(rsi: unknown): 'overbought' | 'oversold' | 'neutral' {
  const rsiNum = getNum(rsi)
  if (rsiNum == null) return 'neutral'
  if (rsiNum > 70) return 'overbought'
  if (rsiNum < 30) return 'oversold'
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

// Extract indicators from API response (handles both array and object format)
function extractIndicators(data: unknown): Record<string, unknown> {
  if (!data) return {}

  // If it's an array, try to find the latest or combine them
  if (Array.isArray(data)) {
    if (data.length === 0) return {}
    // Use the last item (most recent) or merge all
    const result: Record<string, unknown> = {}
    for (const item of data) {
      if (item && typeof item === 'object') {
        Object.assign(result, item)
      }
    }
    return result
  }

  // If it's an object, use it directly
  if (typeof data === 'object') {
    return data as Record<string, unknown>
  }

  return {}
}

export function StockIndicators({ data, currentPrice, className }: StockIndicatorsProps) {
  const indicators = extractIndicators(data)

  if (!data || Object.keys(indicators).length === 0) {
    return (
      <div className={cn('flex h-[200px] items-center justify-center rounded-lg border border-dashed bg-muted/50', className)}>
        <p className="text-muted-foreground">暂无技术指标数据</p>
      </div>
    )
  }

  const macdDif = getNum(indicators.macd_dif)
  const macdDea = getNum(indicators.macd_dea)
  const macdHist = getNum(indicators.macd_hist)
  const rsi12 = getNum(indicators.rsi_12)
  const kdjK = getNum(indicators.kdj_k)
  const kdjD = getNum(indicators.kdj_d)
  const kdjJ = getNum(indicators.kdj_j)

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
            value={formatValue(indicators.ma_5)}
            signal={getMaSignal(indicators.ma_5, currentPrice)}
          />
          <IndicatorRow
            label="MA10"
            value={formatValue(indicators.ma_10)}
            signal={getMaSignal(indicators.ma_10, currentPrice)}
          />
          <IndicatorRow
            label="MA20"
            value={formatValue(indicators.ma_20)}
            signal={getMaSignal(indicators.ma_20, currentPrice)}
          />
          <IndicatorRow
            label="MA60"
            value={formatValue(indicators.ma_60)}
            signal={getMaSignal(indicators.ma_60, currentPrice)}
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
            value={formatValue(indicators.macd_dif, 4)}
            signal={macdDif != null ? (macdDif > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <IndicatorRow
            label="DEA"
            value={formatValue(indicators.macd_dea, 4)}
            signal={macdDea != null ? (macdDea > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <IndicatorRow
            label="MACD柱"
            value={formatValue(indicators.macd_hist, 4)}
            signal={macdHist != null ? (macdHist > 0 ? 'bullish' : 'bearish') : 'neutral'}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {macdDif != null && macdDea != null && (
              macdDif > macdDea
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
            value={formatValue(indicators.rsi_6, 1)}
            signal={getRsiSignal(indicators.rsi_6)}
          />
          <IndicatorRow
            label="RSI(12)"
            value={formatValue(indicators.rsi_12, 1)}
            signal={getRsiSignal(indicators.rsi_12)}
          />
          <IndicatorRow
            label="RSI(24)"
            value={formatValue(indicators.rsi_24, 1)}
            signal={getRsiSignal(indicators.rsi_24)}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {rsi12 != null && (
              rsi12 > 70
                ? <span className="text-loss">超买区域 ({'>'}70)</span>
                : rsi12 < 30
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
            value={formatValue(indicators.kdj_k, 1)}
          />
          <IndicatorRow
            label="D"
            value={formatValue(indicators.kdj_d, 1)}
          />
          <IndicatorRow
            label="J"
            value={formatValue(indicators.kdj_j, 1)}
            signal={kdjJ != null ? (kdjJ > 100 ? 'overbought' : kdjJ < 0 ? 'oversold' : 'neutral') : 'neutral'}
          />
          <div className="pt-2 text-xs text-muted-foreground">
            {kdjK != null && kdjD != null && (
              kdjK > kdjD
                ? <span className="text-profit">K线在D线上方</span>
                : <span className="text-loss">K线在D线下方</span>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
