import { useEffect, useRef } from 'react'
import { createChart, type IChartApi, ColorType } from 'lightweight-charts'
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown, Building2, Landmark, Users } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useGetAssetDetailApiV1UniverseCodeGet } from '@/api/generated/universe-cockpit/universe-cockpit'
import {
  getSizeColor,
  getSizeLabel,
  getVolColor,
  getVolLabel,
  getValueColor,
  getValueLabel,
  getPriceChangeColor,
  formatPriceChange,
  formatMarketCap,
  formatPrice,
  formatRatio,
  formatTurnover,
} from '@/lib/universe-colors'

interface UniverseDetailDrawerProps {
  code: string | null
  open: boolean
  onClose: () => void
}

export function UniverseDetailDrawer({ code, open, onClose }: UniverseDetailDrawerProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)

  const { data: detail, isLoading } = useGetAssetDetailApiV1UniverseCodeGet(
    code || '',
    { query: { enabled: !!code && open } }
  )

  // Initialize chart
  useEffect(() => {
    if (!chartContainerRef.current || !detail?.recent_klines?.length) return

    // Clean up existing chart
    if (chartRef.current) {
      chartRef.current.remove()
      chartRef.current = null
    }

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 200,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#64748b',
      },
      grid: {
        vertLines: { color: '#e2e8f0' },
        horzLines: { color: '#e2e8f0' },
      },
      rightPriceScale: {
        borderColor: '#e2e8f0',
      },
      timeScale: {
        borderColor: '#e2e8f0',
        timeVisible: false,
      },
    })

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#22c55e',
      downColor: '#ef4444',
      borderUpColor: '#22c55e',
      borderDownColor: '#ef4444',
      wickUpColor: '#22c55e',
      wickDownColor: '#ef4444',
    })

    const klineData = detail.recent_klines
      .filter((k) => k.open && k.high && k.low && k.close)
      .map((k) => ({
        time: k.date as string,
        open: k.open as number,
        high: k.high as number,
        low: k.low as number,
        close: k.close as number,
      }))

    candlestickSeries.setData(klineData)
    chart.timeScale().fitContent()

    chartRef.current = chart

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [detail?.recent_klines])

  return (
    <Sheet open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <SheetContent className="w-[400px] sm:w-[540px] overflow-y-auto">
        {isLoading ? (
          <div className="space-y-4">
            <Skeleton className="h-8 w-48" />
            <Skeleton className="h-[200px] w-full" />
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        ) : detail ? (
          <div className="space-y-4">
            <SheetHeader>
              <div className="flex items-start justify-between">
                <div>
                  <SheetTitle className="text-xl">{detail.name}</SheetTitle>
                  <p className="text-sm text-muted-foreground font-mono mt-1">
                    {detail.code}
                  </p>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    <Badge variant="outline">{detail.exchange?.toUpperCase()}</Badge>
                    {detail.industry_l1 && (
                      <Badge variant="secondary">{detail.industry_l1}</Badge>
                    )}
                    {detail.is_st && (
                      <Badge variant="destructive">ST</Badge>
                    )}
                    {detail.is_new && (
                      <Badge className="bg-green-500 text-white">新股</Badge>
                    )}
                  </div>
                </div>
                <div className="text-right">
                  <p className={cn(
                    'text-2xl font-bold font-mono',
                    getPriceChangeColor(detail.change_pct)
                  )}>
                    {formatPrice(detail.price)}
                  </p>
                  <div className={cn(
                    'flex items-center justify-end gap-1',
                    getPriceChangeColor(detail.change_pct)
                  )}>
                    {detail.change_pct != null && Number(detail.change_pct) > 0 ? (
                      <TrendingUp className="h-4 w-4" />
                    ) : detail.change_pct != null && Number(detail.change_pct) < 0 ? (
                      <TrendingDown className="h-4 w-4" />
                    ) : null}
                    <span className="font-mono">
                      {formatPriceChange(detail.change_pct)}
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">
                    {detail.price_date}
                  </p>
                </div>
              </div>
            </SheetHeader>

            {/* Mini K-line Chart */}
            <Card>
              <CardHeader className="py-3">
                <CardTitle className="text-sm">60日走势</CardTitle>
              </CardHeader>
              <CardContent className="pb-3">
                <div ref={chartContainerRef} className="w-full" />
              </CardContent>
            </Card>

            {/* Style Factors */}
            <Card>
              <CardHeader className="py-3">
                <CardTitle className="text-sm">风格因子</CardTitle>
              </CardHeader>
              <CardContent className="pb-3">
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">市值规模</p>
                    <Badge className={cn('text-xs', getSizeColor(detail.size_factor?.category))}>
                      {getSizeLabel(detail.size_factor?.category)}
                    </Badge>
                    {detail.size_factor?.percentile && (
                      <div className="h-1.5 bg-muted rounded-full mt-1">
                        <div
                          className="h-full bg-purple-500 rounded-full"
                          style={{ width: `${Number(detail.size_factor.percentile) * 100}%` }}
                        />
                      </div>
                    )}
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">波动率</p>
                    <Badge className={cn('text-xs', getVolColor(detail.vol_factor?.category))}>
                      {getVolLabel(detail.vol_factor?.category)}
                    </Badge>
                    {detail.vol_factor?.percentile && (
                      <div className="h-1.5 bg-muted rounded-full mt-1">
                        <div
                          className="h-full bg-red-500 rounded-full"
                          style={{ width: `${Number(detail.vol_factor.percentile) * 100}%` }}
                        />
                      </div>
                    )}
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">价值风格</p>
                    <Badge className={cn('text-xs', getValueColor(detail.value_factor?.category))}>
                      {getValueLabel(detail.value_factor?.category)}
                    </Badge>
                    {detail.value_factor?.percentile && (
                      <div className="h-1.5 bg-muted rounded-full mt-1">
                        <div
                          className="h-full bg-amber-500 rounded-full"
                          style={{ width: `${Number(detail.value_factor.percentile) * 100}%` }}
                        />
                      </div>
                    )}
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">动量因子</p>
                    <div className="flex gap-2 text-sm font-mono">
                      <span className={getPriceChangeColor(detail.momentum_20d)}>
                        20D: {formatPriceChange(detail.momentum_20d)}
                      </span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Microstructure */}
            <Card>
              <CardHeader className="py-3">
                <CardTitle className="text-sm">微观结构</CardTitle>
              </CardHeader>
              <CardContent className="pb-3">
                <div className="grid grid-cols-3 gap-3">
                  <div className="flex flex-col items-center p-2 rounded bg-muted/50">
                    <Building2 className={cn('h-5 w-5 mb-1', detail.is_institutional ? 'text-blue-500' : 'text-muted-foreground')} />
                    <p className="text-xs text-muted-foreground">机构重仓</p>
                    <p className="text-sm font-medium">
                      {detail.is_institutional ? '是' : '否'}
                    </p>
                    {detail.fund_holding_ratio != null && (
                      <p className="text-xs text-muted-foreground">
                        {(parseFloat(String(detail.fund_holding_ratio)) * 100).toFixed(2)}%
                      </p>
                    )}
                  </div>
                  <div className="flex flex-col items-center p-2 rounded bg-muted/50">
                    <Landmark className={cn('h-5 w-5 mb-1', detail.is_northbound_heavy ? 'text-amber-500' : 'text-muted-foreground')} />
                    <p className="text-xs text-muted-foreground">北向重仓</p>
                    <p className="text-sm font-medium">
                      {detail.is_northbound_heavy ? '是' : '否'}
                    </p>
                    {detail.northbound_holding_ratio != null && (
                      <p className="text-xs text-muted-foreground">
                        {(parseFloat(String(detail.northbound_holding_ratio)) * 100).toFixed(2)}%
                      </p>
                    )}
                  </div>
                  <div className="flex flex-col items-center p-2 rounded bg-muted/50">
                    <Users className={cn('h-5 w-5 mb-1', detail.is_retail_hot ? 'text-orange-500' : 'text-muted-foreground')} />
                    <p className="text-xs text-muted-foreground">散户热门</p>
                    <p className="text-sm font-medium">
                      {detail.is_retail_hot ? '是' : '否'}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Valuation */}
            <Card>
              <CardHeader className="py-3">
                <CardTitle className="text-sm">估值指标</CardTitle>
              </CardHeader>
              <CardContent className="pb-3">
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <p className="text-xs text-muted-foreground">市值</p>
                    <p className="text-sm font-mono font-medium">
                      {formatMarketCap(detail.market_cap)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">PE (TTM)</p>
                    <p className="text-sm font-mono font-medium">
                      {formatRatio(detail.pe_ttm)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">PB (MRQ)</p>
                    <p className="text-sm font-mono font-medium">
                      {formatRatio(detail.pb_mrq)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">流通市值</p>
                    <p className="text-sm font-mono font-medium">
                      {formatMarketCap(detail.circ_mv)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">PS (TTM)</p>
                    <p className="text-sm font-mono font-medium">
                      {formatRatio(detail.ps_ttm)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground">换手率</p>
                    <p className="text-sm font-mono font-medium">
                      {formatTurnover(detail.turnover)}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
