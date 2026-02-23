import { useCallback, useState } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown, ArrowLeft, Shuffle, Loader2 } from 'lucide-react'
import { StockChart, HoverData } from '@/components/stock/StockChart'
import { cn } from '@/lib/utils'
import { useGetAssetDetailApiV1UniverseCodeGet } from '@/api/generated/universe-cockpit/universe-cockpit'
import {
  getPriceChangeColor,
  formatPriceChange,
  formatMarketCap,
  formatPrice,
  formatRatio,
  formatTurnover,
} from '@/lib/universe-colors'

export default function UniverseDetailPage() {
  const { code } = useParams<{ code: string }>()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const [hoverData, setHoverData] = useState<HoverData | null>(null)
  const [latestData, setLatestData] = useState<HoverData | null>(null)
  const [isChartLoading, setIsChartLoading] = useState(false)

  const fromPage = searchParams.get('from')
  const activeDate = searchParams.get('date')

  const handleBack = () => {
    if (fromPage) {
      navigate(-1)
    } else {
      navigate('/universe')
    }
  }

  const handleLatestData = useCallback((data: HoverData) => {
    setLatestData(data)
  }, [])

  const { data: detail, isLoading } = useGetAssetDetailApiV1UniverseCodeGet(
    code || '',
    { query: { enabled: !!code } }
  )

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-[500px] w-full" />
        <Skeleton className="h-32 w-full" />
      </div>
    )
  }

  if (!detail) {
    return (
      <div className="flex flex-col items-center justify-center py-12">
        <p className="text-muted-foreground">未找到资产信息</p>
        <Button variant="link" onClick={handleBack}>
          返回
        </Button>
      </div>
    )
  }

  const d = hoverData ?? latestData
  const isHover = !!hoverData

  const formatVolume = (volume: number | undefined | null) => {
    if (volume == null) return '-'
    const lots = volume / 100
    if (lots >= 1e4) {
      return `${(lots / 1e4).toFixed(2)}万手`
    }
    return `${lots.toFixed(0)}手`
  }

  const formatAmount = (amount: number | string | undefined | null) => {
    if (amount == null) return '-'
    const num = typeof amount === 'string' ? parseFloat(amount) : amount
    if (isNaN(num)) return '-'
    if (num >= 1e8) {
      return `${(num / 1e8).toFixed(2)}亿`
    } else if (num >= 1e4) {
      return `${(num / 1e4).toFixed(2)}万`
    }
    return num.toLocaleString()
  }

  const priceColor = getPriceChangeColor(d?.change_pct ?? detail.change_pct)

  return (
    <Card className={cn(
      "flex flex-col h-[calc(100vh-6rem)] transition-colors",
      isHover && "bg-muted/30"
    )}>
      <div className="flex items-center gap-4 px-2 py-1 border-b shrink-0">
        <div className="flex items-center gap-2 shrink-0">
          <Button variant="ghost" size="icon" onClick={handleBack} className="-ml-1 h-8 w-8">
            <ArrowLeft className="h-4 w-4" />
          </Button>
          
          <div className="flex flex-col leading-tight">
            <h1 className="text-base font-bold">{detail.name}</h1>
            <span className="text-[10px] text-muted-foreground font-mono">{detail.code}</span>
          </div>

          <div className="flex flex-col text-[10px] text-muted-foreground leading-tight">
            {detail.industry_l1 && <span>{detail.industry_l1}</span>}
            <span>{detail.exchange?.toUpperCase()}</span>
          </div>

          {(detail.is_st || detail.is_new) && (
            <div className="flex gap-1">
              {detail.is_st && <Badge variant="destructive" className="text-[10px] px-1 py-0 h-4">ST</Badge>}
              {detail.is_new && <Badge className="bg-green-500 text-white text-[10px] px-1 py-0 h-4">新股</Badge>}
            </div>
          )}
        </div>

        <div className="flex items-center gap-6 flex-1 min-w-0 text-xs">
          <div className="flex flex-col leading-tight shrink-0">
            <span className={cn("text-lg font-bold font-mono", priceColor)}>
              {formatPrice(d?.close ?? detail.price, detail.asset_type)}
            </span>
            <span className={cn("font-mono flex items-center", priceColor)}>
              {(d?.change_pct ?? detail.change_pct) != null && Number(d?.change_pct ?? detail.change_pct) > 0 ? (
                <TrendingUp className="h-3 w-3 mr-0.5" />
              ) : (d?.change_pct ?? detail.change_pct) != null && Number(d?.change_pct ?? detail.change_pct) < 0 ? (
                <TrendingDown className="h-3 w-3 mr-0.5" />
              ) : null}
              {formatPriceChange(d?.change_pct ?? detail.change_pct)}
            </span>
            <span className="text-[10px] text-muted-foreground font-mono">
              {d?.date ?? activeDate ?? detail.price_date}
            </span>
          </div>

          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>高 <span className={cn("font-mono", priceColor)}>{formatPrice(d?.high ?? detail.high, detail.asset_type)}</span></span>
            <span>低 <span className={cn("font-mono", priceColor)}>{formatPrice(d?.low ?? detail.low, detail.asset_type)}</span></span>
            <span>开 <span className={cn("font-mono", priceColor)}>{formatPrice(d?.open ?? null, detail.asset_type)}</span></span>
          </div>

          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>市值 <span className="font-mono text-foreground">{formatMarketCap(d?.total_mv ?? detail.market_cap)}</span></span>
            <span>流值 <span className="font-mono text-foreground">{formatMarketCap(d?.circ_mv ?? detail.circ_mv)}</span></span>
            <span>市盈 <span className="font-mono text-foreground">{formatRatio(d?.pe_ttm ?? detail.pe_ttm)}</span></span>
          </div>

          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>总手 <span className="font-mono text-foreground">{formatVolume(d?.volume ?? detail.volume)}</span></span>
            <span>金额 <span className="font-mono text-foreground">{formatAmount(d?.amount ?? detail.amount)}</span></span>
            <span>换手 <span className="font-mono text-foreground">{formatTurnover(d?.turnover_rate ?? detail.turnover)}</span></span>
          </div>

          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>市净 <span className="font-mono text-foreground">{formatRatio(d?.pb_mrq ?? detail.pb_mrq)}</span></span>
            <span>市销 <span className="font-mono text-foreground">{formatRatio(d?.ps_ttm ?? detail.ps_ttm)}</span></span>
            <span>量比 <span className="font-mono text-foreground">{
              d?.volume_ratio != null ? d.volume_ratio.toFixed(2) : '-'
            }</span></span>
          </div>

          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>股息 <span className="font-mono text-foreground">{
              d?.dv_ratio != null ? `${d.dv_ratio.toFixed(2)}%` : '-'
            }</span></span>
            <span>静PE <span className="font-mono text-foreground">{formatRatio(d?.pe ?? null)}</span></span>
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {isChartLoading && (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          )}
          <Button
            variant="outline"
            size="sm"
            onClick={() => navigate(`/universe/${code}/inverse`)}
            className="h-7 text-xs"
          >
            <Shuffle className="h-3 w-3 mr-1" />
            查找反向
          </Button>
        </div>
      </div>

      <CardContent className="p-0 flex-1 min-h-0">
        <StockChart 
          code={code || ''} 
          height="100%"
          endDate={activeDate || undefined} 
          onHoverData={setHoverData}
          onLatestData={handleLatestData}
          onLoadingChange={setIsChartLoading}
        />
      </CardContent>
    </Card>
  )
}
