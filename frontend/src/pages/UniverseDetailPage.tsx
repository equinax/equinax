import { useState } from 'react'
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
  const [isChartLoading, setIsChartLoading] = useState(false)

  const fromPage = searchParams.get('from')
  const activeDate = searchParams.get('date')

  const handleBack = () => {
    if (fromPage) {
      // If we have a known source, use browser history
      navigate(-1)
    } else {
      // Default fallback to universe page
      navigate('/universe')
    }
  }

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

  const displayData = hoverData ? {
    price: hoverData.close,
    change_pct: hoverData.change_pct,
    preclose: hoverData.preclose,
    open: hoverData.open,
    high: hoverData.high,
    low: hoverData.low,
    volume: hoverData.volume,
    amount: hoverData.amount,
    turn: hoverData.turn,
    date: hoverData.date,
    isHover: true
  } : {
    price: detail.price,
    change_pct: detail.change_pct,
    preclose: null as number | null,
    open: null as number | null,
    high: detail.high,
    low: detail.low,
    volume: detail.volume,
    amount: detail.amount,
    turn: detail.turnover,
    date: activeDate || detail.price_date,
    isHover: false
  }

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

  const priceColor = getPriceChangeColor(displayData.change_pct)

  return (
    <Card className={cn(
      "flex flex-col h-[calc(100vh-6rem)] transition-colors",
      displayData.isHover && "bg-muted/30"
    )}>
      {/* Header */}
      <div className="flex items-center gap-4 px-2 py-1 border-b shrink-0">
        {/* Back + Title */}
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

        {/* Metrics Groups */}
        <div className="flex items-center gap-6 flex-1 min-w-0 text-xs">
          {/* Group 1: Price / Change / Date */}
          <div className="flex flex-col leading-tight shrink-0">
            <span className={cn("text-lg font-bold font-mono", priceColor)}>
              {formatPrice(displayData.price, detail.asset_type)}
            </span>
            <span className={cn("font-mono flex items-center", priceColor)}>
              {displayData.change_pct != null && Number(displayData.change_pct) > 0 ? (
                <TrendingUp className="h-3 w-3 mr-0.5" />
              ) : displayData.change_pct != null && Number(displayData.change_pct) < 0 ? (
                <TrendingDown className="h-3 w-3 mr-0.5" />
              ) : null}
              {formatPriceChange(displayData.change_pct)}
            </span>
            <span className="text-[10px] text-muted-foreground font-mono">{displayData.date}</span>
          </div>

          {/* Group 2: High / Low / Open */}
          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>高 <span className={cn("font-mono", priceColor)}>{formatPrice(displayData.high, detail.asset_type)}</span></span>
            <span>低 <span className={cn("font-mono", priceColor)}>{formatPrice(displayData.low, detail.asset_type)}</span></span>
            <span>开 <span className={cn("font-mono", priceColor)}>{formatPrice(displayData.open, detail.asset_type)}</span></span>
          </div>

          {!displayData.isHover && (
            <div className="flex flex-col leading-tight text-muted-foreground">
              <span>市值 <span className="font-mono text-foreground">{formatMarketCap(detail.market_cap)}</span></span>
              <span>流值 <span className="font-mono text-foreground">{formatMarketCap(detail.circ_mv)}</span></span>
              <span>市盈 <span className="font-mono text-foreground">{formatRatio(detail.pe_ttm)}</span></span>
            </div>
          )}

          {/* Group 4: Volume / Amount / Turnover */}
          <div className="flex flex-col leading-tight text-muted-foreground">
            <span>总手 <span className="font-mono text-foreground">{formatVolume(displayData.volume)}</span></span>
            <span>金额 <span className="font-mono text-foreground">{formatAmount(displayData.amount)}</span></span>
            <span>换手 <span className="font-mono text-foreground">{formatTurnover(displayData.turn)}</span></span>
          </div>
        </div>

        {/* Loading indicator + Button */}
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

      {/* Chart */}
      <CardContent className="p-0 flex-1 min-h-0">
        <StockChart 
          code={code || ''} 
          height="100%"
          endDate={activeDate || undefined} 
          onHoverData={setHoverData}
          onLoadingChange={setIsChartLoading}
        />
      </CardContent>
    </Card>
  )
}
