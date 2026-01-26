import { useState } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown, ArrowLeft, Shuffle } from 'lucide-react'
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

  // Determine what data to show (hover vs current)
  // Note: 'open' is only available from K-line hover data, not from UniverseAssetDetail
  const displayData = hoverData ? {
    price: hoverData.close,
    change_pct: hoverData.change_pct,
    open: hoverData.open,
    high: hoverData.high,
    low: hoverData.low,
    volume: hoverData.volume,
    date: hoverData.date,
    isHover: true
  } : {
    price: detail.price,
    change_pct: detail.change_pct,
    open: null as number | null, // 'open' not available in detail API
    high: detail.high,
    low: detail.low,
    volume: detail.volume,
    date: activeDate || detail.price_date,
    isHover: false
  }

  const formatVolume = (volume: number | undefined | null) => {
    if (!volume) return '-'
    if (volume >= 1e8) {
      return `${(volume / 1e8).toFixed(2)}亿`
    } else if (volume >= 1e4) {
      return `${(volume / 1e4).toFixed(2)}万`
    }
    return volume.toLocaleString()
  }

  return (
    <Card className={cn(
      "flex flex-col h-[calc(100vh-6rem)] transition-colors",
      displayData.isHover && "bg-muted/30"
    )}>
      {/* Header: [Title Group] [Metrics] [Button] */}
      <div className="flex items-center gap-4 p-3 border-b shrink-0">
        {/* Left: Back + Title Info */}
        <div className="flex items-center gap-3 shrink-0">
          <Button variant="ghost" size="icon" onClick={handleBack} className="-ml-1">
            <ArrowLeft className="h-5 w-5" />
          </Button>
          
          <div className="flex flex-col">
            <h1 className="text-lg font-bold leading-tight">{detail.name}</h1>
            <span className="text-xs text-muted-foreground font-mono">{detail.code}</span>
          </div>

          <div className="flex flex-col text-xs">
            {detail.industry_l1 && (
              <span className="text-muted-foreground">{detail.industry_l1}</span>
            )}
            <span className="text-muted-foreground">{detail.exchange?.toUpperCase()}</span>
          </div>

          {(detail.is_st || detail.is_new) && (
            <div className="flex gap-1">
              {detail.is_st && <Badge variant="destructive" className="text-xs px-1 py-0 h-5">ST</Badge>}
              {detail.is_new && <Badge className="bg-green-500 text-white text-xs px-1 py-0 h-5">新股</Badge>}
            </div>
          )}
        </div>

        {/* Center: Price + Metrics */}
        <div className="flex items-center gap-6 flex-1 min-w-0">
          <div className="shrink-0">
            <div className={cn(
              "text-2xl font-bold font-mono tracking-tight leading-none",
              getPriceChangeColor(displayData.change_pct)
            )}>
              {formatPrice(displayData.price)}
            </div>
            <div className="flex items-center gap-1.5 mt-0.5">
              <div className={cn(
                "flex items-center text-sm font-medium",
                getPriceChangeColor(displayData.change_pct)
              )}>
                {displayData.change_pct != null && Number(displayData.change_pct) > 0 ? (
                  <TrendingUp className="h-3.5 w-3.5 mr-0.5" />
                ) : displayData.change_pct != null && Number(displayData.change_pct) < 0 ? (
                  <TrendingDown className="h-3.5 w-3.5 mr-0.5" />
                ) : null}
                <span className="font-mono">{formatPriceChange(displayData.change_pct)}</span>
              </div>
              <span className="text-[10px] text-muted-foreground font-mono">
                {displayData.date}
              </span>
            </div>
          </div>

          <div className="flex items-center gap-4 text-xs overflow-x-auto">
            <div>
              <div className="text-muted-foreground">高</div>
              <div className="font-mono">{formatPrice(displayData.high)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">低</div>
              <div className="font-mono">{formatPrice(displayData.low)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">开</div>
              <div className="font-mono">{formatPrice(displayData.open)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">量</div>
              <div className="font-mono">{formatVolume(displayData.volume)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">市值</div>
              <div className="font-mono">{formatMarketCap(detail.market_cap)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">流值</div>
              <div className="font-mono">{formatMarketCap(detail.circ_mv)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">PE</div>
              <div className="font-mono">{formatRatio(detail.pe_ttm)}</div>
            </div>
            <div>
              <div className="text-muted-foreground">换手</div>
              <div className="font-mono">{formatTurnover(detail.turnover)}</div>
            </div>
          </div>
        </div>

        {/* Right: Button */}
        <Button
          variant="outline"
          size="sm"
          onClick={() => navigate(`/universe/${code}/inverse`)}
          className="shrink-0"
        >
          <Shuffle className="h-3.5 w-3.5 mr-1" />
          查找反向
        </Button>
      </div>

      {/* Chart fills remaining space */}
      <CardContent className="p-0 flex-1 min-h-0">
        <StockChart 
          code={code || ''} 
          height="100%"
          endDate={activeDate || undefined} 
          onHoverData={setHoverData}
        />
      </CardContent>
    </Card>
  )
}
