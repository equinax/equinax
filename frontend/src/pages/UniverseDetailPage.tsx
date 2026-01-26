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
    <div className="space-y-4">
      {/* Header Row 1: Navigation & Identity */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" onClick={handleBack} className="-ml-2">
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div className="flex items-baseline gap-2">
            <h1 className="text-xl font-bold">{detail.name}</h1>
            <span className="text-sm text-muted-foreground font-mono">{detail.code}</span>
          </div>
          <div className="flex gap-1 ml-2">
             <Badge variant="outline" className="text-xs px-1 py-0 h-5">{detail.exchange?.toUpperCase()}</Badge>
             {detail.industry_l1 && <Badge variant="secondary" className="text-xs px-1 py-0 h-5">{detail.industry_l1}</Badge>}
             {detail.is_st && <Badge variant="destructive" className="text-xs px-1 py-0 h-5">ST</Badge>}
             {detail.is_new && <Badge className="bg-green-500 text-white text-xs px-1 py-0 h-5">新股</Badge>}
          </div>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => navigate(`/universe/${code}/inverse`)}
          className="h-8"
        >
          <Shuffle className="h-3.5 w-3.5 mr-1" />
          查找反向
        </Button>
      </div>

      {/* Header Row 2: Price & Metrics */}
      <div className={cn(
        "grid grid-cols-1 lg:grid-cols-[auto_1fr] gap-4 items-center p-4 rounded-lg transition-colors",
        displayData.isHover ? "bg-muted/50" : "bg-card border"
      )}>
        {/* Price Section */}
        <div className="flex items-baseline gap-4 min-w-[200px]">
          <div>
            <div className={cn(
              "text-4xl font-bold font-mono tracking-tight",
              getPriceChangeColor(displayData.change_pct)
            )}>
              {formatPrice(displayData.price)}
            </div>
            <div className="flex items-center gap-2 mt-1">
              <div className={cn(
                "flex items-center text-lg font-medium",
                getPriceChangeColor(displayData.change_pct)
              )}>
                {displayData.change_pct != null && Number(displayData.change_pct) > 0 ? (
                  <TrendingUp className="h-5 w-5 mr-1" />
                ) : displayData.change_pct != null && Number(displayData.change_pct) < 0 ? (
                  <TrendingDown className="h-5 w-5 mr-1" />
                ) : null}
                <span className="font-mono">{formatPriceChange(displayData.change_pct)}</span>
              </div>
              <span className="text-xs text-muted-foreground font-mono">
                {displayData.date}
                {activeDate && activeDate !== detail.price_date && !displayData.isHover && " (选股)"}
              </span>
            </div>
          </div>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-4 lg:grid-cols-8 gap-x-4 gap-y-2 text-sm">
           {/* Dynamic */}
           <div>
             <div className="text-xs text-muted-foreground">高</div>
             <div className="font-mono">{formatPrice(displayData.high)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">低</div>
             <div className="font-mono">{formatPrice(displayData.low)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">开</div>
             <div className="font-mono">{formatPrice(displayData.open)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">量</div>
             <div className="font-mono">{formatVolume(displayData.volume)}</div>
           </div>

           {/* Static */}
           <div>
             <div className="text-xs text-muted-foreground">市值</div>
             <div className="font-mono">{formatMarketCap(detail.market_cap)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">流值</div>
             <div className="font-mono">{formatMarketCap(detail.circ_mv)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">PE</div>
             <div className="font-mono">{formatRatio(detail.pe_ttm)}</div>
           </div>
           <div>
             <div className="text-xs text-muted-foreground">换手</div>
             <div className="font-mono">{formatTurnover(detail.turnover)}</div>
           </div>
        </div>
      </div>

      {/* Stock Chart */}
      <Card className="border-0 shadow-none md:border md:shadow-sm">
        <CardContent className="p-0">
          <StockChart 
            code={code || ''} 
            height={500} 
            endDate={activeDate || undefined} 
            onHoverData={setHoverData}
          />
        </CardContent>
      </Card>
    </div>
  )
}
