import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown, ArrowLeft } from 'lucide-react'
import { StockChart } from '@/components/stock/StockChart'
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

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={handleBack}
            className="mt-1"
          >
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-2xl font-bold">{detail.name}</h1>
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
        </div>
        <div className="text-right">
          <p className={cn(
            'text-3xl font-bold font-mono',
            getPriceChangeColor(detail.change_pct)
          )}>
            {formatPrice(detail.price)}
          </p>
          <div className={cn(
            'flex items-center justify-end gap-1 text-lg',
            getPriceChangeColor(detail.change_pct)
          )}>
            {detail.change_pct != null && Number(detail.change_pct) > 0 ? (
              <TrendingUp className="h-5 w-5" />
            ) : detail.change_pct != null && Number(detail.change_pct) < 0 ? (
              <TrendingDown className="h-5 w-5" />
            ) : null}
            <span className="font-mono">
              {formatPriceChange(detail.change_pct)}
            </span>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            {activeDate || detail.price_date}
          </p>
          {activeDate && activeDate !== detail.price_date && (
            <p className="text-xs text-muted-foreground">
              (选股日期: {activeDate})
            </p>
          )}
        </div>
      </div>

      {/* Stock Chart */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between py-3">
          <CardTitle className="text-sm">行情走势</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <StockChart code={code || ''} height={500} endDate={activeDate || undefined} />
        </CardContent>
      </Card>

      {/* Valuation */}
      <Card>
        <CardHeader className="py-3">
          <CardTitle className="text-sm">估值指标</CardTitle>
        </CardHeader>
        <CardContent className="pb-3">
          <div className="grid grid-cols-3 md:grid-cols-6 gap-4">
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
  )
}
