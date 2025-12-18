import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { TrendingUp, TrendingDown, Activity, Building2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { UniverseStatsResponse } from '@/api/generated/schemas'
import { getRegimeColor, getRegimeLabel } from '@/lib/universe-colors'

interface UniverseStatsBarProps {
  stats?: UniverseStatsResponse
  isLoading?: boolean
}

export function UniverseStatsBar({ stats, isLoading }: UniverseStatsBarProps) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i}>
            <CardContent className="p-4">
              <Skeleton className="h-4 w-16 mb-2" />
              <Skeleton className="h-8 w-24" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  if (!stats) return null

  const regime = stats.market_regime
  const topIndustries = Object.entries(stats.by_industry_l1 || {})
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 3)

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      {/* Total Stocks */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs text-muted-foreground">股票总数</p>
              <p className="text-2xl font-bold mt-1">
                {stats.total_stocks.toLocaleString()}
              </p>
            </div>
            <div className="h-10 w-10 rounded-full bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
              <TrendingUp className="h-5 w-5 text-blue-600 dark:text-blue-400" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Total ETFs */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs text-muted-foreground">ETF总数</p>
              <p className="text-2xl font-bold mt-1">
                {stats.total_etfs.toLocaleString()}
              </p>
            </div>
            <div className="h-10 w-10 rounded-full bg-violet-100 dark:bg-violet-900/30 flex items-center justify-center">
              <Activity className="h-5 w-5 text-violet-600 dark:text-violet-400" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Market Regime */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs text-muted-foreground">市场状态</p>
              <div className="mt-1">
                {regime ? (
                  <Badge className={cn('text-sm', getRegimeColor(regime.regime))}>
                    {getRegimeLabel(regime.regime)}
                  </Badge>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </div>
              {regime?.up_count !== undefined && regime?.down_count !== undefined && (
                <div className="flex items-center gap-2 mt-1 text-xs">
                  <span className="text-profit flex items-center gap-0.5">
                    <TrendingUp className="h-3 w-3" />
                    {regime.up_count}
                  </span>
                  <span className="text-loss flex items-center gap-0.5">
                    <TrendingDown className="h-3 w-3" />
                    {regime.down_count}
                  </span>
                </div>
              )}
            </div>
            <div className="h-10 w-10 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center">
              <Activity className="h-5 w-5 text-amber-600 dark:text-amber-400" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Top Industries */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="min-w-0 flex-1">
              <p className="text-xs text-muted-foreground">热门行业</p>
              {topIndustries.length > 0 ? (
                <div className="flex flex-wrap gap-1 mt-1">
                  {topIndustries.map(([name, data]) => (
                    <Badge key={name} variant="secondary" className="text-xs">
                      {name} ({data.count})
                    </Badge>
                  ))}
                </div>
              ) : (
                <span className="text-muted-foreground text-sm">-</span>
              )}
            </div>
            <div className="h-10 w-10 rounded-full bg-green-100 dark:bg-green-900/30 flex items-center justify-center shrink-0 ml-2">
              <Building2 className="h-5 w-5 text-green-600 dark:text-green-400" />
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
