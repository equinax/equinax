import { useEffect, useMemo } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { ArrowLeft } from 'lucide-react'
import { StockChart } from '@/components/stock/StockChart'
import { useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost } from '@/api/generated/alpha-radar/alpha-radar'
import { cn } from '@/lib/utils'

// Helper to format percentage strings
const formatPercent = (val: string | null | undefined) => {
  if (!val) return '—'
  return `${parseFloat(val).toFixed(2)}%`
}

// Helper to format number strings
const formatNumber = (val: string | null | undefined) => {
  if (!val) return '—'
  return parseFloat(val).toFixed(2)
}

// Helper for color based on value (A-share: Red=Up, Green=Down)
const getValueColor = (val: string | null | undefined, isWinRate = false) => {
  if (!val) return 'text-muted-foreground'
  const num = parseFloat(val)
  
  if (isWinRate) {
    if (num >= 60) return 'text-red-500 font-medium'
    if (num < 50) return 'text-green-500 font-medium'
    return 'text-foreground'
  }

  if (num > 0) return 'text-red-500 font-medium'
  if (num < 0) return 'text-green-500 font-medium'
  return 'text-muted-foreground'
}

// Helper for assessment badge color
const getAssessmentColor = (assessment: string) => {
  if (assessment.includes('✅')) return 'bg-green-100 text-green-800 hover:bg-green-100/80 dark:bg-green-900/30 dark:text-green-400'
  if (assessment.includes('👍')) return 'bg-blue-100 text-blue-800 hover:bg-blue-100/80 dark:bg-blue-900/30 dark:text-blue-400'
  if (assessment.includes('⚠️')) return 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100/80 dark:bg-yellow-900/30 dark:text-yellow-400'
  if (assessment.includes('❌')) return 'bg-red-100 text-red-800 hover:bg-red-100/80 dark:bg-red-900/30 dark:text-red-400'
  return 'bg-secondary text-secondary-foreground'
}

export default function MultiStockBrowsePage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  
  const codes = useMemo(() => 
    searchParams.get('codes')?.split(',').filter(Boolean) ?? [], 
    [searchParams]
  )
  const date = searchParams.get('date') ?? ''

  const evalMutation = useEvaluatePerformanceApiV1AlphaRadarEvaluatePerformancePost()

  useEffect(() => {
    if (codes.length > 0 && date) {
      evalMutation.mutate({ data: { codes, date } })
    }
  }, [codes.join(','), date])

  const stockMap = useMemo(() => {
    if (!evalMutation.data?.stocks) return {}
    return evalMutation.data.stocks.reduce((acc, stock) => {
      acc[stock.code] = stock
      return acc
    }, {} as Record<string, typeof evalMutation.data.stocks[0]>)
  }, [evalMutation.data])

  const periods = [1, 3, 5, 10, 20]

  // Find stats for a specific period
  const getStatsForPeriod = (period: number) => {
    return evalMutation.data?.period_stats?.find(s => s.period === period)
  }

  if (codes.length === 0 || !date) {
    return (
      <div className="container py-6 space-y-6">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <h1 className="text-2xl font-bold tracking-tight">多股浏览</h1>
        </div>
        <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
          请提供有效的股票代码和日期
        </div>
      </div>
    )
  }

  return (
    <div className="container py-6 space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" className="-ml-2 gap-1 text-muted-foreground hover:text-foreground" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-4 w-4" />
            返回选股
          </Button>
        </div>
        <div className="flex items-baseline justify-between">
          <h1 className="text-3xl font-bold tracking-tight">多股浏览</h1>
          <span className="text-muted-foreground font-mono">
            {date} · {codes.length}只股票
          </span>
        </div>
      </div>

      {/* Performance Panel */}
      <Card className="border-l-4 border-l-primary/50 shadow-sm">
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg font-medium flex items-center gap-2">
              表现评估
              {evalMutation.isPending && <Skeleton className="h-5 w-24" />}
              {evalMutation.data && (
                <Badge variant="outline" className={cn("ml-2 font-normal text-sm px-2.5 py-0.5", getAssessmentColor(evalMutation.data.assessment))}>
                  {evalMutation.data.assessment}
                </Badge>
              )}
            </CardTitle>
          </div>
        </CardHeader>
        <CardContent>
          {evalMutation.isPending ? (
            <div className="space-y-2">
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
            </div>
          ) : evalMutation.data ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 font-medium text-muted-foreground w-24">指标</th>
                    {periods.map(p => (
                      <th key={p} className="text-right py-2 font-medium text-muted-foreground">T+{p}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {/* Win Rate */}
                  <tr className="border-b border-muted/50">
                    <td className="py-2 font-medium">胜率</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.win_rate
                      return (
                        <td key={p} className={cn("text-right py-2", getValueColor(val, true))}>
                          {formatPercent(val)}
                        </td>
                      )
                    })}
                  </tr>
                  {/* Avg Return */}
                  <tr className="border-b border-muted/50">
                    <td className="py-2 font-medium">平均收益</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.avg_return
                      return (
                        <td key={p} className={cn("text-right py-2", getValueColor(val))}>
                          {formatPercent(val)}
                        </td>
                      )
                    })}
                  </tr>
                  {/* Profit/Loss Ratio */}
                  <tr className="border-b border-muted/50">
                    <td className="py-2 font-medium">盈亏比</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      const val = stats?.profit_loss_ratio
                      return (
                        <td key={p} className="text-right py-2 font-mono">
                          {formatNumber(val)}
                        </td>
                      )
                    })}
                  </tr>
                  {/* Up/Down Count */}
                  <tr>
                    <td className="py-2 font-medium">上涨/下跌</td>
                    {periods.map(p => {
                      const stats = getStatsForPeriod(p)
                      if (!stats) return <td key={p} className="text-right py-2 text-muted-foreground">—</td>
                      return (
                        <td key={p} className="text-right py-2 font-mono text-xs">
                          <span className="text-red-500">{stats.win_count}</span>
                          <span className="text-muted-foreground mx-1">/</span>
                          <span className="text-green-500">{stats.lose_count}</span>
                        </td>
                      )
                    })}
                  </tr>
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-4 text-muted-foreground">
              无法加载评估数据
            </div>
          )}
        </CardContent>
      </Card>

      {/* Charts List */}
      <div className="space-y-6">
        {codes.map(code => {
          const stockInfo = stockMap[code]
          const t5Return = stockInfo?.returns?.['5']
          
          return (
            <Card key={code} className="overflow-hidden">
              <CardHeader className="py-3 bg-muted/20 border-b flex flex-row items-center justify-between">
                <div className="flex items-center gap-3">
                  <Badge variant="outline" className="font-mono bg-background">
                    {code}
                  </Badge>
                  <span className="font-medium text-lg">
                    {stockInfo?.name || 'Loading...'}
                  </span>
                </div>
                {t5Return && (
                  <Badge variant="secondary" className={cn("font-mono", getValueColor(t5Return))}>
                    T+5: {formatPercent(t5Return)}
                  </Badge>
                )}
              </CardHeader>
              <CardContent className="p-0">
                <div className="h-[400px] w-full">
                  <StockChart 
                    code={code} 
                    endDate={date} 
                    height={400} 
                  />
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
