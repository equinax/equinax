import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { formatPercent } from '@/lib/utils'
import { ArrowUpRight, ArrowDownRight, Download, Eye } from 'lucide-react'

// Mock data
const results = [
  {
    id: '1',
    jobName: '双均线全市场测试',
    strategy: '双均线策略',
    stockCount: 50,
    dateRange: '2024-01-01 ~ 2024-12-31',
    avgReturn: 0.245,
    bestReturn: 0.82,
    worstReturn: -0.35,
    avgSharpe: 1.82,
    winRate: 0.62,
    totalTrades: 1250,
    createdAt: '2024-01-20 14:30',
    status: 'completed',
  },
  {
    id: '2',
    jobName: 'MACD科技板块测试',
    strategy: 'MACD动量策略',
    stockCount: 30,
    dateRange: '2024-01-01 ~ 2024-06-30',
    avgReturn: 0.156,
    bestReturn: 0.45,
    worstReturn: -0.18,
    avgSharpe: 1.45,
    winRate: 0.58,
    totalTrades: 680,
    createdAt: '2024-01-19 09:15',
    status: 'completed',
  },
  {
    id: '3',
    jobName: 'RSI金融板块测试',
    strategy: 'RSI反转策略',
    stockCount: 25,
    dateRange: '2024-01-01 ~ 2024-12-31',
    avgReturn: -0.023,
    bestReturn: 0.15,
    worstReturn: -0.42,
    avgSharpe: -0.32,
    winRate: 0.38,
    totalTrades: 520,
    createdAt: '2024-01-18 16:45',
    status: 'completed',
  },
]

export default function ResultsPage() {
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">结果分析</h1>
          <p className="text-muted-foreground">查看和分析回测结果</p>
        </div>
        <Button variant="outline">
          <Download className="mr-2 h-4 w-4" />
          导出报告
        </Button>
      </div>

      {/* Results list */}
      <div className="space-y-4">
        {results.map((result) => (
          <Card key={result.id}>
            <CardContent className="p-6">
              <div className="flex items-start justify-between">
                {/* Left: Basic info */}
                <div className="space-y-1">
                  <h3 className="text-lg font-semibold">{result.jobName}</h3>
                  <p className="text-sm text-muted-foreground">
                    {result.strategy} · {result.stockCount}只股票 · {result.dateRange}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {result.createdAt}
                  </p>
                </div>

                {/* Right: Metrics */}
                <div className="flex items-center gap-8">
                  {/* Average Return */}
                  <div className="text-center">
                    <p className="text-sm text-muted-foreground">平均收益</p>
                    <div className="flex items-center justify-center gap-1">
                      {result.avgReturn >= 0 ? (
                        <ArrowUpRight className="h-4 w-4 text-profit" />
                      ) : (
                        <ArrowDownRight className="h-4 w-4 text-loss" />
                      )}
                      <span
                        className={`text-xl font-bold ${
                          result.avgReturn >= 0 ? 'text-profit' : 'text-loss'
                        }`}
                      >
                        {formatPercent(result.avgReturn)}
                      </span>
                    </div>
                  </div>

                  {/* Sharpe Ratio */}
                  <div className="text-center">
                    <p className="text-sm text-muted-foreground">夏普比率</p>
                    <span
                      className={`text-xl font-bold ${
                        result.avgSharpe >= 0 ? 'text-profit' : 'text-loss'
                      }`}
                    >
                      {result.avgSharpe.toFixed(2)}
                    </span>
                  </div>

                  {/* Win Rate */}
                  <div className="text-center">
                    <p className="text-sm text-muted-foreground">胜率</p>
                    <span className="text-xl font-bold">
                      {(result.winRate * 100).toFixed(0)}%
                    </span>
                  </div>

                  {/* Total Trades */}
                  <div className="text-center">
                    <p className="text-sm text-muted-foreground">交易次数</p>
                    <span className="text-xl font-bold">
                      {result.totalTrades}
                    </span>
                  </div>

                  {/* Actions */}
                  <Button>
                    <Eye className="mr-2 h-4 w-4" />
                    查看详情
                  </Button>
                </div>
              </div>

              {/* Range bar */}
              <div className="mt-4">
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span>最差: {formatPercent(result.worstReturn)}</span>
                  <span>最佳: {formatPercent(result.bestReturn)}</span>
                </div>
                <div className="mt-1 h-2 rounded-full bg-gradient-to-r from-loss via-muted to-profit" />
                <div
                  className="relative -mt-3"
                  style={{
                    left: `${((result.avgReturn - result.worstReturn) / (result.bestReturn - result.worstReturn)) * 100}%`,
                  }}
                >
                  <div className="h-4 w-0.5 bg-primary" />
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
