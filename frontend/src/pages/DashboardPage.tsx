import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatCurrency, formatPercent } from '@/lib/utils'
import {
  TrendingUp,
  TrendingDown,
  Code2,
  PlayCircle,
  BarChart3,
} from 'lucide-react'

// Mock data - will be replaced with API calls
const stats = [
  {
    name: '策略总数',
    value: 12,
    icon: Code2,
    change: '+2',
    changeType: 'positive' as const,
  },
  {
    name: '今日回测',
    value: 48,
    icon: PlayCircle,
    change: '+12',
    changeType: 'positive' as const,
  },
  {
    name: '最佳收益',
    value: 0.3245,
    icon: TrendingUp,
    isPercent: true,
    change: '+5.2%',
    changeType: 'positive' as const,
  },
  {
    name: '平均夏普',
    value: 1.85,
    icon: BarChart3,
    change: '-0.12',
    changeType: 'negative' as const,
  },
]

const recentBacktests = [
  {
    id: '1',
    strategy: '双均线策略',
    stocks: 50,
    avgReturn: 0.125,
    sharpe: 1.42,
    status: 'completed',
    time: '10分钟前',
  },
  {
    id: '2',
    strategy: 'MACD突破',
    stocks: 30,
    avgReturn: 0.089,
    sharpe: 1.15,
    status: 'completed',
    time: '25分钟前',
  },
  {
    id: '3',
    strategy: 'RSI反转',
    stocks: 100,
    avgReturn: -0.023,
    sharpe: -0.32,
    status: 'completed',
    time: '1小时前',
  },
]

export default function DashboardPage() {
  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-3xl font-bold">仪表盘</h1>
        <p className="text-muted-foreground">量化回测系统概览</p>
      </div>

      {/* Stats cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {stats.map((stat) => (
          <Card key={stat.name}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {stat.name}
              </CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {stat.isPercent
                  ? formatPercent(stat.value)
                  : stat.value.toLocaleString()}
              </div>
              <p
                className={`text-xs ${
                  stat.changeType === 'positive'
                    ? 'text-profit'
                    : 'text-loss'
                }`}
              >
                {stat.change} 较昨日
              </p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Recent backtests */}
      <Card>
        <CardHeader>
          <CardTitle>最近回测</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {recentBacktests.map((backtest) => (
              <div
                key={backtest.id}
                className="flex items-center justify-between rounded-lg border p-4"
              >
                <div className="space-y-1">
                  <p className="font-medium">{backtest.strategy}</p>
                  <p className="text-sm text-muted-foreground">
                    {backtest.stocks} 只股票 · {backtest.time}
                  </p>
                </div>
                <div className="flex items-center gap-6">
                  <div className="text-right">
                    <p className="text-sm text-muted-foreground">平均收益</p>
                    <p
                      className={`font-medium ${
                        backtest.avgReturn >= 0 ? 'text-profit' : 'text-loss'
                      }`}
                    >
                      {formatPercent(backtest.avgReturn)}
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-muted-foreground">夏普比率</p>
                    <p
                      className={`font-medium ${
                        backtest.sharpe >= 0 ? 'text-profit' : 'text-loss'
                      }`}
                    >
                      {backtest.sharpe.toFixed(2)}
                    </p>
                  </div>
                  <div className="flex items-center gap-1">
                    {backtest.avgReturn >= 0 ? (
                      <TrendingUp className="h-4 w-4 text-profit" />
                    ) : (
                      <TrendingDown className="h-4 w-4 text-loss" />
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
