import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatCurrency, formatPercent, formatDate } from '@/lib/utils'
import {
  TrendingUp,
  TrendingDown,
  Code2,
  PlayCircle,
  BarChart3,
  Database,
  Loader2,
} from 'lucide-react'
import { useGetDashboardStatsApiV1StatsGet } from '@/api/generated/statistics/statistics'
import { useListBacktestsApiV1BacktestsGet } from '@/api/generated/backtests/backtests'
import { useNavigate } from 'react-router-dom'

export default function DashboardPage() {
  const navigate = useNavigate()

  // Fetch dashboard stats
  const { data: stats, isLoading: isLoadingStats } = useGetDashboardStatsApiV1StatsGet()

  // Fetch recent backtests
  const { data: backtestsData, isLoading: isLoadingBacktests } = useListBacktestsApiV1BacktestsGet({
    page: 1,
    page_size: 5,
  })

  const recentBacktests = backtestsData?.items || []

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-3xl font-bold">仪表盘</h1>
        <p className="text-muted-foreground">量化回测系统概览</p>
      </div>

      {/* Stats cards */}
      {isLoadingStats ? (
        <div className="flex items-center justify-center p-8">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                策略总数
              </CardTitle>
              <Code2 className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats?.total_strategies || 0}</div>
              <p className="text-xs text-muted-foreground">
                {stats?.active_strategies || 0} 个启用中
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                回测任务
              </CardTitle>
              <PlayCircle className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats?.total_backtests || 0}</div>
              <p className="text-xs text-muted-foreground">
                {stats?.completed_backtests || 0} 已完成，{stats?.running_backtests || 0} 运行中
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                最佳收益
              </CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className={`text-2xl font-bold ${(Number(stats?.best_return) || 0) >= 0 ? 'text-profit' : 'text-loss'}`}>
                {stats?.best_return != null
                  ? formatPercent(Number(stats.best_return))
                  : '-'}
              </div>
              <p className="text-xs text-muted-foreground">
                最佳 Sharpe: {stats?.best_sharpe != null ? Number(stats.best_sharpe).toFixed(2) : '-'}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                股票数据
              </CardTitle>
              <Database className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats?.total_stocks || 0}</div>
              <p className="text-xs text-muted-foreground">
                平均 Sharpe: {stats?.avg_sharpe != null ? Number(stats.avg_sharpe).toFixed(2) : '-'}
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Recent backtests */}
      <Card>
        <CardHeader>
          <CardTitle>最近回测</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoadingBacktests ? (
            <div className="flex items-center justify-center p-8">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : recentBacktests.length === 0 ? (
            <div className="text-center text-muted-foreground py-8">
              暂无回测记录
            </div>
          ) : (
            <div className="space-y-4">
              {recentBacktests.map((backtest) => (
                <div
                  key={backtest.id}
                  className="flex items-center justify-between rounded-lg border p-4 cursor-pointer hover:bg-accent/50 transition-colors"
                  onClick={() => navigate(`/results/${backtest.id}`)}
                >
                  <div className="space-y-1">
                    <p className="font-medium">
                      {backtest.name || `回测任务 ${backtest.id.slice(0, 8)}`}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      {backtest.strategy_ids?.length || 0} 个策略 · {backtest.stock_codes?.length || 0} 只股票 · {formatDate(backtest.created_at)}
                    </p>
                  </div>
                  <div className="flex items-center gap-6">
                    <div className="text-right">
                      <p className="text-sm text-muted-foreground">状态</p>
                      <p className={`font-medium ${
                        backtest.status === 'COMPLETED' ? 'text-profit' :
                        backtest.status === 'RUNNING' ? 'text-primary' :
                        backtest.status === 'FAILED' ? 'text-loss' :
                        'text-muted-foreground'
                      }`}>
                        {backtest.status === 'COMPLETED' ? '已完成' :
                         backtest.status === 'RUNNING' ? '运行中' :
                         backtest.status === 'FAILED' ? '失败' :
                         backtest.status === 'PENDING' ? '等待中' :
                         backtest.status}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm text-muted-foreground">进度</p>
                      <p className="font-medium">
                        {backtest.successful_backtests}/{backtest.total_backtests}
                      </p>
                    </div>
                    <div className="flex items-center gap-1">
                      {backtest.status === 'COMPLETED' && backtest.successful_backtests > 0 ? (
                        <TrendingUp className="h-4 w-4 text-profit" />
                      ) : backtest.status === 'FAILED' ? (
                        <TrendingDown className="h-4 w-4 text-loss" />
                      ) : (
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
