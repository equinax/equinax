import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { formatPercent, formatDate, formatCurrency } from '@/lib/utils'
import {
  TrendingUp,
  Code2,
  PlayCircle,
  Database,
  Loader2,
  Trophy,
  Crown,
} from 'lucide-react'
import { useGetDashboardStatsApiV1StatsGet } from '@/api/generated/statistics/statistics'
import { useNavigate } from 'react-router-dom'

// Helper component for metric display
function MetricItem({ label, value, negative = false }: { label: string; value: string | number | undefined | null; negative?: boolean }) {
  return (
    <div>
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={`font-medium ${negative ? 'text-loss' : ''}`}>{value ?? '-'}</p>
    </div>
  )
}

export default function DashboardPage() {
  const navigate = useNavigate()

  // Fetch dashboard stats
  const { data: stats, isLoading: isLoadingStats } = useGetDashboardStatsApiV1StatsGet()

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

      {/* Best Strategy & Best Backtest Cards */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Best Strategy Card */}
        <Card
          className="cursor-pointer hover:bg-accent/30 transition-colors"
          onClick={() => stats?.best_strategy && navigate(`/strategies`)}
        >
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">最佳策略</CardTitle>
            <Trophy className="h-5 w-5 text-amber-500" />
          </CardHeader>
          <CardContent>
            {isLoadingStats ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : stats?.best_strategy ? (
              <div className="space-y-4">
                <div>
                  <p className="text-lg font-semibold">{stats.best_strategy.strategy_name}</p>
                  <p className="text-xs text-muted-foreground">
                    {stats.best_strategy.strategy_type || '未分类'}
                  </p>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <MetricItem
                    label="平均收益"
                    value={stats.best_strategy.avg_return != null ? formatPercent(Number(stats.best_strategy.avg_return)) : undefined}
                  />
                  <MetricItem
                    label="平均 Sharpe"
                    value={stats.best_strategy.avg_sharpe != null ? Number(stats.best_strategy.avg_sharpe).toFixed(2) : undefined}
                  />
                  <MetricItem
                    label="回测次数"
                    value={stats.best_strategy.backtest_count}
                  />
                  <MetricItem
                    label="平均胜率"
                    value={stats.best_strategy.avg_win_rate != null ? formatPercent(Number(stats.best_strategy.avg_win_rate)) : undefined}
                  />
                </div>
              </div>
            ) : (
              <div className="text-center text-muted-foreground py-4">
                暂无策略数据
              </div>
            )}
          </CardContent>
        </Card>

        {/* Best Backtest Card */}
        <Card
          className="cursor-pointer hover:bg-accent/30 transition-colors"
          onClick={() => stats?.best_backtest && navigate(`/analysis`)}
        >
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">最佳回测</CardTitle>
            <Crown className="h-5 w-5 text-amber-500" />
          </CardHeader>
          <CardContent>
            {isLoadingStats ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : stats?.best_backtest ? (
              <div className="space-y-4">
                <div className="flex justify-between items-start">
                  <div>
                    <p className="text-lg font-semibold">{stats.best_backtest.stock_code}</p>
                    <p className="text-xs text-muted-foreground">{stats.best_backtest.strategy_name}</p>
                  </div>
                  <p className={`text-2xl font-bold ${Number(stats.best_backtest.total_return) >= 0 ? 'text-profit' : 'text-loss'}`}>
                    {formatPercent(Number(stats.best_backtest.total_return))}
                  </p>
                </div>
                <div className="grid grid-cols-3 gap-3">
                  <MetricItem
                    label="年化收益"
                    value={stats.best_backtest.annual_return != null ? formatPercent(Number(stats.best_backtest.annual_return)) : undefined}
                  />
                  <MetricItem
                    label="Sharpe"
                    value={stats.best_backtest.sharpe_ratio != null ? Number(stats.best_backtest.sharpe_ratio).toFixed(2) : undefined}
                  />
                  <MetricItem
                    label="最大回撤"
                    value={stats.best_backtest.max_drawdown != null ? formatPercent(Number(stats.best_backtest.max_drawdown)) : undefined}
                    negative
                  />
                  <MetricItem
                    label="交易次数"
                    value={stats.best_backtest.total_trades}
                  />
                  <MetricItem
                    label="胜率"
                    value={stats.best_backtest.win_rate != null ? formatPercent(Number(stats.best_backtest.win_rate)) : undefined}
                  />
                  <MetricItem
                    label="盈亏比"
                    value={stats.best_backtest.profit_factor != null ? Number(stats.best_backtest.profit_factor).toFixed(2) : undefined}
                  />
                </div>
              </div>
            ) : (
              <div className="text-center text-muted-foreground py-4">
                暂无回测数据
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
