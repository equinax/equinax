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

      {/* Top Strategies & Top Backtests Cards */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Top Strategies Card */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">最佳策略 Top 3</CardTitle>
            <Trophy className="h-5 w-5 text-amber-500" />
          </CardHeader>
          <CardContent>
            {isLoadingStats ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : stats?.top_strategies && stats.top_strategies.length > 0 ? (
              <div className="space-y-4">
                {stats.top_strategies.map((strategy, index) => (
                  <div
                    key={strategy.strategy_id}
                    className="p-3 rounded-lg bg-muted/50 hover:bg-muted cursor-pointer transition-colors"
                    onClick={() => navigate(`/strategies`)}
                  >
                    <div className="flex items-start gap-3">
                      <div className={`flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                        index === 0 ? 'bg-amber-500 text-white' :
                        index === 1 ? 'bg-gray-400 text-white' :
                        'bg-amber-700 text-white'
                      }`}>
                        {index + 1}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="font-semibold truncate">{strategy.strategy_name}</p>
                            <p className="text-xs text-muted-foreground">
                              {strategy.strategy_type || '未分类'} · {strategy.backtest_count} 次回测
                            </p>
                          </div>
                          <p className={`text-lg font-bold ${Number(strategy.avg_return) >= 0 ? 'text-profit' : 'text-loss'}`}>
                            {formatPercent(Number(strategy.avg_return))}
                          </p>
                        </div>
                        <div className="grid grid-cols-2 gap-2 mt-2 text-xs">
                          <MetricItem
                            label="平均 Sharpe"
                            value={strategy.avg_sharpe != null ? Number(strategy.avg_sharpe).toFixed(2) : undefined}
                          />
                          <MetricItem
                            label="平均胜率"
                            value={strategy.avg_win_rate != null ? formatPercent(Number(strategy.avg_win_rate)) : undefined}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center text-muted-foreground py-4">
                暂无策略数据
              </div>
            )}
          </CardContent>
        </Card>

        {/* Top Backtests Card */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-base">最佳回测 Top 3</CardTitle>
            <Crown className="h-5 w-5 text-amber-500" />
          </CardHeader>
          <CardContent>
            {isLoadingStats ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
              </div>
            ) : stats?.top_backtests && stats.top_backtests.length > 0 ? (
              <div className="space-y-4">
                {stats.top_backtests.map((backtest, index) => (
                  <div
                    key={backtest.result_id}
                    className="p-3 rounded-lg bg-muted/50 hover:bg-muted cursor-pointer transition-colors"
                    onClick={() => navigate(`/analysis`)}
                  >
                    <div className="flex items-start gap-3">
                      <div className={`flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                        index === 0 ? 'bg-amber-500 text-white' :
                        index === 1 ? 'bg-gray-400 text-white' :
                        'bg-amber-700 text-white'
                      }`}>
                        {index + 1}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="font-semibold">{backtest.stock_code}</p>
                            <p className="text-xs text-muted-foreground">{backtest.strategy_name}</p>
                          </div>
                          <p className={`text-lg font-bold ${Number(backtest.total_return) >= 0 ? 'text-profit' : 'text-loss'}`}>
                            {formatPercent(Number(backtest.total_return))}
                          </p>
                        </div>
                        <div className="grid grid-cols-3 gap-2 mt-2 text-xs">
                          <MetricItem
                            label="Sharpe"
                            value={backtest.sharpe_ratio != null ? Number(backtest.sharpe_ratio).toFixed(2) : undefined}
                          />
                          <MetricItem
                            label="最大回撤"
                            value={backtest.max_drawdown != null ? formatPercent(Number(backtest.max_drawdown)) : undefined}
                            negative
                          />
                          <MetricItem
                            label="胜率"
                            value={backtest.win_rate != null ? formatPercent(Number(backtest.win_rate)) : undefined}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
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
