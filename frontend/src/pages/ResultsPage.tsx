import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { formatPercent, formatDate } from '@/lib/utils'
import { Download, Eye, Loader2, FileX2, Clock, CheckCircle, XCircle, RefreshCw, TrendingUp, TrendingDown } from 'lucide-react'
import { useListBacktestsApiV1BacktestsGet } from '@/api/generated/backtests/backtests'
import { useNavigate } from 'react-router-dom'

const statusConfig: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  QUEUED: { label: '排队中', color: 'text-muted-foreground', icon: <Clock className="h-4 w-4" /> },
  queued: { label: '排队中', color: 'text-muted-foreground', icon: <Clock className="h-4 w-4" /> },
  PENDING: { label: '等待中', color: 'text-muted-foreground', icon: <Clock className="h-4 w-4" /> },
  pending: { label: '等待中', color: 'text-muted-foreground', icon: <Clock className="h-4 w-4" /> },
  RUNNING: { label: '运行中', color: 'text-primary', icon: <Loader2 className="h-4 w-4 animate-spin" /> },
  running: { label: '运行中', color: 'text-primary', icon: <Loader2 className="h-4 w-4 animate-spin" /> },
  COMPLETED: { label: '已完成', color: 'text-profit', icon: <CheckCircle className="h-4 w-4" /> },
  completed: { label: '已完成', color: 'text-profit', icon: <CheckCircle className="h-4 w-4" /> },
  FAILED: { label: '失败', color: 'text-loss', icon: <XCircle className="h-4 w-4" /> },
  failed: { label: '失败', color: 'text-loss', icon: <XCircle className="h-4 w-4" /> },
  CANCELLED: { label: '已取消', color: 'text-muted-foreground', icon: <XCircle className="h-4 w-4" /> },
  cancelled: { label: '已取消', color: 'text-muted-foreground', icon: <XCircle className="h-4 w-4" /> },
}

// Helper to check if status is completed (case-insensitive)
const isCompleted = (status: string) => status.toLowerCase() === 'completed'
const isRunning = (status: string) => ['running', 'queued', 'pending'].includes(status.toLowerCase())

export default function ResultsPage() {
  const navigate = useNavigate()

  // Fetch all backtests (not just completed)
  const { data: backtestsData, isLoading, refetch } = useListBacktestsApiV1BacktestsGet({
    page: 1,
    page_size: 50,
  }, {
    query: {
      // Auto-refresh every 5 seconds if there are running jobs
      refetchInterval: (query) => {
        const items = query.state.data?.items
        const hasRunning = items?.some(
          (item: { status: string }) => item.status === 'RUNNING' || item.status === 'QUEUED' || item.status === 'PENDING'
        )
        return hasRunning ? 5000 : false
      }
    }
  })

  const backtests = backtestsData?.items || []

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">结果分析</h1>
          <p className="text-muted-foreground">查看和分析回测结果</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => refetch()}>
            <RefreshCw className="mr-2 h-4 w-4" />
            刷新
          </Button>
          <Button variant="outline">
            <Download className="mr-2 h-4 w-4" />
            导出报告
          </Button>
        </div>
      </div>

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center p-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Results list */}
      {!isLoading && backtests.length > 0 && (
        <div className="space-y-4">
          {backtests.map((backtest) => {
            const summary = backtest.summary
            const avgReturn = summary?.avg_return != null ? Number(summary.avg_return) : null
            const isProfit = avgReturn != null && avgReturn >= 0
            const completed = isCompleted(backtest.status)
            const running = isRunning(backtest.status)

            return (
              <Card key={backtest.id} className="overflow-hidden">
                <CardContent className="p-0">
                  <div className="flex">
                    {/* Left section: Basic info */}
                    <div className="flex-1 p-6">
                      <div className="flex items-start justify-between">
                        <div className="space-y-1">
                          <p className="text-sm text-muted-foreground">
                            {backtest.strategy_ids?.length || 0} 个策略 · {backtest.stock_codes?.length || 0} 只股票 · {backtest.start_date} ~ {backtest.end_date}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            {formatDate(backtest.created_at)}
                          </p>
                        </div>

                        {/* Status badge - only show for non-completed */}
                        {!completed && (
                          <div className={`flex items-center gap-1 px-2 py-1 rounded-full text-sm ${statusConfig[backtest.status]?.color || 'text-muted-foreground'} bg-muted`}>
                            {statusConfig[backtest.status]?.icon}
                            <span>{statusConfig[backtest.status]?.label || backtest.status}</span>
                          </div>
                        )}
                      </div>

                      {/* Progress bar for running jobs */}
                      {running && (
                        <div className="mt-4">
                          <div className="flex justify-between text-xs text-muted-foreground mb-1">
                            <span>进度</span>
                            <span>{Number(backtest.progress).toFixed(0)}%</span>
                          </div>
                          <div className="h-2 rounded-full bg-muted">
                            <div
                              className="h-2 rounded-full bg-primary transition-all"
                              style={{ width: `${backtest.progress}%` }}
                            />
                          </div>
                          <p className="mt-1 text-xs text-muted-foreground">
                            已完成 {backtest.successful_backtests + backtest.failed_backtests}/{backtest.total_backtests}
                          </p>
                        </div>
                      )}

                      {/* Summary metrics for completed - compact row */}
                      {completed && summary && (
                        <div className="mt-3 flex items-center gap-6 text-sm">
                          <div className="flex items-center gap-1">
                            <span className="text-muted-foreground">盈利:</span>
                            <span className="font-medium text-profit">{summary.profitable_count}</span>
                            <span className="text-muted-foreground">/</span>
                            <span className="font-medium">{backtest.successful_backtests}</span>
                          </div>
                          <div className="flex items-center gap-1">
                            <span className="text-muted-foreground">平均夏普:</span>
                            <span className="font-medium">
                              {summary.avg_sharpe != null ? Number(summary.avg_sharpe).toFixed(2) : '-'}
                            </span>
                          </div>
                          <div className="flex items-center gap-1">
                            <span className="text-muted-foreground">平均回撤:</span>
                            <span className="font-medium text-loss">
                              {summary.avg_max_drawdown != null ? formatPercent(Number(summary.avg_max_drawdown)) : '-'}
                            </span>
                          </div>
                          <div className="flex items-center gap-1">
                            <span className="text-muted-foreground">平均胜率:</span>
                            <span className="font-medium">
                              {summary.avg_win_rate != null ? formatPercent(Number(summary.avg_win_rate)) : '-'}
                            </span>
                          </div>
                        </div>
                      )}

                      {/* Completed without summary data */}
                      {completed && !summary && (
                        <div className="mt-3 text-sm text-muted-foreground">
                          共 {backtest.successful_backtests} 个成功回测
                        </div>
                      )}
                    </div>

                    {/* Right section: Key metric highlight for completed */}
                    {completed && summary && (
                      <div className={`w-48 flex flex-col items-center justify-center p-4 ${isProfit ? 'bg-profit/10' : 'bg-loss/10'}`}>
                        <div className="flex items-center gap-1 mb-1">
                          {isProfit ? (
                            <TrendingUp className="h-5 w-5 text-profit" />
                          ) : (
                            <TrendingDown className="h-5 w-5 text-loss" />
                          )}
                          <span className="text-sm text-muted-foreground">平均收益</span>
                        </div>
                        <span className={`text-2xl font-bold ${isProfit ? 'text-profit' : 'text-loss'}`}>
                          {avgReturn != null ? formatPercent(avgReturn) : '-'}
                        </span>
                        <div className="mt-2 text-xs text-muted-foreground space-y-0.5 text-center">
                          <div>
                            最佳 <span className="text-profit font-medium">{summary.best_return != null ? formatPercent(Number(summary.best_return)) : '-'}</span>
                          </div>
                          <div>
                            最差 <span className="text-loss font-medium">{summary.worst_return != null ? formatPercent(Number(summary.worst_return)) : '-'}</span>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Right section: View button */}
                    <div className="flex items-center px-4 border-l">
                      <Button onClick={() => navigate(`/results/${backtest.id}`)}>
                        <Eye className="mr-2 h-4 w-4" />
                        详情
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}

      {/* Empty state */}
      {!isLoading && backtests.length === 0 && (
        <Card className="p-12 text-center">
          <FileX2 className="mx-auto h-12 w-12 text-muted-foreground" />
          <h3 className="mt-4 text-lg font-semibold">暂无回测结果</h3>
          <p className="mt-2 text-muted-foreground">
            前往回测页面创建您的第一个回测任务
          </p>
          <Button className="mt-4" onClick={() => navigate('/backtest')}>
            开始回测
          </Button>
        </Card>
      )}

      {/* Pagination info */}
      {backtestsData && backtestsData.total > 0 && (
        <div className="text-sm text-muted-foreground text-center">
          共 {backtestsData.total} 个回测任务
        </div>
      )}
    </div>
  )
}
