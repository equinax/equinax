import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { formatPercent, formatDate } from '@/lib/utils'
import { ArrowUpRight, ArrowDownRight, Download, Eye, Loader2, FileX2 } from 'lucide-react'
import { useListBacktestsApiV1BacktestsGet } from '@/api/generated/backtests/backtests'
import { useNavigate } from 'react-router-dom'

export default function ResultsPage() {
  const navigate = useNavigate()

  // Fetch completed backtests
  const { data: backtestsData, isLoading } = useListBacktestsApiV1BacktestsGet({
    page: 1,
    page_size: 50,
    status: 'COMPLETED',
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
        <Button variant="outline">
          <Download className="mr-2 h-4 w-4" />
          导出报告
        </Button>
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
          {backtests.map((backtest) => (
            <Card key={backtest.id}>
              <CardContent className="p-6">
                <div className="flex items-start justify-between">
                  {/* Left: Basic info */}
                  <div className="space-y-1">
                    <h3 className="text-lg font-semibold">
                      {backtest.name || `回测任务 ${backtest.id.slice(0, 8)}`}
                    </h3>
                    <p className="text-sm text-muted-foreground">
                      {backtest.strategy_ids?.length || 0} 个策略 · {backtest.stock_codes?.length || 0} 只股票 · {backtest.start_date} ~ {backtest.end_date}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      {formatDate(backtest.created_at)}
                    </p>
                  </div>

                  {/* Right: Metrics */}
                  <div className="flex items-center gap-8">
                    {/* Success rate */}
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">成功率</p>
                      <span className="text-xl font-bold">
                        {backtest.total_backtests > 0
                          ? ((backtest.successful_backtests / backtest.total_backtests) * 100).toFixed(0)
                          : 0}%
                      </span>
                    </div>

                    {/* Total backtests */}
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">回测数量</p>
                      <span className="text-xl font-bold">
                        {backtest.total_backtests}
                      </span>
                    </div>

                    {/* Status */}
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">状态</p>
                      <span className={`text-lg font-medium ${
                        backtest.status === 'COMPLETED' ? 'text-profit' : 'text-muted-foreground'
                      }`}>
                        {backtest.status === 'COMPLETED' ? '已完成' : backtest.status}
                      </span>
                    </div>

                    {/* Actions */}
                    <Button onClick={() => navigate(`/results/${backtest.id}`)}>
                      <Eye className="mr-2 h-4 w-4" />
                      查看详情
                    </Button>
                  </div>
                </div>

                {/* Progress bar for non-completed */}
                {backtest.status !== 'COMPLETED' && (
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
                  </div>
                )}

                {/* Summary metrics for completed */}
                {backtest.status === 'COMPLETED' && (
                  <div className="mt-4 grid grid-cols-4 gap-4 rounded-lg bg-muted/50 p-4">
                    <div>
                      <p className="text-xs text-muted-foreground">成功数</p>
                      <p className="font-medium text-profit">{backtest.successful_backtests}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">失败数</p>
                      <p className="font-medium text-loss">{backtest.failed_backtests}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">初始资金</p>
                      <p className="font-medium">¥{Number(backtest.initial_capital).toLocaleString()}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">手续费率</p>
                      <p className="font-medium">{(Number(backtest.commission_rate) * 100).toFixed(2)}%</p>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          ))}
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
