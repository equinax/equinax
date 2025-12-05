import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { formatPercent } from '@/lib/utils'
import {
  ArrowLeft,
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  BarChart3,
  ExternalLink,
  List,
  GitCompare,
} from 'lucide-react'
import {
  useGetBacktestApiV1BacktestsJobIdGet,
  useGetBacktestResultsApiV1BacktestsJobIdResultsGet,
} from '@/api/generated/backtests/backtests'
import { ResultDetailSheet } from '@/components/backtest/ResultDetailSheet'
import { ResultComparisonView } from '@/components/backtest/ResultComparisonView'

const statusConfig: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  queued: { label: '排队中', color: 'text-muted-foreground', icon: <Clock className="h-5 w-5" /> },
  pending: { label: '等待中', color: 'text-muted-foreground', icon: <Clock className="h-5 w-5" /> },
  running: { label: '运行中', color: 'text-primary', icon: <Loader2 className="h-5 w-5 animate-spin" /> },
  completed: { label: '已完成', color: 'text-profit', icon: <CheckCircle className="h-5 w-5" /> },
  failed: { label: '失败', color: 'text-loss', icon: <XCircle className="h-5 w-5" /> },
  cancelled: { label: '已取消', color: 'text-muted-foreground', icon: <XCircle className="h-5 w-5" /> },
}

export default function ResultDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()

  // Fetch backtest job
  const { data: job, isLoading: isLoadingJob } = useGetBacktestApiV1BacktestsJobIdGet(
    jobId || '',
    { query: { enabled: !!jobId, refetchInterval: (data) => {
      // Auto-refresh if job is still running
      if (data?.status === 'running' || data?.status === 'queued' || data?.status === 'pending') {
        return 3000
      }
      return false
    }}}
  )

  // Fetch results
  const { data: results, isLoading: isLoadingResults } = useGetBacktestResultsApiV1BacktestsJobIdResultsGet(
    jobId || '',
    { query: { enabled: !!jobId && job?.status === 'completed' }}
  )

  // State for result detail sheet
  const [selectedResultId, setSelectedResultId] = useState<string | null>(null)
  const [isSheetOpen, setIsSheetOpen] = useState(false)

  const handleResultClick = (resultId: string) => {
    setSelectedResultId(resultId)
    setIsSheetOpen(true)
  }

  const status = statusConfig[job?.status || 'pending'] || statusConfig.pending

  if (isLoadingJob) {
    return (
      <div className="flex items-center justify-center p-12">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!job) {
    return (
      <div className="flex flex-col items-center justify-center p-12 space-y-4">
        <p className="text-muted-foreground">回测任务不存在</p>
        <Button onClick={() => navigate('/results')}>返回列表</Button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate('/results')}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-3xl font-bold">
              {job.name || `回测任务 ${job.id.slice(0, 8)}`}
            </h1>
            <p className="text-muted-foreground">
              {job.strategy_ids?.length || 0} 个策略 · {job.stock_codes?.length || 0} 只股票 · {job.start_date} ~ {job.end_date}
            </p>
          </div>
        </div>
        <div className={`flex items-center gap-2 ${status.color}`}>
          {status.icon}
          <span className="font-medium">{status.label}</span>
        </div>
      </div>

      {/* Progress */}
      {(job.status === 'running' || job.status === 'queued' || job.status === 'pending') && (
        <Card>
          <CardContent className="pt-6">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>进度</span>
                <span>{Number(job.progress).toFixed(0)}%</span>
              </div>
              <div className="h-3 rounded-full bg-muted overflow-hidden">
                <div
                  className="h-full bg-primary transition-all duration-500"
                  style={{ width: `${job.progress}%` }}
                />
              </div>
              <p className="text-sm text-muted-foreground">
                已完成 {job.successful_backtests} / {job.total_backtests} 个回测
                {job.failed_backtests > 0 && (
                  <span className="text-loss"> ({job.failed_backtests} 个失败)</span>
                )}
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Job Info */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">初始资金</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">¥{Number(job.initial_capital).toLocaleString()}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">手续费率</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{(Number(job.commission_rate) * 100).toFixed(2)}%</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">滑点</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{(Number(job.slippage) * 100).toFixed(2)}%</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">回测数量</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">
              <span className="text-profit">{job.successful_backtests}</span>
              <span className="text-muted-foreground"> / {job.total_backtests}</span>
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Results */}
      {job.status === 'completed' && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <BarChart3 className="h-5 w-5" />
              回测结果
            </CardTitle>
          </CardHeader>
          <CardContent>
            {isLoadingResults ? (
              <div className="flex items-center justify-center p-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : results && results.length > 0 ? (
              <Tabs defaultValue="list" className="space-y-4">
                <TabsList>
                  <TabsTrigger value="list" className="gap-1">
                    <List className="h-4 w-4" />
                    股票列表
                  </TabsTrigger>
                  <TabsTrigger value="comparison" className="gap-1">
                    <GitCompare className="h-4 w-4" />
                    对比分析
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="list" className="space-y-4">
                  {/* Summary stats */}
                  <div className="grid gap-4 md:grid-cols-4 p-4 bg-muted/50 rounded-lg">
                    <div>
                      <p className="text-sm text-muted-foreground">最佳收益</p>
                      <p className={`text-xl font-bold ${
                        Math.max(...results.map(r => Number(r.total_return) || 0)) >= 0 ? 'text-profit' : 'text-loss'
                      }`}>
                        {formatPercent(Math.max(...results.map(r => Number(r.total_return) || 0)))}
                      </p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">平均收益</p>
                      <p className={`text-xl font-bold ${
                        results.reduce((sum, r) => sum + (Number(r.total_return) || 0), 0) / results.length >= 0 ? 'text-profit' : 'text-loss'
                      }`}>
                        {formatPercent(results.reduce((sum, r) => sum + (Number(r.total_return) || 0), 0) / results.length)}
                      </p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">最佳 Sharpe</p>
                      <p className="text-xl font-bold">
                        {Math.max(...results.map(r => Number(r.sharpe_ratio) || 0)).toFixed(2)}
                      </p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">平均胜率</p>
                      <p className="text-xl font-bold">
                        {formatPercent(results.reduce((sum, r) => sum + (Number(r.win_rate) || 0), 0) / results.length)}
                      </p>
                    </div>
                  </div>

                  {/* Results table */}
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b text-left text-sm text-muted-foreground">
                          <th className="pb-3 font-medium">股票</th>
                          <th className="pb-3 font-medium text-right">总收益</th>
                          <th className="pb-3 font-medium text-right">年化收益</th>
                          <th className="pb-3 font-medium text-right">Sharpe</th>
                          <th className="pb-3 font-medium text-right">最大回撤</th>
                          <th className="pb-3 font-medium text-right">胜率</th>
                          <th className="pb-3 font-medium text-right">交易次数</th>
                          <th className="pb-3 font-medium text-right">最终价值</th>
                        </tr>
                      </thead>
                      <tbody>
                        {results.map((result) => (
                          <tr
                            key={result.id}
                            className="border-b hover:bg-muted/50 cursor-pointer transition-colors"
                            onClick={() => handleResultClick(result.id)}
                          >
                            <td className="py-3 font-medium">
                              <span className="flex items-center gap-1">
                                {result.stock_code}
                                <ExternalLink className="h-3 w-3 text-muted-foreground" />
                              </span>
                            </td>
                            <td className={`py-3 text-right ${(Number(result.total_return) || 0) >= 0 ? 'text-profit' : 'text-loss'}`}>
                              {result.total_return != null ? formatPercent(Number(result.total_return)) : '-'}
                            </td>
                            <td className={`py-3 text-right ${(Number(result.annual_return) || 0) >= 0 ? 'text-profit' : 'text-loss'}`}>
                              {result.annual_return != null ? formatPercent(Number(result.annual_return)) : '-'}
                            </td>
                            <td className="py-3 text-right">
                              {result.sharpe_ratio != null ? Number(result.sharpe_ratio).toFixed(2) : '-'}
                            </td>
                            <td className="py-3 text-right text-loss">
                              {result.max_drawdown != null ? formatPercent(Number(result.max_drawdown)) : '-'}
                            </td>
                            <td className="py-3 text-right">
                              {result.win_rate != null ? formatPercent(Number(result.win_rate)) : '-'}
                            </td>
                            <td className="py-3 text-right">
                              {result.total_trades ?? '-'}
                            </td>
                            <td className="py-3 text-right">
                              {result.final_value != null ? `¥${Number(result.final_value).toLocaleString()}` : '-'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </TabsContent>

                <TabsContent value="comparison">
                  <ResultComparisonView
                    jobId={jobId || ''}
                    results={results.map(r => ({ id: r.id, stock_code: r.stock_code }))}
                  />
                </TabsContent>
              </Tabs>
            ) : (
              <div className="text-center text-muted-foreground py-8">
                暂无结果数据
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Error message */}
      {job.status === 'failed' && job.error_message && (
        <Card className="border-loss/50">
          <CardHeader>
            <CardTitle className="text-loss flex items-center gap-2">
              <XCircle className="h-5 w-5" />
              错误信息
            </CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="bg-muted p-4 rounded-lg text-sm overflow-x-auto">
              {job.error_message}
            </pre>
          </CardContent>
        </Card>
      )}

      {/* Stocks list */}
      <Card>
        <CardHeader>
          <CardTitle>股票池</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {job.stock_codes?.map((code) => (
              <span
                key={code}
                className="rounded-full bg-secondary px-3 py-1 text-sm"
              >
                {code}
              </span>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Result Detail Sheet */}
      <ResultDetailSheet
        jobId={jobId || ''}
        resultId={selectedResultId}
        open={isSheetOpen}
        onOpenChange={setIsSheetOpen}
      />
    </div>
  )
}
