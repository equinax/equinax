import { useState, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { formatPercent, cn } from '@/lib/utils'
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
  Terminal,
  Code2,
  ChevronUp,
  ChevronDown,
  ArrowUpDown,
} from 'lucide-react'
import {
  useGetBacktestApiV1BacktestsJobIdGet,
  useGetBacktestResultsApiV1BacktestsJobIdResultsGet,
} from '@/api/generated/backtests/backtests'
import { ResultDetailSheet } from '@/components/backtest/ResultDetailSheet'
import { ResultComparisonView } from '@/components/backtest/ResultComparisonView'
import { ReturnDistributionChart } from '@/components/analytics/ReturnDistributionChart'
import { useBacktestSSE } from '@/hooks/useBacktestSSE'

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
    { query: { enabled: !!jobId, refetchInterval: (query) => {
      // Use longer polling interval as backup (SSE handles real-time updates)
      const data = query.state.data
      if (data?.status === 'running' || data?.status === 'queued' || data?.status === 'pending') {
        return 10000  // 10 seconds as fallback
      }
      return false
    }}}
  )

  // Determine if job is running
  const isRunning = job?.status === 'running' || job?.status === 'queued' || job?.status === 'pending'

  // SSE for real-time updates when job is running
  const { logs, isConnected } = useBacktestSSE({
    jobId: jobId || '',
    enabled: isRunning,
  })

  // Fetch results
  const { data: results, isLoading: isLoadingResults } = useGetBacktestResultsApiV1BacktestsJobIdResultsGet(
    jobId || '',
    { query: { enabled: !!jobId && job?.status === 'completed' }}
  )

  // State for result detail sheet
  const [selectedResultId, setSelectedResultId] = useState<string | null>(null)
  const [isSheetOpen, setIsSheetOpen] = useState(false)

  // State for sorting
  const [sortConfig, setSortConfig] = useState<{
    key: string
    direction: 'asc' | 'desc'
  } | null>(null)

  // Sort handler
  const handleSort = (key: string) => {
    setSortConfig(prev => {
      if (prev?.key === key) {
        // Toggle direction or clear
        if (prev.direction === 'desc') {
          return { key, direction: 'asc' }
        }
        return null // Clear sort
      }
      // Default to desc for most metrics (higher is better)
      // Exception: max_drawdown where lower (less negative) is better
      const defaultDirection = key === 'max_drawdown' ? 'asc' : 'desc'
      return { key, direction: defaultDirection }
    })
  }

  // Sorted results
  const sortedResults = useMemo(() => {
    if (!results || !sortConfig) return results
    return [...results].sort((a, b) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const aVal = Number((a as any)[sortConfig.key]) || 0
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const bVal = Number((b as any)[sortConfig.key]) || 0
      return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal
    })
  }, [results, sortConfig])

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
      {isRunning && (
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

      {/* Execution Logs */}
      {isRunning && (
        <Card>
          <CardHeader className="py-3">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Terminal className="h-4 w-4" />
              执行日志
              {isConnected && (
                <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse" title="SSE 已连接" />
              )}
              {!isConnected && (
                <span className="w-2 h-2 bg-yellow-500 rounded-full" title="正在连接..." />
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <ScrollArea className="h-48">
              <div className="font-mono text-xs p-3 space-y-0.5 bg-muted/30">
                {logs.length === 0 ? (
                  <div className="text-muted-foreground py-4 text-center">
                    等待日志...
                  </div>
                ) : (
                  logs.map((log, i) => (
                    <div
                      key={i}
                      className={cn(
                        'py-0.5',
                        log.level === 'error' && 'text-red-500',
                        log.level === 'warning' && 'text-yellow-500',
                      )}
                    >
                      <span className="text-muted-foreground">
                        [{log.timestamp.split('T')[1]?.slice(0, 8) || ''}]
                      </span>{' '}
                      {log.message}
                    </div>
                  ))
                )}
              </div>
            </ScrollArea>
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

      {/* Strategy Info */}
      {job.strategy_snapshots && Object.keys(job.strategy_snapshots).length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Code2 className="h-5 w-5" />
              策略配置
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {Object.entries(job.strategy_snapshots).map(([id, strategy]) => (
                <div key={id} className="p-4 border rounded-lg space-y-2">
                  <div className="flex items-start justify-between">
                    <h4 className="font-medium">{strategy.name}</h4>
                    <span className="text-xs bg-secondary px-2 py-0.5 rounded">
                      v{strategy.version}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    {strategy.strategy_type || '自定义策略'}
                  </p>
                  {strategy.parameters && Object.keys(strategy.parameters).length > 0 && (
                    <div className="pt-2 border-t">
                      <p className="text-xs text-muted-foreground mb-1">参数</p>
                      <div className="flex flex-wrap gap-1">
                        {Object.entries(strategy.parameters).slice(0, 4).map(([key, value]) => (
                          <span key={key} className="text-xs bg-muted px-1.5 py-0.5 rounded">
                            {key}: {String(value)}
                          </span>
                        ))}
                        {Object.keys(strategy.parameters).length > 4 && (
                          <span className="text-xs text-muted-foreground">
                            +{Object.keys(strategy.parameters).length - 4} 更多
                          </span>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

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
                  <TabsTrigger value="distribution" className="gap-1">
                    <BarChart3 className="h-4 w-4" />
                    收益分布
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
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('total_return')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              总收益
                              {sortConfig?.key === 'total_return' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('annual_return')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              年化收益
                              {sortConfig?.key === 'annual_return' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('sharpe_ratio')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              Sharpe
                              {sortConfig?.key === 'sharpe_ratio' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('max_drawdown')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              最大回撤
                              {sortConfig?.key === 'max_drawdown' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('win_rate')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              胜率
                              {sortConfig?.key === 'win_rate' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('total_trades')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              交易次数
                              {sortConfig?.key === 'total_trades' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                          <th
                            className="pb-3 font-medium text-right cursor-pointer hover:text-foreground transition-colors select-none"
                            onClick={() => handleSort('final_value')}
                          >
                            <span className="inline-flex items-center justify-end gap-1">
                              最终价值
                              {sortConfig?.key === 'final_value' ? (
                                sortConfig.direction === 'asc' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ArrowUpDown className="h-3 w-3 opacity-50" />
                              )}
                            </span>
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {(sortedResults || results).map((result) => (
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

                <TabsContent value="distribution" className="space-y-4">
                  <div className="grid gap-4 lg:grid-cols-2">
                    <ReturnDistributionChart jobId={jobId || ''} metric="total_return" />
                    <ReturnDistributionChart jobId={jobId || ''} metric="sharpe_ratio" />
                  </div>
                  <div className="grid gap-4 lg:grid-cols-2">
                    <ReturnDistributionChart jobId={jobId || ''} metric="max_drawdown" />
                    <ReturnDistributionChart jobId={jobId || ''} metric="win_rate" />
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
