import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Code2, ChevronRight, ChevronLeft, PanelRightClose, PanelRight } from 'lucide-react'
import Editor from '@monaco-editor/react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import { useTheme } from '@/components/theme-provider'
import { formatPercent } from '@/lib/utils'
import { cn } from '@/lib/utils'
import {
  useGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet,
  useGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet,
  useGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet,
} from '@/api/generated/backtests/backtests'
import { useGetStrategyApiV1StrategiesStrategyIdGet } from '@/api/generated/strategies/strategies'
import { useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { EquityCurveWithIndicators } from '@/components/backtest/EquityCurveWithIndicators'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

export default function TechnicalAnalysisPage() {
  const { jobId, resultId } = useParams<{ jobId: string; resultId: string }>()
  const navigate = useNavigate()
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const [rightPanelOpen, setRightPanelOpen] = useState(true)
  const [dataTableOpen, setDataTableOpen] = useState(false)

  // Fetch backtest result detail
  const { data: result, isLoading: resultLoading } = useGetBacktestResultDetailApiV1BacktestsJobIdResultsResultIdGet(
    jobId || '',
    resultId || '',
    {
      query: {
        enabled: !!jobId && !!resultId,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch strategy details (including code)
  const { data: strategy, isLoading: strategyLoading } = useGetStrategyApiV1StrategiesStrategyIdGet(
    result?.strategy_id || '',
    {
      query: {
        enabled: !!result?.strategy_id,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch equity curve data
  const { data: equityData } = useGetBacktestEquityCurveApiV1BacktestsJobIdResultsResultIdEquityGet(
    jobId || '',
    resultId || '',
    undefined,
    {
      query: {
        enabled: !!jobId && !!resultId,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch K-line data
  const { data: klineData, isLoading: klineLoading } = useGetKlineApiV1StocksCodeKlineGet(
    result?.stock_code || '',
    undefined,
    {
      query: {
        enabled: !!result?.stock_code,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch trades
  const { data: tradesData } = useGetBacktestTradesApiV1BacktestsJobIdResultsResultIdTradesGet(
    jobId || '',
    resultId || '',
    { page_size: 200 },
    {
      query: {
        enabled: !!jobId && !!resultId,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  const isLoading = resultLoading || strategyLoading

  // Transform trades for display
  const trades: TradeRecord[] | undefined = tradesData?.items?.map(t => ({
    id: t.id,
    stock_code: t.stock_code,
    entry_date: t.entry_date,
    exit_date: t.exit_date ?? undefined,
    entry_price: Number(t.entry_price),
    exit_price: t.exit_price ? Number(t.exit_price) : undefined,
    size: t.size,
    pnl: t.pnl ? Number(t.pnl) : 0,
    pnl_percent: t.pnl_percent ? Number(t.pnl_percent) : 0,
    type: t.direction as 'long' | 'short',
    bars_held: t.bars_held ?? undefined,
  }))

  // Transform equity curve for display
  const equityCurve: EquityCurvePoint[] | undefined = equityData?.map(p => ({
    date: p.date,
    value: Number(p.value),
    drawdown: p.drawdown ? Number(p.drawdown) : undefined,
  }))

  if (!jobId || !resultId) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <p className="text-muted-foreground">缺少必要参数</p>
      </div>
    )
  }

  // Build header info
  const totalReturn = result ? Number(result.total_return) : 0
  const headerInfo = result && strategy ? (
    <div className="flex items-center gap-4 text-sm">
      <Badge variant="outline" className="font-mono">{result.stock_code}</Badge>
      <span className="text-muted-foreground">{strategy.name}</span>
      <span className="text-muted-foreground">|</span>
      <span className={totalReturn >= 0 ? 'text-profit' : 'text-loss'}>
        {formatPercent(totalReturn)}
      </span>
      <span className="text-muted-foreground">Sharpe {Number(result.sharpe_ratio).toFixed(2)}</span>
      <span className="text-muted-foreground">回撤 {formatPercent(Number(result.max_drawdown))}</span>
      <span className="text-muted-foreground">胜率 {formatPercent(Number(result.win_rate))}</span>
    </div>
  ) : null

  return (
    <div className="flex flex-col h-[calc(100vh-7rem)]">
      {/* Compact Header */}
      <div className="flex items-center justify-between pb-3 border-b mb-3">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          {isLoading ? <Skeleton className="h-6 w-64" /> : headerInfo}
        </div>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setRightPanelOpen(!rightPanelOpen)}
        >
          {rightPanelOpen ? (
            <><PanelRightClose className="h-4 w-4 mr-1" /> 收起代码</>
          ) : (
            <><PanelRight className="h-4 w-4 mr-1" /> 展开代码</>
          )}
        </Button>
      </div>

      {isLoading ? (
        <Skeleton className="flex-1" />
      ) : (
        <div className="flex gap-4 flex-1 overflow-hidden">
          {/* Main Content - Charts */}
          <div className="flex-1 flex flex-col min-w-0 overflow-y-auto">
            {/* K-line Chart */}
            <div className="border rounded-lg bg-card p-3 mb-3">
              {result && (
                <EquityCurveWithIndicators
                  stockCode={result.stock_code}
                  equityCurve={equityCurve}
                  trades={trades}
                  height={450}
                />
              )}
            </div>

            {/* Collapsible Data Table */}
            <Collapsible open={dataTableOpen} onOpenChange={setDataTableOpen}>
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-between border rounded-lg">
                  <span>日线数据 & 交易记录</span>
                  {dataTableOpen ? <ChevronLeft className="h-4 w-4 rotate-90" /> : <ChevronRight className="h-4 w-4 rotate-90" />}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <div className="grid grid-cols-2 gap-3 mt-3">
                  {/* Daily Data */}
                  <div className="border rounded-lg p-3 max-h-[250px] overflow-auto bg-card">
                    <h4 className="text-sm font-medium mb-2">日线数据</h4>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="text-xs">日期</TableHead>
                          <TableHead className="text-xs text-right">收盘</TableHead>
                          <TableHead className="text-xs text-right">成交量</TableHead>
                          <TableHead className="text-xs text-right">涨跌</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {klineData?.data?.slice(0, 20).map((row) => (
                          <TableRow key={row.date}>
                            <TableCell className="text-xs py-1">{row.date}</TableCell>
                            <TableCell className="text-xs py-1 text-right">{Number(row.close).toFixed(2)}</TableCell>
                            <TableCell className="text-xs py-1 text-right">{(Number(row.volume) / 10000).toFixed(0)}万</TableCell>
                            <TableCell className={cn('text-xs py-1 text-right', Number(row.pct_chg) > 0 ? 'text-profit' : 'text-loss')}>
                              {formatPercent(Number(row.pct_chg) / 100)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>

                  {/* Trades */}
                  <div className="border rounded-lg p-3 max-h-[250px] overflow-auto bg-card">
                    <h4 className="text-sm font-medium mb-2">交易记录</h4>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="text-xs">入场</TableHead>
                          <TableHead className="text-xs">出场</TableHead>
                          <TableHead className="text-xs text-right">盈亏</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {trades?.map((trade) => (
                          <TableRow key={trade.id}>
                            <TableCell className="text-xs py-1">{trade.entry_date}</TableCell>
                            <TableCell className="text-xs py-1">{trade.exit_date || '-'}</TableCell>
                            <TableCell className={cn('text-xs py-1 text-right', trade.pnl_percent > 0 ? 'text-profit' : 'text-loss')}>
                              {formatPercent(trade.pnl_percent)}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              </CollapsibleContent>
            </Collapsible>
          </div>

          {/* Right Panel - Strategy Code */}
          {rightPanelOpen && (
            <div className="w-[400px] flex-shrink-0 border rounded-lg bg-card flex flex-col">
              <div className="flex items-center justify-between p-3 border-b">
                <div className="flex items-center gap-2">
                  <Code2 className="h-4 w-4" />
                  <span className="font-medium text-sm">{strategy?.name || '策略代码'}</span>
                </div>
                <Badge variant="secondary" className="text-xs">{strategy?.strategy_type || '未分类'}</Badge>
              </div>
              <div className="flex-1 min-h-0">
                {strategy?.code ? (
                  <Editor
                    height="100%"
                    language="python"
                    value={strategy.code}
                    theme={isDark ? 'vs-dark' : 'light'}
                    options={{
                      readOnly: true,
                      minimap: { enabled: false },
                      scrollBeyondLastLine: false,
                      fontSize: 12,
                      lineNumbers: 'on',
                      folding: true,
                      wordWrap: 'on',
                    }}
                  />
                ) : (
                  <div className="h-full flex items-center justify-center text-muted-foreground">
                    加载中...
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
