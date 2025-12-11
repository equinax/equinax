import { useState, useRef, useCallback } from 'react'
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
  useGetBacktestApiV1BacktestsJobIdGet,
} from '@/api/generated/backtests/backtests'
import { useGetStrategyApiV1StrategiesStrategyIdGet } from '@/api/generated/strategies/strategies'
import { useGetStockApiV1StocksCodeGet, useGetAdjustFactorsApiV1StocksCodeAdjustFactorsGet } from '@/api/generated/stocks/stocks'
import { EquityCurveWithIndicators } from '@/components/backtest/EquityCurveWithIndicators'
import type { EquityCurvePoint, TradeRecord } from '@/types/backtest'

export default function TechnicalAnalysisPage() {
  const { jobId, resultId } = useParams<{ jobId: string; resultId: string }>()
  const navigate = useNavigate()
  const { theme } = useTheme()
  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  const [rightPanelOpen, setRightPanelOpen] = useState(false)
  const [dataTableOpen, setDataTableOpen] = useState(false)
  const [adjustFactorOpen, setAdjustFactorOpen] = useState(false)
  const [rightPanelWidth, setRightPanelWidth] = useState(450)
  const isDragging = useRef(false)
  const containerRef = useRef<HTMLDivElement>(null)

  // Handle drag to resize
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    isDragging.current = true
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'

    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging.current || !containerRef.current) return
      const containerRect = containerRef.current.getBoundingClientRect()
      const newWidth = containerRect.right - e.clientX
      const maxWidth = containerRect.width * 0.7 // Allow up to 70% of container
      // Clamp between 250 and 70% of container
      setRightPanelWidth(Math.max(250, Math.min(maxWidth, newWidth)))
    }

    const handleMouseUp = () => {
      isDragging.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }

    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
  }, [])

  // Fetch backtest job (for start_date/end_date)
  const { data: job } = useGetBacktestApiV1BacktestsJobIdGet(
    jobId || '',
    {
      query: {
        enabled: !!jobId,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

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

  // Fetch stock info (for stock name)
  const { data: stockInfo } = useGetStockApiV1StocksCodeGet(
    result?.stock_code || '',
    {
      query: {
        enabled: !!result?.stock_code,
        staleTime: 5 * 60 * 1000,
      },
    }
  )

  // Fetch adjust factors
  const { data: adjustFactors } = useGetAdjustFactorsApiV1StocksCodeAdjustFactorsGet(
    result?.stock_code || '',
    { limit: 100 },
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
  const stockName = stockInfo?.code_name || ''
  const dateRange = job ? `${job.start_date.replace(/-/g, '/')} - ${job.end_date.replace(/-/g, '/')}` : ''
  const headerInfo = result && strategy ? (
    <div className="flex items-center gap-3 text-sm">
      <Badge variant="outline" className="font-mono">{result.stock_code}</Badge>
      {stockName && <span className="font-medium">{stockName}</span>}
      <span className="text-muted-foreground">|</span>
      <span className="text-muted-foreground">{strategy.name}</span>
      <span className="text-muted-foreground">|</span>
      {dateRange && <span className="text-muted-foreground">{dateRange}</span>}
      {dateRange && <span className="text-muted-foreground">|</span>}
      <span className={totalReturn >= 0 ? 'text-profit' : 'text-loss'}>
        {formatPercent(totalReturn)}
      </span>
      <span className="text-muted-foreground">回撤 {formatPercent(Number(result.max_drawdown))}</span>
      <span className="text-muted-foreground">Sharpe {Number(result.sharpe_ratio).toFixed(2)}</span>
      <span className="text-muted-foreground">胜率 {formatPercent(Number(result.win_rate))}</span>
    </div>
  ) : null

  return (
    <div className="flex flex-col min-h-0">
      {/* Compact Header */}
      <div className="flex items-center justify-between pb-3 mb-3">
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
        <Skeleton className="h-96" />
      ) : (
        <div className="flex" ref={containerRef}>
          {/* Main Content - Charts */}
          <div className="flex-1 flex flex-col min-w-0">
            {/* K-line Chart */}
            <div className="border rounded-lg bg-card pt-3 mb-3">
              {result && (
                <EquityCurveWithIndicators
                  stockCode={result.stock_code}
                  equityCurve={equityCurve}
                  trades={trades}
                  height={450}
                />
              )}
            </div>

            {/* Collapsible Adjust Factors */}
            <Collapsible open={adjustFactorOpen} onOpenChange={setAdjustFactorOpen} className="border rounded-lg bg-card mb-2">
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-between rounded-lg rounded-b-none border-b-0">
                  <span className="text-sm font-medium">复权因子 ({adjustFactors?.length || 0} 条)</span>
                  {adjustFactorOpen ? <ChevronLeft className="h-4 w-4 rotate-90" /> : <ChevronRight className="h-4 w-4 rotate-90" />}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <div className="px-3 pb-3">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="text-xs">除权日期</TableHead>
                        <TableHead className="text-xs text-right">前复权因子</TableHead>
                        <TableHead className="text-xs text-right">后复权因子</TableHead>
                        <TableHead className="text-xs text-right">复权因子</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {adjustFactors?.map((factor) => (
                        <TableRow key={factor.divid_operate_date}>
                          <TableCell className="text-xs py-1">{factor.divid_operate_date}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{factor.fore_adjust_factor ?? '-'}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{factor.back_adjust_factor ?? '-'}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{factor.adjust_factor ?? '-'}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  {(!adjustFactors || adjustFactors.length === 0) && (
                    <div className="text-center text-muted-foreground py-4 text-sm">暂无复权因子数据</div>
                  )}
                </div>
              </CollapsibleContent>
            </Collapsible>

            {/* Collapsible Trade Records */}
            <Collapsible open={dataTableOpen} onOpenChange={setDataTableOpen} className="border rounded-lg bg-card">
              <CollapsibleTrigger asChild>
                <Button variant="ghost" size="sm" className="w-full justify-between rounded-lg rounded-b-none border-b-0">
                  <span className="text-sm font-medium">交易记录 ({trades?.length || 0} 笔)</span>
                  {dataTableOpen ? <ChevronLeft className="h-4 w-4 rotate-90" /> : <ChevronRight className="h-4 w-4 rotate-90" />}
                </Button>
              </CollapsibleTrigger>
              <CollapsibleContent>
                <div className="px-3 pb-3">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="text-xs">方向</TableHead>
                        <TableHead className="text-xs">入场日期</TableHead>
                        <TableHead className="text-xs text-right">入场价</TableHead>
                        <TableHead className="text-xs">出场日期</TableHead>
                        <TableHead className="text-xs text-right">出场价</TableHead>
                        <TableHead className="text-xs text-right">数量</TableHead>
                        <TableHead className="text-xs text-right">持仓天数</TableHead>
                        <TableHead className="text-xs text-right">盈亏</TableHead>
                        <TableHead className="text-xs text-right">盈亏%</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {trades?.map((trade) => (
                        <TableRow key={trade.id}>
                          <TableCell className="text-xs py-1">
                            <Badge variant={trade.type === 'long' ? 'default' : 'secondary'} className="text-xs">
                              {trade.type === 'long' ? '做多' : '做空'}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-xs py-1">{trade.entry_date}</TableCell>
                          <TableCell className="text-xs py-1 text-right">¥{trade.entry_price?.toFixed(2) || '-'}</TableCell>
                          <TableCell className="text-xs py-1">{trade.exit_date || '-'}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{trade.exit_price ? `¥${trade.exit_price.toFixed(2)}` : '-'}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{trade.size?.toLocaleString() || '-'}</TableCell>
                          <TableCell className="text-xs py-1 text-right">{trade.bars_held || '-'}</TableCell>
                          <TableCell className={cn('text-xs py-1 text-right', trade.pnl > 0 ? 'text-profit' : trade.pnl < 0 ? 'text-loss' : '')}>
                            {trade.pnl ? (trade.pnl > 0 ? '+' : '') + `¥${trade.pnl.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '-'}
                          </TableCell>
                          <TableCell className={cn('text-xs py-1 text-right font-medium', trade.pnl_percent > 0 ? 'text-profit' : trade.pnl_percent < 0 ? 'text-loss' : '')}>
                            {formatPercent(trade.pnl_percent)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  {(!trades || trades.length === 0) && (
                    <div className="text-center text-muted-foreground py-4 text-sm">暂无交易记录</div>
                  )}
                </div>
              </CollapsibleContent>
            </Collapsible>
          </div>

          {/* Right Panel - Strategy Code */}
          {rightPanelOpen && (
            <>
              {/* Resize handle - elegant dotted design */}
              <div
                className="group relative w-4 flex-shrink-0 cursor-col-resize flex flex-col items-center justify-center gap-1.5"
                onMouseDown={handleMouseDown}
              >
                {/* Custom dots - smaller at ends, larger in middle */}
                {[3, 4, 5, 5, 5, 5, 4, 3].map((size, i) => (
                  <div
                    key={i}
                    style={{ width: size, height: size }}
                    className="rounded-full bg-border group-hover:bg-primary/50 group-active:bg-primary transition-colors"
                  />
                ))}
                {/* Wider invisible hit area */}
                <div className="absolute inset-0" />
              </div>
              <div className="flex-shrink-0 border rounded-lg bg-card" style={{ width: rightPanelWidth }}>
                <div className="flex items-center justify-between p-3 border-b">
                  <div className="flex items-center gap-2">
                    <Code2 className="h-4 w-4" />
                    <span className="font-medium text-sm">{strategy?.name || '策略代码'}</span>
                  </div>
                  <Badge variant="secondary" className="text-xs">{strategy?.strategy_type || '未分类'}</Badge>
                </div>
                <div>
                  {strategy?.code ? (
                    <Editor
                      height="500px"
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
                    <div className="h-[500px] flex items-center justify-center text-muted-foreground">
                      加载中...
                    </div>
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
