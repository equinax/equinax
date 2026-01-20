import { useState, useEffect, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { ArrowLeft, RotateCcw, Loader2, Pencil, Check, XCircle, ChevronDown, ChevronUp, Plus, Sparkles } from 'lucide-react'
import { Card, CardContent, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useDynamicBacktestStore, BENCHMARK_OPTIONS, StockData, BenchmarkData, chartSyncManager } from '@/lib/dynamic-backtest'
import { useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { MultiStockKlinePanel } from './components/MultiStockKlinePanel'
import { TradeList } from './components/TradeList'
import { PositionPanel } from './components/PositionPanel'
import { EquityChart } from './components/EquityChart'
import { StockSelectorSheet } from './components/StockSelectorSheet'

// 默认个股图表高度
const DEFAULT_STOCK_CHART_HEIGHT = 140

export default function DynamicBacktestPage() {
  const store = useDynamicBacktestStore()

  const [pendingStockCode, setPendingStockCode] = useState<string | null>(null)
  const [isSelectorOpen, setIsSelectorOpen] = useState(false)
  const [stockChartHeight, setStockChartHeight] = useState(DEFAULT_STOCK_CHART_HEIGHT)
  const [isConfigCollapsed, setIsConfigCollapsed] = useState(false)
  
  const handleStockChartHeightChange = useCallback((height: number) => {
    setStockChartHeight(height)
  }, [])

  const handleGenerateOptimalTrades = () => {
    const result = store.generateOptimalTrades()
    if (!result.success) {
      alert(result.error || '生成失败')
    }
  }

  // 页面离开时重置同步管理器
  useEffect(() => {
    return () => {
      chartSyncManager.reset()
    }
  }, [])

  // 获取待添加股票的K线数据（使用后复权价格）
  const { data: klineData, isLoading: isLoadingKline } = useGetKlineApiV1StocksCodeKlineGet(
    pendingStockCode || '',
    {
      start_date: store.startDate,
      end_date: store.endDate,
      limit: 1000,
      adjust: 'hfq',  // 后复权
    },
    { query: { enabled: !!pendingStockCode } }
  )

  // 获取基准ETF的K线数据（使用后复权价格）
  const { data: benchmarkKline } = useGetKlineApiV1StocksCodeKlineGet(
    store.benchmarkCode,
    {
      start_date: store.startDate,
      end_date: store.endDate,
      limit: 1000,
      adjust: 'hfq',  // 后复权
    },
    { query: { enabled: !!store.benchmarkCode } }
  )

  // 当基准数据加载完成，更新store
  useEffect(() => {
    if (benchmarkKline && benchmarkKline.data && benchmarkKline.data.length > 0) {
      const benchmarkOption = BENCHMARK_OPTIONS.find(b => b.code === store.benchmarkCode)
      const benchmarkData: BenchmarkData = {
        code: benchmarkKline.code,
        name: benchmarkOption?.name || benchmarkKline.code_name || benchmarkKline.code,
        kline: benchmarkKline.data.map(k => ({
          date: k.date,
          open: Number(k.open),
          high: Number(k.high),
          low: Number(k.low),
          close: Number(k.close),
          volume: Number(k.volume),
        })),
        returns: [],
      }
      store.setBenchmarkData(benchmarkData)
    }
  }, [benchmarkKline, store.benchmarkCode])

  // 当K线数据加载完成，添加到store
  useEffect(() => {
    if (klineData && pendingStockCode) {
      const stockData: StockData = {
        code: klineData.code,
        name: klineData.code_name || klineData.code,
        kline: (klineData.data || []).map(k => ({
          date: k.date,
          open: Number(k.open),
          high: Number(k.high),
          low: Number(k.low),
          close: Number(k.close),
          volume: Number(k.volume),
          amount: k.amount ? Number(k.amount) : undefined,
          pctChg: k.pct_chg ? Number(k.pct_chg) : undefined,
          turn: k.turn ? Number(k.turn) : undefined,
        })),
      }
      store.addStock(stockData)
      setPendingStockCode(null)
    }
  }, [klineData, pendingStockCode])

  const handleAddStock = (code: string) => {
    if (!store.stocks.has(code)) {
      setPendingStockCode(code)
    }
  }

  const handleToggleStock = (code: string) => {
    if (store.stocks.has(code)) {
      store.removeStock(code)
      return
    }
    handleAddStock(code)
  }

  return (
    <div className="space-y-2">
      {/* 顶部导航 */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="sm" asChild>
            <Link to="/backtest">
              <ArrowLeft className="mr-2 h-4 w-4" />
              返回
            </Link>
          </Button>
          <div>
            <h1 className="text-2xl font-bold">动态模拟</h1>
            {/* <p className="text-sm text-muted-foreground">手动模拟买卖，实时计算收益</p> */}
          </div>
        </div>
        <Button variant="outline" size="sm" onClick={store.reset}>
          <RotateCcw className="mr-2 h-4 w-4" />
          重置
        </Button>
      </div>

      {/* 主布局 */}
      <div className="grid gap-3 lg:grid-cols-[300px_1fr]">
        {/* 左侧面板 */}
        <div className="space-y-3">
          <Card>
            <CardContent className="p-3 space-y-3">
              {/* 配置区域 - 支持锁定/编辑模式 */}
              <div className="flex items-center justify-between">
                <CardTitle className="text-base shrink-0 whitespace-nowrap">模拟配置</CardTitle>
                <div className="flex items-center gap-1">
                  {store.isConfigLocked ? (
                    <>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={store.startEditConfig}
                      >
                        <Pencil className="h-3 w-3 mr-1" />
                        修改
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 w-7 p-0"
                        onClick={() => setIsConfigCollapsed(!isConfigCollapsed)}
                        title={isConfigCollapsed ? '展开' : '折叠'}
                      >
                        {isConfigCollapsed ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronUp className="h-4 w-4" />
                        )}
                      </Button>
                    </>
                  ) : (
                    <>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={store.cancelEditConfig}
                      >
                        <XCircle className="h-3 w-3 mr-1" />
                        取消
                      </Button>
                      <Button
                        variant="default"
                        size="sm"
                        className="h-7 px-2 text-xs"
                        onClick={store.confirmEditConfig}
                      >
                        <Check className="h-3 w-3 mr-1" />
                        确认
                      </Button>
                    </>
                  )}
                </div>
              </div>

              {/* 可折叠的配置内容 */}
              {!isConfigCollapsed && (
                <>
              {/* 编辑模式警告 */}
              {!store.isConfigLocked && (
                <div className="px-2 py-1.5 rounded-md bg-amber-500/10 border border-amber-500/20 text-xs text-amber-600 dark:text-amber-400">
                  ⚠️ 确认修改将清空所有交易记录和持仓
                </div>
              )}

              {/* 初始资金 */}
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">初始资金</span>
                {store.isConfigLocked ? (
                  <span className="ml-auto font-mono text-sm">
                    ¥{store.initialCapital.toLocaleString('zh-CN')}
                  </span>
                ) : (
                  <Input
                    type="number"
                    value={store.tempConfig?.initialCapital ?? store.initialCapital}
                    onChange={(e) => store.updateTempConfig({ initialCapital: Number(e.target.value) })}
                    step={10000}
                    className="ml-auto w-28 h-8 text-sm text-right"
                  />
                )}
              </div>

              {/* 时间范围 */}
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <label className="text-sm font-medium">开始日期</label>
                  {store.isConfigLocked ? (
                    <div className="px-2 py-1.5 rounded-md border bg-muted/30 text-sm font-mono">
                      {store.startDate}
                    </div>
                  ) : (
                    <Input
                      type="date"
                      value={store.tempConfig?.startDate ?? store.startDate}
                      onChange={(e) => store.updateTempConfig({ startDate: e.target.value })}
                    />
                  )}
                </div>
                <div className="space-y-1.5">
                  <label className="text-sm font-medium">结束日期</label>
                  {store.isConfigLocked ? (
                    <div className="px-2 py-1.5 rounded-md border bg-muted/30 text-sm font-mono">
                      {store.endDate}
                    </div>
                  ) : (
                    <Input
                      type="date"
                      value={store.tempConfig?.endDate ?? store.endDate}
                      onChange={(e) => store.updateTempConfig({ endDate: e.target.value })}
                    />
                  )}
                </div>
              </div>

              {/* 基准选择 - 不锁定 */}
              <div className="flex items-center gap-2">
                <label className="text-xs text-muted-foreground">基准指数</label>
                <select
                  className="flex-1 rounded-md border border-input bg-background px-2 py-1 text-sm"
                  value={store.benchmarkCode}
                  onChange={(e) => store.setBenchmark(e.target.value)}
                >
                  {BENCHMARK_OPTIONS.map(opt => (
                    <option key={opt.code} value={opt.code}>
                      {opt.name}
                    </option>
                  ))}
                </select>
              </div>

              {/* 持仓与资金 */}
              <div className="border-t pt-3 space-y-2">
                <PositionPanel variant="embedded" />
              </div>
                </>
              )}

              {/* 添加股票 + 最优交易按钮行 */}
              <div className="border-t pt-3">
                <div className="flex items-center justify-between gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 px-3 text-xs"
                    onClick={() => setIsSelectorOpen(true)}
                  >
                    <Plus className="h-3 w-3 mr-1" />
                    添加股票
                  </Button>
                  {store.stocks.size > 0 && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 px-3 text-xs"
                      onClick={handleGenerateOptimalTrades}
                      title="基于股票池自动生成最优买卖点"
                    >
                      <Sparkles className="h-3 w-3 mr-1" />
                      最优交易
                    </Button>
                  )}
                </div>

                {/* 加载状态 */}
                {isLoadingKline && pendingStockCode && (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground mt-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    正在加载 {pendingStockCode}...
                  </div>
                )}
              </div>

              {/* 交易记录 */}
              <div className="border-t pt-3 space-y-2">
                <TradeList variant="embedded" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 右侧主区域 */}
        <div className="space-y-0">
          {/* 权益曲线 */}
          <EquityChart 
            height={160} 
            stockChartHeight={stockChartHeight}
            onStockChartHeightChange={handleStockChartHeightChange}
          />

          {/* 多股票K线列表 */}
          <MultiStockKlinePanel stockChartHeight={stockChartHeight} />
        </div>
      </div>

      <StockSelectorSheet
        open={isSelectorOpen}
        onOpenChange={setIsSelectorOpen}
        selectedCodes={store.stocks}
        pendingCode={pendingStockCode}
        onToggleStock={handleToggleStock}
      />
    </div>
  )
}
