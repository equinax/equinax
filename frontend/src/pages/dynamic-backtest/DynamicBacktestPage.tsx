import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { ArrowLeft, RotateCcw, Plus, Search, X, Loader2, Pencil, Check, XCircle } from 'lucide-react'
import { Card, CardContent, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useDynamicBacktestStore, BENCHMARK_OPTIONS, StockData, BenchmarkData, chartSyncManager } from '@/lib/dynamic-backtest'
import { useSearchAssetsApiV1StocksSearchGet, useGetKlineApiV1StocksCodeKlineGet } from '@/api/generated/stocks/stocks'
import { MultiStockKlinePanel } from './components/MultiStockKlinePanel'
import { TradeList } from './components/TradeList'
import { PositionPanel } from './components/PositionPanel'
import { EquityChart } from './components/EquityChart'

export default function DynamicBacktestPage() {
  const store = useDynamicBacktestStore()

  const [stockSearch, setStockSearch] = useState('')
  const [showSearchResults, setShowSearchResults] = useState(false)
  const [pendingStockCode, setPendingStockCode] = useState<string | null>(null)

  // 页面离开时重置同步管理器
  useEffect(() => {
    return () => {
      chartSyncManager.reset()
    }
  }, [])

  // 搜索股票
  const { data: searchResults } = useSearchAssetsApiV1StocksSearchGet(
    { q: stockSearch },
    { query: { enabled: stockSearch.length >= 2 } }
  )

  // 获取待添加股票的K线数据
  const { data: klineData, isLoading: isLoadingKline } = useGetKlineApiV1StocksCodeKlineGet(
    pendingStockCode || '',
    {
      start_date: store.startDate,
      end_date: store.endDate,
      limit: 1000,
    },
    { query: { enabled: !!pendingStockCode } }
  )

  // 获取基准ETF的K线数据
  const { data: benchmarkKline } = useGetKlineApiV1StocksCodeKlineGet(
    store.benchmarkCode,
    {
      start_date: store.startDate,
      end_date: store.endDate,
      limit: 1000,
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
    setStockSearch('')
    setShowSearchResults(false)
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
            <h1 className="text-2xl font-bold">动态回测</h1>
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
                <CardTitle className="text-base shrink-0 whitespace-nowrap">回测配置</CardTitle>
                {store.isConfigLocked ? (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 px-2 text-xs"
                    onClick={store.startEditConfig}
                  >
                    <Pencil className="h-3 w-3 mr-1" />
                    修改
                  </Button>
                ) : (
                  <div className="flex items-center gap-1">
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
                  </div>
                )}
              </div>

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

              <div className="border-t pt-3 space-y-2">
                <div className="flex items-center justify-between gap-4">
                  <CardTitle className="text-base shrink-0 whitespace-nowrap">股票池</CardTitle>
                  <div className="flex-1 relative min-w-0">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      placeholder="搜索股票代码或名称..."
                      value={stockSearch}
                      onChange={(e) => {
                        setStockSearch(e.target.value)
                        setShowSearchResults(true)
                      }}
                      onFocus={() => setShowSearchResults(true)}
                      onBlur={() => setTimeout(() => setShowSearchResults(false), 200)}
                      className="pl-9 h-8 text-sm w-full"
                    />
                    {showSearchResults && stockSearch.length >= 2 && searchResults && searchResults.length > 0 && (
                      <div className="absolute z-10 mt-1 w-full rounded-md border bg-background shadow-lg max-h-60 overflow-auto">
                        {searchResults.slice(0, 10).map((stock) => (
                          <button
                            key={stock.code}
                            type="button"
                            className="w-full px-3 py-2 text-left text-sm hover:bg-accent flex justify-between items-center"
                            onMouseDown={(e) => {
                              e.preventDefault()
                              handleAddStock(stock.code)
                            }}
                          >
                            <span>{stock.code} - {stock.name}</span>
                            <Plus className="h-4 w-4 text-muted-foreground" />
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* 加载状态 */}
              {isLoadingKline && pendingStockCode && (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  正在加载 {pendingStockCode}...
                </div>
              )}

              {/* 已添加的股票列表 */}
              <div className="space-y-1">
                {Array.from(store.stocks.values()).map(stock => (
                  <div
                    key={stock.code}
                    className={`flex items-center justify-between px-2 py-1.5 rounded text-sm cursor-pointer transition-colors ${store.selectedStockCode === stock.code
                      ? 'bg-primary/10 text-primary'
                      : 'hover:bg-muted'
                      }`}
                    onClick={() => store.selectStock(stock.code)}
                  >
                    <span className="font-mono">{stock.code}</span>
                    <div className="flex items-center gap-2">
                      <span className="text-muted-foreground truncate max-w-[80px]">{stock.name}</span>
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          store.removeStock(stock.code)
                        }}
                        className="text-muted-foreground hover:text-destructive"
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </div>
                  </div>
                ))}
                {store.stocks.size === 0 && (
                  <p className="text-sm text-muted-foreground text-center py-4">
                    搜索添加股票开始回测
                  </p>
                )}
              </div>
              
              <div className="border-t pt-3 space-y-2">
                <PositionPanel variant="embedded" />
              </div>

              <div className="border-t pt-3 space-y-2">
                <TradeList variant="embedded" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* 右侧主区域 */}
        <div className="space-y-0">
          {/* 权益曲线 */}
          <EquityChart height={160} />

          {/* 多股票K线列表 */}
          <MultiStockKlinePanel />
        </div>
      </div>
    </div>
  )
}
