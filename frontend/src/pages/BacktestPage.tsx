import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { PlayCircle, Loader2, X, ListFilter, Plus, Trash2, Database, Users, PieChart, TrendingUp } from 'lucide-react'
import { useListStrategiesApiV1StrategiesGet } from '@/api/generated/strategies/strategies'
import { useListStocksApiV1StocksGet, useSearchAssetsApiV1StocksSearchGet } from '@/api/generated/stocks/stocks'
import { useCreateBacktestApiV1BacktestsPost } from '@/api/generated/backtests/backtests'
import { useListPredefinedPoolsApiV1PoolsPredefinedGet, usePreviewPoolApiV1PoolsPoolIdPreviewGet } from '@/api/generated/stock-pools/stock-pools'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'

type SelectionMode = 'manual' | 'pool'
type PoolCategory = 'stock' | 'etf'

export default function BacktestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  // Form state
  const [selectedStrategies, setSelectedStrategies] = useState<string[]>([])
  const [selectedStocks, setSelectedStocks] = useState<{ code: string; name: string }[]>([])
  const [stockSearch, setStockSearch] = useState('')
  const [showSearchResults, setShowSearchResults] = useState(false)
  const [startDate, setStartDate] = useState('2024-01-01')
  const [endDate, setEndDate] = useState('2024-12-31')
  const [initialCapital, setInitialCapital] = useState('1000000')
  const [commissionRate, setCommissionRate] = useState('0.0003')
  const [slippage, setSlippage] = useState('0.001')

  // Stock pool filter state
  const [stockPoolExchange, setStockPoolExchange] = useState<string>('')
  const [stockPoolSearch, setStockPoolSearch] = useState('')
  const [stockPoolPage, setStockPoolPage] = useState(1)
  const [showStockPool, setShowStockPool] = useState(false)

  // Pool selection mode state
  const [selectionMode, setSelectionMode] = useState<SelectionMode>('pool')
  const [selectedPoolId, setSelectedPoolId] = useState<string | null>(null)
  const [poolCategory, setPoolCategory] = useState<PoolCategory>('etf')

  // Fetch strategies
  const { data: strategiesData } = useListStrategiesApiV1StrategiesGet({
    page_size: 100,
    is_active: true,
  })

  // Search stocks for input
  const { data: stockSearchData } = useSearchAssetsApiV1StocksSearchGet(
    { q: stockSearch },
    { query: { enabled: stockSearch.length >= 2 } }
  )

  // List stocks for stock pool
  const { data: stockPoolData, isLoading: isLoadingStockPool } = useListStocksApiV1StocksGet(
    {
      page: stockPoolPage,
      page_size: 50,
      exchange: stockPoolExchange || undefined,
      search: stockPoolSearch || undefined,
    },
    { query: { enabled: showStockPool } }
  )

  // Fetch predefined pools
  const { data: predefinedPools } = useListPredefinedPoolsApiV1PoolsPredefinedGet()

  // Fetch pool preview when a pool is selected
  const { data: poolPreview, isLoading: isLoadingPoolPreview } = usePreviewPoolApiV1PoolsPoolIdPreviewGet(
    selectedPoolId || '',
    {},
    { query: { enabled: !!selectedPoolId && selectionMode === 'pool' } }
  )

  // Create backtest mutation
  const createMutation = useCreateBacktestApiV1BacktestsPost({
    mutation: {
      onSuccess: (data) => {
        queryClient.invalidateQueries({ queryKey: ['/api/v1/backtests'] })
        navigate(`/results/${data.id}`)
      },
    },
  })

  const strategies = strategiesData?.items || []
  const searchResults = stockSearchData || []
  const stockPoolItems = stockPoolData?.items || []
  const stockPoolTotal = stockPoolData?.total || 0
  const stockPoolPages = stockPoolData?.pages || 1

  const handleAddStock = (code: string, name: string) => {
    if (!selectedStocks.find((s) => s.code === code)) {
      setSelectedStocks([...selectedStocks, { code, name }])
    }
    setStockSearch('')
    setShowSearchResults(false)
  }

  const handleRemoveStock = (code: string) => {
    setSelectedStocks(selectedStocks.filter((s) => s.code !== code))
  }

  const handleAddAllFromPool = () => {
    const newStocks = stockPoolItems
      .filter((stock) => !selectedStocks.find((s) => s.code === stock.code))
      .map((stock) => ({ code: stock.code, name: stock.code_name || stock.code }))

    if (newStocks.length > 0) {
      setSelectedStocks([...selectedStocks, ...newStocks])
    }
  }

  const handleClearAllStocks = () => {
    setSelectedStocks([])
  }

  const handleSubmit = () => {
    if (selectedStrategies.length === 0) {
      return
    }

    // Check stock source based on mode
    if (selectionMode === 'manual' && selectedStocks.length === 0) {
      return
    }
    if (selectionMode === 'pool' && !selectedPoolId) {
      return
    }

    const baseData = {
      strategy_ids: selectedStrategies,
      start_date: startDate,
      end_date: endDate,
      initial_capital: parseFloat(initialCapital),
      commission_rate: parseFloat(commissionRate),
      slippage: parseFloat(slippage),
      position_sizing: { type: 'percent', value: 10 },
    }

    if (selectionMode === 'pool' && selectedPoolId) {
      // Use pool_id for batch backtest
      createMutation.mutate({
        data: {
          ...baseData,
          pool_id: selectedPoolId,
        },
      })
    } else {
      // Use manual stock_codes
      createMutation.mutate({
        data: {
          ...baseData,
          stock_codes: selectedStocks.map((s) => s.code),
        },
      })
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold">回测执行</h1>
        <p className="text-muted-foreground">配置并执行策略回测</p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Configuration */}
        <Card>
          <CardHeader>
            <CardTitle>回测配置</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Strategy selection */}
            <div className="space-y-2">
              <label className="text-sm font-medium">选择策略 (可多选)</label>
              <select
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                value=""
                onChange={(e) => {
                  if (e.target.value && !selectedStrategies.includes(e.target.value)) {
                    setSelectedStrategies([...selectedStrategies, e.target.value])
                  }
                }}
              >
                <option value="">添加策略...</option>
                {strategies.map((s) => (
                  <option key={s.id} value={s.id} disabled={selectedStrategies.includes(s.id)}>
                    {s.name} (v{s.version})
                  </option>
                ))}
              </select>
              {selectedStrategies.length > 0 && (
                <div className="flex flex-wrap gap-2">
                  {selectedStrategies.map((id) => {
                    const strategy = strategies.find((s) => s.id === id)
                    return (
                      <span
                        key={id}
                        className="flex items-center gap-1 rounded-full bg-primary/10 px-3 py-1 text-sm"
                      >
                        {strategy?.name || id.slice(0, 8)}
                        <button onClick={() => setSelectedStrategies(selectedStrategies.filter((s) => s !== id))}>
                          <X className="h-3 w-3" />
                        </button>
                      </span>
                    )
                  })}
                </div>
              )}
            </div>

            {/* Date range */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">开始日期</label>
                <Input
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">结束日期</label>
                <Input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                />
              </div>
            </div>

            {/* Capital */}
            <div className="space-y-2">
              <label className="text-sm font-medium">初始资金</label>
              <Input
                type="number"
                value={initialCapital}
                onChange={(e) => setInitialCapital(e.target.value)}
              />
            </div>

            {/* Commission */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">手续费率</label>
                <Input
                  type="number"
                  value={commissionRate}
                  onChange={(e) => setCommissionRate(e.target.value)}
                  step="0.0001"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">滑点</label>
                <Input
                  type="number"
                  value={slippage}
                  onChange={(e) => setSlippage(e.target.value)}
                  step="0.0001"
                />
              </div>
            </div>

            {/* Submit */}
            <Button
              className="w-full"
              onClick={handleSubmit}
              disabled={createMutation.isPending}
            >
              {createMutation.isPending ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <PlayCircle className="mr-2 h-4 w-4" />
              )}
              开始回测
            </Button>
          </CardContent>
        </Card>

        {/* Stock Pool */}
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle>股票池</CardTitle>
              <div className="flex items-center gap-2">
                {/* Mode selector */}
                <div className="flex rounded-md border p-0.5 bg-muted/30">
                  <button
                    type="button"
                    onClick={() => setSelectionMode('pool')}
                    className={`flex items-center gap-1 px-2 py-1 text-xs rounded ${
                      selectionMode === 'pool'
                        ? 'bg-background shadow text-foreground'
                        : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    <Database className="h-3 w-3" />
                    快速选择
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectionMode('manual')}
                    className={`flex items-center gap-1 px-2 py-1 text-xs rounded ${
                      selectionMode === 'manual'
                        ? 'bg-background shadow text-foreground'
                        : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    <Users className="h-3 w-3" />
                    手动选择
                  </button>
                </div>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            {selectionMode === 'pool' ? (
              /* Pool Selection Mode */
              <div className="space-y-4">
                {/* Pool category tabs */}
                <div className="flex rounded-md border p-0.5 bg-muted/30">
                  <button
                    type="button"
                    onClick={() => {
                      setPoolCategory('stock')
                      setSelectedPoolId(null)
                    }}
                    className={`flex-1 flex items-center justify-center gap-1 px-3 py-1.5 text-sm rounded ${
                      poolCategory === 'stock'
                        ? 'bg-background shadow text-foreground'
                        : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    <TrendingUp className="h-4 w-4" />
                    股票池
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setPoolCategory('etf')
                      setSelectedPoolId(null)
                    }}
                    className={`flex-1 flex items-center justify-center gap-1 px-3 py-1.5 text-sm rounded ${
                      poolCategory === 'etf'
                        ? 'bg-background shadow text-foreground'
                        : 'text-muted-foreground hover:text-foreground'
                    }`}
                  >
                    <PieChart className="h-4 w-4" />
                    ETF池
                  </button>
                </div>

                <div className="text-sm text-muted-foreground">
                  选择预定义{poolCategory === 'stock' ? '股票' : 'ETF'}池进行批量回测
                </div>

                {/* Predefined pools grid - filtered by category */}
                <div className="grid grid-cols-2 gap-2">
                  {(() => {
                    const filteredPools = predefinedPools?.filter((pool) => {
                      const predefinedKey = pool.predefined_key || ''
                      if (poolCategory === 'etf') {
                        return predefinedKey.startsWith('etf_')
                      }
                      return !predefinedKey.startsWith('etf_')
                    }) || []

                    if (filteredPools.length === 0) {
                      return (
                        <div className="col-span-2 text-center py-8 text-muted-foreground border rounded-lg">
                          {poolCategory === 'stock' ? (
                            <div className="space-y-2">
                              <p>暂无预定义股票池</p>
                              <p className="text-xs">请使用"手动选择"模式或创建自定义股票池</p>
                            </div>
                          ) : (
                            <p>暂无预定义 ETF 池</p>
                          )}
                        </div>
                      )
                    }

                    return filteredPools.map((pool) => (
                      <button
                        key={pool.id}
                        type="button"
                        onClick={() => setSelectedPoolId(pool.id === selectedPoolId ? null : pool.id)}
                        className={`p-3 rounded-lg border text-left transition-colors ${
                          pool.id === selectedPoolId
                            ? 'border-primary bg-primary/5'
                            : 'border-border hover:bg-muted/50'
                        }`}
                      >
                        <div className="font-medium text-sm">{pool.name}</div>
                        <div className="text-xs text-muted-foreground mt-1">{pool.description}</div>
                      </button>
                    ))
                  })()}
                </div>

                {/* Pool preview */}
                {selectedPoolId && (
                  <div className="border rounded-lg p-4 bg-muted/30">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium">池预览</span>
                      {isLoadingPoolPreview ? (
                        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                      ) : (
                        <span className="text-sm text-muted-foreground">
                          共 {poolPreview?.total_count || 0} 只{poolCategory === 'etf' ? 'ETF' : '股票'}
                        </span>
                      )}
                    </div>
                    {poolPreview && (
                      <>
                        {/* Distribution stats */}
                        {poolPreview.exchange_distribution && (
                          <div className="grid grid-cols-2 gap-2 text-xs mb-3">
                            {Object.entries(poolPreview.exchange_distribution).map(([exchange, count]) => (
                              <div key={exchange} className="flex justify-between p-2 bg-background rounded">
                                <span className="text-muted-foreground">{exchange === 'sh' ? '沪市' : '深市'}</span>
                                <span>{count as number} 只</span>
                              </div>
                            ))}
                          </div>
                        )}
                        {/* Sample stocks */}
                        <div className="flex flex-wrap gap-1 max-h-20 overflow-auto">
                          {poolPreview.stock_codes?.slice(0, 20).map((code) => (
                            <span key={code} className="text-xs bg-background px-1.5 py-0.5 rounded font-mono">
                              {code}
                            </span>
                          ))}
                          {poolPreview.stock_codes && poolPreview.stock_codes.length > 20 && (
                            <span className="text-xs text-muted-foreground px-1.5 py-0.5">
                              +{poolPreview.stock_codes.length - 20} 更多...
                            </span>
                          )}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>
            ) : (
              /* Manual Selection Mode */
              <>
                {/* Search input */}
                <div className="relative">
                  <Input
                    placeholder="搜索股票代码或名称..."
                    value={stockSearch}
                    onChange={(e) => {
                      setStockSearch(e.target.value)
                      setShowSearchResults(true)
                    }}
                    onFocus={() => setShowSearchResults(true)}
                    onBlur={() => {
                      setTimeout(() => setShowSearchResults(false), 200)
                    }}
                  />
                  {showSearchResults && stockSearch.length >= 2 && searchResults.length > 0 && (
                    <div className="absolute z-10 mt-1 w-full rounded-md border bg-background shadow-lg max-h-60 overflow-auto">
                      {searchResults.slice(0, 10).map((stock) => (
                        <button
                          key={stock.code}
                          type="button"
                          className="w-full px-3 py-2 text-left text-sm hover:bg-accent flex justify-between items-center"
                          onMouseDown={(e) => {
                            e.preventDefault()
                            handleAddStock(stock.code, stock.name || stock.code)
                          }}
                        >
                          <span>{stock.code} - {stock.name}</span>
                          <Plus className="h-4 w-4 text-muted-foreground" />
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                {/* Selected stocks */}
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">已选 {selectedStocks.length} 只</span>
                  {selectedStocks.length > 0 && (
                    <Button variant="ghost" size="sm" onClick={handleClearAllStocks}>
                      <Trash2 className="h-4 w-4 mr-1" />
                      清空
                    </Button>
                  )}
                </div>
                <div className="flex flex-wrap gap-2 min-h-[60px] p-2 border rounded-md bg-muted/30">
                  {selectedStocks.length > 0 ? (
                    selectedStocks.map((stock) => (
                      <span
                        key={stock.code}
                        className="flex items-center gap-1 rounded-full bg-primary/10 px-2 py-0.5 text-xs"
                      >
                        {stock.code}
                        <button onClick={() => handleRemoveStock(stock.code)}>
                          <X className="h-3 w-3" />
                        </button>
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-muted-foreground m-auto">搜索或从下方列表添加股票</span>
                  )}
                </div>

                {/* Stock pool browser toggle */}
                <div className="border-t pt-4">
                  <Button
                    variant="outline"
                    className="w-full"
                    onClick={() => setShowStockPool(!showStockPool)}
                  >
                    <ListFilter className="mr-2 h-4 w-4" />
                    {showStockPool ? '收起股票列表' : '浏览所有股票'}
                  </Button>
                </div>

                {/* Stock pool browser */}
                {showStockPool && (
                  <div className="space-y-3 border-t pt-4">
                    {/* Filters */}
                    <div className="flex gap-2">
                      <select
                        className="flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm"
                        value={stockPoolExchange}
                        onChange={(e) => {
                          setStockPoolExchange(e.target.value)
                          setStockPoolPage(1)
                        }}
                      >
                        <option value="">全部交易所</option>
                        <option value="sh">上海 (sh)</option>
                        <option value="sz">深圳 (sz)</option>
                      </select>
                      <Input
                        className="flex-1"
                        placeholder="筛选..."
                        value={stockPoolSearch}
                        onChange={(e) => {
                          setStockPoolSearch(e.target.value)
                          setStockPoolPage(1)
                        }}
                      />
                    </div>

                    {/* Add all button */}
                    <Button
                      variant="secondary"
                      size="sm"
                      className="w-full"
                      onClick={handleAddAllFromPool}
                      disabled={isLoadingStockPool || stockPoolItems.length === 0}
                    >
                      <Plus className="mr-2 h-4 w-4" />
                      添加当前页全部 ({stockPoolItems.length} 只)
                    </Button>

                    {/* Stock list */}
                    <div className="max-h-[300px] overflow-auto border rounded-md">
                      {isLoadingStockPool ? (
                        <div className="flex items-center justify-center py-8">
                          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                        </div>
                      ) : stockPoolItems.length > 0 ? (
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-muted">
                            <tr>
                              <th className="text-left px-3 py-2 font-medium">代码</th>
                              <th className="text-left px-3 py-2 font-medium">名称</th>
                              <th className="text-right px-3 py-2 font-medium">操作</th>
                            </tr>
                          </thead>
                          <tbody>
                            {stockPoolItems.map((stock) => {
                              const isSelected = selectedStocks.some((s) => s.code === stock.code)
                              return (
                                <tr
                                  key={stock.code}
                                  className={`border-t hover:bg-muted/50 ${isSelected ? 'bg-primary/5' : ''}`}
                                >
                                  <td className="px-3 py-2 font-mono">{stock.code}</td>
                                  <td className="px-3 py-2 truncate max-w-[150px]">{stock.code_name}</td>
                                  <td className="px-3 py-2 text-right">
                                    {isSelected ? (
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => handleRemoveStock(stock.code)}
                                      >
                                        <X className="h-4 w-4 text-muted-foreground" />
                                      </Button>
                                    ) : (
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => handleAddStock(stock.code, stock.code_name || stock.code)}
                                      >
                                        <Plus className="h-4 w-4" />
                                      </Button>
                                    )}
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      ) : (
                        <div className="text-center py-8 text-muted-foreground">
                          无匹配股票
                        </div>
                      )}
                    </div>

                    {/* Pagination */}
                    {stockPoolPages > 1 && (
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">
                          共 {stockPoolTotal} 只，第 {stockPoolPage}/{stockPoolPages} 页
                        </span>
                        <div className="flex gap-1">
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={stockPoolPage <= 1}
                            onClick={() => setStockPoolPage(stockPoolPage - 1)}
                          >
                            上一页
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={stockPoolPage >= stockPoolPages}
                            onClick={() => setStockPoolPage(stockPoolPage + 1)}
                          >
                            下一页
                          </Button>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
