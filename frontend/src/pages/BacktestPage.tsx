import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/use-toast'
import { PlayCircle, Loader2, X, ListFilter, Plus, Trash2 } from 'lucide-react'
import { useListStrategiesApiV1StrategiesGet } from '@/api/generated/strategies/strategies'
import { useListStocksApiV1StocksGet, useSearchStocksApiV1StocksSearchGet } from '@/api/generated/stocks/stocks'
import { useCreateBacktestApiV1BacktestsPost } from '@/api/generated/backtests/backtests'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'

export default function BacktestPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { toast } = useToast()

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

  // Fetch strategies
  const { data: strategiesData } = useListStrategiesApiV1StrategiesGet({
    page_size: 100,
    is_active: true,
  })

  // Search stocks for input
  const { data: stockSearchData } = useSearchStocksApiV1StocksSearchGet(
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

  // Create backtest mutation
  const createMutation = useCreateBacktestApiV1BacktestsPost({
    mutation: {
      onSuccess: (data) => {
        queryClient.invalidateQueries({ queryKey: ['/api/v1/backtests'] })
        toast({
          title: '回测已创建',
          description: '任务已添加到队列',
        })
        navigate(`/results/${data.id}`)
      },
      onError: (error) => {
        toast({
          variant: 'destructive',
          title: '创建回测失败',
          description: error.message || '请稍后重试',
        })
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
      toast({
        title: '批量添加成功',
        description: `已添加 ${newStocks.length} 只股票`,
      })
    } else {
      toast({
        title: '无新股票可添加',
        description: '当前页所有股票已在列表中',
      })
    }
  }

  const handleClearAllStocks = () => {
    setSelectedStocks([])
    toast({
      title: '已清空股票池',
    })
  }

  const handleSubmit = () => {
    if (selectedStrategies.length === 0) {
      toast({
        variant: 'destructive',
        title: '验证失败',
        description: '请选择至少一个策略',
      })
      return
    }
    if (selectedStocks.length === 0) {
      toast({
        variant: 'destructive',
        title: '验证失败',
        description: '请选择至少一只股票',
      })
      return
    }

    createMutation.mutate({
      data: {
        strategy_ids: selectedStrategies,
        stock_codes: selectedStocks.map((s) => s.code),
        start_date: startDate,
        end_date: endDate,
        initial_capital: parseFloat(initialCapital),
        commission_rate: parseFloat(commissionRate),
        slippage: parseFloat(slippage),
        position_sizing: { type: 'percent', value: 10 },
      },
    })
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
                <span className="text-sm text-muted-foreground">
                  已选 {selectedStocks.length} 只
                </span>
                {selectedStocks.length > 0 && (
                  <Button variant="ghost" size="sm" onClick={handleClearAllStocks}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                )}
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
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
                        handleAddStock(stock.code, stock.code_name || stock.code)
                      }}
                    >
                      <span>{stock.code} - {stock.code_name}</span>
                      <Plus className="h-4 w-4 text-muted-foreground" />
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Selected stocks */}
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
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
