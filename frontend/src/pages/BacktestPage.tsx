import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/use-toast'
import { PlayCircle, Loader2, X } from 'lucide-react'
import { useListStrategiesApiV1StrategiesGet } from '@/api/generated/strategies/strategies'
import { useSearchStocksApiV1StocksSearchGet } from '@/api/generated/stocks/stocks'
import { useListBacktestsApiV1BacktestsGet, useCreateBacktestApiV1BacktestsPost } from '@/api/generated/backtests/backtests'
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

  // Fetch strategies
  const { data: strategiesData } = useListStrategiesApiV1StrategiesGet({
    page_size: 100,
    is_active: true,
  })

  // Search stocks
  const { data: stockSearchData } = useSearchStocksApiV1StocksSearchGet(
    { q: stockSearch },
    { query: { enabled: stockSearch.length >= 2 } }
  )

  // Fetch running/pending backtests for queue
  const { data: queueData } = useListBacktestsApiV1BacktestsGet({
    page_size: 10,
  })

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
  // API returns array directly, not paginated object
  const searchResults = stockSearchData || []
  const queuedJobs = queueData?.items?.filter(
    (j) => j.status === 'PENDING' || j.status === 'RUNNING' || j.status === 'QUEUED'
  ) || []

  const handleAddStock = (code: string, name: string) => {
    if (!selectedStocks.find((s) => s.code === code)) {
      setSelectedStocks([...selectedStocks, { code, name }])
      toast({
        title: '已添加股票',
        description: `${code} ${name}`,
      })
    }
    setStockSearch('')
    setShowSearchResults(false)
  }

  const handleRemoveStock = (code: string) => {
    setSelectedStocks(selectedStocks.filter((s) => s.code !== code))
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

            {/* Stock selection */}
            <div className="space-y-2">
              <label className="text-sm font-medium">股票池</label>
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
                    // Delay hiding to allow click events on results
                    setTimeout(() => setShowSearchResults(false), 200)
                  }}
                />
                {showSearchResults && stockSearch.length >= 2 && searchResults.length > 0 && (
                  <div className="absolute z-10 mt-1 w-full rounded-md border bg-background shadow-lg max-h-60 overflow-auto">
                    {searchResults.slice(0, 10).map((stock) => (
                      <button
                        key={stock.code}
                        type="button"
                        className="w-full px-3 py-2 text-left text-sm hover:bg-accent"
                        onMouseDown={(e) => {
                          e.preventDefault()
                          handleAddStock(stock.code, stock.code_name || stock.code)
                        }}
                      >
                        {stock.code} - {stock.code_name}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <div className="flex flex-wrap gap-2">
                {selectedStocks.map((stock) => (
                  <span
                    key={stock.code}
                    className="flex items-center gap-1 rounded-full bg-primary/10 px-3 py-1 text-sm"
                  >
                    {stock.code} {stock.name}
                    <button onClick={() => handleRemoveStock(stock.code)}>
                      <X className="h-3 w-3" />
                    </button>
                  </span>
                ))}
                {selectedStocks.length === 0 && (
                  <span className="text-sm text-muted-foreground">搜索并添加股票</span>
                )}
              </div>
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

        {/* Queue & Progress */}
        <Card>
          <CardHeader>
            <CardTitle>回测队列</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {queuedJobs.length > 0 ? (
                queuedJobs.map((job) => (
                  <div key={job.id} className="rounded-lg border p-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {job.status === 'RUNNING' && (
                          <Loader2 className="h-4 w-4 animate-spin text-primary" />
                        )}
                        <span className="font-medium">
                          {job.name || `任务 ${job.id.slice(0, 8)}`}
                        </span>
                      </div>
                      <span className="text-sm text-muted-foreground">
                        {Number(job.progress).toFixed(0)}%
                      </span>
                    </div>
                    <div className="mt-2 h-2 overflow-hidden rounded-full bg-secondary">
                      <div
                        className="h-full bg-primary transition-all"
                        style={{ width: `${job.progress}%` }}
                      />
                    </div>
                    <p className="mt-2 text-sm text-muted-foreground">
                      {job.status} - {job.successful_backtests}/{job.total_backtests} 完成
                    </p>
                  </div>
                ))
              ) : (
                <div className="rounded-lg border border-dashed p-4 text-center text-muted-foreground">
                  <p>暂无运行中的任务</p>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
