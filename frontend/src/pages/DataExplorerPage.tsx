import { useState } from 'react'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { cn } from '@/lib/utils'
import {
  Search,
  TrendingUp,
  TrendingDown,
  Loader2,
  LineChart,
  BarChart3,
  History,
  Calendar,
  PieChart,
} from 'lucide-react'
import {
  useListStocksApiV1StocksGet,
  useGetKlineApiV1StocksCodeKlineGet,
  useGetIndicatorsApiV1StocksCodeIndicatorsGet,
  useGetFundamentalsApiV1StocksCodeFundamentalsGet,
} from '@/api/generated/stocks/stocks'
import { StockKlineChart } from '@/components/stock/StockKlineChart'
import { StockMetricsGrid } from '@/components/stock/StockMetricsGrid'
import { StockIndicators } from '@/components/stock/StockIndicators'
import { StockHistoryTable } from '@/components/stock/StockHistoryTable'

type ExchangeFilter = 'all' | 'sh' | 'sz'
type AssetTypeFilter = 'STOCK' | 'ETF'

export default function DataExplorerPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedStock, setSelectedStock] = useState<string | null>(null)
  const [exchangeFilter, setExchangeFilter] = useState<ExchangeFilter>('all')
  const [assetTypeFilter, setAssetTypeFilter] = useState<AssetTypeFilter>('STOCK')

  // Fetch stock list
  const { data: stocksData, isLoading: isLoadingStocks } = useListStocksApiV1StocksGet({
    page_size: 100,
    search: searchQuery || undefined,
    exchange: exchangeFilter === 'all' ? undefined : exchangeFilter,
    asset_type: assetTypeFilter,
  })

  // Fetch K-line data for selected stock (get more data for chart)
  const { data: klineData, isLoading: isLoadingKline } = useGetKlineApiV1StocksCodeKlineGet(
    selectedStock || '',
    { limit: 365 },
    { query: { enabled: !!selectedStock } }
  )

  // Fetch technical indicators for selected stock
  const { data: indicatorsData } = useGetIndicatorsApiV1StocksCodeIndicatorsGet(
    selectedStock || '',
    {},
    { query: { enabled: !!selectedStock } }
  )

  // Fetch fundamentals for selected stock
  const { data: fundamentalsData } = useGetFundamentalsApiV1StocksCodeFundamentalsGet(
    selectedStock || '',
    { query: { enabled: !!selectedStock } }
  )

  const stocks = stocksData?.items || []
  const selectedStockInfo = stocks.find((s) => s.code === selectedStock)

  // Get latest K-line data for price info
  const latestKline = klineData?.data?.[klineData.data.length - 1]
  const priceChange = Number(latestKline?.pct_chg) || 0
  const isPositive = priceChange >= 0

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">数据浏览</h1>
          <p className="text-muted-foreground">
            浏览{assetTypeFilter === 'STOCK' ? '股票' : 'ETF'}行情和指标数据
          </p>
        </div>
        {/* Asset type filter */}
        <div className="flex gap-2">
          <Button
            variant={assetTypeFilter === 'STOCK' ? 'default' : 'outline'}
            onClick={() => {
              setAssetTypeFilter('STOCK')
              setSelectedStock(null)
            }}
          >
            <TrendingUp className="h-4 w-4 mr-2" />
            股票
          </Button>
          <Button
            variant={assetTypeFilter === 'ETF' ? 'default' : 'outline'}
            onClick={() => {
              setAssetTypeFilter('ETF')
              setSelectedStock(null)
            }}
          >
            <PieChart className="h-4 w-4 mr-2" />
            ETF
          </Button>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        {/* Stock list */}
        <Card className="lg:col-span-1">
          <CardHeader className="pb-3 space-y-3">
            {/* Search */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder={assetTypeFilter === 'STOCK' ? '搜索股票...' : '搜索ETF...'}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9"
              />
            </div>
            {/* Exchange filter */}
            <div className="flex gap-1">
              <Button
                variant={exchangeFilter === 'all' ? 'default' : 'outline'}
                size="sm"
                onClick={() => setExchangeFilter('all')}
              >
                全部
              </Button>
              <Button
                variant={exchangeFilter === 'sh' ? 'default' : 'outline'}
                size="sm"
                onClick={() => setExchangeFilter('sh')}
              >
                上海
              </Button>
              <Button
                variant={exchangeFilter === 'sz' ? 'default' : 'outline'}
                size="sm"
                onClick={() => setExchangeFilter('sz')}
              >
                深圳
              </Button>
            </div>
          </CardHeader>
          <CardContent className="max-h-[600px] overflow-auto p-0">
            {isLoadingStocks ? (
              <div className="flex items-center justify-center p-8">
                <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              </div>
            ) : stocks.length === 0 ? (
              <div className="p-8 text-center text-muted-foreground">
                暂无数据
              </div>
            ) : (
              <div className="divide-y">
                {stocks.map((stock) => (
                  <button
                    key={stock.code}
                    onClick={() => setSelectedStock(stock.code)}
                    className={cn(
                      'w-full p-4 text-left transition-colors hover:bg-accent',
                      selectedStock === stock.code && 'bg-accent'
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <div className="min-w-0 flex-1">
                        <p className="font-medium truncate">
                          {stock.code_name || stock.code}
                        </p>
                        <p className="text-sm text-muted-foreground">
                          {stock.code}
                        </p>
                      </div>
                      <Badge variant="secondary" className="ml-2 shrink-0">
                        {stock.exchange?.toUpperCase()}
                      </Badge>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Stock detail */}
        <div className="lg:col-span-2 space-y-4">
          {selectedStock && selectedStockInfo ? (
            <>
              {/* Header Card */}
              <Card>
                <CardContent className="pt-6">
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2 mb-1">
                        <h2 className="text-2xl font-bold">
                          {selectedStockInfo.code_name || selectedStockInfo.code}
                        </h2>
                        <Badge variant="outline">
                          {selectedStockInfo.exchange?.toUpperCase()}
                        </Badge>
                        {selectedStockInfo.industry && (
                          <Badge variant="secondary">
                            {selectedStockInfo.industry}
                          </Badge>
                        )}
                      </div>
                      <p className="text-muted-foreground">
                        {selectedStockInfo.code}
                      </p>
                    </div>
                    <div className="text-right">
                      {isLoadingKline ? (
                        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                      ) : latestKline ? (
                        <>
                          <p className={cn(
                            'text-3xl font-bold',
                            isPositive ? 'text-profit' : 'text-loss'
                          )}>
                            ¥{Number(latestKline.close)?.toFixed(2)}
                          </p>
                          <div className={cn(
                            'flex items-center justify-end gap-1',
                            isPositive ? 'text-profit' : 'text-loss'
                          )}>
                            {isPositive ? (
                              <TrendingUp className="h-4 w-4" />
                            ) : (
                              <TrendingDown className="h-4 w-4" />
                            )}
                            <span className="font-medium">
                              {priceChange >= 0 ? '+' : ''}{priceChange.toFixed(2)}%
                            </span>
                          </div>
                          <p className="text-sm text-muted-foreground flex items-center justify-end gap-1 mt-1">
                            <Calendar className="h-3 w-3" />
                            {latestKline.date}
                          </p>
                        </>
                      ) : null}
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Metrics Grid */}
              <StockMetricsGrid
                kline={latestKline}
                fundamentals={fundamentalsData}
              />

              {/* Tabs for different views */}
              <Card>
                <CardContent className="pt-6">
                  <Tabs defaultValue="chart" className="space-y-4">
                    <TabsList>
                      <TabsTrigger value="chart" className="gap-1">
                        <LineChart className="h-4 w-4" />
                        价格走势
                      </TabsTrigger>
                      <TabsTrigger value="indicators" className="gap-1">
                        <BarChart3 className="h-4 w-4" />
                        技术分析
                      </TabsTrigger>
                      <TabsTrigger value="history" className="gap-1">
                        <History className="h-4 w-4" />
                        历史数据
                      </TabsTrigger>
                    </TabsList>

                    <TabsContent value="chart">
                      {isLoadingKline ? (
                        <div className="flex items-center justify-center h-[400px]">
                          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                        </div>
                      ) : (
                        <StockKlineChart data={klineData?.data || []} />
                      )}
                    </TabsContent>

                    <TabsContent value="indicators">
                      <StockIndicators
                        data={indicatorsData}
                        currentPrice={latestKline?.close}
                      />
                    </TabsContent>

                    <TabsContent value="history">
                      <StockHistoryTable
                        data={klineData?.data || []}
                        stockCode={selectedStock}
                      />
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>
            </>
          ) : (
            <Card>
              <CardContent className="flex h-[500px] items-center justify-center">
                <div className="text-center text-muted-foreground">
                  {assetTypeFilter === 'ETF' ? (
                    <PieChart className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  ) : (
                    <LineChart className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  )}
                  <p>从左侧列表选择{assetTypeFilter === 'STOCK' ? '股票' : 'ETF'}以查看详情</p>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  )
}
