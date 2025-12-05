import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { formatCurrency, formatPercent } from '@/lib/utils'
import { Search, TrendingUp, TrendingDown, Loader2 } from 'lucide-react'
import {
  useListStocksApiV1StocksGet,
  useGetKlineApiV1StocksCodeKlineGet,
  useGetIndicatorsApiV1StocksCodeIndicatorsGet,
  useGetFundamentalsApiV1StocksCodeFundamentalsGet,
} from '@/api/generated/stocks/stocks'

export default function DataExplorerPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedStock, setSelectedStock] = useState<string | null>(null)

  // Fetch stock list
  const { data: stocksData, isLoading: isLoadingStocks } = useListStocksApiV1StocksGet({
    page_size: 100,
    search: searchQuery || undefined,
  })

  // Fetch K-line data for selected stock
  const { data: klineData } = useGetKlineApiV1StocksCodeKlineGet(
    selectedStock || '',
    { limit: 30 },
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

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold">数据浏览</h1>
        <p className="text-muted-foreground">浏览股票行情和指标数据</p>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        {/* Stock list */}
        <Card className="lg:col-span-1">
          <CardHeader className="pb-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="搜索股票..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-9"
              />
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
                    className={`w-full p-4 text-left transition-colors hover:bg-accent ${
                      selectedStock === stock.code ? 'bg-accent' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-medium">{stock.code_name || stock.code}</p>
                        <p className="text-sm text-muted-foreground">
                          {stock.code}
                        </p>
                      </div>
                      <div className="text-right">
                        <p className="text-sm text-muted-foreground">
                          {stock.exchange?.toUpperCase()}
                        </p>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Stock detail */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>
              {selectedStockInfo
                ? `${selectedStockInfo.code_name} (${selectedStockInfo.code})`
                : '选择股票查看详情'}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {selectedStock && selectedStockInfo ? (
              <div className="space-y-6">
                {/* Price info from K-line */}
                <div className="grid grid-cols-4 gap-4">
                  <div className="rounded-lg bg-muted p-4">
                    <p className="text-sm text-muted-foreground">收盘价</p>
                    <p className="text-2xl font-bold">
                      {latestKline?.close?.toFixed(2) || '-'}
                    </p>
                  </div>
                  <div className="rounded-lg bg-muted p-4">
                    <p className="text-sm text-muted-foreground">涨跌幅</p>
                    <p
                      className={`text-2xl font-bold ${
                        (latestKline?.pct_chg || 0) >= 0 ? 'text-profit' : 'text-loss'
                      }`}
                    >
                      {latestKline?.pct_chg != null
                        ? formatPercent(latestKline.pct_chg / 100)
                        : '-'}
                    </p>
                  </div>
                  <div className="rounded-lg bg-muted p-4">
                    <p className="text-sm text-muted-foreground">成交量</p>
                    <p className="text-2xl font-bold">
                      {latestKline?.volume
                        ? formatCurrency(latestKline.volume)
                        : '-'}
                    </p>
                  </div>
                  <div className="rounded-lg bg-muted p-4">
                    <p className="text-sm text-muted-foreground">PE(TTM)</p>
                    <p className="text-2xl font-bold">
                      {fundamentalsData?.pe_ttm?.toFixed(1) || '-'}
                    </p>
                  </div>
                </div>

                {/* Chart placeholder */}
                <div className="flex h-[400px] items-center justify-center rounded-lg border border-dashed bg-muted/50">
                  <p className="text-muted-foreground">
                    K线图表区域 (共 {klineData?.data?.length || 0} 条数据)
                  </p>
                </div>

                {/* Technical indicators & Fundamentals */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="rounded-lg border p-4">
                    <h4 className="font-medium">技术指标</h4>
                    <div className="mt-4 space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MA5</span>
                        <span>{indicatorsData?.ma_5?.toFixed(2) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MA20</span>
                        <span>{indicatorsData?.ma_20?.toFixed(2) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">RSI(12)</span>
                        <span>{indicatorsData?.rsi_12?.toFixed(1) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MACD DIF</span>
                        <span
                          className={
                            (indicatorsData?.macd_dif || 0) >= 0
                              ? 'text-profit'
                              : 'text-loss'
                          }
                        >
                          {indicatorsData?.macd_dif?.toFixed(3) || '-'}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="rounded-lg border p-4">
                    <h4 className="font-medium">基本面</h4>
                    <div className="mt-4 space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">PE(TTM)</span>
                        <span>{fundamentalsData?.pe_ttm?.toFixed(1) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">PB(MRQ)</span>
                        <span>{fundamentalsData?.pb_mrq?.toFixed(2) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">PS(TTM)</span>
                        <span>{fundamentalsData?.ps_ttm?.toFixed(2) || '-'}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">ST状态</span>
                        <span>{fundamentalsData?.is_st ? '是' : '否'}</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="flex h-[400px] items-center justify-center text-muted-foreground">
                从左侧列表选择股票以查看详情
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
