import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { formatCurrency, formatPercent } from '@/lib/utils'
import { Search, TrendingUp, TrendingDown } from 'lucide-react'

// Mock data
const stocks = [
  {
    code: 'sh.600519',
    name: '贵州茅台',
    price: 1688.88,
    change: 0.0235,
    volume: 12500000,
    pe: 28.5,
    pb: 8.2,
  },
  {
    code: 'sz.000001',
    name: '平安银行',
    price: 10.25,
    change: -0.0125,
    volume: 85000000,
    pe: 5.2,
    pb: 0.6,
  },
  {
    code: 'sz.300750',
    name: '宁德时代',
    price: 185.50,
    change: 0.0456,
    volume: 35000000,
    pe: 22.8,
    pb: 4.5,
  },
  {
    code: 'sh.601318',
    name: '中国平安',
    price: 42.80,
    change: -0.0085,
    volume: 45000000,
    pe: 8.5,
    pb: 1.1,
  },
  {
    code: 'sz.002594',
    name: '比亚迪',
    price: 225.60,
    change: 0.0312,
    volume: 28000000,
    pe: 35.2,
    pb: 5.8,
  },
]

export default function DataExplorerPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedStock, setSelectedStock] = useState<string | null>(null)

  const filteredStocks = stocks.filter(
    (s) =>
      s.code.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.name.toLowerCase().includes(searchQuery.toLowerCase())
  )

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
            <div className="divide-y">
              {filteredStocks.map((stock) => (
                <button
                  key={stock.code}
                  onClick={() => setSelectedStock(stock.code)}
                  className={`w-full p-4 text-left transition-colors hover:bg-accent ${
                    selectedStock === stock.code ? 'bg-accent' : ''
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="font-medium">{stock.name}</p>
                      <p className="text-sm text-muted-foreground">
                        {stock.code}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="font-medium">{stock.price.toFixed(2)}</p>
                      <div
                        className={`flex items-center justify-end gap-1 text-sm ${
                          stock.change >= 0 ? 'text-profit' : 'text-loss'
                        }`}
                      >
                        {stock.change >= 0 ? (
                          <TrendingUp className="h-3 w-3" />
                        ) : (
                          <TrendingDown className="h-3 w-3" />
                        )}
                        {formatPercent(stock.change)}
                      </div>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Stock detail */}
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>
              {selectedStock
                ? stocks.find((s) => s.code === selectedStock)?.name
                : '选择股票查看详情'}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {selectedStock ? (
              <div className="space-y-6">
                {/* Price info */}
                <div className="grid grid-cols-4 gap-4">
                  {(() => {
                    const stock = stocks.find((s) => s.code === selectedStock)!
                    return (
                      <>
                        <div className="rounded-lg bg-muted p-4">
                          <p className="text-sm text-muted-foreground">
                            当前价格
                          </p>
                          <p className="text-2xl font-bold">
                            {stock.price.toFixed(2)}
                          </p>
                        </div>
                        <div className="rounded-lg bg-muted p-4">
                          <p className="text-sm text-muted-foreground">涨跌幅</p>
                          <p
                            className={`text-2xl font-bold ${
                              stock.change >= 0 ? 'text-profit' : 'text-loss'
                            }`}
                          >
                            {formatPercent(stock.change)}
                          </p>
                        </div>
                        <div className="rounded-lg bg-muted p-4">
                          <p className="text-sm text-muted-foreground">
                            成交量
                          </p>
                          <p className="text-2xl font-bold">
                            {formatCurrency(stock.volume)}
                          </p>
                        </div>
                        <div className="rounded-lg bg-muted p-4">
                          <p className="text-sm text-muted-foreground">PE</p>
                          <p className="text-2xl font-bold">
                            {stock.pe.toFixed(1)}
                          </p>
                        </div>
                      </>
                    )
                  })()}
                </div>

                {/* Chart placeholder */}
                <div className="flex h-[400px] items-center justify-center rounded-lg border border-dashed bg-muted/50">
                  <p className="text-muted-foreground">K线图表区域</p>
                </div>

                {/* Technical indicators placeholder */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="rounded-lg border p-4">
                    <h4 className="font-medium">技术指标</h4>
                    <div className="mt-4 space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MA5</span>
                        <span>1685.20</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MA20</span>
                        <span>1672.50</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">RSI(14)</span>
                        <span>58.5</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">MACD</span>
                        <span className="text-profit">+2.35</span>
                      </div>
                    </div>
                  </div>
                  <div className="rounded-lg border p-4">
                    <h4 className="font-medium">基本面</h4>
                    <div className="mt-4 space-y-2 text-sm">
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">PE(TTM)</span>
                        <span>28.5</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">PB(MRQ)</span>
                        <span>8.2</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">ROE</span>
                        <span>32.5%</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">市值</span>
                        <span>2.12万亿</span>
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
