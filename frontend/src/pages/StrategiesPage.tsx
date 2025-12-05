import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Plus, Search, Play, Edit, Trash2, Code2, CheckCircle, XCircle } from 'lucide-react'

// Mock data
const strategies = [
  {
    id: '1',
    name: '双均线策略',
    description: '基于5日和20日均线交叉的经典趋势策略',
    type: 'trend_following',
    version: 3,
    isValidated: true,
    isActive: true,
    lastBacktest: {
      return: 0.245,
      sharpe: 1.82,
    },
    createdAt: '2024-01-15',
  },
  {
    id: '2',
    name: 'MACD动量策略',
    description: '基于MACD指标的动量突破策略',
    type: 'momentum',
    version: 2,
    isValidated: true,
    isActive: true,
    lastBacktest: {
      return: 0.156,
      sharpe: 1.45,
    },
    createdAt: '2024-01-10',
  },
  {
    id: '3',
    name: 'RSI反转策略',
    description: '基于RSI超买超卖的均值回归策略',
    type: 'mean_reversion',
    version: 1,
    isValidated: false,
    isActive: false,
    lastBacktest: null,
    createdAt: '2024-01-20',
  },
]

const strategyTypes: Record<string, string> = {
  trend_following: '趋势跟踪',
  momentum: '动量策略',
  mean_reversion: '均值回归',
  arbitrage: '套利策略',
}

export default function StrategiesPage() {
  const [searchQuery, setSearchQuery] = useState('')

  const filteredStrategies = strategies.filter(
    (s) =>
      s.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.description.toLowerCase().includes(searchQuery.toLowerCase())
  )

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">策略管理</h1>
          <p className="text-muted-foreground">创建和管理量化交易策略</p>
        </div>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          新建策略
        </Button>
      </div>

      {/* Search */}
      <div className="flex items-center gap-4">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="搜索策略..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-9"
          />
        </div>
      </div>

      {/* Strategy cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {filteredStrategies.map((strategy) => (
          <Card key={strategy.id} className="flex flex-col">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-2">
                  <Code2 className="h-5 w-5 text-primary" />
                  <CardTitle className="text-lg">{strategy.name}</CardTitle>
                </div>
                <div className="flex items-center gap-1">
                  {strategy.isValidated ? (
                    <CheckCircle className="h-4 w-4 text-profit" />
                  ) : (
                    <XCircle className="h-4 w-4 text-muted-foreground" />
                  )}
                </div>
              </div>
              <CardDescription>{strategy.description}</CardDescription>
            </CardHeader>
            <CardContent className="flex-1">
              <div className="space-y-4">
                {/* Metadata */}
                <div className="flex flex-wrap gap-2 text-sm">
                  <span className="rounded-full bg-secondary px-2 py-1">
                    {strategyTypes[strategy.type]}
                  </span>
                  <span className="rounded-full bg-secondary px-2 py-1">
                    v{strategy.version}
                  </span>
                  <span
                    className={`rounded-full px-2 py-1 ${
                      strategy.isActive
                        ? 'bg-profit/20 text-profit'
                        : 'bg-muted text-muted-foreground'
                    }`}
                  >
                    {strategy.isActive ? '启用' : '禁用'}
                  </span>
                </div>

                {/* Last backtest results */}
                {strategy.lastBacktest && (
                  <div className="grid grid-cols-2 gap-4 rounded-lg bg-muted/50 p-3">
                    <div>
                      <p className="text-xs text-muted-foreground">最近收益</p>
                      <p
                        className={`font-medium ${
                          strategy.lastBacktest.return >= 0
                            ? 'text-profit'
                            : 'text-loss'
                        }`}
                      >
                        {strategy.lastBacktest.return >= 0 ? '+' : ''}
                        {(strategy.lastBacktest.return * 100).toFixed(1)}%
                      </p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">夏普比率</p>
                      <p className="font-medium">
                        {strategy.lastBacktest.sharpe.toFixed(2)}
                      </p>
                    </div>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-2">
                  <Button variant="outline" size="sm" className="flex-1">
                    <Edit className="mr-2 h-4 w-4" />
                    编辑
                  </Button>
                  <Button variant="outline" size="sm" className="flex-1">
                    <Play className="mr-2 h-4 w-4" />
                    回测
                  </Button>
                  <Button variant="ghost" size="icon">
                    <Trash2 className="h-4 w-4 text-muted-foreground" />
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Empty state */}
      {filteredStrategies.length === 0 && (
        <Card className="p-12 text-center">
          <Code2 className="mx-auto h-12 w-12 text-muted-foreground" />
          <h3 className="mt-4 text-lg font-semibold">未找到策略</h3>
          <p className="mt-2 text-muted-foreground">
            {searchQuery
              ? '尝试调整搜索条件'
              : '点击"新建策略"创建您的第一个量化策略'}
          </p>
        </Card>
      )}
    </div>
  )
}
