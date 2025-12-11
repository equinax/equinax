import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Plus, Search, Play, Edit, Trash2, Code2, CheckCircle, XCircle, Loader2 } from 'lucide-react'
import {
  useListStrategiesApiV1StrategiesGet,
  useDeleteStrategyApiV1StrategiesStrategyIdDelete,
} from '@/api/generated/strategies/strategies'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'

const strategyTypes: Record<string, string> = {
  trend_following: '趋势跟踪',
  momentum: '动量策略',
  mean_reversion: '均值回归',
  arbitrage: '套利策略',
  other: '其他',
}

export default function StrategiesPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; name: string } | null>(null)
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  // Fetch strategies
  const { data: strategiesData, isLoading } = useListStrategiesApiV1StrategiesGet({
    page: 1,
    page_size: 50,
    search: searchQuery || undefined,
  })

  // Delete mutation
  const deleteMutation = useDeleteStrategyApiV1StrategiesStrategyIdDelete({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: ['/api/v1/strategies'] })
        setDeleteTarget(null)
      },
    },
  })

  const strategies = strategiesData?.items || []

  const handleDelete = (id: string, name: string) => {
    setDeleteTarget({ id, name })
  }

  const handleConfirmDelete = () => {
    if (deleteTarget) {
      deleteMutation.mutate({ strategyId: deleteTarget.id })
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">策略管理</h1>
          <p className="text-muted-foreground">创建和管理量化交易策略</p>
        </div>
        <Button onClick={() => navigate('/strategies/new')}>
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

      {/* Loading state */}
      {isLoading && (
        <div className="flex items-center justify-center p-12">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* Strategy cards */}
      {!isLoading && strategies.length > 0 && (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {strategies.map((strategy) => (
            <Card key={strategy.id} className="flex flex-col">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-2">
                    <Code2 className="h-5 w-5 text-primary" />
                    <CardTitle className="text-lg">{strategy.name}</CardTitle>
                  </div>
                  <div className="flex items-center gap-1">
                    {strategy.is_validated ? (
                      <CheckCircle className="h-4 w-4 text-profit" />
                    ) : (
                      <XCircle className="h-4 w-4 text-muted-foreground" />
                    )}
                  </div>
                </div>
                <CardDescription>
                  {strategy.description || '暂无描述'}
                </CardDescription>
              </CardHeader>
              <CardContent className="flex-1">
                <div className="space-y-4">
                  {/* Metadata */}
                  <div className="flex flex-wrap gap-2 text-sm">
                    {strategy.strategy_type && (
                      <span className="rounded-full bg-secondary px-2 py-1">
                        {strategyTypes[strategy.strategy_type] || strategy.strategy_type}
                      </span>
                    )}
                    <span className="rounded-full bg-secondary px-2 py-1">
                      v{strategy.version}
                    </span>
                    <span
                      className={`rounded-full px-2 py-1 ${
                        strategy.is_active
                          ? 'bg-profit/20 text-profit'
                          : 'bg-muted text-muted-foreground'
                      }`}
                    >
                      {strategy.is_active ? '启用' : '禁用'}
                    </span>
                  </div>

                  {/* Indicators */}
                  {strategy.indicators_used && strategy.indicators_used.length > 0 && (
                    <div className="text-sm text-muted-foreground">
                      指标: {strategy.indicators_used.join(', ')}
                    </div>
                  )}

                  {/* Actions */}
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-1"
                      onClick={() => navigate(`/strategies/${strategy.id}`)}
                    >
                      <Edit className="mr-2 h-4 w-4" />
                      编辑
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-1"
                      onClick={() => navigate(`/backtest?strategy=${strategy.id}`)}
                    >
                      <Play className="mr-2 h-4 w-4" />
                      回测
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleDelete(strategy.id, strategy.name)}
                      disabled={deleteMutation.isPending}
                    >
                      <Trash2 className="h-4 w-4 text-muted-foreground" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Empty state */}
      {!isLoading && strategies.length === 0 && (
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

      {/* Pagination info */}
      {strategiesData && strategiesData.total > 0 && (
        <div className="text-sm text-muted-foreground text-center">
          共 {strategiesData.total} 个策略，第 {strategiesData.page}/{strategiesData.pages} 页
        </div>
      )}

      {/* Delete confirmation dialog */}
      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除策略 "{deleteTarget?.name}" 吗？此操作不可撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmDelete}>删除</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
