import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { PlayCircle, Loader2 } from 'lucide-react'

export default function BacktestPage() {
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
              <label className="text-sm font-medium">选择策略</label>
              <select className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm">
                <option value="">请选择策略...</option>
                <option value="1">双均线策略 (v3)</option>
                <option value="2">MACD动量策略 (v2)</option>
              </select>
            </div>

            {/* Stock selection */}
            <div className="space-y-2">
              <label className="text-sm font-medium">股票池</label>
              <Input placeholder="搜索股票代码或名称..." />
              <div className="flex flex-wrap gap-2">
                <span className="rounded-full bg-primary/10 px-3 py-1 text-sm">
                  sh.600519 贵州茅台
                </span>
                <span className="rounded-full bg-primary/10 px-3 py-1 text-sm">
                  sz.000001 平安银行
                </span>
                <Button variant="ghost" size="sm">
                  + 添加更多
                </Button>
              </div>
            </div>

            {/* Date range */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">开始日期</label>
                <Input type="date" defaultValue="2024-01-01" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">结束日期</label>
                <Input type="date" defaultValue="2024-12-31" />
              </div>
            </div>

            {/* Capital */}
            <div className="space-y-2">
              <label className="text-sm font-medium">初始资金</label>
              <Input type="number" defaultValue="1000000" />
            </div>

            {/* Commission */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <label className="text-sm font-medium">手续费率</label>
                <Input type="number" defaultValue="0.0003" step="0.0001" />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">滑点</label>
                <Input type="number" defaultValue="0.001" step="0.0001" />
              </div>
            </div>

            {/* Submit */}
            <Button className="w-full">
              <PlayCircle className="mr-2 h-4 w-4" />
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
              {/* Running task */}
              <div className="rounded-lg border p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Loader2 className="h-4 w-4 animate-spin text-primary" />
                    <span className="font-medium">双均线策略</span>
                  </div>
                  <span className="text-sm text-muted-foreground">45%</span>
                </div>
                <div className="mt-2 h-2 overflow-hidden rounded-full bg-secondary">
                  <div
                    className="h-full bg-primary transition-all"
                    style={{ width: '45%' }}
                  />
                </div>
                <p className="mt-2 text-sm text-muted-foreground">
                  处理中: sh.600519 (23/50)
                </p>
              </div>

              {/* Queued tasks */}
              <div className="rounded-lg border border-dashed p-4 text-center text-muted-foreground">
                <p>暂无排队任务</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
