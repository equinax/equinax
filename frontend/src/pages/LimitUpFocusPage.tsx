import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ArrowLeft, TrendingUp, Filter } from 'lucide-react'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { useGetLimitUpListApiV1MarketLimitUpGet } from '@/api/generated/market/market'

// 筛选选项
const timeRangeOptions = [
  { value: '7', label: '最近7天' },
  { value: '30', label: '最近30天' },
  { value: '90', label: '最近90天' },
  { value: '365', label: '近一年' },
]

const consecutiveOptions = [
  { value: '0', label: '全部' },
  { value: '1', label: '首板' },
  { value: '2', label: '2连板+' },
  { value: '3', label: '3连板+' },
  { value: '5', label: '5连板+' },
]

const countOptions = [
  { value: '0', label: '全部' },
  { value: '3', label: '3次+' },
  { value: '5', label: '5次+' },
  { value: '10', label: '10次+' },
]

// 格式化函数
const formatVolume = (vol: number) => {
  if (vol >= 100000000) return `${(vol / 100000000).toFixed(2)}亿`
  if (vol >= 10000) return `${(vol / 10000).toFixed(0)}万`
  return vol.toLocaleString()
}

const formatAmount = (amt: number) => {
  if (amt >= 100000000) return `${(amt / 100000000).toFixed(2)}亿`
  if (amt >= 10000) return `${(amt / 10000).toFixed(0)}万`
  return amt.toLocaleString()
}

const formatDate = (dateStr: string) => {
  const d = new Date(dateStr)
  return `${d.getMonth() + 1}/${d.getDate()}`
}

export default function LimitUpFocusPage() {
  const navigate = useNavigate()
  
  // 筛选状态
  const [days, setDays] = useState('365')
  const [minConsecutive, setMinConsecutive] = useState('0')
  const [minCount, setMinCount] = useState('0')
  const [page, setPage] = useState(1)
  const pageSize = 50
  
  // 获取数据
  const { data, isLoading } = useGetLimitUpListApiV1MarketLimitUpGet({
    days: parseInt(days),
    min_consecutive: parseInt(minConsecutive),
    min_count: parseInt(minCount),
    page,
    page_size: pageSize,
  })
  
  const items = data?.items || []
  const total = data?.total || 0
  const totalPages = Math.ceil(total / pageSize)
  
  // 处理行点击
  const handleRowClick = (code: string) => {
    navigate(`/universe/${code}`)
  }
  
  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon"
          onClick={() => navigate('/universe')}
        >
          <ArrowLeft className="h-5 w-5" />
        </Button>
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <TrendingUp className="h-6 w-6 text-profit" />
            涨停聚焦
          </h1>
          <p className="text-muted-foreground text-sm">
            近一年涨停股票统计，按最近涨停日期排序
          </p>
        </div>
      </div>
      
      {/* Filters */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">筛选条件</span>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap items-center gap-4">
            {/* 时间范围 */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">时间范围:</span>
              <Select value={days} onValueChange={(v) => { setDays(v); setPage(1) }}>
                <SelectTrigger className="w-[120px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {timeRangeOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            {/* 连板筛选 */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">连板数:</span>
              <Select value={minConsecutive} onValueChange={(v) => { setMinConsecutive(v); setPage(1) }}>
                <SelectTrigger className="w-[100px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {consecutiveOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            {/* 涨停次数 */}
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">涨停次数:</span>
              <Select value={minCount} onValueChange={(v) => { setMinCount(v); setPage(1) }}>
                <SelectTrigger className="w-[100px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {countOptions.map((opt) => (
                    <SelectItem key={opt.value} value={opt.value}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            {/* 统计信息 */}
            <div className="ml-auto text-sm text-muted-foreground">
              共 <span className="font-mono font-medium text-foreground">{total.toLocaleString()}</span> 只股票
            </div>
          </div>
        </CardContent>
      </Card>
      
      {/* Data Table */}
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[100px]">代码</TableHead>
                <TableHead>名称</TableHead>
                <TableHead className="text-center">最近涨停</TableHead>
                <TableHead className="text-right">涨幅</TableHead>
                <TableHead className="text-center">连板</TableHead>
                <TableHead className="text-center">涨停次数</TableHead>
                <TableHead className="text-right">成交量</TableHead>
                <TableHead className="text-right">成交额</TableHead>
                <TableHead className="text-right">换手率</TableHead>
                <TableHead className="text-right">收盘价</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading ? (
                // Loading skeleton
                Array.from({ length: 10 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 10 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-4 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="text-center py-8 text-muted-foreground">
                    暂无数据
                  </TableCell>
                </TableRow>
              ) : (
                items.map((item) => (
                  <TableRow
                    key={item.code}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => handleRowClick(item.code)}
                  >
                    <TableCell className="font-mono text-sm">{item.code}</TableCell>
                    <TableCell className="font-medium">{item.name}</TableCell>
                    <TableCell className="text-center">{formatDate(item.latest_date)}</TableCell>
                    <TableCell className="text-right text-profit font-medium">
                      +{item.pct_chg.toFixed(2)}%
                    </TableCell>
                    <TableCell className="text-center">
                      <Badge
                        variant={item.consecutive >= 2 ? 'destructive' : item.consecutive >= 1 ? 'default' : 'secondary'}
                        className="font-medium"
                      >
                        {item.consecutive_label}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-center font-mono">
                      {item.total_count}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {formatVolume(item.volume)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {formatAmount(item.amount)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-sm">
                      {item.turn > 0 ? `${item.turn.toFixed(2)}%` : '-'}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      ¥{item.close.toFixed(2)}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      
      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            第 {page} / {totalPages} 页
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
            >
              上一页
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
            >
              下一页
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
