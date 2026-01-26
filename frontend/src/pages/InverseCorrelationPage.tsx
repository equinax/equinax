import { useState, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from '@tanstack/react-table'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { ArrowLeft, Shuffle, RefreshCw, Play, Crown } from 'lucide-react'
import { Checkbox } from '@/components/ui/checkbox'
import { cn } from '@/lib/utils'
import { useGetCorrelationAnalysisApiV1UniverseCodeCorrelationGet } from '@/api/generated/universe-cockpit/universe-cockpit'
import type { CorrelationItem, WindowDays } from '@/api/generated/schemas'
import { formatMarketCap, getPriceChangeColor, formatPriceChange } from '@/lib/universe-colors'

function isLeaderStock(item: CorrelationItem): boolean {
  const marketCap = item.market_cap ? Number(item.market_cap) : null
  const turnover = item.turnover ? Number(item.turnover) : null
  
  if (marketCap === null || turnover === null) return false
  
  const marketCapInYi = marketCap / 1e8
  return marketCapInYi >= 30 && marketCapInYi <= 1000 && turnover >= 3 && turnover <= 25
}

// Time window options
const WINDOW_OPTIONS = [
  { value: '30', label: '30天', description: '近1个月' },
  { value: '60', label: '60天', description: '近2个月' },
  { value: '90', label: '90天', description: '近3个月' },
  { value: '180', label: '半年', description: '近6个月' },
  { value: '250', label: '一年', description: '约250交易日' },
] as const

// Correlation type badge styles
const CORRELATION_TYPE_STYLES: Record<string, { label: string; className: string }> = {
  inverse: { 
    label: '反向节奏', 
    className: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300' 
  },
  sync: { 
    label: '同步', 
    className: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' 
  },
  neutral: { 
    label: '中性', 
    className: 'bg-gray-100 text-gray-700 dark:bg-gray-900/30 dark:text-gray-300' 
  },
}

// Asset type badge styles
const ASSET_TYPE_STYLES: Record<string, { label: string; className: string }> = {
  STOCK: { label: '股票', className: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300' },
  ETF: { label: 'ETF', className: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300' },
}

// Format correlation value with color
function formatCorrelation(value: number) {
  const formatted = value.toFixed(4)
  const colorClass = value < -0.6 
    ? 'text-blue-600 dark:text-blue-400 font-semibold' 
    : value < -0.4 
      ? 'text-blue-500 dark:text-blue-300' 
      : 'text-muted-foreground'
  return <span className={cn('font-mono', colorClass)}>{formatted}</span>
}

export default function InverseCorrelationPage() {
  const { code } = useParams<{ code: string }>()
  const navigate = useNavigate()
  
  // State for time window selection
  const [windowDays, setWindowDays] = useState<WindowDays>(60 as WindowDays)
  
  // State for row selection
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set())
  
  // State for leader filter
  const [onlyLeader, setOnlyLeader] = useState(false)

  // Fetch correlation data
  const { data, isLoading, refetch, isFetching } = useGetCorrelationAnalysisApiV1UniverseCodeCorrelationGet(
    code || '',
    {
      window: windowDays,
      threshold: -0.4,
      limit: 100,
      include_stocks: true,
      include_etfs: true,
    },
    { query: { enabled: !!code } }
  )

  const filteredItems = useMemo(() => {
    if (!data?.items) return []
    if (!onlyLeader) return data.items
    return data.items.filter(isLeaderStock)
  }, [data?.items, onlyLeader])

  const handleBack = () => {
    navigate(`/universe/${code}`)
  }

  const handleRowClick = (itemCode: string) => {
    navigate(`/universe/${itemCode}`)
  }

  const handleSelectRow = (itemCode: string, checked: boolean) => {
    setSelectedRows(prev => {
      const next = new Set(prev)
      if (checked) {
        next.add(itemCode)
      } else {
        next.delete(itemCode)
      }
      return next
    })
  }

  const handleDynamicSimulation = () => {
    if (selectedRows.size === 0) return
    const stockCodes = Array.from(selectedRows)
    const params = new URLSearchParams()
    if (code) {
      params.set('base', code)
    }
    params.set('stocks', stockCodes.join(','))
    navigate(`/backtest/dynamic?${params.toString()}`)
  }

  // Table columns definition
  const columns: ColumnDef<CorrelationItem>[] = [
    {
      id: 'select',
      header: () => null,
      cell: ({ row }) => (
        <Checkbox
          checked={selectedRows.has(row.original.code)}
          onCheckedChange={(checked) => handleSelectRow(row.original.code, !!checked)}
          onClick={(e) => e.stopPropagation()}
          aria-label="选择行"
        />
      ),
      size: 40,
    },
    {
      accessorKey: 'code',
      header: '代码',
      cell: ({ row }) => (
        <div className="font-mono text-sm">{row.getValue('code')}</div>
      ),
    },
    {
      accessorKey: 'name',
      header: '名称',
      cell: ({ row }) => (
        <div className="font-medium">{row.getValue('name')}</div>
      ),
    },
    {
      accessorKey: 'asset_type',
      header: '类型',
      cell: ({ row }) => {
        const type = row.getValue('asset_type') as string
        const style = ASSET_TYPE_STYLES[type] || { label: type, className: '' }
        return (
          <Badge variant="secondary" className={cn('text-xs', style.className)}>
            {style.label}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'correlation',
      header: '相关系数',
      cell: ({ row }) => formatCorrelation(row.getValue('correlation')),
    },
    {
      accessorKey: 'correlation_type',
      header: '节奏类型',
      cell: ({ row }) => {
        const type = row.getValue('correlation_type') as string
        const style = CORRELATION_TYPE_STYLES[type] || { label: type, className: '' }
        return (
          <Badge variant="secondary" className={cn('text-xs', style.className)}>
            {style.label}
          </Badge>
        )
      },
    },
    {
      accessorKey: 'industry_l1',
      header: '行业',
      cell: ({ row }) => (
        <span className="text-sm text-muted-foreground">
          {row.getValue('industry_l1') || '-'}
        </span>
      ),
    },
    {
      accessorKey: 'price',
      header: '现价',
      cell: ({ row }) => {
        const price = row.getValue('price') as number | null
        return (
          <span className="font-mono text-sm">
            {price != null ? `¥${Number(price).toFixed(2)}` : '-'}
          </span>
        )
      },
    },
    {
      accessorKey: 'change_pct',
      header: '涨跌幅',
      cell: ({ row }) => {
        const change = row.getValue('change_pct') as number | null
        if (change == null) return <span className="text-muted-foreground">-</span>
        return (
          <span className={cn('font-mono text-sm', getPriceChangeColor(change))}>
            {formatPriceChange(change)}
          </span>
        )
      },
    },
    {
      accessorKey: 'market_cap',
      header: '市值',
      cell: ({ row }) => (
        <span className="font-mono text-sm">
          {formatMarketCap(row.getValue('market_cap'))}
        </span>
      ),
    },
  ]

  const table = useReactTable({
    data: filteredItems,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-12 w-full" />
        <Skeleton className="h-[600px] w-full" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="icon"
            onClick={handleBack}
          >
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <div className="flex items-center gap-2">
              <Shuffle className="h-5 w-5 text-muted-foreground" />
              <h1 className="text-2xl font-bold">反向节奏分析</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              基准: <span className="font-mono font-medium">{data?.reference_name}</span>
              <span className="mx-2">·</span>
              <span className="font-mono">{data?.reference_code}</span>
              <span className="mx-2">·</span>
              计算日期: {data?.calculation_date}
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          {/* Leader filter toggle */}
          <div className="flex items-center gap-2">
            <Switch
              id="leader-filter"
              checked={onlyLeader}
              onCheckedChange={setOnlyLeader}
            />
            <Label htmlFor="leader-filter" className="flex items-center gap-1 text-sm cursor-pointer">
              <Crown className="h-4 w-4 text-amber-500" />
              只看龙头
            </Label>
          </div>
          
          {/* Time window selector */}
          <Select
            value={String(windowDays)}
            onValueChange={(value) => setWindowDays(Number(value) as WindowDays)}
          >
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="选择时间窗口" />
            </SelectTrigger>
            <SelectContent>
              {WINDOW_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
          >
            <RefreshCw className={cn('h-4 w-4 mr-1.5', isFetching && 'animate-spin')} />
            刷新
          </Button>
        </div>
      </div>

      {/* Algorithm explanation */}
      <Card>
        <CardHeader className="py-3">
          <CardTitle className="text-sm">算法说明</CardTitle>
          <CardDescription>
            使用皮尔森相关系数计算与基准资产的价格走势相关性，筛选负相关标的用于对冲配置。
          </CardDescription>
        </CardHeader>
        <CardContent className="pb-3">
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-blue-500" />
              <span>ρ &lt; -0.4: 反向节奏（理想对冲）</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-gray-400" />
              <span>-0.4 ≤ ρ ≤ 0.4: 节奏中性（震荡配仓）</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-green-500" />
              <span>ρ &gt; 0.7: 高度同步（同节奏）</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Results table */}
      <Card>
        <CardHeader className="py-3 flex flex-row items-center justify-between">
          <div>
            <CardTitle className="text-sm">负相关标的列表</CardTitle>
            <CardDescription>
              共找到 {filteredItems.length} 个与基准负相关的股票/ETF{onlyLeader && '（仅龙头）'}，按相关系数升序排列
            </CardDescription>
          </div>
          <Button
            variant="default"
            size="sm"
            onClick={handleDynamicSimulation}
            disabled={selectedRows.size === 0}
          >
            <Play className="h-4 w-4 mr-1.5" />
            动态模拟 {selectedRows.size > 0 && `(${selectedRows.size})`}
          </Button>
        </CardHeader>
        <CardContent className="p-0">
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                {table.getHeaderGroups().map((headerGroup) => (
                  <TableRow key={headerGroup.id}>
                    {headerGroup.headers.map((header) => (
                      <TableHead key={header.id}>
                        {header.isPlaceholder
                          ? null
                          : flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                      </TableHead>
                    ))}
                  </TableRow>
                ))}
              </TableHeader>
              <TableBody>
                {table.getRowModel().rows?.length ? (
                  table.getRowModel().rows.map((row) => (
                    <TableRow
                      key={row.id}
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() => handleRowClick(row.original.code)}
                    >
                      {row.getVisibleCells().map((cell) => (
                        <TableCell key={cell.id}>
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext()
                          )}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                ) : (
                  <TableRow>
                    <TableCell
                      colSpan={columns.length}
                      className="h-24 text-center"
                    >
                      {isFetching ? (
                        <div className="flex items-center justify-center gap-2">
                          <RefreshCw className="h-4 w-4 animate-spin" />
                          <span>计算中...</span>
                        </div>
                      ) : (
                        '未找到符合条件的负相关标的'
                      )}
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
