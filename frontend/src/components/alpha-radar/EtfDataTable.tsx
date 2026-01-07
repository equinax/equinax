import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from '@tanstack/react-table'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { ComputingConsole } from '@/components/ui/computing-console'
import { motion } from 'motion/react'
import { cn } from '@/lib/utils'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import type { EtfScreenerItem } from '@/api/generated/schemas'

interface EtfDataTableProps {
  data: EtfScreenerItem[]
  isLoading?: boolean
  activeDate?: string
}

// Label styles
const LABEL_STYLES: Record<string, { label: string; className: string }> = {
  liquidity_king: { label: '流动性王', className: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300' },
  high_premium: { label: '高溢价', className: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300' },
  medium_premium: { label: '中溢价', className: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300' },
  discount: { label: '折价', className: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' },
  t_plus_zero: { label: 'T+0', className: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300' },
}

// ETF type labels
const ETF_TYPE_LABELS: Record<string, string> = {
  BROAD_BASED: '宽基',
  SECTOR: '行业',
  THEME: '主题',
  CROSS_BORDER: '跨境',
  COMMODITY: '商品',
  BOND: '债券',
  CURRENCY: '货币',
}

// Format change percentage with color
function formatChangePct(value: string | number | null | undefined) {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  const formatted = num >= 0 ? `+${num.toFixed(2)}%` : `${num.toFixed(2)}%`
  const colorClass = num > 0 ? 'text-profit' : num < 0 ? 'text-loss' : 'text-muted-foreground'
  return <span className={colorClass}>{formatted}</span>
}

// Format large numbers in Chinese units
function formatAmount(value: string | number | null | undefined) {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (num >= 1e8) {
    return `${(num / 1e8).toFixed(2)}亿`
  }
  if (num >= 1e4) {
    return `${(num / 1e4).toFixed(0)}万`
  }
  return num.toFixed(0)
}

export function EtfDataTable({
  data,
  isLoading,
  activeDate,
}: EtfDataTableProps) {
  const navigate = useNavigate()

  const handleRowClick = (code: string) => {
    const params = new URLSearchParams()
    if (activeDate) {
      params.set('date', activeDate)
    }
    params.set('from', 'alpha-radar')
    const queryString = params.toString()
    navigate(`/universe/${code}${queryString ? `?${queryString}` : ''}`)
  }

  const { steps, progress } = useComputingProgress(isLoading, 'etf-screener')

  // Define columns
  const columns = useMemo<ColumnDef<EtfScreenerItem>[]>(() => [
    // Code & Name
    {
      accessorKey: 'code',
      header: '代码/名称',
      cell: ({ row }) => {
        const code = row.original.code
        const name = row.original.name || ''
        const etfType = row.original.etf_type

        return (
          <div className="flex items-center gap-1.5 whitespace-nowrap">
            <span className="font-mono text-sm">{code}</span>
            <span className="text-xs text-muted-foreground">{name}</span>
            {etfType && (
              <span className="inline-flex items-center justify-center px-1 h-4 text-[10px] rounded bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-300">
                {ETF_TYPE_LABELS[etfType] || etfType}
              </span>
            )}
          </div>
        )
      },
    },
    // Underlying Index
    {
      accessorKey: 'underlying_index_name',
      header: '跟踪指数',
      cell: ({ row }) => (
        <div className="text-xs text-muted-foreground max-w-[100px] truncate">
          {row.original.underlying_index_name || '-'}
        </div>
      ),
    },
    // Price & Change
    {
      id: 'price_change',
      header: '现价/涨幅',
      cell: ({ row }) => (
        <div className="flex items-center gap-2 whitespace-nowrap">
          <span className="font-mono text-sm">
            {row.original.price ? Number(row.original.price).toFixed(3) : '-'}
          </span>
          <span className="text-xs">
            {formatChangePct(row.original.change_pct)}
          </span>
        </div>
      ),
    },
    // Premium/Discount Rate
    {
      accessorKey: 'discount_rate',
      header: '折溢价率',
      cell: ({ row }) => {
        const value = row.original.discount_rate
        if (value === null || value === undefined) return <span className="text-muted-foreground">-</span>

        const num = Number(value)
        let colorClass = 'text-muted-foreground'
        if (num > 5) colorClass = 'text-red-500 font-bold'
        else if (num > 3) colorClass = 'text-orange-500'
        else if (num < -3) colorClass = 'text-green-500'

        return (
          <span className={cn('font-mono text-sm', colorClass)}>
            {num >= 0 ? '+' : ''}{num.toFixed(2)}%
          </span>
        )
      },
    },
    // Trading Amount
    {
      accessorKey: 'amount',
      header: '成交额',
      cell: ({ row }) => (
        <span className="font-mono text-sm">
          {formatAmount(row.original.amount)}
        </span>
      ),
    },
    // Turnover Rate
    {
      accessorKey: 'turn',
      header: '换手率',
      cell: ({ row }) => {
        const value = row.original.turn
        if (value === null || value === undefined) return '-'
        return (
          <span className="font-mono text-sm">
            {Number(value).toFixed(2)}%
          </span>
        )
      },
    },
    // Score
    {
      accessorKey: 'score',
      header: '评分',
      cell: ({ row }) => {
        const score = Number(row.original.score || 0)
        return (
          <div className="flex items-center gap-2 w-[80px]">
            <span className="text-sm font-medium w-8">{score.toFixed(0)}</span>
            <Progress value={score} className="h-1.5 flex-1" />
          </div>
        )
      },
    },
    // Labels
    {
      accessorKey: 'labels',
      header: '标签',
      cell: ({ row }) => {
        const labels = row.original.labels || []
        if (labels.length === 0) return <span className="text-muted-foreground">-</span>
        return (
          <div className="flex flex-wrap gap-1 max-w-[120px]">
            {labels.slice(0, 2).map((label) => {
              const style = LABEL_STYLES[label]
              if (!style) return null
              return (
                <Badge key={label} variant="secondary" className={cn('text-[10px] px-1.5 py-0', style.className)}>
                  {style.label}
                </Badge>
              )
            })}
          </div>
        )
      },
    },
    // Fund Company
    {
      accessorKey: 'fund_company',
      header: '基金公司',
      cell: ({ row }) => (
        <div className="text-xs text-muted-foreground max-w-[60px] truncate">
          {row.original.fund_company?.replace(/基金管理有限公司|基金管理公司|基金/, '') || '-'}
        </div>
      ),
    },
    // Management Fee
    {
      accessorKey: 'management_fee',
      header: '费率',
      cell: ({ row }) => {
        const value = row.original.management_fee
        if (value === null || value === undefined) return '-'
        const num = Number(value) * 100
        return (
          <span className="font-mono text-xs text-muted-foreground">
            {num.toFixed(2)}%
          </span>
        )
      },
    },
  ], [])

  // Create table instance
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  // Loading state with computing console
  if (isLoading) {
    return (
      <div className="rounded-md border p-4">
        <ComputingConsole
          title="正在加载ETF数据..."
          steps={steps}
          progress={progress}
        />
      </div>
    )
  }

  return (
    <div className="rounded-md border overflow-hidden">
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <TableHead key={header.id} className="text-xs h-8 px-2">
                  {header.isPlaceholder
                    ? null
                    : flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {table.getRowModel().rows?.length ? (
            table.getRowModel().rows.map((row, index) => (
              <motion.tr
                key={row.id}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{
                  duration: 0.2,
                  delay: index * 0.02,
                  ease: 'easeOut',
                }}
                className="cursor-pointer hover:bg-muted/50 border-b transition-colors"
                onClick={() => handleRowClick(row.original.code)}
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="py-1 px-2">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </motion.tr>
            ))
          ) : (
            <TableRow>
              <TableCell colSpan={columns.length} className="h-24 text-center">
                暂无数据
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  )
}
