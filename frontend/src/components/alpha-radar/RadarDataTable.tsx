import { useMemo } from 'react'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type OnChangeFn,
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
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import type { ScreenerItem, TimeMode } from '@/api/generated/schemas'

interface RadarDataTableProps {
  data: ScreenerItem[]
  isLoading?: boolean
  sorting: SortingState
  onSortingChange: OnChangeFn<SortingState>
  timeMode: TimeMode
}

// Quant label styles
const LABEL_STYLES: Record<string, { label: string; className: string }> = {
  main_accumulation: { label: '主力吸筹', className: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300' },
  undervalued: { label: '低估', className: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' },
  oversold: { label: '超跌', className: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300' },
  high_volatility: { label: '高波', className: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300' },
  breakout: { label: '突破', className: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300' },
  volume_surge: { label: '放量', className: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300' },
}

// Valuation level styles
const VALUATION_STYLES: Record<string, { className: string }> = {
  LOW: { className: 'text-green-600 dark:text-green-400' },
  MEDIUM: { className: 'text-amber-600 dark:text-amber-400' },
  HIGH: { className: 'text-orange-600 dark:text-orange-400' },
  EXTREME: { className: 'text-red-600 dark:text-red-400' },
}

// Format change percentage with color
function formatChangePct(value: string | null | undefined) {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  const formatted = num >= 0 ? `+${num.toFixed(2)}%` : `${num.toFixed(2)}%`
  const colorClass = num > 0 ? 'text-profit' : num < 0 ? 'text-loss' : 'text-muted-foreground'
  return <span className={colorClass}>{formatted}</span>
}

export function RadarDataTable({
  data,
  isLoading,
  sorting,
  onSortingChange,
  timeMode,
}: RadarDataTableProps) {
  const { steps, progress } = useComputingProgress(isLoading, 'screener')

  // Define columns
  const columns = useMemo<ColumnDef<ScreenerItem>[]>(() => {
    const baseColumns: ColumnDef<ScreenerItem>[] = [
      // Code & Name
      {
        accessorKey: 'code',
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground"
            onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
          >
            代码/名称
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3" />
            )}
          </button>
        ),
        cell: ({ row }) => (
          <div className="flex items-center gap-2 whitespace-nowrap">
            <span className="font-mono text-sm">{row.original.code}</span>
            <span className="text-xs text-muted-foreground">{row.original.name}</span>
          </div>
        ),
      },
      // Composite Score
      {
        accessorKey: 'composite_score',
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground"
            onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
          >
            综合评分
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3" />
            )}
          </button>
        ),
        cell: ({ row }) => {
          const score = Number(row.original.composite_score)
          return (
            <div className="flex items-center gap-2 w-[100px]">
              <span className="text-sm font-medium w-8">{score.toFixed(1)}</span>
              <Progress value={score} className="h-1.5 flex-1" />
            </div>
          )
        },
      },
      // Quant Labels
      {
        accessorKey: 'quant_labels',
        header: '量化标签',
        cell: ({ row }) => {
          const labels = row.original.quant_labels || []
          if (labels.length === 0) return <span className="text-muted-foreground">-</span>
          return (
            <div className="flex flex-wrap gap-1 max-w-[150px]">
              {labels.slice(0, 3).map((label) => {
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
      // Price & Change (snapshot mode only)
      ...(timeMode === 'snapshot' ? [{
        id: 'price_change',
        header: '现价/涨幅',
        cell: ({ row }: { row: { original: ScreenerItem } }) => (
          <div className="flex items-center gap-2 whitespace-nowrap">
            <span className="font-mono text-sm">
              {row.original.price ? Number(row.original.price).toFixed(2) : '-'}
            </span>
            <span className="text-xs">
              {formatChangePct(row.original.change_pct)}
            </span>
          </div>
        ),
      }] as ColumnDef<ScreenerItem>[] : []),
      // Main Strength Proxy
      {
        accessorKey: 'main_strength_proxy',
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground"
            onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
          >
            主力强度
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3" />
            )}
          </button>
        ),
        cell: ({ row }) => {
          const value = row.original.main_strength_proxy
          if (value === null || value === undefined) return '-'
          const num = Number(value)
          return (
            <div className="flex items-center gap-2 w-[80px]">
              <span className="text-xs font-mono w-6">{num.toFixed(0)}</span>
              <Progress
                value={num}
                className={cn(
                  'h-1.5 flex-1',
                  num > 60 ? '[&>div]:bg-green-500' :
                  num > 40 ? '[&>div]:bg-amber-500' : '[&>div]:bg-gray-400'
                )}
              />
            </div>
          )
        },
      },
      // Valuation
      {
        accessorKey: 'valuation_percentile',
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground"
            onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
          >
            估值水位
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3" />
            )}
          </button>
        ),
        cell: ({ row }) => {
          const level = row.original.valuation_level
          const pct = row.original.valuation_percentile
          if (!level) return '-'
          const style = VALUATION_STYLES[level] || {}
          return (
            <div className="flex items-center gap-1.5 whitespace-nowrap">
              <span className={cn('text-xs font-medium', style.className)}>
                {level === 'LOW' ? '低估' :
                  level === 'MEDIUM' ? '中等' :
                  level === 'HIGH' ? '偏高' : '极高'}
              </span>
              {pct && (
                <span className="text-xs text-muted-foreground font-mono">
                  {Number(pct).toFixed(0)}%
                </span>
              )}
            </div>
          )
        },
      },
      // Size Category
      {
        accessorKey: 'size_category',
        header: '规模',
        cell: ({ row }) => {
          const size = row.original.size_category
          if (!size) return '-'
          const sizeLabels: Record<string, string> = {
            MEGA: '超大',
            LARGE: '大盘',
            MID: '中盘',
            SMALL: '小盘',
            MICRO: '微盘',
          }
          return (
            <Badge variant="outline" className="text-xs">
              {sizeLabels[size] || size}
            </Badge>
          )
        },
      },
      // Industry
      {
        accessorKey: 'industry_l1',
        header: '行业',
        cell: ({ row }) => (
          <div className="text-xs text-muted-foreground max-w-[80px] truncate">
            {row.original.industry_l1 || '-'}
          </div>
        ),
      },
    ]

    // Period mode specific columns
    if (timeMode === 'period') {
      baseColumns.push(
        {
          accessorKey: 'period_return',
          header: '区间收益',
          cell: ({ row }) => formatChangePct(row.original.period_return),
        },
        {
          accessorKey: 'max_drawdown',
          header: '最大回撤',
          cell: ({ row }) => {
            const value = row.original.max_drawdown
            if (value === null || value === undefined) return '-'
            return <span className="text-loss">{Number(value).toFixed(2)}%</span>
          },
        }
      )
    }

    return baseColumns
  }, [timeMode])

  // Create table instance
  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    onSortingChange,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
  })

  // Loading state with computing console
  if (isLoading) {
    return (
      <div className="rounded-md border p-4">
        <ComputingConsole
          title="正在筛选股票..."
          steps={steps}
          progress={progress}
        />
      </div>
    )
  }

  return (
    <div className="rounded-md border">
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
            table.getRowModel().rows.map((row) => (
              <TableRow
                key={row.id}
                className="cursor-pointer hover:bg-muted/50"
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="py-1 px-2">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
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
