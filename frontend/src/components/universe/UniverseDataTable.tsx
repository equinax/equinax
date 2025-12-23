import { useMemo } from 'react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
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
import { Skeleton } from '@/components/ui/skeleton'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import {
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  TrendingUp,
  TrendingDown,
  Building2,
  Landmark,
} from 'lucide-react'
import type { UniverseAssetItem } from '@/api/generated/schemas'
import {
  getSizeColor,
  getSizeLabel,
  getVolColor,
  getVolLabel,
  getValueColor,
  getValueLabel,
  getBoardColor,
  getBoardLabel,
  getPriceChangeColor,
  formatPriceChange,
  formatMarketCap,
  formatPrice,
  formatTurnover,
  formatRatio,
} from '@/lib/universe-colors'

const columnHelper = createColumnHelper<UniverseAssetItem>()

interface UniverseDataTableProps {
  data: UniverseAssetItem[]
  isLoading?: boolean
  sorting: SortingState
  onSortingChange: (sorting: SortingState) => void
  onRowClick?: (code: string) => void
  selectedCode?: string | null
}

export function UniverseDataTable({
  data,
  isLoading,
  sorting,
  onSortingChange,
  onRowClick,
  selectedCode,
}: UniverseDataTableProps) {
  const columns = useMemo(
    () => [
      columnHelper.accessor('code', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground"
            onClick={() => column.toggleSorting()}
          >
            代码
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ row }) => (
          <span className="font-mono text-sm font-medium">
            {row.original.code.replace(/^(sh\.|sz\.)/, '')}
          </span>
        ),
        size: 80,
      }),
      columnHelper.accessor('name', {
        header: '名称',
        cell: ({ getValue }) => (
          <span className="truncate max-w-[120px] block" title={getValue()}>
            {getValue()}
          </span>
        ),
        size: 120,
      }),
      columnHelper.accessor('industry_l1', {
        header: '行业',
        cell: ({ getValue }) => {
          const industry = getValue()
          if (!industry) return <span className="text-muted-foreground">-</span>
          return (
            <Badge variant="outline" className="text-xs font-normal whitespace-nowrap">
              {industry}
            </Badge>
          )
        },
        size: 80,
      }),
      columnHelper.accessor('board', {
        header: '板块',
        cell: ({ getValue }) => {
          const board = getValue()
          if (!board) return <span className="text-muted-foreground">-</span>
          return (
            <Badge className={cn('text-xs font-normal', getBoardColor(board))}>
              {getBoardLabel(board)}
            </Badge>
          )
        },
        size: 60,
      }),
      columnHelper.accessor('price', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            价格
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-right block">
            {formatPrice(Number(getValue()))}
          </span>
        ),
        size: 80,
      }),
      columnHelper.accessor('change_pct', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            涨跌
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => {
          const change = Number(getValue())
          return (
            <div className={cn('font-mono text-sm text-right flex items-center justify-end gap-1', getPriceChangeColor(change))}>
              {change > 0 ? (
                <TrendingUp className="h-3 w-3" />
              ) : change < 0 ? (
                <TrendingDown className="h-3 w-3" />
              ) : null}
              {formatPriceChange(change)}
            </div>
          )
        },
        size: 90,
      }),
      columnHelper.accessor('market_cap', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            市值
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-right block">
            {formatMarketCap(Number(getValue()))}
          </span>
        ),
        size: 80,
      }),
      columnHelper.accessor('size_category', {
        header: '规模',
        cell: ({ getValue }) => {
          const size = getValue()
          if (!size) return <span className="text-muted-foreground">-</span>
          return (
            <Badge className={cn('text-xs font-normal whitespace-nowrap', getSizeColor(size))}>
              {getSizeLabel(size)}
            </Badge>
          )
        },
        size: 70,
      }),
      columnHelper.accessor('vol_category', {
        header: '波动',
        cell: ({ getValue }) => {
          const vol = getValue()
          if (!vol) return <span className="text-muted-foreground">-</span>
          return (
            <Badge className={cn('text-xs font-normal whitespace-nowrap', getVolColor(vol))}>
              {getVolLabel(vol)}
            </Badge>
          )
        },
        size: 70,
      }),
      columnHelper.accessor('value_category', {
        header: '风格',
        cell: ({ getValue }) => {
          const value = getValue()
          if (!value) return <span className="text-muted-foreground">-</span>
          return (
            <Badge className={cn('text-xs font-normal whitespace-nowrap', getValueColor(value))}>
              {getValueLabel(value)}
            </Badge>
          )
        },
        size: 70,
      }),
      columnHelper.accessor('pe_ttm', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            PE
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-right block">
            {formatRatio(Number(getValue()))}
          </span>
        ),
        size: 60,
      }),
      columnHelper.accessor('pb_mrq', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            PB
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-right block">
            {formatRatio(Number(getValue()))}
          </span>
        ),
        size: 60,
      }),
      columnHelper.accessor('turnover', {
        header: ({ column }) => (
          <button
            className="flex items-center gap-1 hover:text-foreground ml-auto"
            onClick={() => column.toggleSorting()}
          >
            换手
            {column.getIsSorted() === 'asc' ? (
              <ArrowUp className="h-3 w-3" />
            ) : column.getIsSorted() === 'desc' ? (
              <ArrowDown className="h-3 w-3" />
            ) : (
              <ArrowUpDown className="h-3 w-3 opacity-50" />
            )}
          </button>
        ),
        cell: ({ getValue }) => (
          <span className="font-mono text-sm text-right block">
            {formatTurnover(Number(getValue()))}
          </span>
        ),
        size: 70,
      }),
      columnHelper.accessor('is_retail_hot', {
        header: () => (
          <Tooltip>
            <TooltipTrigger asChild>
              <Building2 className="h-4 w-4 mx-auto cursor-help" />
            </TooltipTrigger>
            <TooltipContent>散户活跃</TooltipContent>
          </Tooltip>
        ),
        cell: ({ getValue }) =>
          getValue() ? (
            <Building2 className="h-4 w-4 text-orange-500 mx-auto" />
          ) : null,
        size: 40,
      }),
      columnHelper.accessor('is_main_controlled', {
        header: () => (
          <Tooltip>
            <TooltipTrigger asChild>
              <Landmark className="h-4 w-4 mx-auto cursor-help" />
            </TooltipTrigger>
            <TooltipContent>主力控盘</TooltipContent>
          </Tooltip>
        ),
        cell: ({ getValue }) =>
          getValue() ? (
            <Landmark className="h-4 w-4 text-purple-500 mx-auto" />
          ) : null,
        size: 40,
      }),
    ],
    []
  )

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
    },
    onSortingChange: (updater) => {
      const newSorting = typeof updater === 'function' ? updater(sorting) : updater
      onSortingChange(newSorting)
    },
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualSorting: true, // Server-side sorting
  })

  if (isLoading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 10 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    )
  }

  return (
    <TooltipProvider>
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className="bg-muted/50">
              {headerGroup.headers.map((header) => (
                <TableHead
                  key={header.id}
                  style={{ width: header.column.getSize() }}
                  className="h-10 text-xs font-medium text-muted-foreground"
                >
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
                className={cn(
                  'cursor-pointer transition-colors hover:bg-accent',
                  selectedCode === row.original.code && 'bg-accent'
                )}
                onClick={() => onRowClick?.(row.original.code)}
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell
                    key={cell.id}
                    className="py-2 px-3"
                    style={{ width: cell.column.getSize() }}
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))
          ) : (
            <TableRow>
              <TableCell
                colSpan={columns.length}
                className="h-24 text-center text-muted-foreground"
              >
                暂无数据
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
    </TooltipProvider>
  )
}
