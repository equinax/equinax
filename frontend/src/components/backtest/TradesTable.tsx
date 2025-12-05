import { useMemo, useState } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  flexRender,
  createColumnHelper,
  SortingState,
} from '@tanstack/react-table'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ArrowUpDown, ChevronLeft, ChevronRight } from 'lucide-react'
import { formatDate, formatPercent, formatCurrency } from '@/lib/utils'
import type { TradeRecord } from '@/types/backtest'

interface TradesTableProps {
  trades: TradeRecord[] | { [key: string]: unknown }[] | null | undefined
}

const columnHelper = createColumnHelper<TradeRecord>()

export function TradesTable({ trades }: TradesTableProps) {
  const [sorting, setSorting] = useState<SortingState>([])

  const normalizedTrades = useMemo(() => {
    if (!trades || !Array.isArray(trades)) return []

    return trades
      .filter((trade): trade is TradeRecord =>
        trade !== null &&
        typeof trade === 'object' &&
        'entry_date' in trade &&
        'exit_date' in trade
      )
      .map((trade, index) => ({
        ...trade,
        id: trade.id || String(index),
      }))
  }, [trades])

  const columns = useMemo(() => [
    columnHelper.accessor('entry_date', {
      header: ({ column }) => (
        <Button
          variant="ghost"
          size="sm"
          className="-ml-3 h-8"
          onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
        >
          入场日期
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: info => formatDate(info.getValue()),
    }),
    columnHelper.accessor('exit_date', {
      header: '出场日期',
      cell: info => formatDate(info.getValue()),
    }),
    columnHelper.accessor('type', {
      header: '方向',
      cell: info => {
        const type = info.getValue()
        const isLong = type === 'LONG' || type === 'long'
        return (
          <Badge variant={isLong ? 'profit' : 'loss'}>
            {isLong ? '做多' : '做空'}
          </Badge>
        )
      },
    }),
    columnHelper.accessor('entry_price', {
      header: '入场价',
      cell: info => `¥${Number(info.getValue()).toFixed(2)}`,
    }),
    columnHelper.accessor('exit_price', {
      header: '出场价',
      cell: info => `¥${Number(info.getValue()).toFixed(2)}`,
    }),
    columnHelper.accessor('size', {
      header: '数量',
      cell: info => Number(info.getValue()).toLocaleString(),
    }),
    columnHelper.accessor('pnl', {
      header: ({ column }) => (
        <Button
          variant="ghost"
          size="sm"
          className="-ml-3 h-8"
          onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
        >
          盈亏
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: info => {
        const value = Number(info.getValue())
        return (
          <span className={value >= 0 ? 'text-profit' : 'text-loss'}>
            {value >= 0 ? '+' : ''}{formatCurrency(value)}
          </span>
        )
      },
    }),
    columnHelper.accessor('pnl_percent', {
      header: ({ column }) => (
        <Button
          variant="ghost"
          size="sm"
          className="-ml-3 h-8"
          onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
        >
          盈亏%
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      ),
      cell: info => {
        const value = Number(info.getValue())
        return (
          <span className={value >= 0 ? 'text-profit' : 'text-loss'}>
            {formatPercent(value)}
          </span>
        )
      },
    }),
  ], [])

  const table = useReactTable({
    data: normalizedTrades,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: {
      pagination: { pageSize: 10 },
    },
  })

  if (normalizedTrades.length === 0) {
    return (
      <div className="flex items-center justify-center h-[200px] bg-muted/30 rounded-lg">
        <p className="text-muted-foreground">暂无交易记录</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="rounded-lg border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map(headerGroup => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map(header => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.map(row => (
              <TableRow key={row.id}>
                {row.getVisibleCells().map(cell => (
                  <TableCell key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          共 {normalizedTrades.length} 笔交易
        </p>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>
          <span className="text-sm text-muted-foreground">
            第 {table.getState().pagination.pageIndex + 1} / {table.getPageCount()} 页
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  )
}
