import { useMemo, useState } from 'react'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getPaginationRowModel,
  getFilteredRowModel,
  flexRender,
  createColumnHelper,
  SortingState,
  ColumnFiltersState,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ArrowUpDown, ChevronLeft, ChevronRight } from 'lucide-react'
import { formatDate, formatPercent, formatCurrency } from '@/lib/utils'
import type { TradeRecord } from '@/types/backtest'

interface MultiTradeRecord extends TradeRecord {
  stock_code: string
}

interface MultiTradesTableProps {
  data: Record<string, TradeRecord[]> | null | undefined
}

const columnHelper = createColumnHelper<MultiTradeRecord>()

export function MultiTradesTable({ data }: MultiTradesTableProps) {
  const [sorting, setSorting] = useState<SortingState>([{ id: 'entry_date', desc: true }])
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [selectedStock, setSelectedStock] = useState<string>('all')

  const { normalizedTrades, stockCodes } = useMemo(() => {
    if (!data || typeof data !== 'object') {
      return { normalizedTrades: [], stockCodes: [] }
    }

    const codes = Object.keys(data)
    const allTrades: MultiTradeRecord[] = []

    codes.forEach(stockCode => {
      const trades = data[stockCode]
      if (!Array.isArray(trades)) return

      trades.forEach((trade, index) => {
        if (
          trade !== null &&
          typeof trade === 'object' &&
          'entry_date' in trade &&
          'exit_date' in trade
        ) {
          allTrades.push({
            ...trade,
            stock_code: stockCode,
            id: trade.id || `${stockCode}-${index}`,
          })
        }
      })
    })

    // Sort by entry_date descending by default
    allTrades.sort((a, b) => b.entry_date.localeCompare(a.entry_date))

    return { normalizedTrades: allTrades, stockCodes: codes }
  }, [data])

  const filteredTrades = useMemo(() => {
    if (selectedStock === 'all') return normalizedTrades
    return normalizedTrades.filter(trade => trade.stock_code === selectedStock)
  }, [normalizedTrades, selectedStock])

  const columns = useMemo(() => [
    columnHelper.accessor('stock_code', {
      header: '股票',
      cell: info => (
        <Badge variant="outline" className="font-mono">
          {info.getValue()}
        </Badge>
      ),
    }),
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
    data: filteredTrades,
    columns,
    state: { sorting, columnFilters },
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
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
      {/* Filter */}
      <div className="flex items-center gap-4">
        <span className="text-sm text-muted-foreground">筛选股票:</span>
        <Select value={selectedStock} onValueChange={setSelectedStock}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="全部股票" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部股票</SelectItem>
            {stockCodes.map(code => (
              <SelectItem key={code} value={code}>{code}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

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
            {table.getRowModel().rows.length > 0 ? (
              table.getRowModel().rows.map(row => (
                <TableRow key={row.id}>
                  {row.getVisibleCells().map(cell => (
                    <TableCell key={cell.id}>
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

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          共 {filteredTrades.length} 笔交易
          {selectedStock !== 'all' && ` (${selectedStock})`}
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
            第 {table.getState().pagination.pageIndex + 1} / {table.getPageCount() || 1} 页
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
