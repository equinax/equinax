import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { ChevronLeft, ChevronRight, Download } from 'lucide-react'

interface KLineData {
  date: string
  open?: number | string
  high?: number | string
  low?: number | string
  close?: number | string
  volume?: number | string
  amount?: number | string
  pct_chg?: number | string
  turn?: number | string
}

interface StockHistoryTableProps {
  data: KLineData[]
  stockCode?: string
  className?: string
}

const PAGE_SIZE = 20

function formatPrice(value: number | string | undefined | null): string {
  if (value == null) return '-'
  return Number(value).toFixed(2)
}

function formatVolume(value: number | string | undefined | null): string {
  if (value == null) return '-'
  const num = Number(value)
  if (num >= 100000000) {
    return `${(num / 100000000).toFixed(2)}亿`
  }
  if (num >= 10000) {
    return `${(num / 10000).toFixed(0)}万`
  }
  return num.toLocaleString()
}

function formatAmount(value: number | string | undefined | null): string {
  if (value == null) return '-'
  const num = Number(value)
  if (num >= 100000000) {
    return `${(num / 100000000).toFixed(2)}亿`
  }
  if (num >= 10000) {
    return `${(num / 10000).toFixed(0)}万`
  }
  return num.toFixed(2)
}

export function StockHistoryTable({ data, stockCode, className }: StockHistoryTableProps) {
  const [page, setPage] = useState(1)

  // Sort by date descending (newest first)
  const sortedData = [...data].sort((a, b) => b.date.localeCompare(a.date))

  const totalPages = Math.ceil(sortedData.length / PAGE_SIZE)
  const startIndex = (page - 1) * PAGE_SIZE
  const pageData = sortedData.slice(startIndex, startIndex + PAGE_SIZE)

  const handleExport = () => {
    // Create CSV content
    const headers = ['日期', '开盘', '最高', '最低', '收盘', '涨跌幅%', '成交量', '成交额', '换手率%']
    const rows = sortedData.map(row => [
      row.date,
      row.open != null ? Number(row.open).toFixed(2) : '',
      row.high != null ? Number(row.high).toFixed(2) : '',
      row.low != null ? Number(row.low).toFixed(2) : '',
      row.close != null ? Number(row.close).toFixed(2) : '',
      row.pct_chg != null ? Number(row.pct_chg).toFixed(2) : '',
      row.volume != null ? String(row.volume) : '',
      row.amount != null ? Number(row.amount).toFixed(2) : '',
      row.turn != null ? Number(row.turn).toFixed(2) : '',
    ])

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n')

    // Create and download file
    const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${stockCode || 'stock'}_history.csv`
    link.click()
    URL.revokeObjectURL(url)
  }

  if (data.length === 0) {
    return (
      <div className={cn('flex h-[200px] items-center justify-center rounded-lg border border-dashed bg-muted/50', className)}>
        <p className="text-muted-foreground">暂无历史数据</p>
      </div>
    )
  }

  return (
    <div className={cn('space-y-4', className)}>
      {/* Header with export button */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          共 {sortedData.length} 条记录
        </p>
        <Button variant="outline" size="sm" onClick={handleExport}>
          <Download className="h-4 w-4 mr-1" />
          导出 CSV
        </Button>
      </div>

      {/* Table */}
      <div className="rounded-lg border overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-muted/50 border-b">
                <th className="py-3 px-4 text-left font-medium">日期</th>
                <th className="py-3 px-4 text-right font-medium">开盘</th>
                <th className="py-3 px-4 text-right font-medium">最高</th>
                <th className="py-3 px-4 text-right font-medium">最低</th>
                <th className="py-3 px-4 text-right font-medium">收盘</th>
                <th className="py-3 px-4 text-right font-medium">涨跌幅</th>
                <th className="py-3 px-4 text-right font-medium">成交量</th>
                <th className="py-3 px-4 text-right font-medium">成交额</th>
                <th className="py-3 px-4 text-right font-medium">换手率</th>
              </tr>
            </thead>
            <tbody>
              {pageData.map((row, index) => (
                <tr
                  key={row.date}
                  className={cn(
                    'border-b last:border-0 hover:bg-muted/50 transition-colors',
                    index % 2 === 0 && 'bg-muted/20'
                  )}
                >
                  <td className="py-2 px-4 font-mono">{row.date}</td>
                  <td className="py-2 px-4 text-right font-mono">{formatPrice(row.open)}</td>
                  <td className="py-2 px-4 text-right font-mono text-profit">{formatPrice(row.high)}</td>
                  <td className="py-2 px-4 text-right font-mono text-loss">{formatPrice(row.low)}</td>
                  <td className="py-2 px-4 text-right font-mono font-medium">{formatPrice(row.close)}</td>
                  <td className={cn(
                    'py-2 px-4 text-right font-mono',
                    Number(row.pct_chg || 0) >= 0 ? 'text-profit' : 'text-loss'
                  )}>
                    {row.pct_chg != null ? `${Number(row.pct_chg) >= 0 ? '+' : ''}${Number(row.pct_chg).toFixed(2)}%` : '-'}
                  </td>
                  <td className="py-2 px-4 text-right font-mono">{formatVolume(row.volume)}</td>
                  <td className="py-2 px-4 text-right font-mono">{formatAmount(row.amount)}</td>
                  <td className="py-2 px-4 text-right font-mono">
                    {row.turn != null ? `${Number(row.turn).toFixed(2)}%` : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            第 {page} / {totalPages} 页
          </p>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
            >
              <ChevronLeft className="h-4 w-4" />
              上一页
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
            >
              下一页
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
