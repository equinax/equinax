import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
  type OnChangeFn,
  type RowSelectionState,
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
import { Checkbox } from '@/components/ui/checkbox'
import { Progress } from '@/components/ui/progress'
import { ComputingConsole } from '@/components/ui/computing-console'
import { motion } from 'motion/react'
import { ArrowUpDown, ArrowUp, ArrowDown, ShieldAlert } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useComputingProgress } from '@/hooks/useComputingProgress'
import type { ScreenerItem, TimeMode, DashboardResponse } from '@/api/generated/schemas'

interface RadarDataTableProps {
  data: ScreenerItem[]
  isLoading?: boolean
  sorting: SortingState
  onSortingChange: OnChangeFn<SortingState>
  timeMode: TimeMode
  activeDate?: string
  rowSelection?: RowSelectionState
  onRowSelectionChange?: OnChangeFn<RowSelectionState>
  abstain?: boolean
  abstainReason?: string
  dashboard?: DashboardResponse
}

function MarketAbstainPanel({ dashboard }: { dashboard?: DashboardResponse }) {
  if (!dashboard) {
    return (
      <div className="flex flex-col items-center justify-center p-8 text-center space-y-3 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-lg m-4">
        <ShieldAlert className="h-10 w-10 text-amber-600 dark:text-amber-400" />
        <h3 className="text-lg font-semibold text-amber-800 dark:text-amber-200">今日市场环境不适合选股</h3>
        <p className="text-muted-foreground max-w-md">
          Alpha Radar 检测到市场处于高风险状态，暂停推荐以保护您的资金安全。建议观望。
        </p>
      </div>
    )
  }

  const { market_state, market_breadth, smart_money } = dashboard
  const regimeScore = Number(market_state.regime_score)
  const upDownRatio = Number(market_breadth.up_down_ratio)
  
  // Helper for regime style
  const getRegimeStyle = (regime: string, score: number) => {
    if (regime === 'BULL' || score > 30) return 'text-profit bg-profit/10 border-profit/20'
    if (regime === 'BEAR' || score < -30) return 'text-loss bg-loss/10 border-loss/20'
    return 'text-amber-600 bg-amber-100 dark:bg-amber-900/30 border-amber-200 dark:border-amber-800'
  }

  // Generate analysis text
  let analysisText = "市场情绪偏弱，建议谨慎操作。"
  if (market_state.regime === 'BEAR' || regimeScore < -30) {
    analysisText = "市场处于熊市状态，大盘持续走弱。"
  } else if (market_state.regime === 'RANGE' && regimeScore < 0) {
    analysisText = "市场处于弱势震荡，方向不明。"
  }
  
  if (upDownRatio < 1) {
    analysisText += " 多数个股下跌，赚钱效应较差。"
  }
  
  if (smart_money.money_flow_proxy === 'outflow') {
    analysisText += " 资金整体呈流出态势，市场缺乏增量资金。"
  }

  // Actionable advice
  let advice = "⚠️ 操作建议：建议观望或适当减仓，等待市场企稳信号。不建议追涨或加仓。"
  if (regimeScore < -50) {
    advice = "⚠️ 操作建议：市场风险较高，建议减仓至半仓以下或清仓观望。"
  }

  return (
    <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-lg m-4 p-6">
      {/* Header */}
      <div className="flex items-start gap-4 mb-6">
        <div className="p-2 bg-amber-100 dark:bg-amber-900/40 rounded-full shrink-0">
          <ShieldAlert className="h-6 w-6 text-amber-600 dark:text-amber-400" />
        </div>
        <div>
          <h3 className="text-lg font-bold text-amber-800 dark:text-amber-200">今日市场环境不适合选股</h3>
          <p className="text-amber-700/80 dark:text-amber-300/80 mt-1">
            Alpha Radar 检测到市场处于高风险状态，暂停推荐以保护您的资金安全
          </p>
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        {/* Regime */}
        <div className="bg-background/50 rounded-md p-3 border">
          <div className="text-xs text-muted-foreground mb-1">市场状态</div>
          <div className="flex items-center gap-2">
            <span className={cn("text-sm font-medium px-2 py-0.5 rounded border", getRegimeStyle(market_state.regime, regimeScore))}>
              {market_state.regime_description}
            </span>
            <span className={cn("text-sm font-mono", regimeScore > 0 ? "text-profit" : regimeScore < 0 ? "text-loss" : "text-muted-foreground")}>
              {regimeScore.toFixed(0)}
            </span>
          </div>
        </div>

        {/* Up/Down */}
        <div className="bg-background/50 rounded-md p-3 border">
          <div className="text-xs text-muted-foreground mb-1">涨跌比</div>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-profit font-medium">{market_breadth.up_count}</span>
            <span className="text-muted-foreground">/</span>
            <span className="text-loss font-medium">{market_breadth.down_count}</span>
          </div>
        </div>

        {/* Money Flow */}
        <div className="bg-background/50 rounded-md p-3 border">
          <div className="text-xs text-muted-foreground mb-1">资金流向</div>
          <div className="flex items-center gap-2 text-sm">
             <span className={cn("font-medium", 
               smart_money.money_flow_proxy === 'inflow' ? "text-profit" : 
               smart_money.money_flow_proxy === 'outflow' ? "text-loss" : "text-muted-foreground"
             )}>
               {smart_money.money_flow_proxy === 'inflow' ? '流入' : 
                smart_money.money_flow_proxy === 'outflow' ? '流出' : '平衡'}
             </span>
          </div>
        </div>

        {/* Limit Up/Down */}
        <div className="bg-background/50 rounded-md p-3 border">
          <div className="text-xs text-muted-foreground mb-1">涨跌停</div>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-profit font-medium">{market_breadth.limit_up_count || 0}</span>
            <span className="text-muted-foreground">/</span>
            <span className="text-loss font-medium">{market_breadth.limit_down_count || 0}</span>
          </div>
        </div>
      </div>

      {/* Analysis & Advice */}
      <div className="space-y-3">
        <p className="text-sm text-foreground/80 leading-relaxed">
          <span className="font-semibold">分析：</span>{analysisText}
        </p>
        
        <div className="bg-amber-100/50 dark:bg-amber-900/30 border border-amber-300 dark:border-amber-700 rounded-lg p-3 text-sm font-medium text-amber-900 dark:text-amber-100">
          {advice}
        </div>
      </div>
    </div>
  )
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
  activeDate,
  rowSelection,
  onRowSelectionChange,
  abstain,
  dashboard,
}: RadarDataTableProps) {
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
  const { steps, progress } = useComputingProgress(isLoading, 'screener')

  // Define columns
  const columns = useMemo<ColumnDef<ScreenerItem>[]>(() => {
    const baseColumns: ColumnDef<ScreenerItem>[] = [
      {
        id: 'select',
        header: ({ table }) => (
          <div className="px-1">
            <Checkbox
              checked={
                table.getIsAllPageRowsSelected() ||
                (table.getIsSomePageRowsSelected() && 'indeterminate')
              }
              onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
              aria-label="全选"
              className="h-4 w-4"
            />
          </div>
        ),
        cell: ({ row }) => (
          <div className="px-1" onClick={(e) => e.stopPropagation()}>
            <Checkbox
              checked={row.getIsSelected()}
              onCheckedChange={(value) => row.toggleSelected(!!value)}
              aria-label="选择"
              className="h-4 w-4"
            />
          </div>
        ),
        enableSorting: false,
        enableHiding: false,
        size: 32,
      },
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
        cell: ({ row }) => {
          const code = row.original.code
          const name = row.original.name || ''

          // Derive board type from code
          // sh.6xxxxx or sz.0xxxxx → 主板
          // sz.3xxxxx → 创业板
          // sh.68xxxx → 科创板
          // bj.xxxxxx → 北交所
          const getBoardBadge = () => {
            if (code.startsWith('sz.3')) {
              return <span className="inline-flex items-center justify-center w-4 h-4 text-[10px] rounded bg-orange-500 text-white font-medium">创</span>
            }
            if (code.startsWith('sh.68')) {
              return <span className="inline-flex items-center justify-center w-4 h-4 text-[10px] rounded bg-blue-500 text-white font-medium">科</span>
            }
            if (code.startsWith('bj.')) {
              return <span className="inline-flex items-center justify-center w-4 h-4 text-[10px] rounded bg-purple-500 text-white font-medium">北</span>
            }
            // Main board (sh.6xxxxx or sz.0xxxxx)
            if (code.startsWith('sh.6') || code.startsWith('sz.0')) {
              return <span className="inline-flex items-center justify-center w-4 h-4 text-[10px] rounded bg-gray-500 text-white font-medium">主</span>
            }
            return null
          }

          // Check if ST stock
          const isSTStock = name.includes('ST') || name.includes('st')

          return (
            <div className="flex items-center gap-1.5 whitespace-nowrap">
              <span className="font-mono text-sm">{code}</span>
              <span className="text-xs text-muted-foreground">{name}</span>
              {getBoardBadge()}
              {isSTStock && (
                <span className="inline-flex items-center justify-center px-1 h-4 text-[10px] rounded bg-red-500 text-white font-medium">ST</span>
              )}
            </div>
          )
        },
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
    state: { sorting, rowSelection: rowSelection ?? {} },
    onSortingChange,
    onRowSelectionChange: onRowSelectionChange,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    getRowId: (row) => row.code,
    enableRowSelection: true,
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
                className={cn(
                  "cursor-pointer hover:bg-muted/50 border-b transition-colors",
                  row.getIsSelected() && "bg-primary/5"
                )}
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
              <TableCell colSpan={columns.length} className={cn("p-0", !abstain && "h-24 text-center")}>
                {abstain ? (
                  <MarketAbstainPanel dashboard={dashboard} />
                ) : (
                  "暂无数据"
                )}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  )
}
