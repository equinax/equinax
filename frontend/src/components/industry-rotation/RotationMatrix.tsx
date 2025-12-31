/**
 * Rotation Matrix Component
 *
 * SVG-based heatmap for sector rotation visualization:
 * - X-axis: Industries (sorted, with FLIP animation on reorder)
 * - Y-axis: Dates (T-day at top)
 * - Cells: Red-green gradient with change % text
 */

import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { ChevronDown } from 'lucide-react'
import type { SectorRotationResponse, RotationSortBy } from '@/api/generated/schemas'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { MatrixCell } from './MatrixCell'
import { MatrixTooltip } from './MatrixTooltip'
import type { TooltipData } from './types'

// Metric keys matching page component
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

// Highlight range for color filtering
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

interface RotationMatrixProps {
  data: SectorRotationResponse
  visibleMetrics: MetricKey[]
  highlightRange?: HighlightRange | null
  sortBy: RotationSortBy
  onSortChange: (value: RotationSortBy) => void
}

// Sort options for dropdown
const SORT_OPTIONS: { value: RotationSortBy; label: string }[] = [
  { value: 'upstream', label: '产业链' },
  { value: 'today_change', label: '今日涨跌' },
  { value: 'period_change', label: '区间涨跌' },
  { value: 'money_flow', label: '资金流向' },
  { value: 'momentum', label: '动量' },
]

// Layout constants
const CELL_WIDTH = 42  // Default cell width
const MIN_CELL_WIDTH = 36  // Minimum cell width for readability
const CELL_HEIGHT = 20
const DATE_COLUMN_WIDTH = 38  // Compact - date text "MM-DD" is ~32px
const HEADER_HEIGHT = 32
const COLLAPSED_ROW_HEIGHT = 16  // Height for collapsed industry icons row


export function RotationMatrix({ data, visibleMetrics, highlightRange, sortBy, onSortChange }: RotationMatrixProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipData | null>(null)
  const [containerWidth, setContainerWidth] = useState(0)
  // Track hidden (collapsed) industries
  const [hiddenIndustries, setHiddenIndustries] = useState<Set<string>>(new Set())
  // Hover state for collapsed icons
  const [collapsedHover, setCollapsedHover] = useState<string | null>(null)

  // Toggle industry visibility
  const toggleIndustry = useCallback((code: string) => {
    setHiddenIndustries((prev) => {
      const next = new Set(prev)
      if (next.has(code)) {
        next.delete(code)
      } else {
        next.add(code)
      }
      return next
    })
  }, [])

  // Track container width for responsive sizing
  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerWidth(entry.contentRect.width)
      }
    })

    observer.observe(container)
    // Initial measurement
    setContainerWidth(container.clientWidth)

    return () => observer.disconnect()
  }, [])

  // Industries come pre-sorted from backend based on sort_by parameter
  const allIndustries = data.industries

  // Separate visible and hidden industries, preserving original order
  const { visibleIndustries, hiddenIndustriesWithPosition } = useMemo(() => {
    const visible: typeof allIndustries = []
    const hidden: Array<{ industry: typeof allIndustries[0]; originalIndex: number }> = []

    allIndustries.forEach((industry, index) => {
      if (hiddenIndustries.has(industry.code)) {
        hidden.push({ industry, originalIndex: index })
      } else {
        visible.push(industry)
      }
    })

    return { visibleIndustries: visible, hiddenIndustriesWithPosition: hidden }
  }, [allIndustries, hiddenIndustries])

  // Calculate dimensions - responsive cell width based on VISIBLE industries
  const numIndustries = visibleIndustries.length
  const hasHiddenIndustries = hiddenIndustriesWithPosition.length > 0
  const availableWidth = containerWidth - DATE_COLUMN_WIDTH
  // Use exact division to fill container completely
  const cellWidth = numIndustries > 0 && containerWidth > 0
    ? Math.max(MIN_CELL_WIDTH, availableWidth / numIndustries)
    : CELL_WIDTH
  const matrixHeight = data.trading_days.length * CELL_HEIGHT
  // Add collapsed row height if there are hidden industries
  const collapsedRowHeight = hasHiddenIndustries ? COLLAPSED_ROW_HEIGHT : 0
  // SVG fills container width exactly
  const svgWidth = containerWidth || (DATE_COLUMN_WIDTH + numIndustries * CELL_WIDTH)
  const svgHeight = collapsedRowHeight + HEADER_HEIGHT + matrixHeight

  // Handle cell hover
  const handleCellHover = useCallback(
    (
      industry: string,
      date: string,
      event: React.MouseEvent
    ) => {
      const industryData = data.industries.find((i) => i.name === industry)
      const cell = industryData?.cells.find((c) => c.date === date)

      if (cell && industryData) {
        setTooltip({
          industry,
          date,
          change_pct: Number(cell.change_pct),
          money_flow: cell.money_flow ? Number(cell.money_flow) : null,
          main_strength: cell.main_strength ? Number(cell.main_strength) : null,
          top_stock: cell.top_stock,
          signals: cell.signals || [],
          mouseX: event.clientX,
          mouseY: event.clientY,
        })
      }
    },
    [data.industries]
  )

  const handleCellLeave = useCallback(() => {
    setTooltip(null)
  }, [])

  // Track mouse position for tooltip
  const handleMouseMove = useCallback((event: React.MouseEvent) => {
    if (tooltip) {
      setTooltip((prev) =>
        prev ? { ...prev, mouseX: event.clientX, mouseY: event.clientY } : null
      )
    }
  }, [tooltip])

  // Format date for display (MM-DD)
  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr)
    return `${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
  }

  return (
    <div
      ref={containerRef}
      className="relative w-full overflow-auto"
      onMouseMove={handleMouseMove}
    >
      <svg
        width={svgWidth}
        height={svgHeight}
        className="overflow-visible"
        role="img"
        aria-label="行业轮动矩阵"
      >
        {/* Collapsed industries row - ChevronDown icons */}
        {hasHiddenIndustries && (
          <g transform={`translate(${DATE_COLUMN_WIDTH}, 0)`}>
            {hiddenIndustriesWithPosition.map(({ industry, originalIndex }) => {
              // Calculate x position based on where this industry would be among visible ones
              // Find how many visible industries come before this one in original order
              let visibleBefore = 0
              for (let i = 0; i < originalIndex; i++) {
                if (!hiddenIndustries.has(allIndustries[i].code)) {
                  visibleBefore++
                }
              }
              const xPos = visibleBefore * cellWidth + cellWidth / 2

              return (
                <g
                  key={`collapsed-${industry.code}`}
                  transform={`translate(${xPos}, ${COLLAPSED_ROW_HEIGHT / 2})`}
                  style={{ cursor: 'pointer' }}
                  onClick={() => toggleIndustry(industry.code)}
                  onMouseEnter={() => setCollapsedHover(industry.code)}
                  onMouseLeave={() => setCollapsedHover(null)}
                >
                  <circle
                    r={6}
                    className={`${collapsedHover === industry.code ? 'fill-muted-foreground/30' : 'fill-muted'} transition-colors`}
                  />
                  <ChevronDown
                    x={-4}
                    y={-4}
                    width={8}
                    height={8}
                    className="text-muted-foreground"
                  />
                  {/* Tooltip on hover */}
                  {collapsedHover === industry.code && (
                    <g transform="translate(0, -16)">
                      <rect
                        x={-20}
                        y={-10}
                        width={40}
                        height={14}
                        rx={3}
                        className="fill-popover stroke-border"
                      />
                      <text
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fontSize={8}
                        className="fill-foreground"
                      >
                        {industry.name.slice(0, 4)}
                      </text>
                    </g>
                  )}
                </g>
              )
            })}
          </g>
        )}

        {/* Sort dropdown in top-left cell */}
        <foreignObject
          x={0}
          y={collapsedRowHeight}
          width={DATE_COLUMN_WIDTH}
          height={HEADER_HEIGHT}
        >
          <div className="flex items-center justify-center h-full">
            <Select value={sortBy} onValueChange={onSortChange}>
              <SelectTrigger className="w-[32px] h-[24px] p-0 border-0 bg-transparent focus:ring-0 focus:ring-offset-0 [&>svg]:hidden">
                <SelectValue>
                  <span className="text-xs text-muted-foreground">排序</span>
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {SORT_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </foreignObject>

        {/* Header row - Industry names (clickable to hide) */}
        <g transform={`translate(${DATE_COLUMN_WIDTH}, ${collapsedRowHeight})`}>
          <AnimatePresence>
            {visibleIndustries.map((industry, colIndex) => (
              <motion.g
                key={industry.code}
                initial={{ x: colIndex * cellWidth }}
                animate={{ x: colIndex * cellWidth }}
                transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
                style={{ cursor: 'pointer' }}
                onClick={() => toggleIndustry(industry.code)}
              >
                <text
                  x={cellWidth / 2}
                  y={HEADER_HEIGHT / 2}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  fontSize={9}
                  fill="currentColor"
                  className="text-muted-foreground hover:text-foreground transition-colors"
                >
                  {industry.name.length > 4
                    ? industry.name.slice(0, 4)
                    : industry.name}
                </text>
              </motion.g>
            ))}
          </AnimatePresence>
        </g>

        {/* Date column with soft background */}
        <g transform={`translate(0, ${collapsedRowHeight + HEADER_HEIGHT})`}>
          {/* Background */}
          <rect
            x={0}
            y={0}
            width={DATE_COLUMN_WIDTH}
            height={matrixHeight}
            className="fill-muted"
          />
          {/* Date labels */}
          {data.trading_days.map((dateStr, rowIndex) => (
            <text
              key={dateStr}
              x={DATE_COLUMN_WIDTH - 4}
              y={rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2}
              textAnchor="end"
              dominantBaseline="middle"
              fontSize={9}
              fill="currentColor"
              className="text-muted-foreground font-mono"
            >
              {formatDate(dateStr)}
            </text>
          ))}
        </g>

        {/* Matrix cells */}
        <g
          transform={`translate(${DATE_COLUMN_WIDTH}, ${collapsedRowHeight + HEADER_HEIGHT})`}
        >
          {data.trading_days.map((dateStr, rowIndex) => (
            <g key={dateStr}>
              <AnimatePresence>
                {visibleIndustries.map((industry, colIndex) => {
                  const cell = industry.cells.find((c) => c.date === dateStr)

                  return (
                    <MatrixCell
                      key={`${industry.code}-${dateStr}`}
                      x={colIndex * cellWidth}
                      y={rowIndex * CELL_HEIGHT}
                      width={cellWidth}
                      height={CELL_HEIGHT}
                      visibleMetrics={visibleMetrics}
                      changePct={cell ? Number(cell.change_pct) : 0}
                      volume={cell?.money_flow ? Number(cell.money_flow) : null}
                      flow={cell?.main_strength ? Number(cell.main_strength) : null}
                      momentum={cell?.main_strength ? Number(cell.main_strength) : null}
                      highlightRange={highlightRange}
                      onHover={(e) =>
                        handleCellHover(
                          industry.name,
                          dateStr,
                          e
                        )
                      }
                      onLeave={handleCellLeave}
                    />
                  )
                })}
              </AnimatePresence>
            </g>
          ))}
        </g>
      </svg>

      {/* Tooltip */}
      {tooltip && <MatrixTooltip data={tooltip} />}
    </div>
  )
}
