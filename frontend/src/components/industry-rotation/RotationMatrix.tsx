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

  // Toggle industry visibility (supports multiple codes for grouped expansion)
  const toggleIndustry = useCallback((codes: string | string[]) => {
    const codeArray = Array.isArray(codes) ? codes : [codes]
    setHiddenIndustries((prev) => {
      const next = new Set(prev)
      // Check if first code is hidden - if so, show all; otherwise hide all
      const shouldShow = next.has(codeArray[0])
      codeArray.forEach((code) => {
        if (shouldShow) {
          next.delete(code)
        } else {
          next.add(code)
        }
      })
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

  // Separate visible and hidden industries, group hidden by slot position
  const { visibleIndustries, hiddenGroups } = useMemo(() => {
    const visible: typeof allIndustries = []
    // Group hidden industries by their slot position (number of visible before them)
    const groupMap = new Map<number, Array<typeof allIndustries[0]>>()

    let visibleCount = 0
    allIndustries.forEach((industry) => {
      if (hiddenIndustries.has(industry.code)) {
        // Add to group at current slot position
        const group = groupMap.get(visibleCount) || []
        group.push(industry)
        groupMap.set(visibleCount, group)
      } else {
        visible.push(industry)
        visibleCount++
      }
    })

    // Convert map to array of groups with position info
    const groups = Array.from(groupMap.entries()).map(([slotPosition, industries]) => ({
      slotPosition,
      industries,
      key: industries.map((i) => i.code).join('-'),
    }))

    return { visibleIndustries: visible, hiddenGroups: groups }
  }, [allIndustries, hiddenIndustries])

  // Calculate dimensions - responsive cell width based on VISIBLE industries
  const numIndustries = visibleIndustries.length
  const hasHiddenIndustries = hiddenGroups.length > 0
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
        {/* Collapsed industries row - ChevronDown icons (grouped) */}
        {hasHiddenIndustries && (
          <g transform={`translate(${DATE_COLUMN_WIDTH}, 0)`}>
            {hiddenGroups.map((group) => {
              const xPos = group.slotPosition * cellWidth + cellWidth / 2
              const codes = group.industries.map((i) => i.code)

              return (
                <g
                  key={group.key}
                  transform={`translate(${xPos}, ${COLLAPSED_ROW_HEIGHT / 2})`}
                  style={{ cursor: 'pointer' }}
                  onClick={() => toggleIndustry(codes)}
                  onMouseEnter={() => setCollapsedHover(group.key)}
                  onMouseLeave={() => setCollapsedHover(null)}
                >
                  {/* Larger invisible hit area */}
                  <circle r={12} fill="transparent" />
                  {/* Visible circle */}
                  <circle
                    r={6}
                    className={`${collapsedHover === group.key ? 'fill-muted-foreground/30' : 'fill-muted'} transition-colors`}
                  />
                  <ChevronDown
                    x={-4}
                    y={-4}
                    width={8}
                    height={8}
                    className="text-muted-foreground"
                  />
                </g>
              )
            })}
          </g>
        )}

        {/* Sort dropdown in top-left cell */}
        <rect
          x={0}
          y={collapsedRowHeight}
          width={DATE_COLUMN_WIDTH}
          height={HEADER_HEIGHT}
          className="fill-muted"
        />
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
          {/* Background for header row */}
          <rect
            x={0}
            y={0}
            width={svgWidth - DATE_COLUMN_WIDTH}
            height={HEADER_HEIGHT}
            className="fill-muted"
          />
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

        {/* Collapsed industry tooltip - rendered last to be on top */}
        {collapsedHover && (() => {
          const hoveredGroup = hiddenGroups.find((g) => g.key === collapsedHover)
          if (!hoveredGroup) return null

          // Calculate position - below the arrow, on top of header
          const xPos = DATE_COLUMN_WIDTH + hoveredGroup.slotPosition * cellWidth + cellWidth / 2
          const names = hoveredGroup.industries.map((i) => i.name.slice(0, 4))
          const tooltipText = names.join(' · ')
          const tooltipWidth = Math.max(60, tooltipText.length * 9 + 20)
          const tooltipHeight = 22
          // Position below the arrow (will overlay header row)
          const yPos = COLLAPSED_ROW_HEIGHT + 2

          return (
            <g transform={`translate(${xPos}, ${yPos})`}>
              <rect
                x={-tooltipWidth / 2}
                y={0}
                width={tooltipWidth}
                height={tooltipHeight}
                rx={4}
                className="fill-popover"
                stroke="#888"
                strokeWidth={1}
              />
              <text
                x={0}
                y={tooltipHeight / 2}
                textAnchor="middle"
                dominantBaseline="middle"
                fontSize={11}
                className="fill-foreground"
              >
                {tooltipText}
              </text>
            </g>
          )
        })()}
      </svg>

      {/* Tooltip */}
      {tooltip && <MatrixTooltip data={tooltip} />}
    </div>
  )
}
