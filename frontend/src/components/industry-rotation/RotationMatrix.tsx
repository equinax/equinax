/**
 * Rotation Matrix Component
 *
 * SVG-based heatmap for sector rotation visualization:
 * - Fixed header row with industry names
 * - Scrollable body with date rows and cells
 * - Infinite scroll support
 */

import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import { ChevronDown, ArrowUp, ArrowDown } from 'lucide-react'
import type { SectorRotationResponse } from '@/api/generated/schemas'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { MatrixCell } from './MatrixCell'
import { MatrixTooltip } from './MatrixTooltip'
import { LoadingMoreIndicator } from './LoadingMoreIndicator'
import type { TooltipData } from './types'

// Metric keys matching page component
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

// 3-state metric: off/raw/weighted
type MetricState = 'off' | 'raw' | 'weighted'

// Frontend-only sort options (7 options)
export type SortOption =
  | 'upstream'         // 产业链 - fixed order
  | 'change'           // 涨跌
  | 'change_weighted'  // 涨跌加权
  | 'volume'           // 成交量
  | 'volume_weighted'  // 成交量加权
  | 'flow'             // 资金流入
  | 'momentum'         // 动量

// Highlight range for color filtering
interface HighlightRange {
  metric: MetricKey
  min: number
  max: number
}

interface RotationMatrixProps {
  data: SectorRotationResponse
  visibleMetrics: MetricKey[]
  metricStates: Record<MetricKey, MetricState>
  highlightRange?: HighlightRange | null
  // Infinite scroll props
  isLoadingMore?: boolean
  hasMore?: boolean
  onLoadMore?: () => void
}

// Sort options for dropdown (7 options - all frontend handled)
const SORT_OPTIONS: { value: SortOption; label: string }[] = [
  { value: 'upstream', label: '产业链' },
  { value: 'change', label: '涨跌' },
  { value: 'change_weighted', label: '涨跌加权' },
  { value: 'volume', label: '成交量' },
  { value: 'volume_weighted', label: '成交量加权' },
  { value: 'flow', label: '资金流入' },
  { value: 'momentum', label: '动量' },
]

// Get short label for current sort option
const getSortLabel = (value: SortOption): string => {
  const opt = SORT_OPTIONS.find((o) => o.value === value)
  return opt ? opt.label.slice(0, 2) : '排序'
}

// Calculate weighted volume deviation percentage
const calculateWeightedVolume = (volume: number | null | undefined, baseline: number | null | undefined): number => {
  if (volume == null || baseline == null || baseline === 0) return 0
  const yi = Number(volume) / 100000000 // Convert to 亿
  return ((yi - baseline) / baseline) * 100 // Deviation percentage
}

// Layout constants
const CELL_WIDTH = 42 // Default cell width
const MIN_CELL_WIDTH = 36 // Minimum cell width for readability
const CELL_HEIGHT = 20
const DATE_COLUMN_WIDTH = 38 // Compact - date text "MM-DD" is ~32px
const HEADER_HEIGHT = 32
const COLLAPSED_ROW_HEIGHT = 16 // Height for collapsed industry icons row

export function RotationMatrix({
  data,
  visibleMetrics,
  metricStates,
  highlightRange,
  isLoadingMore = false,
  hasMore = true,
  onLoadMore,
}: RotationMatrixProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipData | null>(null)
  const [containerWidth, setContainerWidth] = useState(0)
  // Track hidden (collapsed) industries
  const [hiddenIndustries, setHiddenIndustries] = useState<Set<string>>(new Set())
  // Hover state for collapsed icons
  const [collapsedHover, setCollapsedHover] = useState<string | null>(null)
  // Frontend sort state (completely frontend-controlled)
  const [sortBy, setSortBy] = useState<SortOption>('change')
  // Frontend date-based sorting
  const [sortByDate, setSortByDate] = useState<string | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')

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

  // Scroll detection for infinite scroll
  const handleScroll = useCallback(() => {
    const container = scrollContainerRef.current
    if (!container || !onLoadMore) return

    const { scrollTop, scrollHeight, clientHeight } = container
    const threshold = 100 // Trigger when 100px from bottom

    if (scrollHeight - scrollTop - clientHeight < threshold) {
      onLoadMore()
    }
  }, [onLoadMore])

  // Handle date click for frontend sorting
  const handleDateClick = useCallback((dateStr: string) => {
    // Disabled for upstream sort (fixed industry chain order)
    if (sortBy === 'upstream') return

    if (sortByDate === dateStr) {
      // Toggle direction between desc and asc
      setSortDirection(sortDirection === 'desc' ? 'asc' : 'desc')
    } else {
      // New date selected - start with descending
      setSortByDate(dateStr)
      setSortDirection('desc')
    }
  }, [sortBy, sortByDate, sortDirection])

  // Reset date sort when sort option changes
  useEffect(() => {
    setSortByDate(null)
    setSortDirection('desc')
  }, [sortBy])

  // Industries sorted by selected option and date (with animation via order change)
  const allIndustries = useMemo(() => {
    // Upstream = fixed industry chain order (backend returns this order)
    if (sortBy === 'upstream') {
      return data.industries
    }

    // Determine target date for sorting (clicked date or most recent)
    const targetDate = sortByDate || data.trading_days[0]
    if (!targetDate) return data.industries

    return [...data.industries].sort((a, b) => {
      const cellA = a.cells.find((c) => c.date === targetDate)
      const cellB = b.cells.find((c) => c.date === targetDate)
      const marketChange = data.market_changes?.[targetDate] ?? 0

      let valA = 0
      let valB = 0

      switch (sortBy) {
        case 'change':
          valA = cellA ? Number(cellA.change_pct) : 0
          valB = cellB ? Number(cellB.change_pct) : 0
          break
        case 'change_weighted':
          valA = (cellA ? Number(cellA.change_pct) : 0) - Number(marketChange)
          valB = (cellB ? Number(cellB.change_pct) : 0) - Number(marketChange)
          break
        case 'volume':
          valA = cellA?.money_flow ? Number(cellA.money_flow) : 0
          valB = cellB?.money_flow ? Number(cellB.money_flow) : 0
          break
        case 'volume_weighted':
          valA = calculateWeightedVolume(cellA?.money_flow ? Number(cellA.money_flow) : null, a.volume_baseline ? Number(a.volume_baseline) : null)
          valB = calculateWeightedVolume(cellB?.money_flow ? Number(cellB.money_flow) : null, b.volume_baseline ? Number(b.volume_baseline) : null)
          break
        case 'flow':
          valA = cellA?.main_strength ? Number(cellA.main_strength) : 0
          valB = cellB?.main_strength ? Number(cellB.main_strength) : 0
          break
        case 'momentum':
          valA = cellA?.main_strength ? Number(cellA.main_strength) : 0
          valB = cellB?.main_strength ? Number(cellB.main_strength) : 0
          break
      }

      return sortDirection === 'desc' ? valB - valA : valA - valB
    })
  }, [data.industries, data.trading_days, data.market_changes, sortBy, sortByDate, sortDirection])

  // Separate visible and hidden industries, group hidden by slot position
  const { visibleIndustries, hiddenGroups } = useMemo(() => {
    const visible: typeof allIndustries = []
    // Group hidden industries by their slot position (number of visible before them)
    const groupMap = new Map<number, Array<(typeof allIndustries)[0]>>()

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
  const cellWidth =
    numIndustries > 0 && containerWidth > 0
      ? Math.max(MIN_CELL_WIDTH, availableWidth / numIndustries)
      : CELL_WIDTH
  const matrixHeight = data.trading_days.length * CELL_HEIGHT
  // Add collapsed row height if there are hidden industries
  const collapsedRowHeight = hasHiddenIndustries ? COLLAPSED_ROW_HEIGHT : 0
  // SVG fills container width exactly
  const svgWidth = containerWidth || DATE_COLUMN_WIDTH + numIndustries * CELL_WIDTH
  const headerSvgHeight = collapsedRowHeight + HEADER_HEIGHT

  // Handle cell hover
  const handleCellHover = useCallback(
    (industry: string, date: string, event: React.MouseEvent) => {
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
          volume_baseline: industryData.volume_baseline ? Number(industryData.volume_baseline) : null,
        })
      }
    },
    [data.industries]
  )

  const handleCellLeave = useCallback(() => {
    setTooltip(null)
  }, [])

  // Track mouse position for tooltip
  const handleMouseMove = useCallback(
    (event: React.MouseEvent) => {
      if (tooltip) {
        setTooltip((prev) =>
          prev ? { ...prev, mouseX: event.clientX, mouseY: event.clientY } : null
        )
      }
    },
    [tooltip]
  )

  // Format date for display (MM-DD)
  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr)
    return `${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`
  }

  // Get background color for market change (same as change color scale)
  const getMarketChangeColor = (change: number): string => {
    // Use same color logic as cell change - red for up, green for down
    const maxMagnitude = 3 // ±3% as max intensity for market index
    const clamped = Math.max(-maxMagnitude, Math.min(maxMagnitude, change))
    const magnitude = Math.abs(clamped) / maxMagnitude
    // Saturation: 0% at center → 70% at max
    const saturation = magnitude * 70
    // Lightness: 95% at center → 75% at max
    const lightness = 95 - magnitude * 20
    const hue = change >= 0 ? 0 : 145 // red for up, green for down
    return `hsl(${hue}, ${saturation}%, ${lightness}%)`
  }

  // State for date hover tooltip
  const [dateHover, setDateHover] = useState<{ date: string; x: number; y: number } | null>(null)

  return (
    <div ref={containerRef} className="relative w-full flex-1 min-h-0 flex flex-col" onMouseMove={handleMouseMove}>
      {/* Fixed Header SVG */}
      <div className="flex-shrink-0">
        <svg
          width={svgWidth}
          height={headerSvgHeight}
          className="overflow-visible"
          role="img"
          aria-label="行业轮动矩阵表头"
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
              <Select value={sortBy} onValueChange={(v) => setSortBy(v as SortOption)}>
                <SelectTrigger className="w-[32px] h-[24px] p-0 border-0 bg-transparent focus:ring-0 focus:ring-offset-0 [&>svg]:hidden">
                  <SelectValue>
                    <span className="text-xs text-muted-foreground">{getSortLabel(sortBy)}</span>
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
                    {industry.name.length > 4 ? industry.name.slice(0, 4) : industry.name}
                  </text>
                </motion.g>
              ))}
            </AnimatePresence>
          </g>

          {/* Collapsed industry tooltip - rendered last to be on top */}
          {collapsedHover &&
            (() => {
              const hoveredGroup = hiddenGroups.find((g) => g.key === collapsedHover)
              if (!hoveredGroup) return null

              // Calculate position - below the arrow, on top of header
              const xPos =
                DATE_COLUMN_WIDTH + hoveredGroup.slotPosition * cellWidth + cellWidth / 2
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
      </div>

      {/* Scrollable Body */}
      <div
        ref={scrollContainerRef}
        className="flex-1 overflow-y-auto overflow-x-hidden min-h-0"
        onScroll={handleScroll}
      >
        <svg
          width={svgWidth}
          height={matrixHeight}
          className="overflow-visible"
          role="img"
          aria-label="行业轮动矩阵数据"
        >
          {/* Date column with soft background - clickable for sorting */}
          <g>
            {/* Background */}
            <rect x={0} y={0} width={DATE_COLUMN_WIDTH} height={matrixHeight} className="fill-muted" />
            {/* Date labels - clickable */}
            {data.trading_days.map((dateStr, rowIndex) => {
              const isActiveSortDate = sortByDate === dateStr
              const marketChange = data.market_changes?.[dateStr]
              const isWeightedChange = metricStates.change === 'weighted'
              const marketChangeNum = marketChange !== undefined ? Number(marketChange) : 0

              return (
                <g
                  key={dateStr}
                  style={{ cursor: 'pointer' }}
                  onClick={() => handleDateClick(dateStr)}
                  onMouseEnter={(e) => {
                    if (isWeightedChange && marketChange !== undefined) {
                      setDateHover({ date: dateStr, x: e.clientX, y: e.clientY })
                    }
                  }}
                  onMouseLeave={() => setDateHover(null)}
                >
                  {/* Background color when weighted change mode */}
                  {isWeightedChange && marketChange !== undefined ? (
                    <rect
                      x={0}
                      y={rowIndex * CELL_HEIGHT}
                      width={DATE_COLUMN_WIDTH}
                      height={CELL_HEIGHT}
                      fill={getMarketChangeColor(marketChangeNum)}
                    />
                  ) : (
                    <rect
                      x={0}
                      y={rowIndex * CELL_HEIGHT}
                      width={DATE_COLUMN_WIDTH}
                      height={CELL_HEIGHT}
                      fill="transparent"
                    />
                  )}
                  {/* Date text - always show */}
                  <text
                    x={DATE_COLUMN_WIDTH / 2}
                    y={rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fontSize={9}
                    fill="currentColor"
                    className={`font-mono ${isActiveSortDate ? 'font-bold' : 'text-muted-foreground'}`}
                  >
                    {formatDate(dateStr)}
                  </text>
                  {/* Sort indicator */}
                  {isActiveSortDate && (
                    <g transform={`translate(${DATE_COLUMN_WIDTH - 4}, ${rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2})`}>
                      {sortDirection === 'desc' ? (
                        <ArrowDown x={-3} y={-3} width={6} height={6} className="text-foreground" />
                      ) : (
                        <ArrowUp x={-3} y={-3} width={6} height={6} className="text-foreground" />
                      )}
                    </g>
                  )}
                </g>
              )
            })}
          </g>

          {/* Matrix cells */}
          <g transform={`translate(${DATE_COLUMN_WIDTH}, 0)`}>
            {data.trading_days.map((dateStr, rowIndex) => {
              const marketChange = data.market_changes?.[dateStr]

              return (
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
                          metricStates={metricStates}
                          changePct={cell ? Number(cell.change_pct) : 0}
                          volume={cell?.money_flow ? Number(cell.money_flow) : null}
                          flow={cell?.main_strength ? Number(cell.main_strength) : null}
                          momentum={cell?.main_strength ? Number(cell.main_strength) : null}
                          marketChange={marketChange !== undefined ? Number(marketChange) : undefined}
                          volumeBaseline={industry.volume_baseline !== undefined ? Number(industry.volume_baseline) : null}
                          highlightRange={highlightRange}
                          onHover={(e) => handleCellHover(industry.name, dateStr, e)}
                          onLeave={handleCellLeave}
                        />
                      )
                    })}
                  </AnimatePresence>
                </g>
              )
            })}
          </g>
        </svg>

        {/* Loading More Indicator */}
        <LoadingMoreIndicator isLoading={isLoadingMore} hasMore={hasMore} />
      </div>

      {/* Tooltip */}
      {tooltip && <MatrixTooltip data={tooltip} />}

      {/* Date hover tooltip for market change */}
      {dateHover && data.market_changes?.[dateHover.date] !== undefined && (
        <div
          className="fixed z-50 px-2 py-1 text-xs bg-popover border rounded shadow-lg pointer-events-none"
          style={{
            left: dateHover.x + 10,
            top: dateHover.y - 10,
          }}
        >
          <span className="text-muted-foreground">基准涨跌: </span>
          <span className={`font-mono font-medium ${Number(data.market_changes[dateHover.date]) > 0 ? 'text-[#c93b3b]' : Number(data.market_changes[dateHover.date]) < 0 ? 'text-[#22c55e]' : ''}`}>
            {Number(data.market_changes[dateHover.date]) > 0 ? '+' : ''}
            {Number(data.market_changes[dateHover.date]).toFixed(2)}%
          </span>
        </div>
      )}
    </div>
  )
}
