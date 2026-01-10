/**
 * ETF Rotation Matrix Component
 *
 * SVG-based heatmap for ETF rotation visualization:
 * - Fits all columns in viewport (responsive cell width)
 * - Two-row header: category groups + sub-category names
 * - Vertical infinite scroll for more dates
 */

import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import { ArrowUp, ArrowDown } from 'lucide-react'
import type { EtfRotationFlatResponse, EtfRotationColumn } from '@/api/generated/schemas'
import { EtfMatrixCell } from './EtfMatrixCell'
import { EtfMatrixTooltip } from './EtfMatrixTooltip'
import { CATEGORY_COLORS, type EtfTooltipData } from './types'

// Layout constants
const CELL_HEIGHT = 18
const DATE_COLUMN_WIDTH = 38
const CATEGORY_HEADER_HEIGHT = 16
const SUB_HEADER_HEIGHT = 38 // For vertical text
const HEADER_HEIGHT = CATEGORY_HEADER_HEIGHT + SUB_HEADER_HEIGHT

// Sort options
type SortOption = 'default' | 'change'

interface EtfRotationMatrixProps {
  data: EtfRotationFlatResponse
  isLoadingMore?: boolean
  hasMore?: boolean
  onLoadMore?: () => void
}

export function EtfRotationMatrix({
  data,
  isLoadingMore = false,
  hasMore = true,
  onLoadMore,
}: EtfRotationMatrixProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<EtfTooltipData | null>(null)
  const [containerWidth, setContainerWidth] = useState(0)
  const [sortBy, setSortBy] = useState<SortOption>('default')
  const [sortByDate, setSortByDate] = useState<string | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')

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
    setContainerWidth(container.clientWidth)

    return () => observer.disconnect()
  }, [])

  // Scroll detection for infinite scroll
  const handleScroll = useCallback(() => {
    const container = scrollContainerRef.current
    if (!container || !onLoadMore) return

    const { scrollTop, scrollHeight, clientHeight } = container
    const threshold = 100

    if (scrollHeight - scrollTop - clientHeight < threshold) {
      onLoadMore()
    }
  }, [onLoadMore])

  // Handle date click for sorting
  const handleDateClick = useCallback(
    (dateStr: string) => {
      if (sortBy === 'default') {
        setSortBy('change')
      }

      if (sortByDate === dateStr) {
        setSortDirection(sortDirection === 'desc' ? 'asc' : 'desc')
      } else {
        setSortByDate(dateStr)
        setSortDirection('desc')
      }
    },
    [sortBy, sortByDate, sortDirection]
  )

  // Sorted sub-categories
  const sortedSubCategories = useMemo(() => {
    if (sortBy === 'default' || !sortByDate) {
      return data.sub_categories
    }

    return [...data.sub_categories].sort((a, b) => {
      const cellA = a.cells.find((c) => c.date === sortByDate)
      const cellB = b.cells.find((c) => c.date === sortByDate)

      const valA = cellA?.change_pct ? Number(cellA.change_pct) : 0
      const valB = cellB?.change_pct ? Number(cellB.change_pct) : 0

      return sortDirection === 'desc' ? valB - valA : valA - valB
    })
  }, [data.sub_categories, sortBy, sortByDate, sortDirection])

  // Group sub-categories by category for header rendering
  const categoryGroups = useMemo(() => {
    const groups: Array<{
      category: string
      label: string
      startIndex: number
      count: number
    }> = []

    let currentCategory = ''
    let startIndex = 0
    let count = 0

    sortedSubCategories.forEach((col, index) => {
      if (col.category !== currentCategory) {
        if (currentCategory) {
          groups.push({
            category: currentCategory,
            label: data.category_labels[currentCategory] || currentCategory,
            startIndex,
            count,
          })
        }
        currentCategory = col.category
        startIndex = index
        count = 1
      } else {
        count++
      }
    })

    // Push last group
    if (currentCategory) {
      groups.push({
        category: currentCategory,
        label: data.category_labels[currentCategory] || currentCategory,
        startIndex,
        count,
      })
    }

    return groups
  }, [sortedSubCategories, data.category_labels])

  // Calculate dimensions - fit all columns in viewport
  const numColumns = sortedSubCategories.length
  const availableWidth = Math.max(containerWidth - DATE_COLUMN_WIDTH, 100)
  const cellWidth = numColumns > 0 ? availableWidth / numColumns : 20
  const matrixHeight = data.trading_days.length * CELL_HEIGHT
  const svgWidth = containerWidth || DATE_COLUMN_WIDTH + numColumns * cellWidth

  // Always show text now since it's compact (just "1.2")
  const showCellText = true

  // Handle cell hover
  const handleCellHover = useCallback(
    (column: EtfRotationColumn, dateStr: string, event: React.MouseEvent) => {
      const cell = column.cells.find((c) => c.date === dateStr)

      if (cell) {
        setTooltip({
          subCategory: column.name,
          category: column.category,
          categoryLabel: column.category_label,
          date: dateStr,
          changePct: cell.change_pct ? Number(cell.change_pct) : null,
          amount: cell.amount ? Number(cell.amount) : null,
          repCode: cell.rep_code || null,
          repName: cell.rep_name || null,
          mouseX: event.clientX,
          mouseY: event.clientY,
        })
      }
    },
    []
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

  return (
    <div
      ref={containerRef}
      className="relative w-full flex-1 min-h-0 flex flex-col"
      onMouseMove={handleMouseMove}
    >
      {/* Fixed Header SVG */}
      <div className="flex-shrink-0">
        <svg
          width={svgWidth}
          height={HEADER_HEIGHT}
          className="overflow-visible"
          role="img"
          aria-label="ETF轮动矩阵表头"
        >
          {/* Date column header */}
          <rect x={0} y={0} width={DATE_COLUMN_WIDTH} height={HEADER_HEIGHT} className="fill-muted" />
          <text
            x={DATE_COLUMN_WIDTH / 2}
            y={HEADER_HEIGHT / 2}
            textAnchor="middle"
            dominantBaseline="middle"
            fontSize={8}
            className="fill-muted-foreground"
          >
            日期
          </text>

          {/* Category group headers (first row) */}
          <g transform={`translate(${DATE_COLUMN_WIDTH}, 0)`}>
            {categoryGroups.map((group) => {
              const colors = CATEGORY_COLORS[group.category] || {
                bg: '#f5f5f5',
                text: '#666',
                border: '#ddd',
              }
              const groupWidth = group.count * cellWidth

              return (
                <g key={group.category} transform={`translate(${group.startIndex * cellWidth}, 0)`}>
                  <rect
                    x={0}
                    y={0}
                    width={groupWidth}
                    height={CATEGORY_HEADER_HEIGHT}
                    fill={colors.bg}
                    stroke={colors.border}
                    strokeWidth={0.5}
                  />
                  {/* Only show label if group is wide enough */}
                  {groupWidth >= 20 && (
                    <text
                      x={groupWidth / 2}
                      y={CATEGORY_HEADER_HEIGHT / 2}
                      textAnchor="middle"
                      dominantBaseline="middle"
                      fontSize={groupWidth >= 40 ? 9 : 7}
                      fontWeight={500}
                      fill={colors.text}
                    >
                      {groupWidth >= 40 ? group.label : group.label.slice(0, 2)}
                    </text>
                  )}
                </g>
              )
            })}
          </g>

          {/* Sub-category headers (second row) - vertical text with cell borders */}
          <g transform={`translate(${DATE_COLUMN_WIDTH}, ${CATEGORY_HEADER_HEIGHT})`}>
            <rect x={0} y={0} width={svgWidth - DATE_COLUMN_WIDTH} height={SUB_HEADER_HEIGHT} className="fill-muted" />
            {sortedSubCategories.map((col, colIndex) => (
              <g key={col.name} transform={`translate(${colIndex * cellWidth}, 0)`}>
                {/* Cell border */}
                <rect
                  x={0}
                  y={0}
                  width={cellWidth}
                  height={SUB_HEADER_HEIGHT}
                  fill="transparent"
                  stroke="#e5e5e5"
                  strokeWidth={0.5}
                />
                {/* Vertical text */}
                <text
                  x={cellWidth / 2}
                  y={4}
                  textAnchor="start"
                  dominantBaseline="middle"
                  fontSize={8}
                  transform={`rotate(90, ${cellWidth / 2}, 4)`}
                  className="fill-muted-foreground"
                >
                  {col.name.slice(0, 3)}
                </text>
              </g>
            ))}
          </g>
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
          aria-label="ETF轮动矩阵数据"
        >
          {/* Date column */}
          <g>
            <rect x={0} y={0} width={DATE_COLUMN_WIDTH} height={matrixHeight} className="fill-muted" />
            {data.trading_days.map((dateStr, rowIndex) => {
              const isActiveSortDate = sortByDate === dateStr

              return (
                <g
                  key={dateStr}
                  style={{ cursor: 'pointer' }}
                  onClick={() => handleDateClick(dateStr)}
                >
                  <rect
                    x={0}
                    y={rowIndex * CELL_HEIGHT}
                    width={DATE_COLUMN_WIDTH}
                    height={CELL_HEIGHT}
                    fill="transparent"
                  />
                  <text
                    x={DATE_COLUMN_WIDTH / 2}
                    y={rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2}
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fontSize={8}
                    fill="currentColor"
                    className={`font-mono ${isActiveSortDate ? 'font-bold' : 'text-muted-foreground'}`}
                  >
                    {formatDate(dateStr)}
                  </text>
                  {isActiveSortDate && (
                    <g transform={`translate(${DATE_COLUMN_WIDTH - 5}, ${rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2})`}>
                      {sortDirection === 'desc' ? (
                        <ArrowDown x={-3} y={-3} width={5} height={5} className="text-foreground" />
                      ) : (
                        <ArrowUp x={-3} y={-3} width={5} height={5} className="text-foreground" />
                      )}
                    </g>
                  )}
                </g>
              )
            })}
          </g>

          {/* Matrix cells */}
          <g transform={`translate(${DATE_COLUMN_WIDTH}, 0)`}>
            {data.trading_days.map((dateStr, rowIndex) => (
              <g key={dateStr}>
                {sortedSubCategories.map((column, colIndex) => {
                  const cell = column.cells.find((c) => c.date === dateStr)

                  return (
                    <EtfMatrixCell
                      key={`${column.name}-${dateStr}`}
                      x={colIndex * cellWidth}
                      y={rowIndex * CELL_HEIGHT}
                      width={cellWidth}
                      height={CELL_HEIGHT}
                      changePct={cell?.change_pct ? Number(cell.change_pct) : null}
                      showText={showCellText}
                      onHover={(e) => handleCellHover(column, dateStr, e)}
                      onLeave={handleCellLeave}
                    />
                  )
                })}
              </g>
            ))}
          </g>
        </svg>

        {/* Loading More Indicator */}
        {(isLoadingMore || !hasMore) && (
          <div className="flex items-center justify-center py-2 text-xs text-muted-foreground">
            {isLoadingMore ? <span>加载中...</span> : !hasMore ? <span>没有更多数据</span> : null}
          </div>
        )}
      </div>

      {/* Tooltip */}
      {tooltip && <EtfMatrixTooltip data={tooltip} />}
    </div>
  )
}
