/**
 * Rotation Matrix Component
 *
 * SVG-based heatmap for sector rotation visualization:
 * - X-axis: Industries (sorted, with FLIP animation on reorder)
 * - Y-axis: Dates (T-day at top)
 * - Cells: Red-green gradient with change % text
 */

import { useState, useRef, useMemo, useCallback } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import type { SectorRotationResponse } from '@/api/generated/schemas'
import { MatrixCell } from './MatrixCell'
import { MatrixTooltip } from './MatrixTooltip'
import type { TooltipData } from './types'

// Metric keys matching page component
type MetricKey = 'change' | 'volume' | 'flow' | 'momentum'

interface RotationMatrixProps {
  data: SectorRotationResponse
  visibleMetrics: MetricKey[]
}

// Layout constants
const CELL_WIDTH = 52
const CELL_HEIGHT = 24
const DATE_COLUMN_WIDTH = 70
const HEADER_HEIGHT = 40
const PADDING = 8

// Industry chain order (upstream -> midstream -> downstream)
// Must match actual SW L1 industry names from database
const INDUSTRY_CHAIN_ORDER: Record<string, number> = {
  // Upstream resources (1-10)
  '煤炭': 1, '石油石化': 2, '钢铁': 3, '有色金属': 4, '基础化工': 5, '建筑材料': 6,
  // Midstream manufacturing (11-20)
  '机械设备': 11, '电力设备': 12, '国防军工': 13, '电子': 14, '计算机': 15,
  '通信': 16, '汽车': 17, '家用电器': 18, '轻工制造': 19, '纺织服饰': 20,
  // Downstream consumption (21-30)
  '食品饮料': 21, '医药生物': 22, '农林牧渔': 23, '商贸零售': 24,
  '社会服务': 25, '美容护理': 26, '传媒': 27,
  // Utilities & infrastructure (31-40)
  '公用事业': 31, '交通运输': 32, '建筑装饰': 33, '环保': 34, '房地产': 35,
  // Finance (41-50)
  '银行': 41, '非银金融': 42, '综合': 50,
}

export function RotationMatrix({ data, visibleMetrics }: RotationMatrixProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [tooltip, setTooltip] = useState<TooltipData | null>(null)

  // Sort industries by upstream/downstream order (fixed)
  const sortedIndustries = useMemo(() => {
    return [...data.industries].sort((a, b) =>
      (INDUSTRY_CHAIN_ORDER[a.name] ?? 99) - (INDUSTRY_CHAIN_ORDER[b.name] ?? 99)
    )
  }, [data.industries])

  // Calculate dimensions
  const matrixWidth = sortedIndustries.length * CELL_WIDTH
  const matrixHeight = data.trading_days.length * CELL_HEIGHT
  const svgWidth = DATE_COLUMN_WIDTH + matrixWidth + PADDING * 2
  const svgHeight = HEADER_HEIGHT + matrixHeight + PADDING * 2

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
      className="relative overflow-auto"
      onMouseMove={handleMouseMove}
    >
      <svg
        width={svgWidth}
        height={svgHeight}
        className="overflow-visible"
        role="img"
        aria-label="行业轮动矩阵"
      >
        {/* Header row - Industry names */}
        <g transform={`translate(${DATE_COLUMN_WIDTH + PADDING}, ${PADDING})`}>
          <AnimatePresence>
            {sortedIndustries.map((industry, colIndex) => (
              <motion.g
                key={industry.code}
                initial={{ x: colIndex * CELL_WIDTH }}
                animate={{ x: colIndex * CELL_WIDTH }}
                transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
              >
                <text
                  x={CELL_WIDTH / 2}
                  y={HEADER_HEIGHT / 2}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  fontSize={10}
                  fill="currentColor"
                  className="text-muted-foreground"
                >
                  {industry.name.length > 4
                    ? industry.name.slice(0, 4)
                    : industry.name}
                </text>
              </motion.g>
            ))}
          </AnimatePresence>
        </g>

        {/* Date column */}
        <g transform={`translate(${PADDING}, ${HEADER_HEIGHT + PADDING})`}>
          {data.trading_days.map((dateStr, rowIndex) => (
            <text
              key={dateStr}
              x={DATE_COLUMN_WIDTH - 8}
              y={rowIndex * CELL_HEIGHT + CELL_HEIGHT / 2}
              textAnchor="end"
              dominantBaseline="middle"
              fontSize={10}
              fill="currentColor"
              className="text-muted-foreground font-mono"
            >
              {formatDate(dateStr)}
            </text>
          ))}
        </g>

        {/* Matrix cells */}
        <g
          transform={`translate(${DATE_COLUMN_WIDTH + PADDING}, ${HEADER_HEIGHT + PADDING})`}
        >
          {data.trading_days.map((dateStr, rowIndex) => (
            <g key={dateStr}>
              <AnimatePresence>
                {sortedIndustries.map((industry, colIndex) => {
                  const cell = industry.cells.find((c) => c.date === dateStr)

                  return (
                    <MatrixCell
                      key={`${industry.code}-${dateStr}`}
                      x={colIndex * CELL_WIDTH}
                      y={rowIndex * CELL_HEIGHT}
                      width={CELL_WIDTH}
                      height={CELL_HEIGHT}
                      visibleMetrics={visibleMetrics}
                      changePct={cell ? Number(cell.change_pct) : 0}
                      volume={cell?.money_flow ? Number(cell.money_flow) : null}
                      flow={cell?.main_strength ? Number(cell.main_strength) : null}
                      momentum={cell?.main_strength ? Number(cell.main_strength) : null}
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
