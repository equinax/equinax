import { useMemo, useState, useCallback, useRef, useEffect } from 'react'
import { Stage, Layer, Line, Circle, Text, Rect, Group } from 'react-konva'
import type Konva from 'konva'
import { Button } from '@/components/ui/button'
import { Save } from 'lucide-react'
import { SCORE_ITEM_COLORS, SCORE_ITEMS_ORDERED } from '@/lib/score-utils'
import type { MinuteCandle } from '@/types/minute-data'

// ─── Trading Time Constants ───────────────────────────────────────────────────
const TIME_SLOTS = [
  // Morning: 9:30 - 11:30
  9 * 60 + 30, 9 * 60 + 45,
  10 * 60, 10 * 60 + 15, 10 * 60 + 30, 10 * 60 + 45,
  11 * 60, 11 * 60 + 15, 11 * 60 + 30,
  // Afternoon: 13:00 - 15:00
  13 * 60, 13 * 60 + 15, 13 * 60 + 30, 13 * 60 + 45,
  14 * 60, 14 * 60 + 15, 14 * 60 + 30, 14 * 60 + 45,
  15 * 60,
]

const MAJOR_TIME_SLOTS = [
  9 * 60 + 30, 10 * 60, 10 * 60 + 30, 11 * 60, 11 * 60 + 30,
  13 * 60, 13 * 60 + 30, 14 * 60, 14 * 60 + 30, 15 * 60,
]

const formatMinutes = (m: number) => {
  const h = Math.floor(m / 60)
  const min = m % 60
  return `${h}:${String(min).padStart(2, '0')}`
}

const snapToSlot = (minutes: number): number => {
  let closest = TIME_SLOTS[0]
  let minDist = Math.abs(minutes - closest)
  for (const slot of TIME_SLOTS) {
    const dist = Math.abs(minutes - slot)
    if (dist < minDist) {
      closest = slot
      minDist = dist
    }
  }
  return closest
}

const snapToMinute = (minutes: number): number => {
  const morningStart = 9 * 60 + 30
  const morningEnd = 11 * 60 + 30
  const afternoonStart = 13 * 60
  const afternoonEnd = 15 * 60
  if (minutes <= morningStart) return morningStart
  if (minutes <= morningEnd) return minutes
  if (minutes <= afternoonStart) return minutes <= (morningEnd + afternoonStart) / 2 ? morningEnd : afternoonStart
  if (minutes <= afternoonEnd) return minutes
  return afternoonEnd
}

// ─── OHLC Point Types ─────────────────────────────────────────────────────────
type PointRole = 'open' | 'high' | 'low' | 'close'

interface OHLCPoint {
  role: PointRole
  time: number  // minutes from midnight
  price: number // percentage from prev_close (e.g. 2.5 = +2.5%)
}

const ROLE_CONFIG: Record<PointRole, { label: string; shortLabel: string; color: string }> = {
  open:  { label: '开盘', shortLabel: 'O', color: '#3b82f6' },
  high:  { label: '最高', shortLabel: 'H', color: '#ef4444' },
  low:   { label: '最低', shortLabel: 'L', color: '#22c55e' },
  close: { label: '收盘', shortLabel: 'C', color: '#f97316' },
}

// ─── Coordinate Transforms (parameterized for reuse) ──────────────────────────
const MORNING_DURATION = 11 * 60 + 30 - (9 * 60 + 30) // 120 min
const AFTERNOON_DURATION = 15 * 60 - 13 * 60           // 120 min
const TOTAL_TRADING_MINUTES = MORNING_DURATION + AFTERNOON_DURATION // 240 min
const PRICE_RANGE = 10 // ±10%

function timeToX(minutes: number, plotLeft: number, plotWidth: number): number {
  const minTime = TIME_SLOTS[0]
  let elapsed: number
  if (minutes <= 11 * 60 + 30) {
    elapsed = minutes - minTime
  } else {
    elapsed = MORNING_DURATION + (minutes - 13 * 60)
  }
  return plotLeft + (elapsed / TOTAL_TRADING_MINUTES) * plotWidth
}

function priceToY(pctChange: number, plotTop: number, plotHeight: number): number {
  return plotTop + ((PRICE_RANGE - pctChange) / (2 * PRICE_RANGE)) * plotHeight
}

// ─── Props Interface ──────────────────────────────────────────────────────────
interface DayChartProps {
  mode: 'thumbnail' | 'detail'
  // Market data
  tradeDate: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  preClose: number | null
  pctChg: number | null
  // Sketch key points (from API)
  keyPoints?: Record<string, { time: string; price: number }> | null
  // Scores
  scores?: { market: number; sector: number; stock: number } | null
  detailedScores?: Record<string, Record<string, number>> | null
  // Other
  pattern?: string | null
  notes?: string | null
  // Thumbnail-specific
  width?: number
  height?: number
  onClick?: () => void
  // Detail-specific (stub for now)
  onSave?: (keyPoints: Record<string, { time: string; price: number }>) => void
  isSaving?: boolean
  autoPattern?: string | null
  // Minute data
  minuteCandles?: MinuteCandle[] | null
  showMinuteLine?: boolean
  showOhlcPoints?: boolean
  minuteLoading?: boolean
  minuteError?: boolean
}

// ─── Thumbnail Grid Lines ─────────────────────────────────────────────────────
// Only show 10:30, 11:30, 14:00 as vertical markers in thumbnail
const THUMB_TIME_MARKERS = [10 * 60 + 30, 11 * 60 + 30, 14 * 60]

// ─── Parse key points helper ──────────────────────────────────────────────────
function parseKeyPoints(
  keyPoints: Record<string, { time: string; price: number }> | null | undefined
): OHLCPoint[] | null {
  if (!keyPoints) return null
  const roles: PointRole[] = ['open', 'high', 'low', 'close']
  const parsed: OHLCPoint[] = []
  for (const role of roles) {
    const kp = keyPoints[role]
    if (!kp) return null // incomplete data → treat as no keyPoints
    const [h, m] = kp.time.split(':').map(Number)
    parsed.push({ role, time: h * 60 + m, price: kp.price })
  }
  return parsed
}

// ─── Thumbnail Renderer ───────────────────────────────────────────────────────
function ThumbnailChart({
  tradeDate,
  open,
  high,
  low,
  close,
  preClose,
  pctChg,
  keyPoints,
  scores,
  pattern,
  notes,
  height = 140,
  onClick,
}: Omit<DayChartProps, 'mode' | 'onSave' | 'isSaving' | 'autoPattern'>) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [measuredWidth, setMeasuredWidth] = useState(0)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        const w = Math.floor(entry.contentRect.width)
        if (w > 0) setMeasuredWidth(w)
      }
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const width = measuredWidth || 140
  const candleStripWidth = Math.round(width / 30) + 1
  const scoreBarHeight = Math.round(height / 20)
  const noteAreaHeight = 16
  const plotLeft = candleStripWidth
  const plotTop = 18
  const plotWidth = width - candleStripWidth
  const plotHeight = height - plotTop - scoreBarHeight - noteAreaHeight

  const dateLabel = useMemo(() => {
    const cleaned = tradeDate.replace(/-/g, '')
    if (cleaned.length === 8) {
      return `${cleaned.slice(4, 6)}-${cleaned.slice(6, 8)}`
    }
    return tradeDate
  }, [tradeDate])

  const ohlcPoints = useMemo(() => parseKeyPoints(keyPoints), [keyPoints])

  const sortedPoints = useMemo(() => {
    if (!ohlcPoints) return null
    return [...ohlcPoints].sort((a, b) => a.time - b.time)
  }, [ohlcPoints])

  const lineCoords = useMemo(() => {
    if (!sortedPoints) return null
    return sortedPoints.flatMap((p) => [
      timeToX(p.time, plotLeft, plotWidth),
      priceToY(p.price, plotTop, plotHeight),
    ])
  }, [sortedPoints, plotLeft, plotWidth, plotTop, plotHeight])

  const candle = useMemo(() => {
    if (open == null || close == null || high == null || low == null || preClose == null) {
      return null
    }
    const openPct = ((open - preClose) / preClose) * 100
    const closePct = ((close - preClose) / preClose) * 100
    const highPct = ((high - preClose) / preClose) * 100
    const lowPct = ((low - preClose) / preClose) * 100
    const isUp = close >= open
    return { openPct, closePct, highPct, lowPct, isUp }
  }, [open, close, high, low, preClose])

  const scoreSegments = useMemo(() => {
    if (!scores) return null
    const total = scores.market + scores.sector + scores.stock
    if (total <= 0) return null
    const MAX_TOTAL = 100
    const barWidth = (total / MAX_TOTAL) * width
    return {
      market: (scores.market / total) * barWidth,
      sector: (scores.sector / total) * barWidth,
      stock: (scores.stock / total) * barWidth,
      total,
    }
  }, [scores, width])

  const zeroY = priceToY(0, plotTop, plotHeight)

  const candleBodyWidth = candleStripWidth
  const candleX = 1

  return (
    <div
      ref={containerRef}
      style={{
        width: '100%',
        height,
        cursor: onClick ? 'pointer' : 'default',
      }}
      onClick={onClick}
    >
      {measuredWidth > 0 && (
      <Stage width={width} height={height}>
        <Layer>
          {/* Background */}
          <Rect
            x={0}
            y={0}
            width={width}
            height={height}
            fill="rgba(255,255,255,0.02)"
          />

          {/* ── Top bar: date + pattern ── */}
          <Text
            x={plotLeft}
            y={4}
            text={dateLabel}
            fontSize={12}
            fontStyle="bold"
            fill="#6b7280"
          />
          {pctChg != null && (
            <Text
              x={plotLeft + 36}
              y={5}
              text={`${pctChg >= 0 ? '+' : ''}${pctChg.toFixed(2)}%`}
              fontSize={10}
              fill={pctChg >= 0 ? '#ef4444' : '#22c55e'}
            />
          )}
          {pattern && (
            <Text
              x={width - 20}
              y={2}
              text={pattern}
              fontSize={16}
              fontStyle="bold"
              fill="#f59e0b"
              align="right"
              width={20}
            />
          )}

          {/* ── Plot area border ── */}
          <Rect
            x={plotLeft}
            y={plotTop}
            width={plotWidth + 1}
            height={plotHeight}
            fill="rgba(0,0,0,0.01)"
            stroke="rgba(120,120,120,0.1)"
            strokeWidth={0.5}
          />

          {/* ── Grid: horizontal 0% reference ── */}
          <Line
            points={[plotLeft, zeroY, plotLeft + plotWidth + 1, zeroY]}
            stroke="rgba(120,120,120,0.35)"
            strokeWidth={0.5}
            dash={[3, 3]}
          />

          {/* ── Grid: vertical time markers ── */}
          {THUMB_TIME_MARKERS.map((slot) => {
            const x = timeToX(slot, plotLeft, plotWidth)
            return (
              <Line
                key={slot}
                points={[x, plotTop, x, plotTop + plotHeight]}
                stroke="rgba(120,120,120,0.25)"
                strokeWidth={0.5}
                dash={[2, 2]}
              />
            )
          })}

          {/* ── Candlestick ── */}
          <Rect x={0} y={plotTop} width={candleStripWidth} height={plotHeight} fill="rgba(0,0,0,0.02)" />
          {candle && (
            <Group>
              {/* Wick */}
              <Line
                points={[
                  candleX + Math.round(candleBodyWidth / 2) - 1,
                  priceToY(candle.highPct, plotTop, plotHeight),
                  candleX + Math.round(candleBodyWidth / 2) - 1,
                  priceToY(candle.lowPct, plotTop, plotHeight),
                ]}
                stroke={candle.isUp ? '#ef4444' : '#22c55e'}
                strokeWidth={1}
              />
              {/* Body */}
              <Rect
                x={candleX -1}
                y={priceToY(
                  Math.max(candle.openPct, candle.closePct),
                  plotTop,
                  plotHeight,
                )}
                width={candleBodyWidth}
                height={Math.max(
                  1,
                  Math.abs(
                    priceToY(candle.openPct, plotTop, plotHeight) -
                      priceToY(candle.closePct, plotTop, plotHeight),
                  ),
                )}
                fill={candle.isUp ? '#ef4444' : '#22c55e'}
              />
            </Group>
          )}

          {/* ── OHLC Line (only when keyPoints exist) ── */}
          {lineCoords && sortedPoints && (
            <Group>
              <Line
                points={lineCoords}
                stroke="#8b5cf6"
                strokeWidth={1.5}
                lineJoin="round"
                lineCap="round"
              />
              {/* Dots at each OHLC point */}
              {sortedPoints.map((p) => (
                <Circle
                  key={p.role}
                  x={timeToX(p.time, plotLeft, plotWidth)}
                  y={priceToY(p.price, plotTop, plotHeight)}
                  radius={3}
                  fill={ROLE_CONFIG[p.role].color}
                />
              ))}
            </Group>
          )}

          {/* ── Score bar (only when scores exist) ── */}
          {scoreSegments && (
            <Group>
              <Rect
                x={0}
                y={plotTop + plotHeight}
                width={scoreSegments.market}
                height={scoreBarHeight}
                fill="#3b82f6"
              />
              <Rect
                x={scoreSegments.market}
                y={plotTop + plotHeight}
                width={scoreSegments.sector}
                height={scoreBarHeight}
                fill="#22c55e"
              />
              <Rect
                x={scoreSegments.market + scoreSegments.sector}
                y={plotTop + plotHeight}
                width={scoreSegments.stock}
                height={scoreBarHeight}
                fill="#ec4899"
              />
            </Group>
          )}

          {notes && (
            <Text
              x={2}
              y={plotTop + plotHeight + scoreBarHeight + 2}
              text={notes}
              fontSize={9}
              fill="#9ca3af"
              width={width - 4}
              ellipsis={true}
              wrap="none"
            />
          )}
        </Layer>
      </Stage>
      )}
    </div>
  )
}

// ─── Detail Layout Constants ──────────────────────────────────────────────────
const DETAIL_PADDING = { top: 24, right: 35, bottom: 20, left: 12 }
const DETAIL_CANDLE_STRIP_WIDTH = 0
const DETAIL_TIME_MARKERS = [
  9 * 60 + 30,   // 9:30
  10 * 60 + 30,  // 10:30
  11 * 60 + 30,  // 11:30 / 13:00 boundary
  14 * 60,       // 14:00
  15 * 60,       // 15:00
]

const DEFAULT_POINTS: OHLCPoint[] = [
  { role: 'open', time: 9 * 60 + 30, price: 0 },
  { role: 'high', time: 10 * 60 + 30, price: 3 },
  { role: 'low', time: 14 * 60, price: -2 },
  { role: 'close', time: 15 * 60, price: 1 },
]

// ─── Detail Renderer ──────────────────────────────────────────────────────────
function DetailChart({
  open,
  high,
  low,
  close,
  preClose,
  keyPoints,
  detailedScores,
  onSave,
  isSaving = false,
  minuteCandles,
  minuteLoading = false,
  minuteError = false,
}: Omit<DayChartProps, 'mode' | 'tradeDate' | 'pctChg' | 'pattern' | 'notes' | 'width' | 'height' | 'onClick' | 'autoPattern'>) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [dimensions, setDimensions] = useState({ width: 600, height: 500 })

  // Internal toggle state (overrides optional props)
  const [minuteDisplay, setMinuteDisplay] = useState<'off' | 'line' | 'candle'>('line')
  const [ohlcVisible, setOhlcVisible] = useState(true)

  // Observe container size
  useEffect(() => {
    if (!containerRef.current) return
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        setDimensions({
          width: Math.max(400, entry.contentRect.width),
          height: Math.max(300, entry.contentRect.height),
        })
      }
    })
    observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [])

  // Parse initial points from API data
  const initialPoints = useMemo((): OHLCPoint[] => {
    const parsed = parseKeyPoints(keyPoints)
    return parsed ?? DEFAULT_POINTS
  }, [keyPoints])

  const [points, setPoints] = useState<OHLCPoint[]>(initialPoints)
  const [isDirty, setIsDirty] = useState(false)
  const [dragging, setDragging] = useState<PointRole | null>(null)
  const [crosshair, setCrosshair] = useState<{ x: number; y: number } | null>(null)

  // Update points when keyPoints prop changes
  useEffect(() => {
    setPoints(initialPoints)
    setIsDirty(false)
  }, [initialPoints])

  const { width, height } = dimensions
  const plotLeft = DETAIL_PADDING.left + DETAIL_CANDLE_STRIP_WIDTH
  const plotTop = DETAIL_PADDING.top
  const plotWidth = width - DETAIL_PADDING.left - DETAIL_CANDLE_STRIP_WIDTH - DETAIL_PADDING.right
  const plotHeight = height - DETAIL_PADDING.top - DETAIL_PADDING.bottom

  // Inverse coordinate transforms (pixel → data)
  const xToTime = useCallback(
    (x: number) => {
      const elapsed = ((x - plotLeft) / plotWidth) * TOTAL_TRADING_MINUTES
      let minutes: number
      if (elapsed <= MORNING_DURATION) {
        minutes = TIME_SLOTS[0] + elapsed
      } else {
        minutes = 13 * 60 + (elapsed - MORNING_DURATION)
      }
      return snapToMinute(Math.round(minutes))
    },
    [plotLeft, plotWidth],
  )

  // Drag handler — only H/L are draggable, only time changes (price locked)
  const handleDrag = useCallback(
    (role: PointRole, e: Konva.KonvaEventObject<DragEvent>) => {
      const node = e.target
      const newTime = xToTime(node.x())

      node.x(timeToX(newTime, plotLeft, plotWidth))

      setPoints((prev) =>
        prev.map((p) =>
          p.role === role ? { ...p, time: newTime } : p,
        ),
      )
      setIsDirty(true)
    },
    [xToTime, plotLeft, plotWidth],
  )

  // Save handler
  const handleSave = useCallback(() => {
    if (!onSave) return
    const result: Record<string, { time: string; price: number }> = {}
    for (const p of points) {
      result[p.role] = {
        time: formatMinutes(p.time),
        price: Math.round(p.price * 100) / 100,
      }
    }
    onSave(result)
    setIsDirty(false)
  }, [onSave, points])

  // Grid lines for price axis
  const priceGridLines = useMemo(() => {
    const lines: { pct: number; label: string }[] = []
    for (let p = -PRICE_RANGE; p <= PRICE_RANGE; p += 2) {
      lines.push({
        pct: p,
        label: p === 0 ? '0%' : `${p > 0 ? '+' : ''}${p}%`,
      })
    }
    return lines
  }, [])

  // Candlestick calculations
  const candle = useMemo(() => {
    if (open == null || close == null || high == null || low == null || preClose == null) {
      return null
    }
    const openPct = ((open - preClose) / preClose) * 100
    const closePct = ((close - preClose) / preClose) * 100
    const highPct = ((high - preClose) / preClose) * 100
    const lowPct = ((low - preClose) / preClose) * 100
    const isUp = close >= open
    return { openPct, closePct, highPct, lowPct, isUp }
  }, [open, close, high, low, preClose])

  // Auto-snap: derive snapped positions when H/L prices match O/C prices
  const snappedPoints = useMemo(() => {
    if (candle == null) return points

    return points.map((p) => {
      if (p.role === 'high') {
        if (candle.highPct === candle.openPct) {
          const openPoint = points.find((pt) => pt.role === 'open')
          if (openPoint) return { ...p, time: openPoint.time }
        }
        if (candle.highPct === candle.closePct) {
          const closePoint = points.find((pt) => pt.role === 'close')
          if (closePoint) return { ...p, time: closePoint.time }
        }
      }
      if (p.role === 'low') {
        if (candle.lowPct === candle.openPct) {
          const openPoint = points.find((pt) => pt.role === 'open')
          if (openPoint) return { ...p, time: openPoint.time }
        }
        if (candle.lowPct === candle.closePct) {
          const closePoint = points.find((pt) => pt.role === 'close')
          if (closePoint) return { ...p, time: closePoint.time }
        }
      }
      return p
    })
  }, [points, candle])

  // Which roles are auto-snapped (non-draggable)
  const snappedRoles = useMemo(() => {
    const roles = new Set<PointRole>()
    if (candle == null) return roles
    if (candle.highPct === candle.openPct || candle.highPct === candle.closePct) roles.add('high')
    if (candle.lowPct === candle.openPct || candle.lowPct === candle.closePct) roles.add('low')
    return roles
  }, [candle])

  const sortedPoints = useMemo(
    () => [...snappedPoints].sort((a, b) => a.time - b.time),
    [snappedPoints],
  )

  const lineCoords = useMemo(
    () =>
      sortedPoints.flatMap((p) => {
        const pct = candle
          ? p.role === 'open' ? candle.openPct
            : p.role === 'high' ? candle.highPct
            : p.role === 'low' ? candle.lowPct
            : candle.closePct
          : p.price
        return [
          timeToX(p.time, plotLeft, plotWidth),
          priceToY(pct, plotTop, plotHeight),
        ]
      }),
    [sortedPoints, candle, plotLeft, plotWidth, plotTop, plotHeight],
  )

  // Score bar segments
  const scoreBarSegments = useMemo(() => {
    if (!detailedScores) return null
    const segments: { x: number; w: number; color: string }[] = []
    let x = 0
    for (const { section, key } of SCORE_ITEMS_ORDERED) {
      const value = detailedScores[section]?.[key] ?? 0
      const w = (value / 100) * width
      if (w > 0) {
        segments.push({ x, w, color: SCORE_ITEM_COLORS[section]?.[key] ?? '#888' })
      }
      x += w
    }
    return segments.length > 0 ? segments : null
  }, [detailedScores, width])

  // Minute line points for intraday price overlay
  const minuteLinePoints = useMemo(() => {
    if (minuteDisplay !== 'line' || !minuteCandles?.length || preClose == null) return null
    const pts: number[] = []
    const firstOpen = minuteCandles[0].open
    const openPct = ((firstOpen - preClose) / preClose) * 100
    pts.push(timeToX(9 * 60 + 30, plotLeft, plotWidth), priceToY(openPct, plotTop, plotHeight))
    for (const c of minuteCandles) {
      const [h, m] = c.time.split(':').map(Number)
      const midMinutes = h * 60 + m - 2.5
      const pct = ((c.close - preClose) / preClose) * 100
      pts.push(timeToX(midMinutes, plotLeft, plotWidth), priceToY(pct, plotTop, plotHeight))
    }
    const lastClose = minuteCandles[minuteCandles.length - 1].close
    const closePct = ((lastClose - preClose) / preClose) * 100
    pts.push(timeToX(15 * 60, plotLeft, plotWidth), priceToY(closePct, plotTop, plotHeight))
    return pts
  }, [minuteDisplay, minuteCandles, preClose, plotLeft, plotWidth, plotTop, plotHeight])

  // Volume bars geometry for intraday volume overlay
  const volumeBars = useMemo(() => {
    if (minuteDisplay === 'off' || !minuteCandles?.length) return null
    const maxVol = Math.max(...minuteCandles.map(c => c.volume))
    if (maxVol <= 0) return null
    const barW = plotWidth / minuteCandles.length
    return minuteCandles.map(c => {
      const [h, m] = c.time.split(':').map(Number)
      const midMinutes = h * 60 + m - 2.5
      const x = timeToX(midMinutes, plotLeft, plotWidth) - barW / 2
      const barH = (c.volume / maxVol) * plotHeight * 0.2
      const y = plotTop + plotHeight - barH
      const color = c.close >= c.open ? '#ef4444' : '#22c55e'
      return { x, y, w: barW, h: barH, color }
    })
  }, [minuteDisplay, minuteCandles, plotLeft, plotWidth, plotTop, plotHeight])

  // Minute candlestick bars for candle mode
  const minuteCandleBars = useMemo(() => {
    if (minuteDisplay !== 'candle' || !minuteCandles?.length || preClose == null) return null
    const barW = plotWidth / minuteCandles.length
    return minuteCandles.map(c => {
      const [h, m] = c.time.split(':').map(Number)
      const midMinutes = h * 60 + m - 2.5
      const x = timeToX(midMinutes, plotLeft, plotWidth)
      const openPct = ((c.open - preClose) / preClose) * 100
      const closePct = ((c.close - preClose) / preClose) * 100
      const highPct = ((c.high - preClose) / preClose) * 100
      const lowPct = ((c.low - preClose) / preClose) * 100
      const isUp = c.close >= c.open
      return { x, openPct, closePct, highPct, lowPct, isUp, barW }
    })
  }, [minuteDisplay, minuteCandles, preClose, plotLeft, plotWidth])

  // Crosshair tooltip info — nearest minute candle to cursor X
  const crosshairInfo = useMemo(() => {
    if (!crosshair || !minuteCandles?.length || preClose == null) return null
    const cursorTime = xToTime(crosshair.x)
    let nearest = minuteCandles[0]
    let minDist = Infinity
    for (const c of minuteCandles) {
      const [h, m] = c.time.split(':').map(Number)
      const mid = h * 60 + m - 2.5
      const dist = Math.abs(mid - cursorTime)
      if (dist < minDist) { minDist = dist; nearest = c }
    }
    const pct = ((nearest.close - preClose) / preClose) * 100
    return {
      time: nearest.time,
      price: nearest.close.toFixed(2),
      pct,
      pctStr: `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`,
      volume: nearest.volume,
      high: nearest.high.toFixed(2),
      low: nearest.low.toFixed(2),
    }
  }, [crosshair, minuteCandles, preClose, xToTime])

  // Candlestick X position (centered in candle strip)
  const candleX = plotLeft - 6

  return (
    <div className="relative">
      {/* Canvas */}
      <div
        ref={containerRef}
        className="w-full border overflow-hidden bg-background"
        style={{ height: 500 }}
      >
        <Stage
          width={width}
          height={height}
          onMouseMove={(e) => {
            const stage = e.target.getStage()
            if (!stage) return
            const pos = stage.getPointerPosition()
            if (!pos) return
            if (pos.x >= plotLeft && pos.x <= plotLeft + plotWidth && pos.y >= plotTop && pos.y <= plotTop + plotHeight) {
              setCrosshair({ x: pos.x, y: pos.y })
            } else {
              setCrosshair(null)
            }
          }}
          onMouseLeave={() => setCrosshair(null)}
        >
          <Layer>
            {/* Background */}
            <Rect
              x={0}
              y={0}
              width={width}
              height={height}
              fill="transparent"
            />

            {/* Plot area background */}
            <Rect
              x={plotLeft}
              y={plotTop}
              width={plotWidth}
              height={plotHeight}
              fill="rgba(120,120,120,0.03)"
            />

            {/* Price grid lines */}
            {priceGridLines.map((gl) => {
              const y = priceToY(gl.pct, plotTop, plotHeight)
              return (
                <Group key={gl.pct}>
                  <Line
                    points={[0, y, width, y]}
                    stroke={
                      gl.pct === 0
                        ? 'rgba(120,120,120,0.5)'
                        : 'rgba(120,120,120,0.15)'
                    }
                    strokeWidth={gl.pct === 0 ? 1 : 0.5}
                    dash={[3, 3]}
                  />

                  {preClose != null && (
                    <Text
                      x={plotLeft + plotWidth + 5}
                      y={y + (gl.pct === PRICE_RANGE ? 2 : gl.pct === -PRICE_RANGE ? -10 : -4)}
                      text={(preClose * (1 + gl.pct / 100)).toFixed(2)}
                      fontSize={9}
                      fill={
                        gl.pct > 0
                          ? 'rgba(239,68,68,0.7)'
                          : gl.pct < 0
                            ? 'rgba(34,197,94,0.7)'
                            : 'rgba(120,120,120,0.6)'
                      }
                    />
                  )}
                </Group>
              )
            })}

            {/* Time grid lines (5 vertical) */}
            {DETAIL_TIME_MARKERS.map((slot) => {
              const x = timeToX(slot, plotLeft, plotWidth)
              return (
                <Line
                  key={slot}
                  points={[x, plotTop, x, plotTop + plotHeight]}
                  stroke="rgba(120,120,120,0.2)"
                  strokeWidth={0.5}
                  dash={[3, 3]}
                />
              )
            })}

            {/* Candlestick */}
            {candle && (
              <Group>
                {/* Wick */}
                <Line
                  points={[
                    candleX + 0,
                    priceToY(candle.highPct, plotTop, plotHeight),
                    candleX,
                    priceToY(candle.lowPct, plotTop, plotHeight),
                  ]}
                  stroke={candle.isUp ? '#ef4444' : '#22c55e'}
                  strokeWidth={2}
                />
                {/* Body */}
                <Rect
                  x={0}
                  y={priceToY(
                    Math.max(candle.openPct, candle.closePct),
                    plotTop,
                    plotHeight,
                  )}
                  width={12}
                  height={Math.max(
                    2,
                    Math.abs(
                      priceToY(candle.openPct, plotTop, plotHeight) -
                        priceToY(candle.closePct, plotTop, plotHeight),
                    ),
                  )}
                  fill={candle.isUp ? '#ef4444' : '#22c55e'}
                />
              </Group>
            )}

            {/* Volume bars */}
            {volumeBars && volumeBars.map((bar, i) => (
              <Rect
                key={i}
                x={bar.x}
                y={bar.y}
                width={bar.w}
                height={bar.h}
                fill={bar.color}
              />
            ))}

            {/* Minute loading/error/empty states */}
            {minuteLoading && (
              <Text
                x={plotLeft + plotWidth / 2 - 40}
                y={plotTop + plotHeight / 2 - 6}
                text="加载分时数据..."
                fontSize={12}
                fill="rgba(120,120,120,0.5)"
              />
            )}
            {minuteError && !minuteLoading && (
              <Text
                x={plotLeft + plotWidth / 2 - 50}
                y={plotTop + plotHeight / 2 - 6}
                text="分时数据加载失败"
                fontSize={12}
                fill="rgba(239,68,68,0.5)"
              />
            )}
            {!minuteLoading && !minuteError && minuteCandles !== undefined && minuteCandles !== null && minuteCandles.length === 0 && (
              <Text
                x={plotLeft + plotWidth / 2 - 30}
                y={plotTop + plotHeight / 2 - 6}
                text="无分时数据"
                fontSize={12}
                fill="rgba(120,120,120,0.4)"
              />
            )}

            {/* Minute price line */}
            {minuteLinePoints && (
              <Line
                points={minuteLinePoints}
                stroke="rgba(59,130,246,0.8)"
                strokeWidth={1.5}
                lineJoin="round"
                lineCap="round"
              />
            )}

            {/* Minute candlestick bars */}
            {minuteCandleBars && minuteCandleBars.map((bar, i) => {
              const bodyTop = priceToY(Math.max(bar.openPct, bar.closePct), plotTop, plotHeight)
              const bodyBot = priceToY(Math.min(bar.openPct, bar.closePct), plotTop, plotHeight)
              const wickTop = priceToY(bar.highPct, plotTop, plotHeight)
              const wickBot = priceToY(bar.lowPct, plotTop, plotHeight)
              const fill = bar.isUp ? '#ef4444' : '#22c55e'
              return (
                <Group key={i}>
                  <Line
                    points={[bar.x, wickTop, bar.x, wickBot]}
                    stroke={fill}
                    strokeWidth={1}
                  />
                  <Rect
                    x={bar.x - bar.barW / 2}
                    y={bodyTop}
                    width={bar.barW}
                    height={Math.max(1, bodyBot - bodyTop)}
                    fill={fill}
                  />
                </Group>
              )
            })}

            {/* Crosshair */}
            {crosshair && (
              <Group>
                <Line
                  points={[crosshair.x, plotTop, crosshair.x, plotTop + plotHeight]}
                  stroke="rgba(120,120,120,0.5)"
                  strokeWidth={0.5}
                  dash={[2, 2]}
                />
                <Line
                  points={[plotLeft, crosshair.y, plotLeft + plotWidth, crosshair.y]}
                  stroke="rgba(120,120,120,0.5)"
                  strokeWidth={0.5}
                  dash={[2, 2]}
                />
              </Group>
            )}
            {/* Crosshair info rendered as HTML overlay below */}

            {/* Connecting line + OHLC points */}
            {ohlcVisible && (
              <>
                <Line
                  points={lineCoords}
                  stroke="rgba(120,120,120,0.4)"
                  strokeWidth={2}
                  lineJoin="round"
                  lineCap="round"
                />

                {snappedPoints.map((p) => {
              const config = ROLE_CONFIG[p.role]
              const cx = timeToX(p.time, plotLeft, plotWidth)
              const actualPct = candle
                ? p.role === 'open' ? candle.openPct
                  : p.role === 'high' ? candle.highPct
                  : p.role === 'low' ? candle.lowPct
                  : candle.closePct
                : p.price
              const cy = priceToY(actualPct, plotTop, plotHeight)
              const isFixed = p.role === 'open' || p.role === 'close' || snappedRoles.has(p.role)
              const isActive = dragging === p.role
              return (
                <Group key={p.role}>
                  {isActive && !isFixed && (
                    <Circle
                      x={cx}
                      y={cy}
                      radius={18}
                      fill={`${config.color}20`}
                      stroke={config.color}
                      strokeWidth={1}
                    />
                  )}
                  <Circle
                    x={cx}
                    y={cy}
                    radius={isFixed ? 6 : isActive ? 10 : 8}
                    fill={config.color}
                    stroke="white"
                    strokeWidth={isFixed ? 1 : 2}
                    opacity={isFixed ? 0.6 : 1}
                    draggable={!isFixed}
                    {...(!isFixed && {
                      onDragStart: () => setDragging(p.role),
                      onDragMove: (e: Konva.KonvaEventObject<DragEvent>) => handleDrag(p.role, e),
                      onDragEnd: () => setDragging(null),
                      dragBoundFunc: (pos: { x: number; y: number }) => ({
                        x: pos.x,
                        y: priceToY(actualPct, plotTop, plotHeight),
                      }),
                    })}
                    {...(!isFixed && {
                      shadowColor: config.color,
                      shadowBlur: isActive ? 12 : 6,
                      shadowOpacity: 0.5,
                    })}
                  />
                  <Text
                    x={cx - 4}
                    y={cy - 22}
                    text={config.shortLabel}
                    fontSize={11}
                    fontStyle="bold"
                    fill={config.color}
                    opacity={isFixed ? 0.6 : 1}
                  />
                  <Text
                    x={cx + 12}
                    y={cy - 5}
                    text={`${actualPct >= 0 ? '+' : ''}${actualPct.toFixed(1)}%`}
                    fontSize={9}
                    fill="rgba(120,120,120,0.7)"
                  />
                  <Text
                    x={cx - 12}
                    y={cy + 12}
                    text={formatMinutes(p.time)}
                    fontSize={9}
                    fill="rgba(120,120,120,0.5)"
                  />
                </Group>
              )
            })}
              </>
            )}

            {/* Score bar */}
            {scoreBarSegments && (
              <Group>
                {scoreBarSegments.map((seg, i) => (
                  <Rect
                    key={i}
                    x={seg.x}
                    y={height - DETAIL_PADDING.bottom}
                    width={seg.w}
                    height={DETAIL_PADDING.bottom}
                    fill={seg.color}
                  />
                ))}
              </Group>
            )}
          </Layer>
        </Stage>
      </div>

      {/* Crosshair info — rendered in the top padding area */}
      {crosshairInfo && (
        <div className="absolute top-1 left-3 z-10 text-[11px] font-mono flex items-center gap-2">
          <span className="text-blue-400">{crosshairInfo.time}</span>
          <span className={crosshairInfo.pct >= 0 ? 'text-red-500' : 'text-green-500'}>
            {crosshairInfo.price} ({crosshairInfo.pctStr})
          </span>
          <span className="text-red-500/70">高:{crosshairInfo.high}</span>
          <span className="text-green-500/70">低:{crosshairInfo.low}</span>
          <span className="text-muted-foreground/70">量:{crosshairInfo.volume}</span>
        </div>
      )}

      {/* Toggle buttons overlay */}
      <div className="absolute top-1 right-2 z-10 flex items-center gap-1.5">
        <button
          className={`px-2 py-0.5 rounded text-xs transition-colors ${minuteDisplay !== 'off' ? 'bg-blue-500/20 text-blue-400' : 'bg-muted text-muted-foreground'}`}
          onClick={() => setMinuteDisplay(prev => prev === 'off' ? 'line' : prev === 'line' ? 'candle' : 'off')}
        >
          {minuteDisplay === 'off' ? '分时' : minuteDisplay === 'line' ? '分时·线' : '分时·K'}
        </button>
        <button
          className={`px-2 py-0.5 rounded text-xs transition-colors ${ohlcVisible ? 'bg-amber-500/20 text-amber-400' : 'bg-muted text-muted-foreground'}`}
          onClick={() => setOhlcVisible(prev => !prev)}
        >
          OHLC
        </button>
        {ohlcVisible && minuteCandles && minuteCandles.length > 0 && (
          <button
            className="px-1.5 py-0.5 rounded text-[10px] bg-muted text-muted-foreground hover:bg-amber-500/20 hover:text-amber-400 transition-colors"
            onClick={() => {
              if (!minuteCandles?.length) return
              let maxHighVal = -Infinity
              let maxHighTime = 0
              let minLowVal = Infinity
              let minLowTime = 0
              for (const c of minuteCandles) {
                const [h, m] = c.time.split(':').map(Number)
                const mid = h * 60 + m - 2.5
                if (c.high > maxHighVal) { maxHighVal = c.high; maxHighTime = mid }
                if (c.low < minLowVal) { minLowVal = c.low; minLowTime = mid }
              }
              setPoints(prev => prev.map(p => {
                if (p.role === 'high') return { ...p, time: snapToMinute(Math.round(maxHighTime)) }
                if (p.role === 'low') return { ...p, time: snapToMinute(Math.round(minLowTime)) }
                return p
              }))
              setIsDirty(true)
            }}
          >
            校
          </button>
        )}
      </div>

      {/* Floating save button — appears only when dirty */}
      {onSave && isDirty && (
        <Button
          size="sm"
          className="absolute top-7 right-2 z-10"
          onClick={handleSave}
          disabled={isSaving}
        >
          <Save className="h-3 w-3 mr-1" />
          {isSaving ? '保存中...' : '保存'}
        </Button>
      )}

      {/* Dragging hint */}
      {dragging && (
        <div className="text-xs text-muted-foreground text-center">
          拖动{ROLE_CONFIG[dragging].label}节点 — 松开吸附到最近时间格
        </div>
      )}
    </div>
  )
}

// ─── Main DayChart Component ──────────────────────────────────────────────────
export default function DayChart(props: DayChartProps) {
  if (props.mode === 'detail') {
    return <DetailChart {...props} />
  }

  return <ThumbnailChart {...props} />
}

// Re-export types and helpers for future use by detail mode
export type { DayChartProps, PointRole, OHLCPoint }
export {
  TIME_SLOTS,
  MAJOR_TIME_SLOTS,
  ROLE_CONFIG,
  PRICE_RANGE,
  formatMinutes,
  snapToSlot,
  timeToX,
  priceToY,
  parseKeyPoints,
}
