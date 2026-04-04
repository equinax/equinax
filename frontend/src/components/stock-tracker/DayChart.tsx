import { useMemo, useState, useCallback, useRef, useEffect } from 'react'
import { Stage, Layer, Line, Circle, Text, Rect, Group } from 'react-konva'
import type Konva from 'konva'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Save } from 'lucide-react'

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

// ─── OHLC Point Types ─────────────────────────────────────────────────────────
type PointRole = 'open' | 'high' | 'low' | 'close'

interface OHLCPoint {
  role: PointRole
  time: number  // minutes from midnight
  price: number // percentage from prev_close (e.g. 2.5 = +2.5%)
}

const ROLE_CONFIG: Record<PointRole, { label: string; shortLabel: string; color: string }> = {
  open:  { label: '开盘', shortLabel: 'O', color: '#3b82f6' },
  high:  { label: '最高', shortLabel: 'H', color: '#22c55e' },
  low:   { label: '最低', shortLabel: 'L', color: '#ef4444' },
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
const DETAIL_PADDING = { top: 40, right: 60, bottom: 50, left: 60 }

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
  scores,
  autoPattern,
  onSave,
  isSaving = false,
}: Omit<DayChartProps, 'mode' | 'tradeDate' | 'pctChg' | 'pattern' | 'notes' | 'width' | 'height' | 'onClick'>) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [dimensions, setDimensions] = useState({ width: 600, height: 400 })

  // Observe container size
  useEffect(() => {
    if (!containerRef.current) return
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        setDimensions({
          width: Math.max(400, entry.contentRect.width),
          height: Math.max(300, 400),
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

  // Update points when keyPoints prop changes
  useEffect(() => {
    setPoints(initialPoints)
    setIsDirty(false)
  }, [initialPoints])

  const { width, height } = dimensions
  const plotLeft = DETAIL_PADDING.left
  const plotTop = DETAIL_PADDING.top
  const plotWidth = width - DETAIL_PADDING.left - DETAIL_PADDING.right
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
      return snapToSlot(Math.round(minutes))
    },
    [plotLeft, plotWidth],
  )

  const yToPrice = useCallback(
    (y: number) => {
      return PRICE_RANGE - ((y - plotTop) / plotHeight) * (2 * PRICE_RANGE)
    },
    [plotTop, plotHeight],
  )

  // Sort points by time for connecting line
  const sortedPoints = useMemo(
    () => [...points].sort((a, b) => a.time - b.time),
    [points],
  )

  // Line points (flat array)
  const lineCoords = useMemo(
    () =>
      sortedPoints.flatMap((p) => [
        timeToX(p.time, plotLeft, plotWidth),
        priceToY(p.price, plotTop, plotHeight),
      ]),
    [sortedPoints, plotLeft, plotWidth, plotTop, plotHeight],
  )

  // Drag handler
  const handleDrag = useCallback(
    (role: PointRole, e: Konva.KonvaEventObject<DragEvent>) => {
      const node = e.target
      const newTime = xToTime(node.x())
      const newPrice = Math.max(
        -PRICE_RANGE,
        Math.min(PRICE_RANGE, yToPrice(node.y())),
      )

      // Snap x to time slot
      node.x(timeToX(newTime, plotLeft, plotWidth))
      // Clamp y
      node.y(priceToY(newPrice, plotTop, plotHeight))

      setPoints((prev) =>
        prev.map((p) =>
          p.role === role ? { ...p, time: newTime, price: newPrice } : p,
        ),
      )
      setIsDirty(true)
    },
    [xToTime, yToPrice, plotLeft, plotWidth, plotTop, plotHeight],
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

  // Score bar segments
  const scoreSegments = useMemo(() => {
    if (!scores) return null
    const total = scores.market + scores.sector + scores.stock
    if (total <= 0) return null
    const barWidth = plotWidth
    return {
      market: (scores.market / total) * barWidth,
      sector: (scores.sector / total) * barWidth,
      stock: (scores.stock / total) * barWidth,
      total,
    }
  }, [scores, plotWidth])

  // Candlestick X position
  const candleX = plotLeft + 6

  return (
    <div className="space-y-2">
      {/* Controls */}
      <div className="flex items-center justify-between px-1">
        <div className="flex items-center gap-2">
          {autoPattern && (
            <Badge variant="outline" className="font-bold text-base">
              天干: {autoPattern}
            </Badge>
          )}
          {/* Point legend */}
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            {(['open', 'high', 'low', 'close'] as PointRole[]).map((role) => (
              <span key={role} className="flex items-center gap-1">
                <span
                  className="inline-block w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: ROLE_CONFIG[role].color }}
                />
                {ROLE_CONFIG[role].label}
              </span>
            ))}
          </div>
        </div>
        {onSave && (
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!isDirty || isSaving}
          >
            <Save className="h-3 w-3 mr-1" />
            {isSaving ? '保存中...' : '保存'}
          </Button>
        )}
      </div>

      {/* Canvas */}
      <div
        ref={containerRef}
        className="w-full border rounded-lg overflow-hidden bg-background"
        style={{ height: 400 }}
      >
        <Stage width={width} height={height}>
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
              fill="rgba(255,255,255,0.02)"
              stroke="rgba(255,255,255,0.1)"
              strokeWidth={1}
            />

            {/* Price grid lines */}
            {priceGridLines.map((gl) => {
              const y = priceToY(gl.pct, plotTop, plotHeight)
              return (
                <Group key={gl.pct}>
                  <Line
                    points={[plotLeft, y, plotLeft + plotWidth, y]}
                    stroke={
                      gl.pct === 0
                        ? 'rgba(255,255,255,0.3)'
                        : 'rgba(255,255,255,0.06)'
                    }
                    strokeWidth={gl.pct === 0 ? 1.5 : 0.5}
                    dash={gl.pct === 0 ? undefined : [4, 4]}
                  />
                  {/* Left labels: percentage */}
                  <Text
                    x={2}
                    y={y - 6}
                    text={gl.label}
                    fontSize={10}
                    fill={
                      gl.pct > 0
                        ? 'rgba(34,197,94,0.7)'
                        : gl.pct < 0
                          ? 'rgba(239,68,68,0.7)'
                          : 'rgba(255,255,255,0.5)'
                    }
                  />
                  {/* Right labels: absolute price if preClose available */}
                  {preClose != null && (
                    <Text
                      x={plotLeft + plotWidth + 5}
                      y={y - 6}
                      text={(preClose * (1 + gl.pct / 100)).toFixed(2)}
                      fontSize={9}
                      fill="rgba(255,255,255,0.35)"
                    />
                  )}
                </Group>
              )
            })}

            {/* Time grid lines (major) */}
            {MAJOR_TIME_SLOTS.map((slot) => {
              const x = timeToX(slot, plotLeft, plotWidth)
              return (
                <Group key={slot}>
                  <Line
                    points={[x, plotTop, x, plotTop + plotHeight]}
                    stroke="rgba(255,255,255,0.06)"
                    strokeWidth={0.5}
                    dash={[4, 4]}
                  />
                  <Text
                    x={x - 12}
                    y={plotTop + plotHeight + 8}
                    text={formatMinutes(slot)}
                    fontSize={10}
                    fill="rgba(255,255,255,0.4)"
                  />
                </Group>
              )
            })}

            {/* Candlestick */}
            {candle && (
              <Group>
                {/* Wick */}
                <Line
                  points={[
                    candleX + 6,
                    priceToY(candle.highPct, plotTop, plotHeight),
                    candleX + 6,
                    priceToY(candle.lowPct, plotTop, plotHeight),
                  ]}
                  stroke={candle.isUp ? '#ef4444' : '#22c55e'}
                  strokeWidth={2}
                />
                {/* Body */}
                <Rect
                  x={candleX}
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

            {/* Connecting line */}
            <Line
              points={lineCoords}
              stroke="rgba(255,255,255,0.6)"
              strokeWidth={2}
              lineJoin="round"
              lineCap="round"
            />

            {/* Draggable OHLC points */}
            {points.map((p) => {
              const config = ROLE_CONFIG[p.role]
              const cx = timeToX(p.time, plotLeft, plotWidth)
              const cy = priceToY(p.price, plotTop, plotHeight)
              const isActive = dragging === p.role
              return (
                <Group key={p.role}>
                  {/* Outer glow when dragging */}
                  {isActive && (
                    <Circle
                      x={cx}
                      y={cy}
                      radius={18}
                      fill={`${config.color}20`}
                      stroke={config.color}
                      strokeWidth={1}
                    />
                  )}
                  {/* Main point */}
                  <Circle
                    x={cx}
                    y={cy}
                    radius={isActive ? 10 : 8}
                    fill={config.color}
                    stroke="white"
                    strokeWidth={2}
                    draggable
                    onDragStart={() => setDragging(p.role)}
                    onDragMove={(e) => handleDrag(p.role, e)}
                    onDragEnd={() => setDragging(null)}
                    shadowColor={config.color}
                    shadowBlur={isActive ? 12 : 6}
                    shadowOpacity={0.5}
                  />
                  {/* Short label above */}
                  <Text
                    x={cx - 4}
                    y={cy - 22}
                    text={config.shortLabel}
                    fontSize={11}
                    fontStyle="bold"
                    fill={config.color}
                  />
                  {/* Price label to right */}
                  <Text
                    x={cx + 12}
                    y={cy - 5}
                    text={`${p.price >= 0 ? '+' : ''}${p.price.toFixed(1)}%`}
                    fontSize={9}
                    fill="rgba(255,255,255,0.6)"
                  />
                  {/* Time label below */}
                  <Text
                    x={cx - 12}
                    y={cy + 12}
                    text={formatMinutes(p.time)}
                    fontSize={9}
                    fill="rgba(255,255,255,0.4)"
                  />
                </Group>
              )
            })}

            {/* Score bar */}
            {scoreSegments && (
              <Group>
                <Rect
                  x={plotLeft}
                  y={height - DETAIL_PADDING.bottom + 2}
                  width={scoreSegments.market}
                  height={6}
                  fill="#3b82f6"
                  cornerRadius={[3, 0, 0, 3]}
                />
                <Rect
                  x={plotLeft + scoreSegments.market}
                  y={height - DETAIL_PADDING.bottom + 2}
                  width={scoreSegments.sector}
                  height={6}
                  fill="#22c55e"
                />
                <Rect
                  x={plotLeft + scoreSegments.market + scoreSegments.sector}
                  y={height - DETAIL_PADDING.bottom + 2}
                  width={scoreSegments.stock}
                  height={6}
                  fill="#ec4899"
                  cornerRadius={[0, 3, 3, 0]}
                />
              </Group>
            )}
          </Layer>
        </Stage>
      </div>

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
