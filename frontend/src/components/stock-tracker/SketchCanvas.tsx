import { useState, useCallback, useMemo, useRef, useEffect } from 'react'
import { Stage, Layer, Line, Circle, Text, Rect, Group } from 'react-konva'
import type Konva from 'konva'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Save } from 'lucide-react'

// Trading hours time slots (minutes from midnight)
const TIME_SLOTS = [
  // Morning: 9:30 - 11:30
  9 * 60 + 30,  // 9:30
  9 * 60 + 45,
  10 * 60,       // 10:00
  10 * 60 + 15,
  10 * 60 + 30,  // 10:30
  10 * 60 + 45,
  11 * 60,       // 11:00
  11 * 60 + 15,
  11 * 60 + 30,  // 11:30
  // Afternoon: 13:00 - 15:00
  13 * 60,       // 13:00
  13 * 60 + 15,
  13 * 60 + 30,  // 13:30
  13 * 60 + 45,
  14 * 60,       // 14:00
  14 * 60 + 15,
  14 * 60 + 30,  // 14:30
  14 * 60 + 45,
  15 * 60,       // 15:00
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

// Snap a time value to nearest 15-min slot
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

type PointRole = 'open' | 'high' | 'low' | 'close'

interface OHLCPoint {
  role: PointRole
  time: number // minutes from midnight
  price: number // percentage from prev_close (e.g. 2.5 = +2.5%)
}

const ROLE_CONFIG: Record<
  PointRole,
  { label: string; shortLabel: string; color: string }
> = {
  open: { label: '开盘', shortLabel: 'O', color: '#3b82f6' },
  high: { label: '最高', shortLabel: 'H', color: '#22c55e' },
  low: { label: '最低', shortLabel: 'L', color: '#ef4444' },
  close: { label: '收盘', shortLabel: 'C', color: '#f97316' },
}

// Default points if no data
const DEFAULT_POINTS: OHLCPoint[] = [
  { role: 'open', time: 9 * 60 + 30, price: 0 },
  { role: 'high', time: 10 * 60 + 30, price: 3 },
  { role: 'low', time: 14 * 60, price: -2 },
  { role: 'close', time: 15 * 60, price: 1 },
]

interface SketchCanvasProps {
  keyPoints?: Record<string, { time: string; price: number }> | null
  autoPattern?: string | null
  onSave: (keyPoints: Record<string, { time: string; price: number }>) => void
  isSaving?: boolean
  preClose?: number | null
}

// Canvas layout constants
const PADDING = { top: 40, right: 60, bottom: 50, left: 60 }
const PRICE_RANGE = 10 // ±10% range

export default function SketchCanvas({
  keyPoints,
  autoPattern,
  onSave,
  isSaving = false,
  preClose,
}: SketchCanvasProps) {
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
          height: Math.max(300, entry.contentRect.height),
        })
      }
    })
    observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [])

  // Parse initial points from API data
  const initialPoints = useMemo((): OHLCPoint[] => {
    if (!keyPoints) return DEFAULT_POINTS
    return (['open', 'high', 'low', 'close'] as PointRole[]).map((role) => {
      const kp = keyPoints[role]
      if (!kp) return DEFAULT_POINTS.find((p) => p.role === role)!
      // Parse time string "HH:MM" to minutes
      const [h, m] = kp.time.split(':').map(Number)
      return { role, time: h * 60 + m, price: kp.price }
    })
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
  const plotW = width - PADDING.left - PADDING.right
  const plotH = height - PADDING.top - PADDING.bottom

  // Coordinate transforms
  const timeToX = useCallback(
    (minutes: number) => {
      const minTime = TIME_SLOTS[0]
      const totalMinutes = 11 * 60 + 30 - (9 * 60 + 30) + (15 * 60 - 13 * 60)
      let elapsed: number
      if (minutes <= 11 * 60 + 30) {
        elapsed = minutes - minTime
      } else {
        elapsed = 11 * 60 + 30 - minTime + (minutes - 13 * 60)
      }
      return PADDING.left + (elapsed / totalMinutes) * plotW
    },
    [plotW]
  )

  const xToTime = useCallback(
    (x: number) => {
      const totalMinutes = 240
      const elapsed = ((x - PADDING.left) / plotW) * totalMinutes
      let minutes: number
      if (elapsed <= 120) {
        minutes = 9 * 60 + 30 + elapsed
      } else {
        minutes = 13 * 60 + (elapsed - 120)
      }
      return snapToSlot(Math.round(minutes))
    },
    [plotW]
  )

  const priceToY = useCallback(
    (pct: number) => {
      // Price goes from +PRICE_RANGE at top to -PRICE_RANGE at bottom
      return PADDING.top + ((PRICE_RANGE - pct) / (2 * PRICE_RANGE)) * plotH
    },
    [plotH]
  )

  const yToPrice = useCallback(
    (y: number) => {
      return PRICE_RANGE - ((y - PADDING.top) / plotH) * (2 * PRICE_RANGE)
    },
    [plotH]
  )

  // Sort points by time for connecting line
  const sortedPoints = useMemo(
    () => [...points].sort((a, b) => a.time - b.time),
    [points]
  )

  // Line points (flat array: x1, y1, x2, y2, ...)
  const linePoints = useMemo(
    () =>
      sortedPoints.flatMap((p) => [timeToX(p.time), priceToY(p.price)]),
    [sortedPoints, timeToX, priceToY]
  )

  // Drag handler
  const handleDrag = useCallback(
    (role: PointRole, e: Konva.KonvaEventObject<DragEvent>) => {
      const node = e.target
      const newTime = xToTime(node.x())
      const newPrice = Math.max(
        -PRICE_RANGE,
        Math.min(PRICE_RANGE, yToPrice(node.y()))
      )

      // Snap x to time slot
      node.x(timeToX(newTime))
      // Clamp y
      node.y(priceToY(newPrice))

      setPoints((prev) =>
        prev.map((p) =>
          p.role === role ? { ...p, time: newTime, price: newPrice } : p
        )
      )
      setIsDirty(true)
    },
    [xToTime, yToPrice, timeToX, priceToY]
  )

  // Save handler
  const handleSave = () => {
    const result: Record<string, { time: string; price: number }> = {}
    for (const p of points) {
      result[p.role] = {
        time: formatMinutes(p.time),
        price: Math.round(p.price * 100) / 100,
      }
    }
    onSave(result)
    setIsDirty(false)
  }

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
        <Button
          size="sm"
          onClick={handleSave}
          disabled={!isDirty || isSaving}
        >
          <Save className="h-3 w-3 mr-1" />
          {isSaving ? '保存中...' : '保存'}
        </Button>
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
              x={PADDING.left}
              y={PADDING.top}
              width={plotW}
              height={plotH}
              fill="rgba(255,255,255,0.02)"
              stroke="rgba(255,255,255,0.1)"
              strokeWidth={1}
            />

            {/* Price grid lines */}
            {priceGridLines.map((gl) => {
              const y = priceToY(gl.pct)
              return (
                <Group key={gl.pct}>
                  <Line
                    points={[PADDING.left, y, PADDING.left + plotW, y]}
                    stroke={
                      gl.pct === 0
                        ? 'rgba(255,255,255,0.3)'
                        : 'rgba(255,255,255,0.06)'
                    }
                    strokeWidth={gl.pct === 0 ? 1.5 : 0.5}
                    dash={gl.pct === 0 ? undefined : [4, 4]}
                  />
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
                  {/* Right side: actual price if preClose available */}
                  {preClose != null && (
                    <Text
                      x={PADDING.left + plotW + 5}
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
              const x = timeToX(slot)
              return (
                <Group key={slot}>
                  <Line
                    points={[x, PADDING.top, x, PADDING.top + plotH]}
                    stroke="rgba(255,255,255,0.06)"
                    strokeWidth={0.5}
                    dash={[4, 4]}
                  />
                  <Text
                    x={x - 12}
                    y={PADDING.top + plotH + 8}
                    text={formatMinutes(slot)}
                    fontSize={10}
                    fill="rgba(255,255,255,0.4)"
                  />
                </Group>
              )
            })}

            {/* Connecting line */}
            <Line
              points={linePoints}
              stroke="rgba(255,255,255,0.6)"
              strokeWidth={2}
              lineJoin="round"
              lineCap="round"
            />

            {/* Draggable OHLC points */}
            {points.map((p) => {
              const config = ROLE_CONFIG[p.role]
              const cx = timeToX(p.time)
              const cy = priceToY(p.price)
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
                  {/* Label */}
                  <Text
                    x={cx - 4}
                    y={cy - 22}
                    text={config.shortLabel}
                    fontSize={11}
                    fontStyle="bold"
                    fill={config.color}
                  />
                  {/* Price label */}
                  <Text
                    x={cx + 12}
                    y={cy - 5}
                    text={`${p.price >= 0 ? '+' : ''}${p.price.toFixed(1)}%`}
                    fontSize={9}
                    fill="rgba(255,255,255,0.6)"
                  />
                  {/* Time label */}
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
          </Layer>
        </Stage>
      </div>

      {/* Dragging info */}
      {dragging && (
        <div className="text-xs text-muted-foreground text-center">
          拖动{ROLE_CONFIG[dragging].label}节点 — 松开吸附到最近时间格
        </div>
      )}
    </div>
  )
}
