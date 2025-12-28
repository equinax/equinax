import { useMemo, useState, useRef, useCallback } from 'react'
import {
  Calendar,
  CalendarRange,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'
import { format, subDays, startOfMonth, isSameDay, getDate, getMonth } from 'date-fns'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import type { TimeMode } from '@/api/generated/schemas'
import { useGetCalendarApiV1AlphaRadarCalendarGet } from '@/api/generated/alpha-radar/alpha-radar'

interface TimeControllerProps {
  mode: TimeMode
  onModeChange: (mode: TimeMode) => void
  selectedDate?: Date
  onDateChange: (date?: Date) => void
  dateRange: { from?: Date; to?: Date }
  onDateRangeChange: (range: { from?: Date; to?: Date }) => void
}

// Quick date presets for period mode
const PERIOD_PRESETS = [
  { label: '近5日', days: 5 },
  { label: '近20日', days: 20 },
  { label: '近60日', days: 60 },
  { label: '本月', days: 0 }, // Special: from start of month
]

const DAYS_TO_SHOW = 45

// Calculate market change color style (涨红跌绿)
function getMarketChangeStyle(change: number | null | undefined): React.CSSProperties | undefined {
  if (change === null || change === undefined || change === 0) {
    return undefined // Use default muted background
  }

  // Normalize intensity: ±3% maps to full intensity
  const intensity = Math.min(Math.abs(change) / 3, 1)
  const opacity = Math.round(15 + intensity * 45) // 15% - 60%

  // 涨红跌绿: profit (red) for up, loss (green) for down
  const cssVar = change > 0 ? '--profit' : '--loss'

  return {
    backgroundColor: `hsl(var(${cssVar}) / ${opacity}%)`,
  }
}

// Selected state: glass morphism effect
function getSelectedStyle(change: number | null | undefined): React.CSSProperties {
  // Base glass effect
  const glassBase: React.CSSProperties = {
    backdropFilter: 'blur(8px) saturate(180%)',
    WebkitBackdropFilter: 'blur(8px) saturate(180%)',
    boxShadow: '0 4px 16px rgba(0, 0, 0, 0.15), inset 0 1px 1px rgba(255, 255, 255, 0.3)',
    border: '1px solid rgba(255, 255, 255, 0.2)',
  }

  if (change === null || change === undefined || change === 0) {
    // Neutral selected - tinted glass
    return {
      ...glassBase,
      backgroundColor: 'hsl(var(--primary) / 70%)',
    }
  }

  // 涨红跌绿: tinted glass with profit/loss color
  const cssVar = change > 0 ? '--profit' : '--loss'

  return {
    ...glassBase,
    backgroundColor: `hsl(var(${cssVar}) / 75%)`,
  }
}

export function TimeController({
  mode,
  onModeChange,
  selectedDate,
  onDateChange,
  dateRange,
  onDateRangeChange,
}: TimeControllerProps) {
  const today = new Date()

  // Scroll offset in pixels (positive = showing earlier dates)
  const [scrollOffset, setScrollOffset] = useState(0)

  // Drag state
  const dragRef = useRef<{
    startX: number
    startScrollOffset: number
    startSelectedIndex: number
    isDraggingActive: boolean
    lastX: number
    lastTime: number
    velocity: number
  } | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const animationRef = useRef<number | null>(null)

  // Get cell width for calculations
  const getCellWidth = useCallback(() => {
    if (!containerRef.current) return 24
    return containerRef.current.offsetWidth / DAYS_TO_SHOW
  }, [])

  // Calculate days offset from scroll position
  const daysOffset = useMemo(() => {
    const cellWidth = getCellWidth()
    return Math.floor(scrollOffset / cellWidth)
  }, [scrollOffset, getCellWidth])

  // Sub-pixel offset for smooth visual scrolling
  // Positive scroll offset means showing earlier dates, so we translate left (negative)
  const subPixelOffset = useMemo(() => {
    const cellWidth = getCellWidth()
    return (scrollOffset % cellWidth)
  }, [scrollOffset, getCellWidth])

  // Calculate date range for calendar API - load extra days for smooth scrolling
  const calendarParams = useMemo(() => {
    const buffer = 30 // Extra days to load for smooth scrolling
    const endDate = subDays(today, Math.max(0, daysOffset - buffer))
    const startDate = subDays(today, daysOffset + DAYS_TO_SHOW + buffer)
    return {
      start_date: format(startDate, 'yyyy-MM-dd'),
      end_date: format(endDate, 'yyyy-MM-dd'),
    }
  }, [daysOffset])

  // Fetch calendar data
  const { data: calendarData } = useGetCalendarApiV1AlphaRadarCalendarGet(calendarParams)

  // Build date map for quick lookup
  const dateInfoMap = useMemo(() => {
    const map = new Map<string, { isTradingDay: boolean; marketChange: number | null }>()
    if (calendarData) {
      for (const day of calendarData) {
        map.set(day.date, {
          isTradingDay: day.is_trading_day,
          marketChange: day.market_change ?? null,
        })
      }
    }
    return map
  }, [calendarData])

  // Generate days array based on scroll offset
  const visibleDays = useMemo(() => {
    const days: Date[] = []
    const endDate = subDays(today, daysOffset)
    for (let i = DAYS_TO_SHOW - 1; i >= 0; i--) {
      days.push(subDays(endDate, i))
    }
    return days
  }, [daysOffset])

  // Smooth scroll with momentum animation
  const animateMomentum = useCallback((velocity: number, isDraggingActive: boolean) => {
    const friction = 0.95
    const minVelocity = 0.5

    const animate = () => {
      if (Math.abs(velocity) < minVelocity) {
        animationRef.current = null
        return
      }

      if (isDraggingActive) {
        // For active day dragging - momentum continues in drag direction
        // Positive velocity (was dragging right) → continue selecting later dates
        // Handled by the decay - selection stays where it was dragged to
      } else {
        // For timeline scrolling, update scroll offset
        // Positive velocity (was dragging right) → continue showing earlier dates (increase offset)
        setScrollOffset(prev => Math.max(0, prev + velocity))
      }

      velocity *= friction
      animationRef.current = requestAnimationFrame(() => animate())
    }

    animate()
  }, [getCellWidth, selectedDate, today, dateInfoMap, onDateChange])

  // Navigate to previous page (earlier dates)
  const handlePrevPage = () => {
    const cellWidth = getCellWidth()
    setScrollOffset(prev => prev + cellWidth * 15)
  }

  // Navigate to next page (more recent dates)
  const handleNextPage = () => {
    const cellWidth = getCellWidth()
    setScrollOffset(prev => Math.max(0, prev - cellWidth * 15))
  }

  // Check if we can go to next page (already at most recent)
  const canGoNext = scrollOffset > 0

  // Handle drag start
  const handleDragStart = useCallback((e: React.MouseEvent | React.TouchEvent, isOnActiveDay: boolean) => {
    // Cancel any ongoing momentum animation
    if (animationRef.current) {
      cancelAnimationFrame(animationRef.current)
      animationRef.current = null
    }

    const clientX = 'touches' in e ? e.touches[0].clientX : e.clientX
    const now = Date.now()

    dragRef.current = {
      startX: clientX,
      startScrollOffset: scrollOffset,
      startSelectedIndex: selectedDate ? visibleDays.findIndex(d => isSameDay(d, selectedDate)) : -1,
      isDraggingActive: isOnActiveDay,
      lastX: clientX,
      lastTime: now,
      velocity: 0,
    }

    e.preventDefault()
  }, [scrollOffset, selectedDate, visibleDays])

  // Handle drag move
  const handleDragMove = useCallback((e: React.MouseEvent | React.TouchEvent) => {
    if (!dragRef.current) return

    const clientX = 'touches' in e ? e.touches[0].clientX : e.clientX
    const now = Date.now()
    const deltaX = clientX - dragRef.current.startX
    const deltaTime = now - dragRef.current.lastTime

    // Calculate velocity (pixels per ms)
    if (deltaTime > 0) {
      const instantVelocity = (clientX - dragRef.current.lastX) / deltaTime
      dragRef.current.velocity = dragRef.current.velocity * 0.7 + instantVelocity * 0.3 * 16 // Smooth and scale
    }

    dragRef.current.lastX = clientX
    dragRef.current.lastTime = now

    if (dragRef.current.isDraggingActive) {
      // Dragging on active day - move selection based on drag distance
      // Drag right (positive deltaX) → select later date (higher index)
      // Drag left (negative deltaX) → select earlier date (lower index)
      const cellWidth = getCellWidth()
      const cellsMoved = Math.round(deltaX / cellWidth)

      if (cellsMoved !== 0 && dragRef.current.startSelectedIndex >= 0) {
        const newIndex = dragRef.current.startSelectedIndex + cellsMoved
        if (newIndex >= 0 && newIndex < visibleDays.length) {
          const newDate = visibleDays[newIndex]
          const dateStr = format(newDate, 'yyyy-MM-dd')
          const dayInfo = dateInfoMap.get(dateStr)
          if (dayInfo?.isTradingDay) {
            onDateChange(newDate)
          }
        }
      }
    } else {
      // Dragging on timeline - smooth scroll
      // Drag right (positive deltaX) → show earlier dates (increase offset)
      // Drag left (negative deltaX) → show more recent dates (decrease offset)
      const newOffset = dragRef.current.startScrollOffset + deltaX
      setScrollOffset(Math.max(0, newOffset))
    }
  }, [visibleDays, dateInfoMap, onDateChange, getCellWidth])

  // Handle drag end with momentum
  const handleDragEnd = useCallback(() => {
    if (!dragRef.current) return

    const velocity = dragRef.current.velocity
    const isDraggingActive = dragRef.current.isDraggingActive

    dragRef.current = null

    // Apply momentum if velocity is significant
    if (Math.abs(velocity) > 2) {
      animateMomentum(velocity, isDraggingActive)
    }
  }, [animateMomentum])

  // Period mode handlers
  const handlePeriodPreset = (preset: (typeof PERIOD_PRESETS)[0]) => {
    if (preset.days === 0) {
      // Start of month
      onDateRangeChange({ from: startOfMonth(today), to: today })
    } else {
      onDateRangeChange({ from: subDays(today, preset.days), to: today })
    }
  }

  const isPeriodPresetActive = (preset: (typeof PERIOD_PRESETS)[0]) => {
    if (!dateRange.from || !dateRange.to) return false
    if (preset.days === 0) {
      return isSameDay(dateRange.from, startOfMonth(today)) && isSameDay(dateRange.to, today)
    }
    return isSameDay(dateRange.from, subDays(today, preset.days)) && isSameDay(dateRange.to, today)
  }

  return (
    <Card className="p-2">
      <div className="flex items-center gap-1">
        {/* Mode Toggle */}
        <div className="flex items-center p-0.5 bg-muted rounded-md shrink-0">
          <Button
            variant="ghost"
            size="sm"
            className={cn(
              'h-7 px-2.5 text-xs rounded-sm',
              mode === 'snapshot' && 'bg-background shadow-sm'
            )}
            onClick={() => onModeChange('snapshot')}
          >
            <Calendar className="h-3.5 w-3.5 mr-1" />
            单日
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className={cn(
              'h-7 px-2.5 text-xs rounded-sm',
              mode === 'period' && 'bg-background shadow-sm'
            )}
            onClick={() => onModeChange('period')}
          >
            <CalendarRange className="h-3.5 w-3.5 mr-1" />
            区间
          </Button>
        </div>

        {mode === 'snapshot' ? (
          <>
            {/* Previous page button */}
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 shrink-0"
              onClick={handlePrevPage}
              title="更早日期"
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>

            {/* Day Cells - Heatmap Style - Fill entire width - no gap with arrows */}
            <div
              ref={containerRef}
              className="flex-1 relative -mx-1 select-none"
              onMouseMove={handleDragMove}
              onMouseUp={handleDragEnd}
              onMouseLeave={handleDragEnd}
              onTouchMove={handleDragMove}
              onTouchEnd={handleDragEnd}
            >
              {/* Date cells row */}
              <div
                className="grid h-full transition-transform duration-75"
                style={{
                  gridTemplateColumns: `repeat(${DAYS_TO_SHOW}, 1fr)`,
                  transform: `translateX(${subPixelOffset}px)`,
                }}
              >
                {visibleDays.map((day) => {
                  const dateStr = format(day, 'yyyy-MM-dd')
                  const dayInfo = dateInfoMap.get(dateStr)
                  const isTradingDay = dayInfo?.isTradingDay ?? false
                  const marketChange = dayInfo?.marketChange ?? null
                  const isSelected = selectedDate ? isSameDay(selectedDate, day) : isSameDay(day, today)
                  const isToday = isSameDay(day, today)
                  const dayNum = getDate(day)
                  const isFirstOfMonth = dayNum === 1
                  const isFirstOfYear = isFirstOfMonth && getMonth(day) === 0

                  const marketStyle = isTradingDay ? getMarketChangeStyle(marketChange) : undefined

                  // For selected state: stronger saturation background + light text
                  const selectedStyle = isSelected && isTradingDay ? getSelectedStyle(marketChange) : undefined

                  return (
                    <div key={day.toISOString()} className="relative flex items-center">
                      <button
                        onClick={() => isTradingDay && onDateChange(day)}
                        onMouseDown={(e) => handleDragStart(e, isSelected)}
                        onTouchStart={(e) => handleDragStart(e, isSelected)}
                        disabled={!isTradingDay}
                        style={selectedStyle || marketStyle}
                        className={cn(
                          'cursor-grab active:cursor-grabbing',
                          'w-full aspect-square text-[10px] font-medium transition-all',
                          'flex items-center justify-center',
                          // Non-trading day style
                          !isTradingDay && 'bg-muted/30 text-muted-foreground/40 cursor-not-allowed',
                          // Trading day without market change (flat/平盘)
                          isTradingDay && !marketStyle && !selectedStyle && 'bg-muted/50',
                          // Hover for trading days
                          isTradingDay && 'hover:brightness-110',
                          // Selected state - glass effect + taller
                          isSelected && isTradingDay && 'text-white font-semibold !aspect-auto !h-[120%] !-my-[10%] z-10 rounded-sm',
                          // Today indicator (if not selected)
                          !isSelected && isToday && isTradingDay && 'ring-1 ring-primary/50 ring-inset',
                        )}
                        title={`${format(day, 'yyyy-MM-dd (EEEE)')}${isTradingDay ? (marketChange !== null ? ` ${marketChange > 0 ? '+' : ''}${marketChange.toFixed(2)}%` : '') : ' (非交易日)'}`}
                      >
                        {dayNum}
                      </button>
                      {/* Month label - positioned absolutely below */}
                      {isFirstOfMonth && (
                        <span className="absolute -bottom-3 left-0 text-[8px] text-muted-foreground whitespace-nowrap">
                          {isFirstOfYear ? format(day, 'yy/M') : format(day, 'M月')}
                        </span>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Next page button */}
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 shrink-0"
              onClick={handleNextPage}
              disabled={!canGoNext}
              title="更近日期"
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </>
        ) : (
          <>
            {/* Period Presets */}
            <div className="flex items-center gap-1">
              {PERIOD_PRESETS.map((preset) => (
                <Button
                  key={preset.label}
                  variant={isPeriodPresetActive(preset) ? 'secondary' : 'ghost'}
                  size="sm"
                  className="h-7 px-3 text-xs"
                  onClick={() => handlePeriodPreset(preset)}
                >
                  {preset.label}
                </Button>
              ))}
            </div>

            {/* Spacer */}
            <div className="flex-1" />

            {/* Period Display */}
            {dateRange.from && dateRange.to && (
              <div className="text-sm text-muted-foreground">
                <span className="font-mono text-foreground">
                  {format(dateRange.from, 'MM-dd')}
                </span>
                <span className="mx-2">→</span>
                <span className="font-mono text-foreground">
                  {format(dateRange.to, 'MM-dd')}
                </span>
                <span className="ml-2 text-xs">
                  ({Math.ceil((dateRange.to.getTime() - dateRange.from.getTime()) / (1000 * 60 * 60 * 24))} 天)
                </span>
              </div>
            )}
          </>
        )}
      </div>
    </Card>
  )
}
