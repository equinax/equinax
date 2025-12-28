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

  // Page offset: 0 = current page (ending today), 1 = previous page, etc.
  const [pageOffset, setPageOffset] = useState(0)

  // Drag state
  const dragRef = useRef<{
    startX: number
    startIndex: number
    isDraggingActive: boolean
    accumulatedDelta: number
  } | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  // Calculate date range for calendar API based on page offset
  const calendarParams = useMemo(() => {
    const endDate = subDays(today, pageOffset * DAYS_TO_SHOW)
    const startDate = subDays(endDate, DAYS_TO_SHOW - 1)
    return {
      start_date: format(startDate, 'yyyy-MM-dd'),
      end_date: format(endDate, 'yyyy-MM-dd'),
    }
  }, [pageOffset])

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

  // Generate days array for current page
  const visibleDays = useMemo(() => {
    const days: Date[] = []
    const endDate = subDays(today, pageOffset * DAYS_TO_SHOW)
    for (let i = DAYS_TO_SHOW - 1; i >= 0; i--) {
      days.push(subDays(endDate, i))
    }
    return days
  }, [pageOffset])

  // Navigate to previous page (earlier dates)
  const handlePrevPage = () => {
    setPageOffset((prev) => prev + 1)
  }

  // Navigate to next page (more recent dates)
  const handleNextPage = () => {
    setPageOffset((prev) => Math.max(0, prev - 1))
  }

  // Check if we can go to next page (already at most recent)
  const canGoNext = pageOffset > 0

  // Get cell width for drag calculations
  const getCellWidth = useCallback(() => {
    if (!containerRef.current) return 30
    return containerRef.current.offsetWidth / DAYS_TO_SHOW
  }, [])

  // Find the index of a date in visibleDays
  const findDateIndex = useCallback((date: Date) => {
    return visibleDays.findIndex(d => isSameDay(d, date))
  }, [visibleDays])

  // Handle drag start
  const handleDragStart = useCallback((e: React.MouseEvent | React.TouchEvent, isOnActiveDay: boolean) => {
    const clientX = 'touches' in e ? e.touches[0].clientX : e.clientX
    const currentIndex = selectedDate ? findDateIndex(selectedDate) : findDateIndex(today)

    dragRef.current = {
      startX: clientX,
      startIndex: currentIndex,
      isDraggingActive: isOnActiveDay,
      accumulatedDelta: 0,
    }

    // Prevent text selection during drag
    e.preventDefault()
  }, [selectedDate, findDateIndex, today])

  // Handle drag move
  const handleDragMove = useCallback((e: React.MouseEvent | React.TouchEvent) => {
    if (!dragRef.current) return

    const clientX = 'touches' in e ? e.touches[0].clientX : e.clientX
    const deltaX = clientX - dragRef.current.startX
    const cellWidth = getCellWidth()
    const cellsMoved = Math.round(deltaX / cellWidth)

    if (dragRef.current.isDraggingActive) {
      // Dragging on active day - move selection
      const newIndex = dragRef.current.startIndex + cellsMoved
      if (newIndex >= 0 && newIndex < visibleDays.length) {
        const newDate = visibleDays[newIndex]
        const dateStr = format(newDate, 'yyyy-MM-dd')
        const dayInfo = dateInfoMap.get(dateStr)
        if (dayInfo?.isTradingDay) {
          onDateChange(newDate)
        }
      }
    } else {
      // Dragging on non-active area - scroll timeline
      // Accumulate delta and shift pages when threshold reached
      dragRef.current.accumulatedDelta = deltaX
      const pagesShifted = Math.floor(Math.abs(deltaX) / (cellWidth * 10))

      if (pagesShifted > 0) {
        if (deltaX > 0) {
          // Dragging right = go to earlier dates
          setPageOffset(prev => prev + 1)
        } else {
          // Dragging left = go to more recent dates
          setPageOffset(prev => Math.max(0, prev - 1))
        }
        // Reset start position after page shift
        dragRef.current.startX = clientX
        dragRef.current.accumulatedDelta = 0
      }
    }
  }, [visibleDays, dateInfoMap, onDateChange, getCellWidth])

  // Handle drag end
  const handleDragEnd = useCallback(() => {
    dragRef.current = null
  }, [])

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
              <div className="grid h-full" style={{ gridTemplateColumns: `repeat(${DAYS_TO_SHOW}, 1fr)` }}>
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
