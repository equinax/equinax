import { useMemo, useState, useRef, useCallback, useEffect } from 'react'
import {
  Calendar,
  CalendarRange,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'
import { format, subDays, startOfMonth, isSameDay, getDate, getMonth, differenceInDays, parseISO, isAfter } from 'date-fns'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import type { TimeMode, CalendarDayInfo } from '@/api/generated/schemas'
import {
  useGetCalendarApiV1AlphaRadarCalendarGet,
  useResolveTimeControllerApiV1AlphaRadarTimeControllerPost,
} from '@/api/generated/alpha-radar/alpha-radar'

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

// Initial load batch size in days
const INITIAL_LOAD_DAYS = 365
const LOAD_MORE_DAYS = 365
const LOAD_THRESHOLD_DAYS = 30 // Load more when this many days from boundary

export function TimeController({
  mode,
  onModeChange,
  selectedDate,
  onDateChange,
  dateRange,
  onDateRangeChange,
}: TimeControllerProps) {
  const today = useMemo(() => new Date(), [])

  // Scroll offset in pixels (positive = showing earlier dates)
  const [scrollOffset, setScrollOffset] = useState(0)

  // Track how far back we've loaded calendar data
  const [loadedStartDate, setLoadedStartDate] = useState(() => subDays(today, INITIAL_LOAD_DAYS))

  // Accumulated calendar data from all loads
  const [allCalendarData, setAllCalendarData] = useState<CalendarDayInfo[]>([])

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

  // Get earliest available date from backend
  const { mutate: fetchTimeController, data: timeControllerData } = useResolveTimeControllerApiV1AlphaRadarTimeControllerPost()

  // Fetch time controller info on mount to get earliest_available_date
  useEffect(() => {
    fetchTimeController({ data: { mode: 'snapshot' } })
  }, [fetchTimeController])

  // Parse earliest available date
  const earliestAvailableDate = useMemo(() => {
    if (!timeControllerData?.earliest_available_date) return null
    return parseISO(timeControllerData.earliest_available_date)
  }, [timeControllerData])

  // Get cell width for calculations
  const getCellWidth = useCallback(() => {
    if (!containerRef.current) return 24
    return containerRef.current.offsetWidth / DAYS_TO_SHOW
  }, [])

  // Calculate total available days (from today to earliest available date)
  const totalAvailableDays = useMemo(() => {
    if (!earliestAvailableDate) return INITIAL_LOAD_DAYS
    return differenceInDays(today, earliestAvailableDate)
  }, [earliestAvailableDate, today])

  // Maximum scroll offset based on actual data availability
  const maxScrollOffset = useMemo(() => {
    const cellWidth = getCellWidth()
    return Math.max(0, (totalAvailableDays - DAYS_TO_SHOW) * cellWidth)
  }, [getCellWidth, totalAvailableDays])

  // Clamp scroll offset to valid range
  const clampOffset = useCallback((offset: number) => {
    return Math.max(0, Math.min(offset, maxScrollOffset))
  }, [maxScrollOffset])

  // Calculate days offset from scroll position
  const daysOffset = useMemo(() => {
    const cellWidth = getCellWidth()
    return Math.floor(scrollOffset / cellWidth)
  }, [scrollOffset, getCellWidth])

  // Sub-pixel offset for smooth visual scrolling
  const subPixelOffset = useMemo(() => {
    const cellWidth = getCellWidth()
    return (scrollOffset % cellWidth)
  }, [scrollOffset, getCellWidth])

  // Initial calendar data load
  const initialCalendarParams = useMemo(() => ({
    start_date: format(subDays(today, INITIAL_LOAD_DAYS), 'yyyy-MM-dd'),
    end_date: format(today, 'yyyy-MM-dd'),
  }), [today])

  const { data: initialCalendarData } = useGetCalendarApiV1AlphaRadarCalendarGet(initialCalendarParams, {
    query: {
      staleTime: 1000 * 60 * 60,
      gcTime: 1000 * 60 * 60 * 2,
    },
  })

  // Calculate dynamic load params when approaching boundary
  const shouldLoadMore = useMemo(() => {
    if (!earliestAvailableDate) return false
    // Check if we're approaching the boundary of loaded data
    const daysFromLoadedStart = differenceInDays(today, loadedStartDate)
    const currentViewStart = daysOffset + DAYS_TO_SHOW
    return currentViewStart > daysFromLoadedStart - LOAD_THRESHOLD_DAYS &&
      isAfter(loadedStartDate, earliestAvailableDate)
  }, [daysOffset, loadedStartDate, earliestAvailableDate, today])

  // Load more params
  const loadMoreParams = useMemo(() => {
    if (!shouldLoadMore || !earliestAvailableDate) return null
    // Load another batch, but don't go past earliest available
    const newStartDate = subDays(loadedStartDate, LOAD_MORE_DAYS)
    const actualStartDate = isAfter(newStartDate, earliestAvailableDate) ? newStartDate : earliestAvailableDate
    return {
      start_date: format(actualStartDate, 'yyyy-MM-dd'),
      end_date: format(subDays(loadedStartDate, 1), 'yyyy-MM-dd'),
    }
  }, [shouldLoadMore, loadedStartDate, earliestAvailableDate])

  // Fetch more data when needed
  const { data: moreCalendarData } = useGetCalendarApiV1AlphaRadarCalendarGet(
    loadMoreParams ?? { start_date: '', end_date: '' },
    {
      query: {
        enabled: !!loadMoreParams,
        staleTime: 1000 * 60 * 60,
        gcTime: 1000 * 60 * 60 * 2,
      },
    }
  )

  // Merge initial data
  useEffect(() => {
    if (initialCalendarData && allCalendarData.length === 0) {
      setAllCalendarData(initialCalendarData)
    }
  }, [initialCalendarData, allCalendarData.length])

  // Merge additional data when loaded
  useEffect(() => {
    if (moreCalendarData && moreCalendarData.length > 0 && loadMoreParams) {
      setAllCalendarData(prev => {
        // Prepend new data (older dates) to existing data
        const existingDates = new Set(prev.map(d => d.date))
        const newData = moreCalendarData.filter(d => !existingDates.has(d.date))
        return [...newData, ...prev]
      })
      // Update loaded start date
      setLoadedStartDate(parseISO(loadMoreParams.start_date))
    }
  }, [moreCalendarData, loadMoreParams])

  // Build date map for quick lookup
  const dateInfoMap = useMemo(() => {
    const map = new Map<string, { isTradingDay: boolean; marketChange: number | null }>()
    for (const day of allCalendarData) {
      map.set(day.date, {
        isTradingDay: day.is_trading_day,
        marketChange: day.market_change ?? null,
      })
    }
    return map
  }, [allCalendarData])

  // Generate days array based on scroll offset
  // Render one extra day on each side for smooth scrolling without gaps
  const visibleDays = useMemo(() => {
    const days: Date[] = []
    const endDate = subDays(today, daysOffset)
    // Start one day earlier (extra on left), end one day later (extra on right, but clamped to today)
    for (let i = DAYS_TO_SHOW; i >= -1; i--) {
      const day = subDays(endDate, i)
      // Don't add future dates
      if (day <= today) {
        days.push(day)
      }
    }
    return days
  }, [daysOffset])

  // Smooth scroll with momentum animation
  const animateMomentum = useCallback((initialVelocity: number, isDraggingActive: boolean) => {
    const friction = 0.95
    const minVelocity = 0.5
    let velocity = initialVelocity
    let shouldStop = false

    const animate = () => {
      if (Math.abs(velocity) < minVelocity || shouldStop) {
        animationRef.current = null
        return
      }

      if (!isDraggingActive) {
        // For timeline scrolling, update scroll offset
        setScrollOffset(prev => {
          const newOffset = clampOffset(prev + velocity)
          // Stop momentum if we hit a boundary
          if (newOffset === prev || newOffset === 0 || newOffset === maxScrollOffset) {
            shouldStop = true
          }
          return newOffset
        })
      }

      velocity *= friction
      if (!shouldStop) {
        animationRef.current = requestAnimationFrame(animate)
      } else {
        animationRef.current = null
      }
    }

    animate()
  }, [clampOffset, maxScrollOffset])

  // Navigate to previous page (earlier dates)
  const handlePrevPage = () => {
    const cellWidth = getCellWidth()
    setScrollOffset(prev => clampOffset(prev + cellWidth * 15))
  }

  // Navigate to next page (more recent dates)
  const handleNextPage = () => {
    const cellWidth = getCellWidth()
    setScrollOffset(prev => clampOffset(prev - cellWidth * 15))
  }

  // Check navigation availability
  const canGoPrev = scrollOffset < maxScrollOffset
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
      setScrollOffset(clampOffset(newOffset))
    }
  }, [visibleDays, dateInfoMap, onDateChange, getCellWidth, clampOffset])

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
              disabled={!canGoPrev}
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
              {/* Date cells row - use fixed cell width for smooth scrolling */}
              {/* Start with extra day off-screen left, then slide in as scrollOffset increases */}
              <div
                className="flex h-full transition-transform duration-75"
                style={{
                  transform: `translateX(calc(${subPixelOffset}px - ${100 / DAYS_TO_SHOW}%))`,
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
                    <div
                      key={day.toISOString()}
                      className="relative flex items-center shrink-0 "
                      style={{ width: `${100 / DAYS_TO_SHOW}%` }}
                    >
                      <button
                        onClick={() => isTradingDay && onDateChange(day)}
                        onMouseDown={(e) => handleDragStart(e, isSelected)}
                        onTouchStart={(e) => handleDragStart(e, isSelected)}
                        style={selectedStyle || marketStyle}
                        className={cn(
                          'cursor-grab active:cursor-grabbing',
                          'w-full aspect-square text-[10px] font-medium transition-all',
                          'flex items-center justify-center',
                          // Non-trading day style - still draggable, just not clickable
                          !isTradingDay && 'bg-muted text-muted-foreground',
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
