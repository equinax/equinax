import { useMemo, useState } from 'react'
import {
  Calendar,
  CalendarRange,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'
import { format, subDays, startOfMonth, isSameDay, getDate } from 'date-fns'
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
      <div className="flex items-center gap-2">
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

        <div className="h-4 w-px bg-border shrink-0" />

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

            {/* Day Cells - Heatmap Style - Fill entire width */}
            <div className="flex-1 grid gap-0.5 overflow-hidden" style={{ gridTemplateColumns: `repeat(${DAYS_TO_SHOW}, 1fr)` }}>
              {visibleDays.map((day) => {
                const dateStr = format(day, 'yyyy-MM-dd')
                const dayInfo = dateInfoMap.get(dateStr)
                const isTradingDay = dayInfo?.isTradingDay ?? false
                const marketChange = dayInfo?.marketChange ?? null
                const isSelected = selectedDate ? isSameDay(selectedDate, day) : isSameDay(day, today)
                const isToday = isSameDay(day, today)
                const dayNum = getDate(day)

                const marketStyle = isTradingDay ? getMarketChangeStyle(marketChange) : undefined

                return (
                  <button
                    key={day.toISOString()}
                    onClick={() => isTradingDay && onDateChange(day)}
                    disabled={!isTradingDay}
                    style={marketStyle}
                    className={cn(
                      'aspect-square rounded-sm text-[10px] font-medium transition-all',
                      'flex items-center justify-center min-w-0',
                      // Non-trading day style
                      !isTradingDay && 'bg-muted/30 text-muted-foreground/40 cursor-not-allowed',
                      // Trading day without market change (flat/平盘)
                      isTradingDay && !marketStyle && 'bg-muted/50',
                      // Hover for trading days
                      isTradingDay && 'hover:ring-1 hover:ring-primary/50',
                      // Selected state - prominent ring
                      isSelected && isTradingDay && 'ring-2 ring-primary ring-offset-1 ring-offset-background font-semibold z-10',
                      // Today indicator (if not selected)
                      !isSelected && isToday && isTradingDay && 'ring-1 ring-primary/30',
                    )}
                    title={`${format(day, 'yyyy-MM-dd (EEEE)')}${isTradingDay ? (marketChange !== null ? ` ${marketChange > 0 ? '+' : ''}${marketChange.toFixed(2)}%` : '') : ' (非交易日)'}`}
                  >
                    {dayNum}
                  </button>
                )
              })}
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
