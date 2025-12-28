import { Calendar, CalendarRange, Clock } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import type { TimeMode } from '@/api/generated/schemas'

interface TimeControllerProps {
  mode: TimeMode
  onModeChange: (mode: TimeMode) => void
  selectedDate?: Date
  onDateChange: (date?: Date) => void
  dateRange: { from?: Date; to?: Date }
  onDateRangeChange: (range: { from?: Date; to?: Date }) => void
}

export function TimeController({
  mode,
  onModeChange,
  selectedDate: _selectedDate,
  onDateChange: _onDateChange,
  dateRange: _dateRange,
  onDateRangeChange: _onDateRangeChange,
}: TimeControllerProps) {
  return (
    <Card>
      <CardContent className="p-3">
        <div className="flex items-center gap-4">
          {/* Mode Toggle */}
          <div className="flex items-center gap-1 p-1 bg-muted rounded-lg">
            <Button
              variant="ghost"
              size="sm"
              className={cn(
                'h-8 px-3 rounded-md',
                mode === 'snapshot' && 'bg-background shadow-sm'
              )}
              onClick={() => onModeChange('snapshot')}
            >
              <Calendar className="h-4 w-4 mr-1.5" />
              单日快照
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className={cn(
                'h-8 px-3 rounded-md',
                mode === 'period' && 'bg-background shadow-sm'
              )}
              onClick={() => onModeChange('period')}
            >
              <CalendarRange className="h-4 w-4 mr-1.5" />
              区间分析
            </Button>
          </div>

          {/* Mode-specific content */}
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Clock className="h-4 w-4" />
            {mode === 'snapshot' ? (
              <span>使用最新交易日数据</span>
            ) : (
              <span>区间模式 (开发中)</span>
            )}
          </div>

          {/* Future: Date picker / range picker */}
          {/* For MVP, we just use the latest trading day */}
        </div>
      </CardContent>
    </Card>
  )
}
