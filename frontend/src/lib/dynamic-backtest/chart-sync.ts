/**
 * 图表时间轴同步管理器
 * 使用原生 API + ref 方式实现多图共享时间轴，避免 React 状态更新的性能问题
 */

import { IChartApi, ISeriesApi, SeriesType, Time, LogicalRange, MouseEventParams } from 'lightweight-charts'

interface ChartEntry {
  chart: IChartApi
  series: ISeriesApi<SeriesType> | null
}

class ChartSyncManager {
  private charts: Map<string, ChartEntry> = new Map()
  private isUpdating = false
  private isCrosshairUpdating = false
  private currentRange: LogicalRange | null = null
  
  register(id: string, chart: IChartApi, series?: ISeriesApi<SeriesType>) {
    this.charts.set(id, { chart, series: series ?? null })
    
    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (this.isUpdating) return
      if (!range) return
      
      this.currentRange = range
      this.syncAll(id, range)
    })

    chart.subscribeCrosshairMove((param: MouseEventParams<Time>) => {
      if (this.isCrosshairUpdating) return
      this.syncCrosshair(id, param)
    })
    
    if (this.currentRange) {
      this.isUpdating = true
      try {
        chart.timeScale().setVisibleLogicalRange(this.currentRange)
      } catch {
        // ignore
      }
      this.isUpdating = false
    } else {
      try {
        const existingRange = chart.timeScale().getVisibleLogicalRange()
        if (existingRange) {
          this.currentRange = existingRange
        }
      } catch {
        // ignore
      }
    }
  }
  
  unregister(id: string) {
    this.charts.delete(id)
  }

  setSeries(id: string, series: ISeriesApi<SeriesType>) {
    const entry = this.charts.get(id)
    if (entry) {
      entry.series = series
    }
  }

  applyCurrentRange(id: string) {
    if (!this.currentRange) return
    const entry = this.charts.get(id)
    if (!entry) return
    this.isUpdating = true
    try {
      entry.chart.timeScale().setVisibleLogicalRange(this.currentRange)
    } catch {
      // ignore
    }
    requestAnimationFrame(() => {
      this.isUpdating = false
    })
  }
  
  private syncAll(sourceId: string, range: LogicalRange) {
    this.isUpdating = true
    
    this.charts.forEach((entry, id) => {
      if (id === sourceId) return
      
      try {
        const currentRange = entry.chart.timeScale().getVisibleLogicalRange()
        if (currentRange && 
            (Math.abs(currentRange.from - range.from) > 0.1 || 
             Math.abs(currentRange.to - range.to) > 0.1)) {
          entry.chart.timeScale().setVisibleLogicalRange(range)
        }
      } catch {
      }
    })
    
    requestAnimationFrame(() => {
      this.isUpdating = false
    })
  }

  private syncCrosshair(sourceId: string, param: MouseEventParams<Time>) {
    this.isCrosshairUpdating = true

    this.charts.forEach((entry, id) => {
      if (id === sourceId) return
      if (!entry.series) return

      try {
        if (!param.time) {
          entry.chart.clearCrosshairPosition()
        } else {
          const data = param.seriesData?.values().next().value
          const price = data && ('close' in data ? (data as { close: number }).close : ('value' in data ? (data as { value: number }).value : 0))
          entry.chart.setCrosshairPosition(price ?? 0, param.time, entry.series)
        }
      } catch {
        // ignore - can happen if target chart's series has no data yet
      }
    })

    requestAnimationFrame(() => {
      this.isCrosshairUpdating = false
    })
  }
  
  setRange(range: LogicalRange) {
    this.currentRange = range
    this.isUpdating = true
    
    this.charts.forEach((entry) => {
      try {
        entry.chart.timeScale().setVisibleLogicalRange(range)
      } catch {
        // ignore
      }
    })
    
    requestAnimationFrame(() => {
      this.isUpdating = false
    })
  }
  
  getRange(): LogicalRange | null {
    return this.currentRange
  }
  
  reset() {
    this.charts.clear()
    this.currentRange = null
    this.isUpdating = false
    this.isCrosshairUpdating = false
  }
  
  scrollToDate(date: string) {
    if (this.charts.size === 0) return
    
    const firstEntry = this.charts.values().next().value
    if (!firstEntry) return
    
    try {
      const timeScale = firstEntry.chart.timeScale()
      const coordinate = timeScale.timeToCoordinate(date as Time)
      
      const currentRange = timeScale.getVisibleLogicalRange()
      if (!currentRange) return
      
      const rangeWidth = Number(currentRange.to) - Number(currentRange.from)
      
      if (coordinate === null) {
        const visibleData = timeScale.getVisibleRange()
        if (visibleData) {
          const targetDate = new Date(date).getTime()
          const fromDate = new Date(visibleData.from as string).getTime()
          const dayMs = 24 * 60 * 60 * 1000
          
          const daysFromStart = Math.floor((targetDate - fromDate) / dayMs)
          const centerOffset = rangeWidth / 2
          
          const newRange = {
            from: daysFromStart - centerOffset,
            to: daysFromStart + centerOffset,
          } as LogicalRange
          
          this.setRange(newRange)
        }
      } else {
        const logicalIndex = timeScale.coordinateToLogical(coordinate)
        
        if (logicalIndex !== null) {
          const centerOffset = rangeWidth / 2
          const newRange = {
            from: Number(logicalIndex) - centerOffset,
            to: Number(logicalIndex) + centerOffset,
          } as LogicalRange
          this.setRange(newRange)
        }
      }
    } catch {
      // ignore errors
    }
  }
  
  getChartCount(): number {
    return this.charts.size
  }

  getChart(id: string): IChartApi | null {
    return this.charts.get(id)?.chart ?? null
  }
}

export { ChartSyncManager }

// 全局单例
export const chartSyncManager = new ChartSyncManager()
