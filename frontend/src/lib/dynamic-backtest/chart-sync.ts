/**
 * 图表时间轴同步管理器
 * 使用原生 API + ref 方式实现多图共享时间轴，避免 React 状态更新的性能问题
 */

import { IChartApi, LogicalRange } from 'lightweight-charts'

class ChartSyncManager {
  private charts: Map<string, IChartApi> = new Map()
  private isUpdating = false
  private currentRange: LogicalRange | null = null
  
  /**
   * 注册图表到同步管理器
   */
  register(id: string, chart: IChartApi) {
    this.charts.set(id, chart)
    
    // 监听时间范围变化
    chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (this.isUpdating) return
      if (!range) return
      
      this.currentRange = range
      this.syncAll(id, range)
    })
    
    // 如果已有范围，同步新图表
    if (this.currentRange) {
      this.isUpdating = true
      try {
        chart.timeScale().setVisibleLogicalRange(this.currentRange)
      } catch {
        // ignore
      }
      this.isUpdating = false
    }
  }
  
  /**
   * 注销图表
   */
  unregister(id: string) {
    this.charts.delete(id)
  }
  
  /**
   * 同步所有图表到指定范围
   */
  private syncAll(sourceId: string, range: LogicalRange) {
    this.isUpdating = true
    
    this.charts.forEach((chart, id) => {
      if (id === sourceId) return
      
      try {
        const currentRange = chart.timeScale().getVisibleLogicalRange()
        if (currentRange && 
            (Math.abs(currentRange.from - range.from) > 0.1 || 
             Math.abs(currentRange.to - range.to) > 0.1)) {
          chart.timeScale().setVisibleLogicalRange(range)
        }
      } catch {
        // 图表可能已销毁
      }
    })
    
    // 使用 requestAnimationFrame 确保同步完成后再解锁
    requestAnimationFrame(() => {
      this.isUpdating = false
    })
  }
  
  /**
   * 强制同步所有图表到指定范围
   */
  setRange(range: LogicalRange) {
    this.currentRange = range
    this.isUpdating = true
    
    this.charts.forEach((chart) => {
      try {
        chart.timeScale().setVisibleLogicalRange(range)
      } catch {
        // ignore
      }
    })
    
    requestAnimationFrame(() => {
      this.isUpdating = false
    })
  }
  
  /**
   * 获取当前范围
   */
  getRange(): LogicalRange | null {
    return this.currentRange
  }
  
  /**
   * 重置
   */
  reset() {
    this.charts.clear()
    this.currentRange = null
    this.isUpdating = false
  }
  
  /**
   * 滚动所有图表到指定日期（将该日期居中显示）
   */
  scrollToDate(date: string) {
    if (this.charts.size === 0) return
    
    // 获取第一个图表来计算时间坐标
    const firstChart = this.charts.values().next().value
    if (!firstChart) return
    
    try {
      const timeScale = firstChart.timeScale()
      const coordinate = timeScale.timeToCoordinate(date as import('lightweight-charts').Time)
      
      // 获取当前可见范围的宽度
      const currentRange = timeScale.getVisibleLogicalRange()
      if (!currentRange) return
      
      const rangeWidth = Number(currentRange.to) - Number(currentRange.from)
      
      if (coordinate === null) {
        // 日期不在可见范围，尝试直接滚动
        const visibleData = timeScale.getVisibleRange()
        if (visibleData) {
          // 计算目标范围，将日期居中
          const targetDate = new Date(date).getTime()
          const fromDate = new Date(visibleData.from as string).getTime()
          const dayMs = 24 * 60 * 60 * 1000
          
          // 估算索引位置
          const daysFromStart = Math.floor((targetDate - fromDate) / dayMs)
          const centerOffset = rangeWidth / 2
          
          const newRange = {
            from: daysFromStart - centerOffset,
            to: daysFromStart + centerOffset,
          } as LogicalRange
          
          this.setRange(newRange)
        }
      } else {
        // 日期在可见范围内，滚动使其居中
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
  
  /**
   * 获取注册的图表数量
   */
  getChartCount(): number {
    return this.charts.size
  }
}

// 全局单例
export const chartSyncManager = new ChartSyncManager()
