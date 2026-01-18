/**
 * 图表时间范围同步状态
 */

import { create } from 'zustand'

export interface TimeRange {
  from: number  // timestamp
  to: number    // timestamp
}

interface ChartSyncState {
  // 共享的时间范围（用于同步缩放）
  visibleRange: TimeRange | null
  // 数据的完整范围（用于限制缩放）
  dataRange: TimeRange | null
  
  setVisibleRange: (range: TimeRange | null) => void
  setDataRange: (range: TimeRange | null) => void
  resetRange: () => void
}

export const useChartSyncStore = create<ChartSyncState>((set) => ({
  visibleRange: null,
  dataRange: null,
  
  setVisibleRange: (range) => set({ visibleRange: range }),
  setDataRange: (range) => set({ dataRange: range }),
  resetRange: () => set({ visibleRange: null }),
}))
