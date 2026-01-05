/**
 * Type definitions for industry rotation components
 */

import type { RotationCellSignal, RotationTopStock } from '@/api/generated/schemas'

export interface TooltipData {
  industry: string
  date: string
  change_pct: number
  money_flow: number | null
  main_strength: number | null
  top_stock: RotationTopStock | null | undefined
  signals: RotationCellSignal[]
  mouseX: number
  mouseY: number
  // For weighted volume display
  volume_baseline?: number | null
  // 涨停榜数据
  limit_up_count?: number
  limit_up_stocks?: RotationTopStock[]
  // 龙头战法筛选
  dragon_stock?: RotationTopStock | null
}
