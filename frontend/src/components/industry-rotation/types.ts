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
}
