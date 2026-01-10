/**
 * Sector Tooltip Component
 *
 * Displays detailed sector information on hover:
 * - Sector name and change percentage
 * - Stock counts (total, up, down)
 * - Trading amount
 * - Related ETFs (for L1 industries)
 */

import { memo, useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { Loader2, Package } from 'lucide-react'
import type { SectorMetric } from '@/api/generated/schemas'
import { useGetIndustryEtfMappingApiV1AlphaRadarIndustryEtfMappingIndustryGet } from '@/api/generated/alpha-radar/alpha-radar'
import type { ProcessedL1Item, ProcessedL2Item } from './types'

// Metric labels and descriptions
const METRIC_LABELS: Record<SectorMetric, { name: string; desc: string }> = {
  change: { name: '涨跌幅', desc: '今日股价变动百分比' },
  amount: { name: '成交额', desc: '今日总成交金额' },
  main_strength: { name: '主力强度', desc: '主力资金净流入强度' },
  score: { name: '综合评分', desc: '多维度综合评估得分' },
}

interface SectorTooltipProps {
  segment: ProcessedL1Item | ProcessedL2Item | null
  type: 'l1' | 'l2'
  parentName?: string // For L2, show parent L1 name
  mouseX: number
  mouseY: number
  metric: SectorMetric
}

// Type guard to check if segment is L1
function isL1Item(
  segment: ProcessedL1Item | ProcessedL2Item
): segment is ProcessedL1Item {
  return 'children' in segment
}

// Format amount in Chinese units
function formatAmount(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-'
  const num = Number(value)
  if (num >= 1e8) return `${(num / 1e8).toFixed(2)}亿`
  if (num >= 1e4) return `${(num / 1e4).toFixed(0)}万`
  return num.toFixed(0)
}

export const SectorTooltip = memo(function SectorTooltip({
  segment,
  type,
  parentName,
  mouseX,
  mouseY,
  metric,
}: SectorTooltipProps) {
  const [position, setPosition] = useState({ x: 0, y: 0 })

  // Fetch related ETFs for L1 industries
  const industryName = type === 'l1' && segment ? segment.name : ''
  const { data: etfData, isLoading: isLoadingEtfs } = useGetIndustryEtfMappingApiV1AlphaRadarIndustryEtfMappingIndustryGet(
    industryName,
    { limit: 3 },
    {
      query: {
        enabled: type === 'l1' && !!industryName,
        staleTime: 5 * 60 * 1000, // Cache for 5 minutes
      },
    }
  )

  useEffect(() => {
    if (!segment) return

    // Calculate tooltip position to avoid viewport overflow
    const tooltipWidth = 220
    // Adjust height for ETF section when on L1
    const tooltipHeight = type === 'l1' ? 280 : 180
    const padding = 12

    let x = mouseX + padding
    let y = mouseY + padding

    // Avoid right edge
    if (x + tooltipWidth > window.innerWidth - padding) {
      x = mouseX - tooltipWidth - padding
    }

    // Avoid bottom edge
    if (y + tooltipHeight > window.innerHeight - padding) {
      y = mouseY - tooltipHeight - padding
    }

    // Avoid left edge
    if (x < padding) {
      x = padding
    }

    // Avoid top edge
    if (y < padding) {
      y = padding
    }

    setPosition({ x, y })
  }, [segment, mouseX, mouseY, type])

  if (!segment) return null

  const { name, changePct, stockCount, upCount, downCount, amount, metricLabel, metricValue } = segment
  const changeStr = `${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`
  const changeColor = changePct >= 0 ? 'text-red-500' : 'text-green-500'

  // Get current metric info
  const metricInfo = METRIC_LABELS[metric]
  // For metric display color - positive values red, negative green (for change metric)
  const metricColor = metric === 'change'
    ? (metricValue >= 0 ? 'text-red-500' : 'text-green-500')
    : 'text-blue-600'

  // Format amount (in 亿)
  const amountInYi = amount / 100000000
  const amountStr =
    amountInYi >= 1
      ? `${amountInYi.toFixed(2)}亿`
      : `${(amount / 10000).toFixed(2)}万`

  // Calculate up/down ratio bar
  const total = upCount + downCount
  const upRatio = total > 0 ? (upCount / total) * 100 : 50

  const tooltipContent = (
    <div
      className="fixed z-[9999] pointer-events-none"
      style={{
        left: position.x,
        top: position.y,
      }}
    >
      <div className="bg-white dark:bg-gray-900 text-foreground rounded-lg shadow-xl p-3 min-w-[180px] border border-border">
        {/* Header */}
        <div className="flex items-center justify-between mb-2">
          <div className="flex flex-col">
            {type === 'l2' && parentName && (
              <span className="text-[10px] text-muted-foreground">{parentName}</span>
            )}
            <span className="font-semibold text-sm text-foreground">{name}</span>
          </div>
          <span className={`font-mono font-bold ${metricColor}`}>
            {metricLabel}
          </span>
        </div>

        {/* Current metric description */}
        <div className="text-[10px] text-muted-foreground mb-2">
          {metricInfo.name}：{metricInfo.desc}
        </div>

        {/* Divider */}
        <div className="border-t border-border my-2" />

        {/* Stats */}
        <div className="space-y-1.5 text-xs">
          {/* Change percentage (show when not the current metric) */}
          {metric !== 'change' && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">涨跌幅</span>
              <span className={`font-mono ${changeColor}`}>{changeStr}</span>
            </div>
          )}

          {/* Stock count */}
          <div className="flex justify-between">
            <span className="text-muted-foreground">股票数量</span>
            <span>{stockCount}</span>
          </div>

          {/* Up/Down counts with visual bar */}
          <div className="flex justify-between items-center">
            <span className="text-muted-foreground">涨跌分布</span>
            <div className="flex items-center gap-1.5">
              <span className="text-red-500">{upCount}</span>
              <span className="text-muted-foreground">/</span>
              <span className="text-green-500">{downCount}</span>
            </div>
          </div>

          {/* Visual ratio bar */}
          <div className="h-1.5 w-full bg-muted rounded-full overflow-hidden flex">
            <div
              className="h-full bg-red-500 transition-all"
              style={{ width: `${upRatio}%` }}
            />
            <div
              className="h-full bg-green-500 transition-all"
              style={{ width: `${100 - upRatio}%` }}
            />
          </div>

          {/* Amount */}
          <div className="flex justify-between">
            <span className="text-muted-foreground">成交额</span>
            <span className="font-mono">{amountStr}</span>
          </div>

          {/* L1 specific: children count */}
          {type === 'l1' && isL1Item(segment) && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">子行业</span>
              <span>
                {segment.children.gainers.length + segment.children.losers.length}个
              </span>
            </div>
          )}
        </div>

        {/* Related ETFs section (L1 only) */}
        {type === 'l1' && (
          <div className="mt-2 pt-2 border-t border-border">
            <div className="flex items-center gap-1 text-[10px] text-muted-foreground mb-1.5">
              <Package className="h-3 w-3" />
              <span>相关ETF</span>
            </div>
            {isLoadingEtfs ? (
              <div className="flex items-center justify-center py-1">
                <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
              </div>
            ) : etfData?.etfs && etfData.etfs.length > 0 ? (
              <div className="space-y-1">
                {etfData.etfs.map((etf) => (
                  <div key={etf.code} className="flex items-center justify-between text-xs">
                    <span className="text-foreground truncate max-w-[100px]">
                      {etf.name}
                    </span>
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-muted-foreground text-[10px]">
                        {formatAmount(etf.amount)}
                      </span>
                      <span className={`font-mono ${
                        etf.change_pct != null
                          ? etf.change_pct >= 0
                            ? 'text-red-500'
                            : 'text-green-500'
                          : 'text-muted-foreground'
                      }`}>
                        {etf.change_pct != null
                          ? `${etf.change_pct >= 0 ? '+' : ''}${etf.change_pct.toFixed(2)}%`
                          : '-'
                        }
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-[10px] text-muted-foreground text-center py-1">
                暂无相关ETF
              </div>
            )}
          </div>
        )}

        {/* Click hint for L1 */}
        {type === 'l1' && (
          <div className="mt-2 pt-2 border-t border-border text-[10px] text-muted-foreground text-center">
            点击展开/收起子行业
          </div>
        )}
      </div>
    </div>
  )

  // Portal to body to avoid SVG clipping
  return createPortal(tooltipContent, document.body)
})
