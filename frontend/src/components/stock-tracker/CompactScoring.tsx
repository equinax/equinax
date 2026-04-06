import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Badge } from '@/components/ui/badge'
import { getSectionSum, SCORE_ITEM_COLORS } from '@/lib/score-utils'
import {
  useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet,
  useUpsertScoresApiV1StockTrackerEntriesEntryIdScoresPut,
  getGetScoresApiV1StockTrackerEntriesEntryIdScoresGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'

interface ScoreItem {
  key: string
  label: string
  max: number
}

interface ScoreSection {
  key: string
  label: string
  maxTotal: number
  items: ScoreItem[]
}

const SCORE_SECTIONS: ScoreSection[] = [
  {
    key: 'market',
    label: '大盘',
    maxTotal: 30,
    items: [
      { key: 'trend', label: '趋势', max: 10 },
      { key: 'volume', label: '量能', max: 10 },
      { key: 'sentiment', label: '情绪', max: 10 },
    ],
  },
  {
    key: 'sector',
    label: '板块',
    maxTotal: 30,
    items: [
      { key: 'strength', label: '强度', max: 10 },
      { key: 'flow', label: '资金', max: 10 },
      { key: 'cohesion', label: '联动', max: 10 },
    ],
  },
  {
    key: 'stock',
    label: '个股',
    maxTotal: 40,
    items: [
      { key: 'pattern', label: '形态', max: 10 },
      { key: 'position', label: '位置', max: 10 },
      { key: 'catalyst', label: '题材', max: 10 },
      { key: 'risk', label: '风险', max: 10 },
    ],
  },
]

const SCORE_STEPS = [0, 2, 4, 6, 8, 10]

type ScoreValues = Record<string, Record<string, number>>

const getEmptyScores = (): ScoreValues => {
  const result: ScoreValues = {}
  for (const section of SCORE_SECTIONS) {
    result[section.key] = {}
    for (const item of section.items) {
      result[section.key][item.key] = 0
    }
  }
  return result
}

const DECISION_CONFIG: Record<string, { className: string }> = {
  做多日: { className: 'bg-green-500/15 text-green-400 border-green-500/30' },
  震荡日: { className: 'bg-yellow-500/15 text-yellow-400 border-yellow-500/30' },
  回撤日: { className: 'bg-red-500/15 text-red-400 border-red-500/30' },
}

const SECTION_LABEL_CONFIG: Record<string, Record<string, string>> = {
  market: { 偏多: 'text-green-400', 震荡: 'text-yellow-400', 偏空: 'text-red-400' },
  sector: { 强: 'text-green-400', 一般: 'text-yellow-400', 弱: 'text-red-400' },
  stock: { 强: 'text-green-400', 震荡: 'text-yellow-400', 弱: 'text-red-400' },
}

interface CompactScoringProps {
  entryId: string
}

export default function CompactScoring({ entryId }: CompactScoringProps) {
  const queryClient = useQueryClient()

  const { data: scoreData } =
    useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet(entryId, {
      query: { enabled: !!entryId },
    })

  const [scores, setScores] = useState<ScoreValues>(getEmptyScores)
  const pendingSaveRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (scoreData?.scores) {
      const loaded = scoreData.scores as Record<string, Record<string, number>>
      const merged = getEmptyScores()
      for (const section of SCORE_SECTIONS) {
        const sectionData = loaded[section.key]
        if (sectionData) {
          for (const item of section.items) {
            if (typeof sectionData[item.key] === 'number') {
              merged[section.key][item.key] = sectionData[item.key]
            }
          }
        }
      }
      setScores(merged)
    }
  }, [scoreData])

  const upsertMutation =
    useUpsertScoresApiV1StockTrackerEntriesEntryIdScoresPut({
      mutation: {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey:
              getGetScoresApiV1StockTrackerEntriesEntryIdScoresGetQueryKey(entryId),
          })
        },
      },
    })

  const debouncedSave = useCallback(
    (newScores: ScoreValues) => {
      if (pendingSaveRef.current) {
        clearTimeout(pendingSaveRef.current)
      }
      pendingSaveRef.current = setTimeout(() => {
        upsertMutation.mutate({
          entryId,
          data: { scores: newScores },
        })
        pendingSaveRef.current = null
      }, 600)
    },
    [entryId, upsertMutation]
  )

  useEffect(() => {
    return () => {
      if (pendingSaveRef.current) {
        clearTimeout(pendingSaveRef.current)
      }
    }
  }, [])

  const handleBarClick = (
    sectionKey: string,
    itemKey: string,
    e: React.MouseEvent<HTMLDivElement>
  ) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const x = e.clientX - rect.left
    const pct = x / rect.width
    const rawValue = pct * 10
    const snapped = SCORE_STEPS.reduce((prev, curr) =>
      Math.abs(curr - rawValue) < Math.abs(prev - rawValue) ? curr : prev
    )
    const newScores = {
      ...scores,
      [sectionKey]: {
        ...scores[sectionKey],
        [itemKey]: snapped,
      },
    }
    setScores(newScores)
    debouncedSave(newScores)
  }

  const totalScore = useMemo(
    () =>
      SCORE_SECTIONS.reduce(
        (sum, section) => sum + getSectionSum(scores[section.key] || {}),
        0
      ),
    [scores]
  )

  const serverLabels = scoreData
    ? {
        market: scoreData.market_label as string | null,
        sector: scoreData.sector_label as string | null,
        stock: scoreData.stock_label as string | null,
        decision: scoreData.decision as string | null,
      }
    : null

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2">
        <span className="text-sm font-bold font-mono">{totalScore}</span>
        <span className="text-[10px] text-muted-foreground">/100</span>
        {serverLabels?.decision && (
          <Badge
            variant="outline"
            className={`text-[10px] px-1.5 py-0 h-4 ${
              DECISION_CONFIG[serverLabels.decision]?.className ?? 'text-muted-foreground'
            }`}
          >
            {serverLabels.decision}
          </Badge>
        )}
      </div>

      {SCORE_SECTIONS.map((section) => {
        const sectionSum = getSectionSum(scores[section.key] || {})
        const sectionLabel = serverLabels
          ? String((serverLabels as Record<string, unknown>)[section.key] ?? '')
          : ''

        return (
          <div key={section.key} className="flex items-center gap-2">
            <span className="text-[11px] text-muted-foreground w-7 shrink-0 font-medium">
              {section.label}
            </span>

            <div className="flex items-center gap-1 flex-1 min-w-0">
              {section.items.map((item) => {
                const value = scores[section.key]?.[item.key] ?? 0
                const color = SCORE_ITEM_COLORS[section.key]?.[item.key] ?? '#888'
                return (
                  <div
                    key={item.key}
                    className="flex items-center gap-1 flex-1 min-w-0"
                  >
                    <span className="text-[10px] text-muted-foreground/70 w-6 shrink-0 text-right">
                      {item.label}
                    </span>
                    <div
                      className="h-3 flex-1 rounded-sm bg-muted/30 cursor-pointer relative overflow-hidden"
                      onClick={(e) => handleBarClick(section.key, item.key, e)}
                      title={`${item.label}: ${value}/10`}
                    >
                      <div
                        className="h-full rounded-sm transition-all duration-150"
                        style={{
                          width: `${(value / item.max) * 100}%`,
                          backgroundColor: color,
                        }}
                      />
                    </div>
                  </div>
                )
              })}
            </div>

            <span className="text-[10px] font-mono text-muted-foreground w-10 text-right shrink-0">
              {sectionSum}/{section.maxTotal}
            </span>
            {sectionLabel && (
              <span
                className={`text-[10px] w-5 shrink-0 ${
                  SECTION_LABEL_CONFIG[section.key]?.[sectionLabel] ?? 'text-muted-foreground'
                }`}
              >
                {sectionLabel}
              </span>
            )}
          </div>
        )
      })}
    </div>
  )
}
