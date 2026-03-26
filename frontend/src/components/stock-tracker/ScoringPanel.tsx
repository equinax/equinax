import { useState, useEffect, useMemo } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Save } from 'lucide-react'
import {
  useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet,
  useUpsertScoresApiV1StockTrackerEntriesEntryIdScoresPut,
  getGetScoresApiV1StockTrackerEntriesEntryIdScoresGetQueryKey,
} from '@/api/generated/stock-tracker/stock-tracker'

interface ScoreItem {
  key: string
  label: string
  max: number
  options: number[]
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
      { key: 'trend', label: '趋势方向', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'volume', label: '量能配合', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'sentiment', label: '市场情绪', max: 10, options: [0, 2, 4, 6, 8, 10] },
    ],
  },
  {
    key: 'sector',
    label: '板块',
    maxTotal: 30,
    items: [
      { key: 'strength', label: '板块强度', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'flow', label: '资金流向', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'cohesion', label: '板块联动', max: 10, options: [0, 2, 4, 6, 8, 10] },
    ],
  },
  {
    key: 'stock',
    label: '个股',
    maxTotal: 40,
    items: [
      { key: 'pattern', label: '形态质量', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'position', label: '位置合理', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'catalyst', label: '驱动/题材', max: 10, options: [0, 2, 4, 6, 8, 10] },
      { key: 'risk', label: '风险可控', max: 10, options: [0, 2, 4, 6, 8, 10] },
    ],
  },
]

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

const getSectionSum = (sectionScores: Record<string, number>): number =>
  Object.values(sectionScores).reduce((a, b) => a + b, 0)

const DECISION_CONFIG: Record<string, { label: string; className: string }> = {
  做多日: { label: '做多日', className: 'bg-green-500/15 text-green-400 border-green-500/30' },
  震荡日: { label: '震荡日', className: 'bg-yellow-500/15 text-yellow-400 border-yellow-500/30' },
  回撤日: { label: '回撤日', className: 'bg-red-500/15 text-red-400 border-red-500/30' },
}

const SECTION_LABEL_CONFIG: Record<string, Record<string, string>> = {
  market: { 偏多: 'text-green-400', 震荡: 'text-yellow-400', 偏空: 'text-red-400' },
  sector: { 强: 'text-green-400', 一般: 'text-yellow-400', 弱: 'text-red-400' },
  stock: { 强: 'text-green-400', 震荡: 'text-yellow-400', 弱: 'text-red-400' },
}

interface ScoringPanelProps {
  entryId: string
}

export default function ScoringPanel({ entryId }: ScoringPanelProps) {
  const queryClient = useQueryClient()

  const { data: scoreData } =
    useGetScoresApiV1StockTrackerEntriesEntryIdScoresGet(entryId, {
      query: { enabled: !!entryId },
    })

  const [scores, setScores] = useState<ScoreValues>(getEmptyScores)
  const [isDirty, setIsDirty] = useState(false)

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
      setIsDirty(false)
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
          setIsDirty(false)
        },
      },
    })

  const handleScoreChange = (sectionKey: string, itemKey: string, value: number) => {
    setScores((prev) => ({
      ...prev,
      [sectionKey]: {
        ...prev[sectionKey],
        [itemKey]: value,
      },
    }))
    setIsDirty(true)
  }

  const handleSave = () => {
    upsertMutation.mutate({
      entryId,
      data: { scores },
    })
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
        total: scoreData.total_score,
      }
    : null

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-lg font-bold font-mono">{totalScore}</span>
          <span className="text-xs text-muted-foreground">/ 100</span>
          {serverLabels?.decision && (
            <Badge
              variant="outline"
              className={
                DECISION_CONFIG[serverLabels.decision]?.className ??
                'text-muted-foreground'
              }
            >
              {serverLabels.decision}
            </Badge>
          )}
        </div>
        <Button
          size="sm"
          onClick={handleSave}
          disabled={!isDirty || upsertMutation.isPending}
        >
          <Save className="h-3 w-3 mr-1" />
          {upsertMutation.isPending ? '保存中...' : '保存'}
        </Button>
      </div>

      {SCORE_SECTIONS.map((section) => {
        const sectionSum = getSectionSum(scores[section.key] || {})
        const sectionLabel = serverLabels
          ? String(
              (serverLabels as Record<string, unknown>)[section.key] ?? ''
            )
          : ''

        return (
          <div key={section.key} className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="font-medium">
                {section.label}
                <span className="text-muted-foreground ml-1">
                  ({sectionSum}/{section.maxTotal})
                </span>
              </span>
              {sectionLabel && (
                <span
                  className={`text-xs font-medium ${SECTION_LABEL_CONFIG[section.key]?.[sectionLabel] ?? ''}`}
                >
                  {sectionLabel}
                </span>
              )}
            </div>

            {section.items.map((item) => (
              <div
                key={item.key}
                className="flex items-center justify-between gap-2"
              >
                <span className="text-xs text-muted-foreground w-16 shrink-0">
                  {item.label}
                </span>
                <div className="flex gap-1">
                  {item.options.map((opt) => (
                    <button
                      key={opt}
                      onClick={() =>
                        handleScoreChange(section.key, item.key, opt)
                      }
                      className={`
                        w-7 h-7 rounded text-xs font-mono transition-colors
                        ${
                          scores[section.key]?.[item.key] === opt
                            ? 'bg-primary text-primary-foreground font-bold'
                            : 'bg-muted/50 text-muted-foreground hover:bg-muted'
                        }
                      `}
                    >
                      {opt}
                    </button>
                  ))}
                </div>
              </div>
            ))}

            {section !== SCORE_SECTIONS[SCORE_SECTIONS.length - 1] && (
              <div className="border-b" />
            )}
          </div>
        )
      })}
    </div>
  )
}
