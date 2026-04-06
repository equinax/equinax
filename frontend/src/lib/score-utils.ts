export function getSectionSum(sectionScores: Record<string, number>): number {
  return Object.values(sectionScores).reduce((a, b) => a + b, 0)
}

export const SCORE_ITEM_COLORS: Record<string, Record<string, string>> = {
  market: {
    trend: '#2563eb',
    volume: '#3b82f6',
    sentiment: '#60a5fa',
  },
  sector: {
    strength: '#16a34a',
    flow: '#22c55e',
    cohesion: '#4ade80',
  },
  stock: {
    pattern: '#dc2626',
    position: '#ef4444',
    catalyst: '#f87171',
    risk: '#fca5a5',
  },
}

export const SCORE_ITEMS_ORDERED = [
  { section: 'market', key: 'trend' },
  { section: 'market', key: 'volume' },
  { section: 'market', key: 'sentiment' },
  { section: 'sector', key: 'strength' },
  { section: 'sector', key: 'flow' },
  { section: 'sector', key: 'cohesion' },
  { section: 'stock', key: 'pattern' },
  { section: 'stock', key: 'position' },
  { section: 'stock', key: 'catalyst' },
  { section: 'stock', key: 'risk' },
] as const
