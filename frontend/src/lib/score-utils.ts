export function getSectionSum(sectionScores: Record<string, number>): number {
  return Object.values(sectionScores).reduce((a, b) => a + b, 0)
}

export const SCORE_ITEM_COLORS: Record<string, Record<string, string>> = {
  market: {
    opening: '#7c3aed',
    timepos: '#8b5cf6',
    breadth: '#a78bfa',
  },
  sector: {
    gain: '#2563eb',
    cohesion: '#3b82f6',
    capital: '#60a5fa',
  },
  stock: {
    opening: '#0891b2',
    timepos: '#06b6d4',
    rebound: '#22d3ee',
    volprice: '#67e8f9',
  },
}

export const SCORE_ITEM_DESCRIPTIONS: Record<string, Record<string, string>> = {
  market: {
    opening: '高开+上冲→10 / 平开震荡→5 / 低开下杀→0',
    timepos: '在均线之上运行→10 / 来回穿均线→5 / 压在均线下→0',
    breadth: '上涨家数明显多→10 / 涨跌差不多→5 / 下跌多→0',
  },
  sector: {
    gain: '板块涨幅靠前→10 / 中等→5 / 靠后→0',
    cohesion: '多只个股一起涨→10 / 个别涨→5 / 基本不动→0',
    capital: '持续放量上行→10 / 有量但不持续→5 / 缩量/出货→0',
  },
  stock: {
    opening: '高开并快速上冲→10 / 平开→5 / 低开→0',
    timepos: '在均线之上→15 / 反复穿→8 / 压制在下→0',
    rebound: '回调后快速拉起≥1%→10 / 弱反抽→5 / 无反抽→0',
    volprice: '涨放量跌缩量→5 / 无明显→2 / 跌放量→0',
  },
}

export const SCORE_ITEMS_ORDERED = [
  { section: 'market', key: 'opening' },
  { section: 'market', key: 'timepos' },
  { section: 'market', key: 'breadth' },
  { section: 'sector', key: 'gain' },
  { section: 'sector', key: 'cohesion' },
  { section: 'sector', key: 'capital' },
  { section: 'stock', key: 'opening' },
  { section: 'stock', key: 'timepos' },
  { section: 'stock', key: 'rebound' },
  { section: 'stock', key: 'volprice' },
] as const

export const SCORE_ITEM_MAX: Record<string, Record<string, number>> = {
  stock: {
    timepos: 15,
    volprice: 5,
  },
}

export const SCORE_ITEM_STEPS: Record<string, Record<string, number[]>> = {
  stock: {
    timepos: [0, 8, 15],
    volprice: [0, 2, 5],
  },
}

export const DEFAULT_SCORE_STEPS = [0, 5, 10]
