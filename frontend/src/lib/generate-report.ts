import { QUANT_LABEL_CN, formatMv, formatVol } from '@/lib/quant-labels'

export interface MarkdownReportStock {
  code: string
  name: string
  buyPrice?: number
  refPrice?: number
  totalMv?: string | number | null
  circMv?: string | number | null
  volume?: string | number | null
  turnover?: string | number | null
  peTtm?: string | number | null
  pbMrq?: string | number | null
  quantLabels?: string[]
  returns?: Record<string, string | null>
}

export interface MarkdownPeriodStats {
  period: number
  winRate?: string | null
  avgReturn?: string | null
  avgWin?: string | null
  avgLoss?: string | null
  profitLossRatio?: string | null
  winCount: number
  loseCount: number
  total: number
}

export interface MarkdownReportData {
  date: string
  tab?: string
  tabLabel?: string
  assessment?: string
  stocks: MarkdownReportStock[]
  periodStats: MarkdownPeriodStats[]
  periods: readonly number[]
}

// ── Markdown Report ──────────────────────────────────────────────

const PERIOD_LABELS_MD: Record<number, string> = { 3: 'B+3', 5: 'B+5', 10: 'B+10', 20: 'B+20' }

function fmtPct(val: string | null | undefined): string {
  if (val == null) return '—'
  const n = parseFloat(val)
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function fmtNum(val: string | number | null | undefined): string {
  if (val == null) return '—'
  return parseFloat(String(val)).toFixed(2)
}

function fmtPrice(val: number | undefined): string {
  if (val == null) return '—'
  return `¥${val.toFixed(2)}`
}

function returnEmoji(val: string | null | undefined): string {
  if (val == null) return ''
  const n = parseFloat(val)
  if (n >= 5) return ' 🔴'
  if (n > 0) return ' 🟠'
  if (n > -3) return ' 🟢'
  return ' 🔻'
}

export function generateMarkdownReport(data: MarkdownReportData): void {
  const { date, tabLabel, assessment, stocks, periodStats, periods } = data
  const lines: string[] = []

  // Header
  const strategyNote = tabLabel ? ` · ${tabLabel}` : ''
  lines.push(`# Alpha Radar 推荐报告${strategyNote}`)
  lines.push('')
  lines.push(`- **推荐日期**: ${date}`)
  lines.push(`- **股票数量**: ${stocks.length}`)
  if (assessment) lines.push(`- **综合评价**: ${assessment}`)
  lines.push(`- **报告生成**: ${new Date().toLocaleString('zh-CN')}`)
  lines.push('')

  // Aggregate stats table
  if (periodStats.length > 0) {
    lines.push('## 整体表现')
    lines.push('')
    const pCols = periods.map(p => PERIOD_LABELS_MD[p] ?? `B+${p}`)
    lines.push(`| 指标 | ${pCols.join(' | ')} |`)
    lines.push(`| --- | ${pCols.map(() => '---:').join(' | ')} |`)

    const findStats = (p: number) => periodStats.find(s => s.period === p)

    // Win rate
    lines.push(`| 胜率 | ${periods.map(p => {
      const s = findStats(p)
      return s?.winRate != null ? `${parseFloat(s.winRate).toFixed(1)}%` : '—'
    }).join(' | ')} |`)

    // Avg return
    lines.push(`| 平均收益 | ${periods.map(p => {
      const s = findStats(p)
      return s?.avgReturn != null ? fmtPct(s.avgReturn) : '—'
    }).join(' | ')} |`)

    // P/L ratio
    lines.push(`| 盈亏比 | ${periods.map(p => {
      const s = findStats(p)
      return s?.profitLossRatio != null ? fmtNum(s.profitLossRatio) : '—'
    }).join(' | ')} |`)

    // Win/Lose count
    lines.push(`| 上涨/下跌 | ${periods.map(p => {
      const s = findStats(p)
      return s ? `${s.winCount}/${s.loseCount}` : '—'
    }).join(' | ')} |`)

    // Avg win
    lines.push(`| 平均盈利 | ${periods.map(p => {
      const s = findStats(p)
      return s?.avgWin != null ? fmtPct(s.avgWin) : '—'
    }).join(' | ')} |`)

    // Avg loss
    lines.push(`| 平均亏损 | ${periods.map(p => {
      const s = findStats(p)
      return s?.avgLoss != null ? fmtPct(s.avgLoss) : '—'
    }).join(' | ')} |`)

    lines.push('')
  }

  // Per-stock summary table
  lines.push('## 个股总览')
  lines.push('')
  const hasReturns = stocks.some(s => s.returns && Object.values(s.returns).some(v => v != null))

  if (hasReturns) {
    const retCols = periods.map(p => PERIOD_LABELS_MD[p] ?? `B+${p}`)
    lines.push(`| 代码 | 名称 | 买入价 | 总市值 | PE(TTM) | PB | ${retCols.join(' | ')} |`)
    lines.push(`| --- | --- | ---: | ---: | ---: | ---: | ${retCols.map(() => '---:').join(' | ')} |`)
    for (const s of stocks) {
      const retVals = periods.map(p => {
        const r = s.returns?.[String(p)]
        return r != null ? `${fmtPct(r)}${returnEmoji(r)}` : '—'
      })
      lines.push(`| ${s.code} | ${s.name} | ${fmtPrice(s.buyPrice)} | ${formatMv(s.totalMv)} | ${fmtNum(s.peTtm)} | ${fmtNum(s.pbMrq)} | ${retVals.join(' | ')} |`)
    }
  } else {
    lines.push(`| 代码 | 名称 | 买入价 | 总市值 | PE(TTM) | PB |`)
    lines.push(`| --- | --- | ---: | ---: | ---: | ---: |`)
    for (const s of stocks) {
      lines.push(`| ${s.code} | ${s.name} | ${fmtPrice(s.buyPrice)} | ${formatMv(s.totalMv)} | ${fmtNum(s.peTtm)} | ${fmtNum(s.pbMrq)} |`)
    }
    lines.push('')
    lines.push('> ⏳ 推荐日期较近，收益数据待验证')
  }
  lines.push('')

  // Per-stock detail sections
  lines.push('## 个股详情')
  lines.push('')

  for (const s of stocks) {
    lines.push(`### ${s.code} ${s.name}`)
    lines.push('')

    // Fundamentals table
    lines.push('| 指标 | 数值 |')
    lines.push('| --- | ---: |')
    lines.push(`| 买入价 (T+1 Open) | ${fmtPrice(s.buyPrice)} |`)
    if (s.refPrice != null) lines.push(`| 推荐日收盘 | ${fmtPrice(s.refPrice)} |`)
    lines.push(`| 总市值 | ${formatMv(s.totalMv)} |`)
    lines.push(`| 流通市值 | ${formatMv(s.circMv)} |`)
    lines.push(`| PE (TTM) | ${fmtNum(s.peTtm)} |`)
    lines.push(`| PB (MRQ) | ${fmtNum(s.pbMrq)} |`)
    lines.push(`| 换手率 | ${s.turnover != null ? `${parseFloat(String(s.turnover)).toFixed(1)}%` : '—'} |`)
    lines.push(`| 成交量 | ${formatVol(s.volume)} |`)
    lines.push('')

    // Returns
    if (s.returns && Object.values(s.returns).some(v => v != null)) {
      lines.push('**持仓收益**:')
      const retParts = periods
        .filter(p => s.returns?.[String(p)] != null)
        .map(p => `${PERIOD_LABELS_MD[p] ?? `B+${p}`}: ${fmtPct(s.returns![String(p)])}${returnEmoji(s.returns![String(p)])}`)
      lines.push(retParts.join(' · '))
      lines.push('')
    }

    // Quant labels
    if (s.quantLabels && s.quantLabels.length > 0) {
      const labelsCn = s.quantLabels.map(l => QUANT_LABEL_CN[l] ?? l)
      lines.push(`**推荐理由**: ${labelsCn.join('、')}`)
      lines.push('')
    }

    lines.push('---')
    lines.push('')
  }

  // Footer
  lines.push('*本报告由 Alpha Radar 量化系统自动生成，仅供参考，不构成投资建议。*')
  lines.push('')

  // Download
  const content = lines.join('\n')
  const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `推荐报告_${date}${tabLabel ? `_${tabLabel}` : ''}.md`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
