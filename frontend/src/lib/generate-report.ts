import { QUANT_LABEL_CN, formatMv } from '@/lib/quant-labels'

export interface ReportStock {
  code: string
  name: string
  buyPrice?: number
  totalMv?: string | number | null
  circMv?: string | number | null
  volume?: string | number | null
  turnover?: string | number | null
  peTtm?: string | number | null
  pbMrq?: string | number | null
  quantLabels?: string[]
  chartImage: string
}

function buildStockHTML(stock: ReportStock, date: string): string {
  const labels = (stock.quantLabels ?? [])
    .map(l => QUANT_LABEL_CN[l] ?? l)
    .join('、')

  const indicators = [
    stock.totalMv != null && `总市值: ${formatMv(stock.totalMv)}`,
    stock.circMv != null && `流通市值: ${formatMv(stock.circMv)}`,
    stock.peTtm != null && `PE(TTM): ${parseFloat(String(stock.peTtm)).toFixed(1)}`,
    stock.pbMrq != null && `PB(MRQ): ${parseFloat(String(stock.pbMrq)).toFixed(2)}`,
    stock.turnover != null && `换手率: ${parseFloat(String(stock.turnover)).toFixed(1)}%`,
    stock.buyPrice != null && `买入价: ¥${stock.buyPrice.toFixed(2)}`,
  ].filter(Boolean)

  return `
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; width: 1050px; padding: 24px; box-sizing: border-box; background: #fff;">
      <div style="display: flex; align-items: baseline; gap: 12px; margin-bottom: 12px;">
        <span style="font-size: 20px; font-weight: 700; font-family: monospace;">${stock.code}</span>
        <span style="font-size: 18px; font-weight: 600;">${stock.name}</span>
        <span style="font-size: 13px; color: #6b7280; margin-left: auto;">推荐日期: ${date}</span>
      </div>

      <img src="${stock.chartImage}" style="width: 100%; border: 1px solid #e5e7eb; border-radius: 4px; margin-bottom: 16px;" />

      <table style="width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 12px;">
        <tbody>
          ${rowsFromPairs(indicators)}
        </tbody>
      </table>

      ${labels ? `
        <div style="font-size: 13px; padding: 8px 12px; background: #fffbeb; border: 1px solid #fde68a; border-radius: 4px;">
          <span style="font-weight: 600; color: #92400e;">推荐理由:</span>
          <span style="color: #78350f; margin-left: 4px;">${labels}</span>
        </div>
      ` : ''}
    </div>
  `
}

function rowsFromPairs(items: (string | false)[]): string {
  const valid = items.filter(Boolean) as string[]
  const rows: string[] = []
  for (let i = 0; i < valid.length; i += 3) {
    const cells = valid.slice(i, i + 3).map(item => {
      const [label, value] = item.split(': ')
      return `
        <td style="padding: 6px 8px; border-bottom: 1px solid #f3f4f6; color: #6b7280; width: 100px;">${label}</td>
        <td style="padding: 6px 8px; border-bottom: 1px solid #f3f4f6; font-weight: 500; font-family: monospace;">${value}</td>
      `
    }).join('')
    rows.push(`<tr>${cells}</tr>`)
  }
  return rows.join('')
}

export async function generateStockReport(date: string, stocks: ReportStock[]): Promise<void> {
  const [{ default: jsPDF }, { default: html2canvas }] = await Promise.all([
    import('jspdf'),
    import('html2canvas'),
  ])

  const pdf = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' })
  const pageW = pdf.internal.pageSize.getWidth()
  const pageH = pdf.internal.pageSize.getHeight()
  const margin = 8

  for (let i = 0; i < stocks.length; i++) {
    if (i > 0) pdf.addPage()

    const container = document.createElement('div')
    container.style.cssText = 'position:fixed;left:-9999px;top:0;width:1050px;background:#fff;'
    container.innerHTML = buildStockHTML(stocks[i], date)
    document.body.appendChild(container)

    try {
      const canvas = await html2canvas(container, {
        scale: 2,
        backgroundColor: '#ffffff',
        logging: false,
        useCORS: true,
      })

      const imgData = canvas.toDataURL('image/jpeg', 0.92)
      const contentW = pageW - margin * 2
      const contentH = (canvas.height / canvas.width) * contentW
      const finalH = Math.min(contentH, pageH - margin * 2)

      pdf.addImage(imgData, 'JPEG', margin, margin, contentW, finalH)
    } finally {
      document.body.removeChild(container)
    }
  }

  pdf.save(`投资参考报告_${date}.pdf`)
}
