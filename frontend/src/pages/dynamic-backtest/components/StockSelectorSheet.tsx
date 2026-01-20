import { useState, useMemo } from 'react'
import { AlertDialog, AlertDialogContent, AlertDialogTitle } from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Toggle } from '@/components/ui/toggle'
import {
  useGetUniverseSnapshotApiV1UniverseSnapshotGet,
  useGetIndustryTreeApiV1UniverseIndustriesTreeGet,
} from '@/api/generated/universe-cockpit/universe-cockpit'
import {
  useGetEtfRotationDetailApiV1AlphaRadarEtfRotationCategoryGet,
  useGetEtfSubcategoryListApiV1AlphaRadarEtfSubcategoryListGet,
} from '@/api/generated/alpha-radar/alpha-radar'
import type { EtfCategory, IndustryTreeItem } from '@/api/generated/schemas'
import type { StockData } from '@/lib/dynamic-backtest'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'

// 股票过滤选项
interface StockFilters {
  hideST: boolean
  hideGEM: boolean      // 创业板 Growth Enterprise Market
  hideBSE: boolean      // 北交所 Beijing Stock Exchange
}

// 过滤函数
function shouldShowStock(code: string, name: string, filters: StockFilters): boolean {
  // ST股票: 名称包含 ST
  if (filters.hideST && name.includes('ST')) return false
  // 创业板: sz.300xxx 或 sz.301xxx
  if (filters.hideGEM && code.match(/^sz\.30[01]/)) return false
  // 北交所: bj.开头
  if (filters.hideBSE && code.startsWith('bj.')) return false
  return true
}

// 产业链顺序 (与后端 INDUSTRY_CHAIN_ORDER 保持一致)
const INDUSTRY_CHAIN_ORDER: Record<string, number> = {
  // 上游资源 (1-10)
  煤炭: 1, 石油石化: 2, 钢铁: 3, 有色金属: 4, 基础化工: 5, 建筑材料: 6,
  // 中游制造 (11-20)
  机械设备: 11, 电力设备: 12, 国防军工: 13, 电子: 14, 计算机: 15,
  通信: 16, 汽车: 17, 家用电器: 18, 轻工制造: 19, 纺织服饰: 20,
  // 下游消费 (21-30)
  食品饮料: 21, 医药生物: 22, 农林牧渔: 23, 商贸零售: 24, 社会服务: 25,
  美容护理: 26, 传媒: 27,
  // 公用基建 (31-40)
  公用事业: 31, 交通运输: 32, 建筑装饰: 33, 环保: 34, 房地产: 35,
  // 金融 (41-50)
  银行: 41, 非银金融: 42, 综合: 50,
}

const ETF_CATEGORIES: { key: EtfCategory; label: string }[] = [
  { key: 'broad', label: '宽基/大盘' },
  { key: 'sector', label: '行业' },
  { key: 'theme', label: '赛道' },
  { key: 'cross_border', label: '跨境/QDII' },
  { key: 'commodity', label: '商品' },
  { key: 'bond', label: '债券' },
]

interface StockSelectorSheetProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  selectedCodes: Map<string, StockData>
  pendingCode: string | null
  onToggleStock: (code: string) => void
}

interface StockItemProps {
  code: string
  name: string
  isSelected: boolean
  isPending: boolean
  onToggle: (code: string) => void
}

function StockItem({ code, name, isSelected, isPending, onToggle }: StockItemProps) {
  const displayCode = code.replace(/^(sh\.|sz\.)/, '')

  return (
    <button
      type="button"
      onClick={() => onToggle(code)}
      disabled={isPending}
      className={cn(
        'flex w-full items-center justify-between px-1 py-0.5 text-left text-[11px] leading-tight transition-colors',
        isSelected ? 'bg-red-500/20 text-red-600 dark:text-red-400' : 'hover:bg-muted',
        isPending && 'opacity-60 cursor-not-allowed'
      )}
    >
      <div className="min-w-0 flex-1">
        <span className="truncate block">{name}</span>
        <span className="text-[10px] text-muted-foreground font-mono">{displayCode}</span>
      </div>
    </button>
  )
}

interface IndustryStockListProps {
  industryL1: string
  selectedCodes: Map<string, StockData>
  pendingCode: string | null
  onToggleStock: (code: string) => void
  filters: StockFilters
}

function IndustryStockList({
  industryL1,
  selectedCodes,
  pendingCode,
  onToggleStock,
  filters,
}: IndustryStockListProps) {
  const pageSize = 500 // Max stocks per industry
  const { data, isLoading } = useGetUniverseSnapshotApiV1UniverseSnapshotGet(
    {
      page: 1,
      page_size: pageSize,
      asset_type: 'stock',
      industry_l1: industryL1,
      sort_by: 'name',
      sort_order: 'asc',
    },
    { query: { enabled: !!industryL1 } }
  )

  const items = useMemo(() => {
    const all = data?.items ?? []
    return all.filter(item => shouldShowStock(item.code, item.name, filters))
  }, [data?.items, filters])

  if (isLoading) {
    return <div className="px-1 py-1 text-xs text-muted-foreground">加载中...</div>
  }

  if (items.length === 0) {
    return <div className="px-1 py-1 text-xs text-muted-foreground">暂无股票</div>
  }

  return (
    <div>
      {items.map((item) => (
        <StockItem
          key={item.code}
          code={item.code}
          name={item.name}
          isSelected={selectedCodes.has(item.code)}
          isPending={pendingCode === item.code}
          onToggle={onToggleStock}
        />
      ))}
      {data && data.total > items.length && (
        <div className="px-1 py-0.5 text-[10px] text-muted-foreground">
          显示 {items.length} / {data.total}
        </div>
      )}
    </div>
  )
}

interface RotationIndustryColumnProps {
  item: IndustryTreeItem
  selectedCodes: Map<string, StockData>
  pendingCode: string | null
  onToggleStock: (code: string) => void
  filters: StockFilters
}

function RotationIndustryColumn({ item, selectedCodes, pendingCode, onToggleStock, filters }: RotationIndustryColumnProps) {
  return (
    <div className="min-w-[57px] w-[57px] flex flex-col border-r bg-card shrink-0 last:border-r-0">
      <div className="flex items-center justify-between border-b px-0.5 py-0.5 bg-muted/30 sticky top-0">
        <span className="text-[10px] font-semibold truncate">{item.name}</span>
        <span className="text-[9px] text-muted-foreground">{item.stock_count}</span>
      </div>
      <div className="px-0.5">
        <IndustryStockList
          industryL1={item.name}
          selectedCodes={selectedCodes}
          pendingCode={pendingCode}
          onToggleStock={onToggleStock}
          filters={filters}
        />
      </div>
    </div>
  )
}

// ETF 子分类列组件 - 类似行业列
interface EtfSubCategoryColumnProps {
  category: EtfCategory
  subCategory: string
  selectedCodes: Map<string, StockData>
  pendingCode: string | null
  onToggleStock: (code: string) => void
}

function EtfSubCategoryColumn({
  category,
  subCategory,
  selectedCodes,
  pendingCode,
  onToggleStock,
}: EtfSubCategoryColumnProps) {
  const { data, isLoading } = useGetEtfSubcategoryListApiV1AlphaRadarEtfSubcategoryListGet(
    { category, sub_category: subCategory },
    { query: { staleTime: 5 * 60 * 1000 } } // 5 min cache
  )

  const etfs = data?.etfs ?? []

  return (
    <div className="min-w-[100px] w-[100px] flex flex-col border-r bg-card shrink-0 last:border-r-0">
      <div className="flex items-center justify-between border-b px-0.5 py-0.5 bg-muted/30 sticky top-0">
        <span className="text-[10px] font-semibold truncate">{subCategory}</span>
        <span className="text-[9px] text-muted-foreground">{etfs.length || '-'}</span>
      </div>
      <div className="px-0.5">
        {isLoading ? (
          <div className="px-1 py-1 text-[10px] text-muted-foreground">加载中...</div>
        ) : etfs.length === 0 ? (
          <div className="px-1 py-1 text-[10px] text-muted-foreground">暂无</div>
        ) : (
          etfs.map((etf) => (
            <StockItem
              key={etf.code}
              code={etf.code}
              name={etf.name}
              isSelected={selectedCodes.has(etf.code)}
              isPending={pendingCode === etf.code}
              onToggle={onToggleStock}
            />
          ))
        )}
      </div>
    </div>
  )
}

// ETF 分类内容 - 展示某个一级分类下的所有二级分类
interface EtfCategoryContentProps {
  category: EtfCategory
  selectedCodes: Map<string, StockData>
  pendingCode: string | null
  onToggleStock: (code: string) => void
}

function EtfCategoryContent({ category, selectedCodes, pendingCode, onToggleStock }: EtfCategoryContentProps) {
  const { data, isLoading } = useGetEtfRotationDetailApiV1AlphaRadarEtfRotationCategoryGet(
    category,
    { days: 5, page_size: 1 },  // API requires days >= 5
    { query: { staleTime: 5 * 60 * 1000 } }
  )

  const subCategories = data?.sub_categories ?? []

  if (isLoading) {
    return <div className="flex items-center justify-center h-40 text-muted-foreground">加载中...</div>
  }

  if (subCategories.length === 0) {
    return <div className="text-sm text-muted-foreground p-4">暂无子分类</div>
  }

  return (
    <div className="h-full overflow-auto">
      <div className="flex border-t">
        {subCategories.map((subCat) => (
          <EtfSubCategoryColumn
            key={`${category}-${subCat}`}
            category={category}
            subCategory={subCat}
            selectedCodes={selectedCodes}
            pendingCode={pendingCode}
            onToggleStock={onToggleStock}
          />
        ))}
      </div>
    </div>
  )
}

export function StockSelectorSheet({
  open,
  onOpenChange,
  selectedCodes,
  pendingCode,
  onToggleStock,
}: StockSelectorSheetProps) {
  const [tab, setTab] = useState<'stock' | 'etf'>('stock')
  const [etfCategory, setEtfCategory] = useState<EtfCategory>('broad')
  const [filters, setFilters] = useState<StockFilters>({
    hideST: true,
    hideGEM: true,
    hideBSE: true,
  })

  // Use Industry Tree API to get complete SW L1 industry list with stock counts
  const { data: industryData, isLoading } = useGetIndustryTreeApiV1UniverseIndustriesTreeGet(
    { system: 'sw', level: 1 },
    { query: { enabled: open && tab === 'stock' } }
  )

  // Sort industries by upstream → downstream chain order
  const sortedIndustries = useMemo(() => {
    if (!industryData?.items) return []
    return [...industryData.items].sort(
      (a, b) => (INDUSTRY_CHAIN_ORDER[a.name] ?? 99) - (INDUSTRY_CHAIN_ORDER[b.name] ?? 99)
    )
  }, [industryData?.items])

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent className="max-w-[calc(100vw-100px)] w-full h-[calc(100vh-60px)] overflow-hidden p-0 flex flex-col">
        <Tabs value={tab} onValueChange={(value) => setTab(value as 'stock' | 'etf')} className="flex-1 flex flex-col min-h-0">
          <div className="flex-shrink-0 flex items-center justify-between border-b px-4 py-1.5">
            <AlertDialogTitle className="text-sm">添加股票</AlertDialogTitle>
            <div className="flex items-center gap-4">
              {tab === 'stock' && (
                <div className="flex items-center gap-1">
                  <Toggle
                    pressed={filters.hideST}
                    onPressedChange={(pressed) => setFilters(f => ({ ...f, hideST: pressed }))}
                    size="sm"
                    className="h-6 px-2 text-xs data-[state=on]:bg-[#d4e5d4] data-[state=on]:text-[#4a6b4a]"
                  >
                    ST
                  </Toggle>
                  <Toggle
                    pressed={filters.hideGEM}
                    onPressedChange={(pressed) => setFilters(f => ({ ...f, hideGEM: pressed }))}
                    size="sm"
                    className="h-6 px-2 text-xs data-[state=on]:bg-[#d4e5d4] data-[state=on]:text-[#4a6b4a]"
                  >
                    创业板
                  </Toggle>
                  <Toggle
                    pressed={filters.hideBSE}
                    onPressedChange={(pressed) => setFilters(f => ({ ...f, hideBSE: pressed }))}
                    size="sm"
                    className="h-6 px-2 text-xs data-[state=on]:bg-[#d4e5d4] data-[state=on]:text-[#4a6b4a]"
                  >
                    北交所
                  </Toggle>
                </div>
              )}
              <TabsList>
                <TabsTrigger value="stock">股票</TabsTrigger>
                <TabsTrigger value="etf">ETF</TabsTrigger>
              </TabsList>
              <Button variant="ghost" size="icon" onClick={() => onOpenChange(false)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
          </div>

          <div className="flex-1 min-h-0">
            <TabsContent value="stock" className="h-full overflow-hidden m-0 data-[state=active]:flex data-[state=active]:flex-col">
              {isLoading ? (
                <div className="flex items-center justify-center h-40 text-muted-foreground">加载中...</div>
              ) : sortedIndustries.length ? (
                <div className="flex-1 min-h-0 overflow-auto">
                  <div className="flex border-t">
                    {sortedIndustries.map((item) => (
                      <RotationIndustryColumn
                        key={`industry-${item.name}`}
                        item={item}
                        selectedCodes={selectedCodes}
                        pendingCode={pendingCode}
                        onToggleStock={onToggleStock}
                        filters={filters}
                      />
                    ))}
                  </div>
                </div>
              ) : (
                <div className="text-sm text-muted-foreground p-4">暂无行业</div>
              )}
            </TabsContent>

            <TabsContent value="etf" className="h-full overflow-hidden m-0 data-[state=active]:flex data-[state=active]:flex-col">
              {/* ETF 一级分类 Tabs */}
              <div className="flex-shrink-0 flex items-center gap-1 px-2 py-1 border-b bg-muted/20">
                {ETF_CATEGORIES.map((cat) => (
                  <Button
                    key={cat.key}
                    variant={etfCategory === cat.key ? 'secondary' : 'ghost'}
                    size="sm"
                    onClick={() => setEtfCategory(cat.key)}
                    className="h-6 px-2 text-xs"
                  >
                    {cat.label}
                  </Button>
                ))}
              </div>
              {/* ETF 二级分类内容 */}
              <div className="flex-1 min-h-0">
                <EtfCategoryContent
                  category={etfCategory}
                  selectedCodes={selectedCodes}
                  pendingCode={pendingCode}
                  onToggleStock={onToggleStock}
                />
              </div>
            </TabsContent>
          </div>
        </Tabs>
      </AlertDialogContent>
    </AlertDialog>
  )
}
