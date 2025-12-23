import { useState, useMemo } from 'react'
import { Card, CardContent, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import type { SortingState } from '@tanstack/react-table'
import {
  useGetUniverseSnapshotApiV1UniverseSnapshotGet,
  useGetUniverseStatsApiV1UniverseStatsGet,
} from '@/api/generated/universe-cockpit/universe-cockpit'
import { UniverseStatsBar } from '@/components/universe/UniverseStatsBar'
import { UniverseScreener, type UniverseFilters } from '@/components/universe/UniverseScreener'
import { UniverseDataTable } from '@/components/universe/UniverseDataTable'
import { UniverseDetailDrawer } from '@/components/universe/UniverseDetailDrawer'

// Map frontend sorting to backend sort_by parameter
const sortFieldMap: Record<string, string> = {
  code: 'code',
  name: 'name',
  market_cap: 'market_cap',
  price: 'price',
  change_pct: 'change',
  pe_ttm: 'pe',
  pb_mrq: 'pb',
  turnover: 'turnover',
}

export default function UniverseCockpitPage() {
  // Filter state
  const [filters, setFilters] = useState<UniverseFilters>({
    assetType: 'stock',
    exchange: 'all',
    search: '',
    industryL1: 'all',
    industryL2: 'all',
    industryL3: 'all',
    emIndustry: 'all',
    isSt: null,
    board: 'all',
    sizeCategory: 'all',
    volCategory: 'all',
    valueCategory: 'all',
  })

  // Pagination state
  const [page, setPage] = useState(1)
  const pageSize = 50

  // Sorting state
  const [sorting, setSorting] = useState<SortingState>([])

  // Drawer state
  const [selectedCode, setSelectedCode] = useState<string | null>(null)
  const [isDrawerOpen, setIsDrawerOpen] = useState(false)

  // Compute sort parameters
  const sortBy = useMemo(() => {
    if (sorting.length === 0) return 'code'
    const field = sorting[0].id
    return sortFieldMap[field] || 'code'
  }, [sorting])

  const sortOrder = useMemo(() => {
    if (sorting.length === 0) return 'asc'
    return sorting[0].desc ? 'desc' : 'asc'
  }, [sorting])

  // Fetch universe stats
  const { data: stats, isLoading: isLoadingStats } = useGetUniverseStatsApiV1UniverseStatsGet({
    asset_type: filters.assetType,
  })

  // Fetch universe snapshot with filters
  // Stock-specific filters are only sent when asset_type is 'stock'
  const isStock = filters.assetType === 'stock'
  const { data: snapshot, isLoading: isLoadingSnapshot } = useGetUniverseSnapshotApiV1UniverseSnapshotGet({
    page,
    page_size: pageSize,
    asset_type: filters.assetType,
    exchange: filters.exchange === 'all' ? undefined : filters.exchange,
    search: filters.search || undefined,
    // Stock-specific filters only for stocks
    industry_l1: isStock && filters.industryL1 !== 'all' ? filters.industryL1 : undefined,
    industry_l2: isStock && filters.industryL2 !== 'all' ? filters.industryL2 : undefined,
    industry_l3: isStock && filters.industryL3 !== 'all' ? filters.industryL3 : undefined,
    em_industry: isStock && filters.emIndustry !== 'all' ? filters.emIndustry : undefined,
    is_st: isStock && filters.isSt !== null ? filters.isSt : undefined,
    board: isStock && filters.board !== 'all' ? filters.board : undefined,
    size_category: isStock && filters.sizeCategory !== 'all' ? filters.sizeCategory : undefined,
    vol_category: isStock && filters.volCategory !== 'all' ? filters.volCategory : undefined,
    value_category: isStock && filters.valueCategory !== 'all' ? filters.valueCategory : undefined,
    sort_by: sortBy as 'code' | 'name' | 'market_cap' | 'price' | 'change' | 'pe' | 'pb' | 'turnover',
    sort_order: sortOrder as 'asc' | 'desc',
  })

  // Handle filter changes - reset to page 1
  const handleFiltersChange = (newFilters: UniverseFilters) => {
    setFilters(newFilters)
    setPage(1)
  }

  // Handle sorting changes - reset to page 1
  const handleSortingChange = (newSorting: SortingState) => {
    setSorting(newSorting)
    setPage(1)
  }

  // Handle row click
  const handleRowClick = (code: string) => {
    setSelectedCode(code)
    setIsDrawerOpen(true)
  }

  // Handle drawer close
  const handleDrawerClose = () => {
    setIsDrawerOpen(false)
  }

  const totalPages = snapshot?.pages || 1

  return (
    <div className="space-y-4">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold">市场发现</h1>
        <p className="text-muted-foreground text-sm">
          浏览和筛选股票、ETF数据
          {snapshot?.date && (
            <span className="ml-2">
              · 数据日期: <span className="font-mono">{snapshot.date}</span>
            </span>
          )}
        </p>
      </div>

      {/* Stats Bar */}
      <UniverseStatsBar stats={stats} isLoading={isLoadingStats} />

      {/* Main Content */}
      <Card>
        <CardHeader className="pb-3">
          {/* Screener */}
          <UniverseScreener
            filters={filters}
            onFiltersChange={handleFiltersChange}
            totalCount={snapshot?.total}
            isLoading={isLoadingSnapshot}
          />
        </CardHeader>
        <CardContent className="pt-0">
          {/* Data Table */}
          <UniverseDataTable
            data={snapshot?.items || []}
            isLoading={isLoadingSnapshot}
            sorting={sorting}
            onSortingChange={handleSortingChange}
            onRowClick={handleRowClick}
            selectedCode={selectedCode}
          />

          {/* Pagination */}
          {snapshot && snapshot.pages > 1 && (
            <div className="flex items-center justify-between mt-4 pt-4 border-t">
              <p className="text-sm text-muted-foreground">
                第 {page} / {totalPages} 页，共 {snapshot.total.toLocaleString()} 条
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1 || isLoadingSnapshot}
                >
                  <ChevronLeft className="h-4 w-4" />
                  上一页
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages || isLoadingSnapshot}
                >
                  下一页
                  <ChevronRight className="h-4 w-4" />
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Detail Drawer */}
      <UniverseDetailDrawer
        code={selectedCode}
        open={isDrawerOpen}
        onClose={handleDrawerClose}
      />
    </div>
  )
}
