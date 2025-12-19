import { useState, useMemo } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Search, X, TrendingUp, PieChart, ChevronDown, ChevronUp } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  UniverseCategoryFilter,
  stockCategoryGroups,
  etfCategoryGroups,
} from './UniverseCategoryFilter'

export interface UniverseFilters {
  assetType: 'stock' | 'etf'
  exchange: string
  search: string
  industryL1: string
  isSt: boolean | null
  board: string
  sizeCategory: string
  volCategory: string
  valueCategory: string
}

interface UniverseScreenerProps {
  filters: UniverseFilters
  onFiltersChange: (filters: UniverseFilters) => void
  industries: string[]
  totalCount?: number
  isLoading?: boolean
}

export function UniverseScreener({
  filters,
  onFiltersChange,
  industries,
  totalCount,
  isLoading: _isLoading,
}: UniverseScreenerProps) {
  const [searchInput, setSearchInput] = useState(filters.search)
  const [isExpanded, setIsExpanded] = useState(true)

  const updateFilter = <K extends keyof UniverseFilters>(
    key: K,
    value: UniverseFilters[K]
  ) => {
    onFiltersChange({ ...filters, [key]: value })
  }

  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    updateFilter('search', searchInput)
  }

  const handleSearchClear = () => {
    setSearchInput('')
    updateFilter('search', '')
  }

  const handleAssetTypeChange = (newType: 'stock' | 'etf') => {
    if (newType === 'etf') {
      // ETF only keeps common filters, reset stock-specific filters
      onFiltersChange({
        assetType: newType,
        exchange: filters.exchange,
        search: filters.search,
        industryL1: 'all',
        board: 'all',
        sizeCategory: 'all',
        volCategory: 'all',
        valueCategory: 'all',
        isSt: null,
      })
    } else {
      updateFilter('assetType', newType)
    }
  }

  const clearAllFilters = () => {
    setSearchInput('')
    onFiltersChange({
      assetType: filters.assetType,
      exchange: 'all',
      search: '',
      industryL1: 'all',
      isSt: null,
      board: 'all',
      sizeCategory: 'all',
      volCategory: 'all',
      valueCategory: 'all',
    })
  }

  // Convert ST filter to string for category filter
  const stFilterValue = filters.isSt === null ? 'all' : filters.isSt ? 'st' : 'non_st'

  // Get selected values for category filter
  const selectedValues: Record<string, string> = {
    exchange: filters.exchange,
    board: filters.board,
    sizeCategory: filters.sizeCategory,
    volCategory: filters.volCategory,
    valueCategory: filters.valueCategory,
    isSt: stFilterValue,
  }

  // Handle category selection
  const handleCategorySelect = (groupId: string, value: string) => {
    if (groupId === 'isSt') {
      if (value === 'all') updateFilter('isSt', null)
      else if (value === 'st') updateFilter('isSt', true)
      else updateFilter('isSt', false)
    } else {
      updateFilter(groupId as keyof UniverseFilters, value)
    }
  }

  // Build industry category group dynamically from API data
  const industryCategoryGroup = useMemo(() => ({
    id: 'industryL1',
    label: '行业',
    options: [
      { value: 'all', label: '全部' },
      ...industries.map((ind) => ({ value: ind, label: ind })),
    ],
  }), [industries])

  // Combine category groups based on asset type
  const categoryGroups = useMemo(() => {
    if (filters.assetType === 'etf') {
      return etfCategoryGroups
    }

    // For stocks, add industry group before other groups
    const groups = [...stockCategoryGroups]
    // Insert industry group after exchange
    const exchangeIndex = groups.findIndex((g) => g.id === 'exchange')
    groups.splice(exchangeIndex + 1, 0, industryCategoryGroup)
    return groups
  }, [filters.assetType, industryCategoryGroup])

  const allSelectedValues = {
    ...selectedValues,
    industryL1: filters.industryL1,
  }

  const hasActiveFilters =
    filters.exchange !== 'all' ||
    filters.search ||
    filters.industryL1 !== 'all' ||
    filters.isSt !== null ||
    filters.board !== 'all' ||
    filters.sizeCategory !== 'all' ||
    filters.volCategory !== 'all' ||
    filters.valueCategory !== 'all'

  // Count active filters
  const activeFilterCount = [
    filters.exchange !== 'all',
    filters.industryL1 !== 'all',
    filters.isSt !== null,
    filters.board !== 'all',
    filters.sizeCategory !== 'all',
    filters.volCategory !== 'all',
    filters.valueCategory !== 'all',
  ].filter(Boolean).length

  return (
    <div className="space-y-4">
      {/* Header Row: Asset Type Toggle + Search + Count */}
      <div className="flex items-center gap-4">
        {/* Asset Type Toggle */}
        <div className="flex gap-1 p-1 bg-muted rounded-lg">
          <button
            onClick={() => handleAssetTypeChange('stock')}
            className={cn(
              'flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md transition-colors',
              filters.assetType === 'stock'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            )}
          >
            <TrendingUp className="h-4 w-4" />
            股票
          </button>
          <button
            onClick={() => handleAssetTypeChange('etf')}
            className={cn(
              'flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md transition-colors',
              filters.assetType === 'etf'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            )}
          >
            <PieChart className="h-4 w-4" />
            ETF
          </button>
        </div>

        {/* Search */}
        <form onSubmit={handleSearchSubmit} className="relative flex-1 max-w-[300px]">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="搜索代码或名称..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="pl-9 pr-8 h-9"
          />
          {searchInput && (
            <button
              type="button"
              onClick={handleSearchClear}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </form>

        {/* Search Badge */}
        {filters.search && (
          <Badge variant="secondary" className="gap-1">
            搜索: {filters.search}
            <button onClick={() => updateFilter('search', '')}>
              <X className="h-3 w-3" />
            </button>
          </Badge>
        )}

        {/* Spacer */}
        <div className="flex-1" />

        {/* Count & Expand Toggle */}
        <div className="flex items-center gap-3">
          {totalCount !== undefined && (
            <div className="text-sm text-muted-foreground">
              共 <span className="font-medium text-foreground">{totalCount.toLocaleString()}</span> 只
            </div>
          )}

          <Button
            variant="ghost"
            size="sm"
            onClick={() => setIsExpanded(!isExpanded)}
            className="text-muted-foreground"
          >
            {isExpanded ? (
              <>
                收起筛选
                <ChevronUp className="h-4 w-4 ml-1" />
              </>
            ) : (
              <>
                展开筛选
                {activeFilterCount > 0 && (
                  <Badge variant="secondary" className="ml-1 h-5 px-1.5">
                    {activeFilterCount}
                  </Badge>
                )}
                <ChevronDown className="h-4 w-4 ml-1" />
              </>
            )}
          </Button>
        </div>
      </div>

      {/* Expanded Category Filters */}
      {isExpanded && (
        <div className="relative">
          {/* Category Filter Grid */}
          <div className="bg-muted/30 rounded-lg p-4 border">
            <UniverseCategoryFilter
              groups={categoryGroups}
              selectedValues={allSelectedValues}
              onSelect={handleCategorySelect}
            />
          </div>

          {/* Clear All Button */}
          {hasActiveFilters && (
            <Button
              variant="ghost"
              size="sm"
              onClick={clearAllFilters}
              className="absolute top-3 right-3 h-7 text-xs text-muted-foreground hover:text-destructive"
            >
              <X className="h-3 w-3 mr-1" />
              清除全部
            </Button>
          )}
        </div>
      )}

      {/* Collapsed Active Filters Summary */}
      {!isExpanded && hasActiveFilters && (
        <div className="flex flex-wrap gap-1.5">
          {filters.exchange !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.exchange === 'sh' ? '上海' : '深圳'}
              <button onClick={() => updateFilter('exchange', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.industryL1 !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.industryL1}
              <button onClick={() => updateFilter('industryL1', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.board !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.board === 'MAIN' ? '主板' : filters.board === 'GEM' ? '创业板' : filters.board === 'STAR' ? '科创板' : '北交所'}
              <button onClick={() => updateFilter('board', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.sizeCategory !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.sizeCategory === 'MEGA' ? '超大盘' : filters.sizeCategory === 'LARGE' ? '大盘' : filters.sizeCategory === 'MID' ? '中盘' : filters.sizeCategory === 'SMALL' ? '小盘' : '微盘'}
              <button onClick={() => updateFilter('sizeCategory', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.volCategory !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.volCategory === 'HIGH' ? '高波动' : filters.volCategory === 'NORMAL' ? '正常' : '低波动'}
              <button onClick={() => updateFilter('volCategory', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.valueCategory !== 'all' && (
            <Badge variant="secondary" className="gap-1">
              {filters.valueCategory === 'VALUE' ? '价值' : filters.valueCategory === 'NEUTRAL' ? '平衡' : '成长'}
              <button onClick={() => updateFilter('valueCategory', 'all')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
          {filters.isSt !== null && (
            <Badge variant="secondary" className="gap-1">
              {filters.isSt ? 'ST' : '非ST'}
              <button onClick={() => updateFilter('isSt', null)}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
        </div>
      )}
    </div>
  )
}
