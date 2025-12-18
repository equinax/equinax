import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Search, X, Filter, TrendingUp, PieChart } from 'lucide-react'

export interface UniverseFilters {
  assetType: 'stock' | 'etf'
  exchange: string
  search: string
  industryL1: string
  isSt: boolean | null
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

  const clearAllFilters = () => {
    setSearchInput('')
    onFiltersChange({
      assetType: 'stock',
      exchange: 'all',
      search: '',
      industryL1: 'all',
      isSt: null,
    })
  }

  const hasActiveFilters =
    filters.exchange !== 'all' ||
    filters.search ||
    filters.industryL1 !== 'all' ||
    filters.isSt !== null

  return (
    <div className="space-y-3">
      {/* Asset Type Toggle */}
      <div className="flex items-center justify-between">
        <div className="flex gap-2">
          <Button
            variant={filters.assetType === 'stock' ? 'default' : 'outline'}
            size="sm"
            onClick={() => updateFilter('assetType', 'stock')}
          >
            <TrendingUp className="h-4 w-4 mr-1.5" />
            股票
          </Button>
          <Button
            variant={filters.assetType === 'etf' ? 'default' : 'outline'}
            size="sm"
            onClick={() => updateFilter('assetType', 'etf')}
          >
            <PieChart className="h-4 w-4 mr-1.5" />
            ETF
          </Button>
        </div>

        {totalCount !== undefined && (
          <div className="text-sm text-muted-foreground">
            共 <span className="font-medium text-foreground">{totalCount.toLocaleString()}</span> 只
          </div>
        )}
      </div>

      {/* Filter Bar */}
      <div className="flex flex-wrap items-center gap-2">
        {/* Search */}
        <form onSubmit={handleSearchSubmit} className="relative flex-1 min-w-[200px] max-w-[300px]">
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

        {/* Exchange Filter */}
        <Select
          value={filters.exchange}
          onValueChange={(value) => updateFilter('exchange', value)}
        >
          <SelectTrigger className="w-[100px] h-9">
            <SelectValue placeholder="交易所" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">全部</SelectItem>
            <SelectItem value="sh">上海</SelectItem>
            <SelectItem value="sz">深圳</SelectItem>
          </SelectContent>
        </Select>

        {/* Industry Filter */}
        {filters.assetType === 'stock' && (
          <Select
            value={filters.industryL1}
            onValueChange={(value) => updateFilter('industryL1', value)}
          >
            <SelectTrigger className="w-[120px] h-9">
              <SelectValue placeholder="行业" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部行业</SelectItem>
              {industries.map((industry) => (
                <SelectItem key={industry} value={industry}>
                  {industry}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}

        {/* ST Filter */}
        {filters.assetType === 'stock' && (
          <Select
            value={filters.isSt === null ? 'all' : filters.isSt ? 'st' : 'non_st'}
            onValueChange={(value) => {
              if (value === 'all') updateFilter('isSt', null)
              else if (value === 'st') updateFilter('isSt', true)
              else updateFilter('isSt', false)
            }}
          >
            <SelectTrigger className="w-[100px] h-9">
              <SelectValue placeholder="ST" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部</SelectItem>
              <SelectItem value="non_st">非ST</SelectItem>
              <SelectItem value="st">仅ST</SelectItem>
            </SelectContent>
          </Select>
        )}

        {/* Clear Filters */}
        {hasActiveFilters && (
          <Button
            variant="ghost"
            size="sm"
            onClick={clearAllFilters}
            className="h-9 text-muted-foreground"
          >
            <X className="h-4 w-4 mr-1" />
            清除筛选
          </Button>
        )}
      </div>

      {/* Active Filters Display */}
      {hasActiveFilters && (
        <div className="flex flex-wrap gap-1.5">
          <Filter className="h-4 w-4 text-muted-foreground mt-0.5" />
          {filters.search && (
            <Badge variant="secondary" className="gap-1">
              搜索: {filters.search}
              <button onClick={() => updateFilter('search', '')}>
                <X className="h-3 w-3" />
              </button>
            </Badge>
          )}
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
          {filters.isSt !== null && (
            <Badge variant="secondary" className="gap-1">
              {filters.isSt ? 'ST股票' : '非ST'}
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
