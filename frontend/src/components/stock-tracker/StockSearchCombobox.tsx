import { useState, useEffect, useCallback } from 'react'
import { Check, ChevronsUpDown, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { useSearchAssetsApiV1StocksSearchGet } from '@/api/generated/stocks/stocks'
import type { AssetSearchResult, AppApiV1StocksAssetTypeFilter } from '@/api/generated/schemas'

interface StockSearchComboboxProps {
  value: AssetSearchResult | null
  onSelect: (asset: AssetSearchResult | null) => void
  assetType?: AppApiV1StocksAssetTypeFilter
  placeholder?: string
}

export default function StockSearchCombobox({
  value,
  onSelect,
  assetType = 'stock',
  placeholder = '搜索股票代码或名称...',
}: StockSearchComboboxProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(search), 300)
    return () => clearTimeout(timer)
  }, [search])

  const { data: results, isFetching } = useSearchAssetsApiV1StocksSearchGet(
    { q: debouncedSearch, limit: 15, asset_type: assetType },
    { query: { enabled: debouncedSearch.length >= 1 } }
  )

  const handleSelect = useCallback(
    (code: string) => {
      const asset = results?.find((a) => a.code === code) ?? null
      onSelect(asset)
      setOpen(false)
      setSearch('')
    },
    [results, onSelect]
  )

  const displayLabel = value ? `${value.code} ${value.name}` : null

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between font-normal"
        >
          {displayLabel ? (
            <span className="truncate">
              <span className="font-mono">{value!.code}</span>
              <span className="ml-2 text-muted-foreground">{value!.name}</span>
            </span>
          ) : (
            <span className="text-muted-foreground">{placeholder}</span>
          )}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[--radix-popover-trigger-width] p-0" align="start">
        <Command shouldFilter={false}>
          <CommandInput
            placeholder={placeholder}
            value={search}
            onValueChange={setSearch}
          />
          <CommandList>
            {isFetching ? (
              <div className="flex items-center justify-center py-6">
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              </div>
            ) : debouncedSearch.length === 0 ? (
              <div className="py-6 text-center text-sm text-muted-foreground">
                输入代码或名称开始搜索
              </div>
            ) : (
              <>
                <CommandEmpty>未找到匹配的股票</CommandEmpty>
                <CommandGroup>
                  {results?.map((asset) => (
                    <CommandItem
                      key={asset.code}
                      value={asset.code}
                      onSelect={handleSelect}
                    >
                      <Check
                        className={cn(
                          'mr-2 h-4 w-4',
                          value?.code === asset.code ? 'opacity-100' : 'opacity-0'
                        )}
                      />
                      <span className="font-mono text-sm">{asset.code}</span>
                      <span className="ml-2 text-sm">{asset.name}</span>
                      <span className="ml-auto text-xs text-muted-foreground">
                        {asset.asset_type}
                      </span>
                    </CommandItem>
                  ))}
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
