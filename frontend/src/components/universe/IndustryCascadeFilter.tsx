/**
 * Industry Cascade Filter Component
 *
 * Provides cascading selection for:
 * - EM (EastMoney): Flat list of 86 industries
 * - SW (Shenwan): Hierarchical L1 -> L2 -> L3 selection
 */

import { cn } from '@/lib/utils'
import { useGetIndustryTree, type IndustryTreeItem } from '@/api/industry-tree'
import { ChevronRight } from 'lucide-react'

interface IndustryCascadeFilterProps {
  // EM (EastMoney) industry selection
  emIndustry: string
  onEmIndustryChange: (value: string) => void
  // SW (Shenwan) industry selections
  swL1: string
  swL2: string
  swL3: string
  onSwL1Change: (value: string) => void
  onSwL2Change: (value: string) => void
  onSwL3Change: (value: string) => void
  className?: string
}

interface IndustryRowProps {
  label: string
  items: IndustryTreeItem[]
  selectedValue: string
  onSelect: (value: string) => void
  isLoading?: boolean
  showCount?: boolean
}

function IndustryRow({
  label,
  items,
  selectedValue,
  onSelect,
  isLoading,
  showCount = false,
}: IndustryRowProps) {
  return (
    <div className="flex items-start gap-3">
      <div className="w-20 shrink-0 text-sm text-muted-foreground pt-1.5 text-right">
        {label}
      </div>
      <div className="flex flex-wrap gap-1.5">
        {/* All option */}
        <button
          type="button"
          onClick={() => onSelect('all')}
          className={cn(
            'px-3 py-1 text-sm rounded-md transition-colors cursor-pointer',
            'border border-transparent',
            'hover:bg-accent hover:text-accent-foreground',
            selectedValue === 'all' && 'bg-muted text-foreground font-medium',
            selectedValue !== 'all' && 'text-muted-foreground'
          )}
        >
          {label === '东财行业' ? '不限' : '全部'}
        </button>

        {isLoading ? (
          <span className="px-3 py-1 text-sm text-muted-foreground">
            ...
          </span>
        ) : (
          items.map((item) => {
            const isSelected = selectedValue === item.name
            return (
              <button
                type="button"
                key={item.name}
                onClick={() => onSelect(item.name)}
                className={cn(
                  'px-3 py-1 text-sm rounded-md transition-colors cursor-pointer',
                  'border border-transparent',
                  'hover:bg-accent hover:text-accent-foreground',
                  'flex items-center gap-1',
                  isSelected && [
                    'bg-primary text-primary-foreground',
                    'hover:bg-primary/90 hover:text-primary-foreground',
                  ],
                  !isSelected && 'text-muted-foreground'
                )}
              >
                {item.name}
                {showCount && (
                  <span className={cn(
                    'text-xs',
                    isSelected ? 'text-primary-foreground/80' : 'text-muted-foreground/60'
                  )}>
                    ({item.stock_count})
                  </span>
                )}
                {item.has_children && !isSelected && (
                  <ChevronRight className="h-3 w-3 text-muted-foreground/50" />
                )}
              </button>
            )
          })
        )}
      </div>
    </div>
  )
}

export function IndustryCascadeFilter({
  emIndustry,
  onEmIndustryChange,
  swL1,
  swL2,
  swL3,
  onSwL1Change,
  onSwL2Change,
  onSwL3Change,
  className,
}: IndustryCascadeFilterProps) {
  // Fetch EM industries (flat)
  const { data: emData, isLoading: emLoading } = useGetIndustryTree({
    system: 'em',
    level: 1,
  })

  // Fetch SW L1 industries
  const { data: swL1Data, isLoading: swL1Loading } = useGetIndustryTree({
    system: 'sw',
    level: 1,
  })

  // Fetch SW L2 industries (only when L1 is selected)
  const { data: swL2Data, isLoading: swL2Loading } = useGetIndustryTree(
    {
      system: 'sw',
      level: 2,
      parent: swL1,
    },
    {
      query: {
        enabled: swL1 !== 'all',
      },
    }
  )

  // Fetch SW L3 industries (only when L2 is selected)
  const { data: swL3Data, isLoading: swL3Loading } = useGetIndustryTree(
    {
      system: 'sw',
      level: 3,
      parent: swL2,
    },
    {
      query: {
        enabled: swL2 !== 'all',
      },
    }
  )

  return (
    <div className={cn('space-y-3', className)}>
      {/* EM Industry (flat) */}
      <IndustryRow
        label="东财行业"
        items={emData?.items ?? []}
        selectedValue={emIndustry}
        onSelect={onEmIndustryChange}
        isLoading={emLoading}
      />

      {/* Divider */}
      <div className="border-t border-border/50" />

      {/* SW L1 */}
      <IndustryRow
        label="申万一级"
        items={swL1Data?.items ?? []}
        selectedValue={swL1}
        onSelect={onSwL1Change}
        isLoading={swL1Loading}
      />

      {/* SW L2 (only show when L1 is selected) */}
      {swL1 !== 'all' && (
        <IndustryRow
          label="申万二级"
          items={swL2Data?.items ?? []}
          selectedValue={swL2}
          onSelect={onSwL2Change}
          isLoading={swL2Loading}
          showCount
        />
      )}

      {/* SW L3 (only show when L2 is selected) */}
      {swL2 !== 'all' && (
        <IndustryRow
          label="申万三级"
          items={swL3Data?.items ?? []}
          selectedValue={swL3}
          onSelect={onSwL3Change}
          isLoading={swL3Loading}
          showCount
        />
      )}
    </div>
  )
}
