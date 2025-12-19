import { cn } from '@/lib/utils'

interface CategoryOption {
  value: string
  label: string
}

interface CategoryGroup {
  id: string
  label: string
  options: CategoryOption[]
}

interface UniverseCategoryFilterProps {
  groups: CategoryGroup[]
  selectedValues: Record<string, string>
  onSelect: (groupId: string, value: string) => void
  className?: string
}

export function UniverseCategoryFilter({
  groups,
  selectedValues,
  onSelect,
  className,
}: UniverseCategoryFilterProps) {
  return (
    <div className={cn('space-y-3', className)}>
      {groups.map((group) => (
        <div key={group.id} className="flex items-start gap-3">
          {/* Category Label */}
          <div className="w-16 shrink-0 text-sm text-muted-foreground pt-1.5 text-right">
            {group.label}
          </div>

          {/* Category Options */}
          <div className="flex flex-wrap gap-1.5">
            {group.options.map((option) => {
              const isSelected = selectedValues[group.id] === option.value
              const isAll = option.value === 'all'

              return (
                <button
                  key={option.value}
                  onClick={() => onSelect(group.id, option.value)}
                  className={cn(
                    'px-3 py-1 text-sm rounded-md transition-colors',
                    'border border-transparent',
                    'hover:bg-accent hover:text-accent-foreground',
                    isSelected && !isAll && [
                      'bg-primary text-primary-foreground',
                      'hover:bg-primary/90 hover:text-primary-foreground',
                    ],
                    isSelected && isAll && [
                      'bg-muted text-foreground font-medium',
                    ],
                    !isSelected && 'text-muted-foreground'
                  )}
                >
                  {option.label}
                </button>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

// Predefined category groups for stock screener
export const stockCategoryGroups: CategoryGroup[] = [
  {
    id: 'exchange',
    label: '交易所',
    options: [
      { value: 'all', label: '全部' },
      { value: 'sh', label: '上海' },
      { value: 'sz', label: '深圳' },
    ],
  },
  {
    id: 'board',
    label: '板块',
    options: [
      { value: 'all', label: '全部' },
      { value: 'MAIN', label: '主板' },
      { value: 'GEM', label: '创业板' },
      { value: 'STAR', label: '科创板' },
      { value: 'BSE', label: '北交所' },
    ],
  },
  {
    id: 'sizeCategory',
    label: '规模',
    options: [
      { value: 'all', label: '全部' },
      { value: 'MEGA', label: '超大盘' },
      { value: 'LARGE', label: '大盘' },
      { value: 'MID', label: '中盘' },
      { value: 'SMALL', label: '小盘' },
      { value: 'MICRO', label: '微盘' },
    ],
  },
  {
    id: 'volCategory',
    label: '波动',
    options: [
      { value: 'all', label: '全部' },
      { value: 'HIGH', label: '高波动' },
      { value: 'NORMAL', label: '正常' },
      { value: 'LOW', label: '低波动' },
    ],
  },
  {
    id: 'valueCategory',
    label: '风格',
    options: [
      { value: 'all', label: '全部' },
      { value: 'VALUE', label: '价值' },
      { value: 'NEUTRAL', label: '平衡' },
      { value: 'GROWTH', label: '成长' },
    ],
  },
  {
    id: 'isSt',
    label: 'ST',
    options: [
      { value: 'all', label: '全部' },
      { value: 'non_st', label: '非ST' },
      { value: 'st', label: 'ST' },
    ],
  },
]

// ETF category groups (simpler)
export const etfCategoryGroups: CategoryGroup[] = [
  {
    id: 'exchange',
    label: '交易所',
    options: [
      { value: 'all', label: '全部' },
      { value: 'sh', label: '上海' },
      { value: 'sz', label: '深圳' },
    ],
  },
]
