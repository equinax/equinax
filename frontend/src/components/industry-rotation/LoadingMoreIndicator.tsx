/**
 * Loading More Indicator Component
 *
 * Displays at the bottom of the scrollable area:
 * - Loading state: spinning animation + "正在加载更多数据..."
 * - No more data: "已加载全部数据"
 */

interface LoadingMoreIndicatorProps {
  isLoading: boolean
  hasMore: boolean
}

export function LoadingMoreIndicator({ isLoading, hasMore }: LoadingMoreIndicatorProps) {
  // Don't show anything if not loading and there's more data
  if (!isLoading && hasMore) return null

  return (
    <div className="flex items-center justify-center gap-3 py-4 text-muted-foreground">
      {isLoading ? (
        <>
          {/* SVG spinning animation */}
          <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
              fill="none"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          <span className="animate-pulse text-sm">正在加载更多数据...</span>
        </>
      ) : (
        <span className="text-sm">已加载全部数据</span>
      )}
    </div>
  )
}
