import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { cn } from '@/lib/utils'
import {
  LayoutDashboard,
  Code2,
  PlayCircle,
  BarChart3,
  Database,
  Settings,
} from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

const navigation = [
  { name: '仪表盘', href: '/', icon: LayoutDashboard },
  { name: '策略管理', href: '/strategies', icon: Code2 },
  { name: '回测执行', href: '/backtest', icon: PlayCircle },
  { name: '结果分析', href: '/results', icon: BarChart3 },
  { name: '数据浏览', href: '/data', icon: Database },
]

// 图标固定位置（居中于收缩后的宽度）
const ICON_CENTER = 32 // w-16 / 2 = 32px

export default function Sidebar() {
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)
  const [isHovering, setIsHovering] = useState(false)

  const toggleCollapse = () => setCollapsed(!collapsed)

  return (
    <TooltipProvider delayDuration={0}>
      <div
        className={cn(
          'group/sidebar relative flex flex-col border-r border-border bg-card',
          'transition-[width] duration-300 ease-out',
          collapsed ? 'w-16' : 'w-64'
        )}
        onMouseEnter={() => setIsHovering(true)}
        onMouseLeave={() => setIsHovering(false)}
      >
        {/* 右侧幻彩光晕 - 只有渐变，无竖条 */}
        <div
          className={cn(
            'absolute right-0 top-0 bottom-0 w-16 z-0',
            'transition-opacity duration-500 pointer-events-none',
            isHovering ? 'opacity-100' : 'opacity-0'
          )}
          style={{
            background: `linear-gradient(
              to left,
              rgba(59, 130, 246, 0.05) 0%,
              rgba(147, 51, 234, 0.03) 30%,
              rgba(236, 72, 153, 0.015) 60%,
              transparent 100%
            )`,
            animation: isHovering ? 'shimmer 3s ease-in-out infinite' : 'none',
          }}
        />

        {/* Logo - clickable to toggle */}
        <div
          className={cn(
            'relative flex h-16 items-center border-b border-border cursor-ew-resize select-none',
            'transition-all duration-300 ease-out',
            'active:scale-[0.98]'
          )}
          onClick={toggleCollapse}
        >
          {/* Logo 容器 - 图标位置固定 */}
          <div
            className="absolute flex items-center justify-center"
            style={{ left: ICON_CENTER, transform: 'translateX(-50%)' }}
          >
            <span
              className={cn(
                'text-lg font-bold tracking-tight text-primary',
                'transition-all duration-300 ease-out',
                collapsed
                  ? 'opacity-100 scale-100'
                  : 'opacity-0 scale-75'
              )}
            >
              Eqx
            </span>
          </div>

          {/* Expanded logo */}
          <div
            className={cn(
              'flex items-center pl-4',
              'transition-all duration-300 ease-out',
              collapsed
                ? 'opacity-0 translate-x-[-20px]'
                : 'opacity-100 translate-x-0'
            )}
          >
            <span className="text-[2rem] tracking-[0.02em] whitespace-nowrap">
              <span className="font-extrabold text-foreground/90">Equi</span>
              <span className="font-semibold text-primary">nax</span>
            </span>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-2 py-4">
          {navigation.map((item, index) => {
            const isActive = location.pathname === item.href
            const linkContent = (
              <Link
                key={item.name}
                to={item.href}
                className={cn(
                  'relative flex items-center h-10 rounded-lg text-sm font-medium',
                  'transition-colors duration-200 ease-out',
                  'mb-1',
                  isActive
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                )}
              >
                {/* 图标 - 固定位置 */}
                <div
                  className="absolute flex items-center justify-center w-10 h-10"
                  style={{ left: ICON_CENTER - 8 - 20 }} // 8 = px-2, 20 = w-10/2
                >
                  <item.icon className="h-5 w-5 flex-shrink-0" />
                </div>

                {/* 文字 - 动画过渡 */}
                <span
                  className={cn(
                    'absolute whitespace-nowrap',
                    'transition-all duration-300 ease-out',
                    collapsed
                      ? 'opacity-0 translate-x-[-8px]'
                      : 'opacity-100 translate-x-0'
                  )}
                  style={{
                    left: ICON_CENTER - 8 + 24, // 图标右侧 + gap
                    transitionDelay: collapsed ? '0ms' : `${100 + index * 30}ms`,
                  }}
                >
                  {item.name}
                </span>
              </Link>
            )

            if (collapsed) {
              return (
                <Tooltip key={item.name}>
                  <TooltipTrigger asChild>{linkContent}</TooltipTrigger>
                  <TooltipContent side="right" sideOffset={10}>
                    {item.name}
                  </TooltipContent>
                </Tooltip>
              )
            }

            return linkContent
          })}
        </nav>

        {/* Clickable empty area to toggle collapse - 整个区域可点击 */}
        <div
          className="flex-grow cursor-ew-resize min-h-[60px]"
          onClick={toggleCollapse}
        />

        {/* Bottom section - Settings */}
        <div
          className="border-t border-border p-2 cursor-ew-resize"
          onClick={(e) => {
            // 只有点击非链接区域才触发收缩
            if ((e.target as HTMLElement).closest('a')) return
            toggleCollapse()
          }}
        >
          {collapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <Link
                  to="/settings"
                  className={cn(
                    'relative flex items-center h-10 rounded-lg text-sm font-medium',
                    'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                    'transition-colors duration-200'
                  )}
                >
                  <div
                    className="absolute flex items-center justify-center w-10 h-10"
                    style={{ left: ICON_CENTER - 8 - 20 }}
                  >
                    <Settings className="h-5 w-5" />
                  </div>
                </Link>
              </TooltipTrigger>
              <TooltipContent side="right" sideOffset={10}>设置</TooltipContent>
            </Tooltip>
          ) : (
            <Link
              to="/settings"
              className={cn(
                'relative flex items-center h-10 rounded-lg text-sm font-medium',
                'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
                'transition-colors duration-200'
              )}
            >
              <div
                className="absolute flex items-center justify-center w-10 h-10"
                style={{ left: ICON_CENTER - 8 - 20 }}
              >
                <Settings className="h-5 w-5" />
              </div>
              <span
                className={cn(
                  'absolute whitespace-nowrap',
                  'transition-all duration-300 ease-out',
                  collapsed
                    ? 'opacity-0 translate-x-[-8px]'
                    : 'opacity-100 translate-x-0'
                )}
                style={{
                  left: ICON_CENTER - 8 + 24,
                  transitionDelay: collapsed ? '0ms' : '250ms',
                }}
              >
                设置
              </span>
            </Link>
          )}
        </div>

        {/* 全局 CSS 动画 */}
        <style>{`
          @keyframes shimmer {
            0%, 100% {
              opacity: 0.6;
              filter: hue-rotate(0deg);
            }
            50% {
              opacity: 1;
              filter: hue-rotate(30deg);
            }
          }
        `}</style>
      </div>
    </TooltipProvider>
  )
}
