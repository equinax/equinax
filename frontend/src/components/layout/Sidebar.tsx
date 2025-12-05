import { Link, useLocation } from 'react-router-dom'
import { cn } from '@/lib/utils'
import {
  LayoutDashboard,
  Code2,
  PlayCircle,
  BarChart3,
  Database,
  Settings,
  TrendingUp,
} from 'lucide-react'

const navigation = [
  { name: '仪表盘', href: '/', icon: LayoutDashboard },
  { name: '策略管理', href: '/strategies', icon: Code2 },
  { name: '回测执行', href: '/backtest', icon: PlayCircle },
  { name: '结果分析', href: '/results', icon: BarChart3 },
  { name: '数据浏览', href: '/data', icon: Database },
]

export default function Sidebar() {
  const location = useLocation()

  return (
    <div className="flex w-64 flex-col border-r border-border bg-card">
      {/* Logo */}
      <div className="flex h-16 items-center gap-2 border-b border-border px-6">
        <TrendingUp className="h-8 w-8 text-primary" />
        <span className="text-lg font-bold">Quant Backtest</span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 space-y-1 px-3 py-4">
        {navigation.map((item) => {
          const isActive = location.pathname === item.href
          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
              )}
            >
              <item.icon className="h-5 w-5" />
              {item.name}
            </Link>
          )
        })}
      </nav>

      {/* Bottom section */}
      <div className="border-t border-border p-3">
        <Link
          to="/settings"
          className="flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground"
        >
          <Settings className="h-5 w-5" />
          设置
        </Link>
      </div>
    </div>
  )
}
