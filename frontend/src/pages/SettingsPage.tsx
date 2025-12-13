import { useNavigate } from 'react-router-dom'
import { ArrowLeft, Settings } from 'lucide-react'
import { Button } from '@/components/ui/button'

export default function SettingsPage() {
  const navigate = useNavigate()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="icon" className="h-8 w-8" onClick={() => navigate(-1)}>
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="flex items-center gap-2">
          <Settings className="h-5 w-5" />
          <h1 className="text-2xl font-semibold">设置</h1>
        </div>
      </div>

      {/* Placeholder content */}
      <div className="flex items-center justify-center h-[60vh] border border-dashed rounded-lg bg-muted/30">
        <p className="text-muted-foreground">设置页面（待开发）</p>
      </div>
    </div>
  )
}
