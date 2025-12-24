/**
 * Sync history panel with slide animation between list and detail views.
 */

import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Clock, ArrowLeft } from 'lucide-react'
import { cn } from '@/lib/utils'
import { SyncHistoryList } from './SyncHistoryList'
import { SyncJobDetailView } from './SyncJobDetailView'

export function SyncHistoryPanel() {
  const [view, setView] = useState<'list' | 'detail'>('list')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [page, setPage] = useState(0)

  const handleSelect = (id: string) => {
    setSelectedId(id)
    setView('detail')
  }

  const handleBack = () => {
    setView('list')
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          {view === 'detail' ? (
            <>
              <Button
                variant="ghost"
                size="sm"
                className="h-8 px-2 -ml-2"
                onClick={handleBack}
              >
                <ArrowLeft className="h-4 w-4 mr-1" />
                返回
              </Button>
              <span className="text-muted-foreground font-normal">|</span>
              <span>同步详情</span>
            </>
          ) : (
            <>
              <Clock className="h-5 w-5" />
              Sync History
            </>
          )}
        </CardTitle>
        {view === 'list' && (
          <CardDescription>Recent synchronization operations</CardDescription>
        )}
      </CardHeader>
      <CardContent>
        <div className="relative overflow-hidden">
          {/* List View */}
          <div
            className={cn(
              'transition-all duration-300 ease-in-out',
              view === 'detail'
                ? '-translate-x-full opacity-0 absolute inset-0'
                : 'translate-x-0 opacity-100'
            )}
          >
            <SyncHistoryList
              onSelect={handleSelect}
              page={page}
              onPageChange={setPage}
            />
          </div>

          {/* Detail View */}
          <div
            className={cn(
              'transition-all duration-300 ease-in-out',
              view === 'detail'
                ? 'translate-x-0 opacity-100'
                : 'translate-x-full opacity-0 absolute inset-0'
            )}
          >
            {selectedId && <SyncJobDetailView jobId={selectedId} />}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
