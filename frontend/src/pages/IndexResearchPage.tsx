import { useState } from 'react'
import ReactECharts from 'echarts-for-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Slider } from '@/components/ui/slider'
import { Loader2, AlertCircle } from 'lucide-react'
import { useAnalyzeIndexCoherenceApiV1IndexResearchAnalyzePost } from '@/api/generated/index-research/index-research'
import type { IndexResearchResponse } from '@/api/generated/schemas'

export default function IndexResearchPage() {
  // State
  const [startDate, setStartDate] = useState('2025-02-01')
  const [endDate, setEndDate] = useState('2026-02-01')
  const [rollingWindow, setRollingWindow] = useState(20)
  const [activeTab, setActiveTab] = useState('ici')
  const [data, setData] = useState<IndexResearchResponse | null>(null)

  // API Mutation
  const { mutate, isPending: loading } = useAnalyzeIndexCoherenceApiV1IndexResearchAnalyzePost({
    mutation: {
      onSuccess: (responseData) => setData(responseData),
    },
  })

  // Run Analysis
  const runAnalysis = () => {
    mutate({
      data: {
        start_date: startDate,
        end_date: endDate,
        window: rollingWindow,
      },
    })
  }

  // --- Chart Options Generators ---

  const makeDataZoom = (sliderHeight = 20) => [
    { type: 'inside', xAxisIndex: 0, filterMode: 'filter' },
    {
      type: 'slider',
      xAxisIndex: 0,
      height: sliderHeight,
      bottom: 4,
      borderColor: 'rgba(99,102,241,0.4)',
      backgroundColor: 'rgba(99,102,241,0.08)',
      fillerColor: 'rgba(99,102,241,0.2)',
      handleStyle: { color: '#818cf8', borderColor: '#818cf8' },
      textStyle: { color: '#a1a1aa', fontSize: 10 },
      dataBackground: { lineStyle: { color: '#a5b4fc' }, areaStyle: { color: 'rgba(165,180,252,0.15)' } },
      selectedDataBackground: { lineStyle: { color: '#818cf8' }, areaStyle: { color: 'rgba(129,140,248,0.25)' } },
    }
  ]

  const getIciTrendOption = (response: IndexResearchResponse) => {
    const dates = response.market.dates
    const ici = response.market.ici
    const dispersion = response.market.dispersion_std
    const returns = response.market.index_returns
    const stockCount = response.market.stock_count

    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 50, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: [
        {
          type: 'value',
          name: 'ICI (一致性)',
          min: 0,
          max: 1,
          position: 'left',
          splitLine: { lineStyle: { color: '#27272a' } },
          axisLabel: { color: '#a1a1aa' }
        },
        {
          type: 'value',
          name: '离散度 (Std)',
          position: 'right',
          splitLine: { show: false },
          axisLabel: { color: '#a1a1aa' }
        },
        {
          type: 'value',
          name: '股票数量',
          show: false,
          min: 0
        }
      ],
      series: [
        {
          name: '指数收益率',
          type: 'bar',
          data: returns,
          yAxisIndex: 0, // Use left axis scale but it's small so it works as background
          itemStyle: { color: '#3b82f6', opacity: 0.15 },
          z: 1
        },
        {
          name: 'ICI',
          type: 'line',
          data: ici,
          yAxisIndex: 0,
          showSymbol: false,
          connectNulls: true,
          itemStyle: { color: '#8b5cf6' },
          lineStyle: { width: 2 },
          markLine: {
            symbol: ['none', 'none'],
            label: { position: 'start' },
            data: [
              { yAxis: 0.5, lineStyle: { type: 'dashed', color: '#a1a1aa' }, label: { formatter: '强一致性阈值' } },
              { yAxis: 0.25, lineStyle: { type: 'dashed', color: '#a1a1aa' }, label: { formatter: '弱一致性阈值' } }
            ]
          },
          z: 3
        },
        {
          name: '离散度',
          type: 'line',
          data: dispersion,
          yAxisIndex: 1,
          showSymbol: false,
          connectNulls: true,
          itemStyle: { color: '#f97316' },
          lineStyle: { width: 2 },
          z: 2
        },
        {
          name: '股票数量',
          type: 'line',
          data: stockCount,
          yAxisIndex: 2,
          showSymbol: false,
          lineStyle: { width: 1, type: 'dashed', opacity: 0.3 },
          itemStyle: { color: '#71717a' },
          z: 1
        }
      ]
    }
  }

  const getSciHeatmapOption = (response: IndexResearchResponse) => {
    const dates = response.sci.dates
    const industries = response.sci.industries.map(i => i.name)
    const matrix = response.sci.matrix // [industryIdx][dateIdx]

    // Convert to [dateIdx, industryIdx, value]
    const data: [number, number, number | null][] = []
    matrix.forEach((row, industryIdx) => {
      row.forEach((val, dateIdx) => {
        data.push([dateIdx, industryIdx, val]) // ECharts heatmap: x, y, value
      })
    })

    return {
      backgroundColor: 'transparent',
      tooltip: {
        position: 'top',
        formatter: (params: any) => {
          const date = dates[params.data[0]]
          const industry = industries[params.data[1]]
          const value = params.data[2]
          return `${date}<br/>${industry}: ${value?.toFixed(4)}`
        }
      },
      grid: { top: 10, right: 10, bottom: 80, left: 100 }, // Left margin for industry names
      xAxis: {
        type: 'category',
        data: dates,
        splitArea: { show: true },
        axisLabel: { interval: 19 }, // Show every ~20th label
        axisLine: { lineStyle: { color: '#52525b' } }
      },
      yAxis: {
        type: 'category',
        data: industries,
        splitArea: { show: true },
        axisLine: { lineStyle: { color: '#52525b' } },
        axisLabel: { color: '#a1a1aa', fontSize: 10 }
      },
      visualMap: {
        min: -0.2,
        max: 1.0,
        calculable: true,
        orient: 'horizontal',
        left: 'center',
        bottom: 0,
        inRange: {
          color: ['#3b82f6', '#fbbf24', '#ef4444'] // blue -> yellow -> red
        },
        textStyle: { color: '#a1a1aa' }
      },
      dataZoom: makeDataZoom(),
      series: [{
        name: 'SCI',
        type: 'heatmap',
        data: data,
        label: { show: false },
        itemStyle: {
          emphasis: { shadowBlur: 10, shadowColor: 'rgba(0, 0, 0, 0.5)' }
        }
      }]
    }
  }

  const getIndustryComparisonOption = (response: IndexResearchResponse) => {
    const industries = response.sci.industries
    const matrix = response.sci.matrix
    
    // Get latest non-null value for each industry
    const latestData = industries.map((ind, idx) => {
      const row = matrix[idx]
      // Find last non-null
      let val = null
      for (let i = row.length - 1; i >= 0; i--) {
        if (row[i] !== null) {
          val = row[i]
          break
        }
      }
      return { name: ind.name, value: val }
    }).filter(item => item.value !== null) as { name: string, value: number }[]

    // Sort descending
    latestData.sort((a, b) => b.value - a.value)

    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      grid: { top: 10, right: 30, bottom: 20, left: 100, containLabel: true },
      xAxis: {
        type: 'value',
        splitLine: { lineStyle: { color: '#27272a' } },
        axisLabel: { color: '#a1a1aa' }
      },
      yAxis: {
        type: 'category',
        data: latestData.map(d => d.name),
        axisLine: { lineStyle: { color: '#52525b' } },
        axisLabel: { color: '#a1a1aa' }
      },
      series: [{
        name: 'Latest SCI',
        type: 'bar',
        data: latestData.map(d => d.value),
        label: { show: true, position: 'right', color: '#a1a1aa', formatter: (p: any) => p.value.toFixed(3) },
        itemStyle: {
          color: (params: any) => {
            const val = params.value as number
            // Simple gradient logic based on value
            if (val > 0.5) return '#ef4444'
            if (val > 0.2) return '#fbbf24'
            return '#3b82f6'
          }
        }
      }]
    }
  }

  // --- Render Helpers ---

  const renderMetricCard = (title: string, value: number | null | undefined, subtext?: string) => (
    <Card className="p-2">
      <div className="text-[10px] text-muted-foreground truncate">{title}</div>
      <div className="text-lg font-bold mt-0.5">
        {value !== null && value !== undefined ? value.toFixed(4) : '-'}
      </div>
      {subtext && <p className="text-[10px] text-muted-foreground">{subtext}</p>}
    </Card>
  )

  const renderRegimeCard = (regime: string) => {
    let colorClass = 'text-muted-foreground'
    if (regime === '系统性主导') colorClass = 'text-green-500'
    else if (regime === '系统性风险') colorClass = 'text-red-500'
    else if (regime === '行业轮动') colorClass = 'text-amber-500'
    else if (regime === '个股分化') colorClass = 'text-blue-500'

    return (
      <Card className="p-2">
        <div className="text-[10px] text-muted-foreground truncate">市场状态</div>
        <div className={`text-lg font-bold mt-0.5 ${colorClass}`}>
          {regime || '未知'}
        </div>
        <p className="text-[10px] text-muted-foreground">当前市场特征</p>
      </Card>
    )
  }

  return (
    <div className="flex h-screen overflow-hidden -m-4">
      {/* Left Sidebar */}
      <div className="w-72 shrink-0 border-r bg-card/30 flex flex-col gap-3 p-3 overflow-y-auto">
        <div>
          <h2 className="text-base font-semibold mb-2">指数研究</h2>
          
          <div className="mb-3 px-2 py-1.5 bg-muted/50 rounded-md">
            <Label className="text-[10px] text-muted-foreground">基准指数</Label>
            <div className="font-mono text-sm font-medium">上证综指 (sh.000001)</div>
          </div>
        </div>

        <div className="space-y-2">
          <div className="grid grid-cols-2 gap-2">
            <div className="space-y-1">
              <Label className="text-xs">开始</Label>
              <Input 
                type="date" 
                value={startDate} 
                onChange={(e) => setStartDate(e.target.value)}
                className="h-8 text-xs"
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">结束</Label>
              <Input 
                type="date" 
                value={endDate} 
                onChange={(e) => setEndDate(e.target.value)}
                className="h-8 text-xs"
              />
            </div>
          </div>
          
          <div className="space-y-1.5">
            <div className="flex justify-between">
              <Label className="text-xs">滚动窗口</Label>
              <span className="text-[10px] font-mono">{rollingWindow} 天</span>
            </div>
            <Slider 
              value={[rollingWindow]} 
              min={10} 
              max={100} 
              step={5}
              onValueChange={(vals) => setRollingWindow(vals[0])} 
            />
          </div>
        </div>

        <Button 
          className="w-full mt-auto h-8 text-sm" 
          onClick={runAnalysis}
          disabled={loading}
        >
          {loading ? <Loader2 className="mr-2 h-3 w-3 animate-spin" /> : null}
          开始分析
        </Button>
      </div>

      {/* Main Workspace */}
      <div className="flex-1 overflow-hidden flex flex-col">
        {data ? (
          <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col min-h-0">
            <div className="border-b px-4 pt-2 shrink-0">
              <TabsList>
                <TabsTrigger value="ici">ICI趋势</TabsTrigger>
                <TabsTrigger value="heatmap">行业热力图</TabsTrigger>
                <TabsTrigger value="comparison">行业对比</TabsTrigger>
                <TabsTrigger value="summary">汇总</TabsTrigger>
              </TabsList>
            </div>

            <div className="flex-1 overflow-y-auto p-3 pb-8 min-h-0">
              {/* Summary Cards - Always Visible */}
              <div className="grid grid-cols-2 md:grid-cols-5 gap-2 mb-3">
                {renderMetricCard("ICI (指数一致性)", data.summary.latest_ici, "↑高=指数主导 ↓低=个股分化")}
                {renderMetricCard("离散度 (Std)", data.summary.latest_dispersion_std, "股票偏离指数的标准差")}
                {renderMetricCard("离散度 (MAD)", data.summary.latest_dispersion_mad, "股票偏离指数的平均绝对偏差")}
                {renderMetricCard("平均离散度", data.summary.avg_dispersion_std, "区间平均离散度")}
                {renderRegimeCard(data.summary.market_regime)}
              </div>

              <TabsContent value="ici" className="space-y-3 mt-0">
                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">ICI (Index Coherence Index) 趋势</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100vh-18rem)] px-2 pb-2">
                    <ReactECharts 
                      option={getIciTrendOption(data)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="heatmap" className="space-y-3 mt-0">
                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">行业一致性 (SCI) 热力图</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100vh-16rem)] px-2 pb-2">
                    <ReactECharts 
                      option={getSciHeatmapOption(data)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="comparison" className="space-y-3 mt-0">
                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">最新行业一致性对比</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100vh-16rem)] px-2 pb-2">
                    <ReactECharts 
                      option={getIndustryComparisonOption(data)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="summary" className="space-y-4 mt-0">
                <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wider border-b border-border pb-1">ICI 趋势</div>
                <Card>
                  <CardHeader className="py-2 px-3"><CardTitle className="text-sm">ICI (Index Coherence Index)</CardTitle></CardHeader>
                  <CardContent className="h-[280px] px-2 pb-2">
                    <ReactECharts option={getIciTrendOption(data)} style={{ height: '100%', width: '100%' }} theme="dark" />
                  </CardContent>
                </Card>

                <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wider border-b border-border pb-1">行业热力图</div>
                <Card>
                  <CardHeader className="py-2 px-3"><CardTitle className="text-sm">行业一致性 (SCI)</CardTitle></CardHeader>
                  <CardContent className="h-[400px] px-2 pb-2">
                    <ReactECharts option={getSciHeatmapOption(data)} style={{ height: '100%', width: '100%' }} theme="dark" />
                  </CardContent>
                </Card>

                <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wider border-b border-border pb-1">行业对比</div>
                <Card>
                  <CardHeader className="py-2 px-3"><CardTitle className="text-sm">最新行业一致性</CardTitle></CardHeader>
                  <CardContent className="h-[500px] px-2 pb-2">
                    <ReactECharts option={getIndustryComparisonOption(data)} style={{ height: '100%', width: '100%' }} theme="dark" />
                  </CardContent>
                </Card>
              </TabsContent>
            </div>
          </Tabs>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
            <AlertCircle className="h-8 w-8 mb-2 opacity-20" />
            <p className="text-sm">请设置参数并点击"开始分析"</p>
          </div>
        )}
      </div>
    </div>
  )
}
