import { useState, useEffect, useRef } from 'react'
import ReactECharts from 'echarts-for-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Slider } from '@/components/ui/slider'
import { Search, Loader2, AlertCircle } from 'lucide-react'
import {
  useSearchStocksApiV1MarketResearchSearchGet,
  getStockContextApiV1MarketResearchContextStockCodeGet,
  useAnalyzeBundleApiV1MarketResearchAnalysisBundlePost,
} from '@/api/generated/market-research/market-research'
import type {
  StockSearchItem,
  StockContextResponse,
  AnalysisBundleItem,
  AnalysisBundleResponse,
  RegressionResult,
  ReturnDistributionAnalysis,
  RollingDistStats,
  RegimeSignal,
} from '@/api/generated/schemas'

// --- Page Component ---

export default function MarketResearchPage() {
  // State
  const [searchQuery, setSearchQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [selectedStock, setSelectedStock] = useState<{code: string, name: string} | null>(null)
  const [stockContext, setStockContext] = useState<StockContextResponse | null>(null)
  const [startDate, setStartDate] = useState('2025-02-01')
  const [endDate, setEndDate] = useState('2026-02-01')
  const [rollingWindow, setRollingWindow] = useState(60)
  const [activeTab, setActiveTab] = useState('overview')
  const [bundleData, setBundleData] = useState<AnalysisBundleResponse | null>(null)
  const [showSearchResults, setShowSearchResults] = useState(false)
  const searchContainerRef = useRef<HTMLDivElement>(null)

  // Popular stocks query (empty q)
  const { data: popularData } = useSearchStocksApiV1MarketResearchSearchGet(
    { q: '', limit: 16 },
  )
  const popularStocks = popularData?.items ?? []

  // Debounced search query
  useEffect(() => {
    if (searchQuery.length < 1) {
      setDebouncedQuery('')
      return
    }
    const timer = setTimeout(() => setDebouncedQuery(searchQuery), 300)
    return () => clearTimeout(timer)
  }, [searchQuery])

  // Search stocks query
  const { data: searchData, isFetching: searchLoading } = useSearchStocksApiV1MarketResearchSearchGet(
    { q: debouncedQuery, limit: 10 },
    { query: { enabled: debouncedQuery.length > 0 } },
  )
  const searchResults = searchData?.items ?? []

  // Show dropdown when search results arrive
  useEffect(() => {
    if (searchResults.length > 0 && debouncedQuery.length > 0) {
      setShowSearchResults(true)
    }
  }, [searchResults, debouncedQuery])

  // Click outside to close dropdown
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (searchContainerRef.current && !searchContainerRef.current.contains(e.target as Node)) {
        setShowSearchResults(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Analysis mutation
  const { mutate: runAnalysisMutation, isPending: loading } = useAnalyzeBundleApiV1MarketResearchAnalysisBundlePost({
    mutation: {
      onSuccess: (data) => setBundleData(data),
    },
  })

  // Get stock context
  const loadContext = async (code: string) => {
    try {
      const data = await getStockContextApiV1MarketResearchContextStockCodeGet(code)
      setStockContext(data)
    } catch (e) {
      console.error("Failed to load context", e)
    }
  }

  // Run analysis
  const runAnalysis = () => {
    if (!selectedStock) return
    runAnalysisMutation({
      data: {
        stock_codes: [selectedStock.code],
        start_date: startDate,
        end_date: endDate,
        rolling_window: rollingWindow,
        include_ohlc: true,
      },
    })
  }

  const handleSelectStock = (item: StockSearchItem) => {
    setSelectedStock({ code: item.code, name: item.name })
    setSearchQuery('')
    setDebouncedQuery('')
    setShowSearchResults(false)
    loadContext(item.code)
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

  const getOverviewChartOption = (item: AnalysisBundleItem, baseName: string) => {
    const dates = item.series_stock.dates
    
    // Normalize data: (price / first_price) * 100
    const normalize = (data: (number | null)[]) => {
      const firstValid = data.find(v => v !== null)
      if (!firstValid) return data
      return data.map(v => v === null ? null : (v / firstValid) * 100)
    }

    const series = [
      {
        name: baseName,
        type: 'line',
        data: normalize(item.series_base.close),
        showSymbol: false,
        lineStyle: { width: 2 },
        itemStyle: { color: '#3b82f6' } // blue
      },
      {
        name: item.stock_name,
        type: 'line',
        data: normalize(item.series_stock.close),
        showSymbol: false,
        lineStyle: { width: 2 },
        itemStyle: { color: '#22c55e' } // green
      }
    ]

    if (item.series_industry) {
      series.push({
        name: item.industry_l1_name || '行业指数',
        type: 'line',
        data: normalize(item.series_industry.close),
        showSymbol: false,
        lineStyle: { width: 2 },
        itemStyle: { color: '#f97316' } // orange
      })
    }

    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 20, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: { 
        type: 'value', 
        scale: true, 
        splitLine: { lineStyle: { color: '#27272a' } },
        axisLabel: { formatter: '{value}' }
      },
      series
    }
  }

  const getRollingCorrChartOption = (item: AnalysisBundleItem, baseName: string) => {
    const data = item.rolling_corr
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 20, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: data.dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: { 
        type: 'value', 
        min: -1, 
        max: 1,
        splitLine: { lineStyle: { color: '#27272a' } }
      },
      series: [
        {
          name: `${item.stock_name} vs ${baseName}`,
          type: 'line',
          data: data.stock_vs_base,
          showSymbol: false,
          itemStyle: { color: '#3b82f6' }
        },
        {
          name: `${item.stock_name} vs ${item.industry_l1_name || '行业'}`,
          type: 'line',
          data: data.stock_vs_industry,
          showSymbol: false,
          itemStyle: { color: '#f97316' }
        },
        {
          name: `${item.industry_l1_name || '行业'} vs ${baseName}`,
          type: 'line',
          data: data.industry_vs_base,
          showSymbol: false,
          lineStyle: { type: 'dashed' },
          itemStyle: { color: '#71717a' }
        }
      ]
    }
  }

  const getScatterOption = (xData: (number|null)[], yData: (number|null)[], xName: string, yName: string, regression: RegressionResult | null | undefined) => {
    // Filter out nulls
    const points: number[][] = []
    for(let i=0; i<xData.length; i++) {
      if (xData[i] !== null && yData[i] !== null) {
        points.push([xData[i] as number * 100, yData[i] as number * 100]) // Convert to percentage
      }
    }

    const series: any[] = [{
      type: 'scatter',
      data: points,
      symbolSize: 6,
      itemStyle: { color: '#3b82f6', opacity: 0.6 }
    }]

    if (regression) {
      // Add regression line
      const minX = Math.min(...points.map(p => p[0]))
      const maxX = Math.max(...points.map(p => p[0]))
      const lineData = [
        [minX, regression.alpha * 100 + regression.beta * minX],
        [maxX, regression.alpha * 100 + regression.beta * maxX]
      ]
      series.push({
        type: 'line',
        data: lineData,
        showSymbol: false,
        itemStyle: { color: '#f97316' },
        lineStyle: { width: 2 }
      })
    }

    return {
      backgroundColor: 'transparent',
      tooltip: { 
        trigger: 'item',
        formatter: (params: any) => {
          if (params.componentType === 'series' && params.seriesType === 'scatter') {
            return `${xName}: ${params.data[0].toFixed(2)}%<br/>${yName}: ${params.data[1].toFixed(2)}%`
          }
          return ''
        }
      },
      grid: { top: 30, right: 30, bottom: 30, left: 50, containLabel: true },
      xAxis: { 
        type: 'value', 
        name: xName, 
        nameLocation: 'middle', 
        nameGap: 25,
        splitLine: { lineStyle: { color: '#27272a' } },
        axisLabel: { formatter: '{value}%' }
      },
      yAxis: { 
        type: 'value', 
        name: yName,
        splitLine: { lineStyle: { color: '#27272a' } },
        axisLabel: { formatter: '{value}%' }
      },
      series
    }
  }

  const getResidualsChartOption = (item: AnalysisBundleItem) => {
    const data = item.residuals
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 20, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: data.dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: { 
        type: 'value', 
        scale: true,
        splitLine: { lineStyle: { color: '#27272a' } }
      },
      series: [
        {
          name: '超额收益 (vs 基准)',
          type: 'line',
          data: data.stock_on_base,
          showSymbol: false,
          itemStyle: { color: '#3b82f6' },
          areaStyle: { opacity: 0.1 }
        },
        {
          name: '超额收益 (vs 行业)',
          type: 'line',
          data: data.stock_on_industry,
          showSymbol: false,
          itemStyle: { color: '#f97316' },
          areaStyle: { opacity: 0.1 }
        }
      ]
    }
  }

  const getDailyReturnsChartOption = (item: AnalysisBundleItem, baseName: string) => {
    const dates = item.returns.dates
    const stockReturns = item.returns.stock.map(v => v !== null ? v * 100 : null)
    const baseReturns = item.returns.base.map(v => v !== null ? v * 100 : null)
    const industryReturns = item.returns.industry.map(v => v !== null ? v * 100 : null)

    return {
      backgroundColor: 'transparent',
      tooltip: { 
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        formatter: (params: any) => {
          let res = params[0].axisValue + '<br/>'
          params.forEach((param: any) => {
            const val = param.value !== null ? param.value.toFixed(2) + '%' : '-'
            res += `${param.marker} ${param.seriesName}: ${val}<br/>`
          })
          return res
        }
      },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 20, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: { 
        type: 'value', 
        splitLine: { lineStyle: { color: '#27272a' } },
        axisLabel: { formatter: '{value}%' }
      },
      series: [
        {
          name: baseName,
          type: 'bar',
          data: baseReturns,
          itemStyle: { color: '#3b82f6', opacity: 0.3 },
          barGap: '-100%',
          z: 1
        },
        {
          name: item.industry_l1_name || '行业',
          type: 'bar',
          data: industryReturns,
          itemStyle: { color: '#f97316', opacity: 0.3 },
          barGap: '-100%',
          z: 2
        },
        {
          name: item.stock_name,
          type: 'bar',
          data: stockReturns,
          itemStyle: {
            color: (params: any) => {
              return params.value >= 0 ? '#22c55e' : '#ef4444'
            }
          },
          z: 3
        }
      ]
    }
  }

  const getRollingDistChartOption = (stats: RollingDistStats, dates: string[]) => {
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 40, bottom: 80, left: 40, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: [
        {
          type: 'value',
          name: '偏度 (Skew)',
          splitLine: { lineStyle: { color: '#27272a' } },
          axisLabel: { color: '#a1a1aa' }
        },
        {
          type: 'value',
          name: '峰度 (Kurtosis)',
          splitLine: { show: false },
          axisLabel: { color: '#a1a1aa' }
        }
      ],
      series: [
        {
          name: '偏度',
          type: 'line',
          data: stats.skew,
          showSymbol: false,
          itemStyle: { color: '#8b5cf6' },
          yAxisIndex: 0
        },
        {
          name: '峰度',
          type: 'line',
          data: stats.kurtosis,
          showSymbol: false,
          itemStyle: { color: '#ec4899' },
          yAxisIndex: 1
        }
      ]
    }
  }

  const getRollingVolChartOption = (dist: ReturnDistributionAnalysis, dates: string[], baseName: string) => {
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#a1a1aa' }, bottom: 28 },
      grid: { top: 30, right: 20, bottom: 80, left: 50, containLabel: true },
      dataZoom: makeDataZoom(),
      xAxis: { type: 'category', data: dates, axisLine: { lineStyle: { color: '#52525b' } } },
      yAxis: { 
        type: 'value', 
        name: '波动率 (Std)',
        splitLine: { lineStyle: { color: '#27272a' } }
      },
      series: [
        {
          name: baseName,
          type: 'line',
          data: dist.base.std,
          showSymbol: false,
          itemStyle: { color: '#3b82f6' }
        },
        {
          name: '行业',
          type: 'line',
          data: dist.industry?.std || [],
          showSymbol: false,
          itemStyle: { color: '#f97316' }
        },
        {
          name: '个股',
          type: 'line',
          data: dist.stock.std,
          showSymbol: false,
          itemStyle: { color: '#22c55e' }
        }
      ]
    }
  }

  const getRegimeChartOption = (regime: RegimeSignal) => {
    const markAreaData: any[] = []
    if (regime.signal.length > 0) {
      let currentSignal = regime.signal[0]
      let startIndex = 0
      
      const colorMap: Record<string, string> = {
        'buy': 'rgba(34, 197, 94, 0.25)',
        'sell': 'rgba(239, 68, 68, 0.25)',
        'neutral': 'rgba(113, 113, 122, 0.2)',
        'caution': 'rgba(245, 158, 11, 0.25)'
      }

      for (let i = 1; i < regime.signal.length; i++) {
        if (regime.signal[i] !== currentSignal) {
          markAreaData.push([
            { xAxis: regime.dates[startIndex], itemStyle: { color: colorMap[currentSignal] || colorMap['neutral'] } },
            { xAxis: regime.dates[i-1] }
          ])
          currentSignal = regime.signal[i]
          startIndex = i
        }
      }
      markAreaData.push([
        { xAxis: regime.dates[startIndex], itemStyle: { color: colorMap[currentSignal] || colorMap['neutral'] } },
        { xAxis: regime.dates[regime.dates.length - 1] }
      ])
    }

    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      grid: { top: 10, right: 20, bottom: 40, left: 50, containLabel: true },
      dataZoom: makeDataZoom(14),
      xAxis: { type: 'category', data: regime.dates, show: false },
      yAxis: { 
        type: 'value', 
        splitLine: { show: false },
        min: -1,
        max: 1
      },
      series: [
        {
          name: 'Regime Score',
          type: 'line',
          data: regime.score,
          showSymbol: false,
          connectNulls: true,
          itemStyle: { color: '#f59e0b' },
          lineStyle: { width: 2, color: '#f59e0b' },
          markArea: {
            data: markAreaData
          }
        }
      ]
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

  const currentItem = bundleData?.items[0]

  return (
    <div className="flex h-screen overflow-hidden -m-4">
      {/* Left Sidebar */}
      <div className="w-72 shrink-0 border-r bg-card/30 flex flex-col gap-3 p-3 overflow-y-auto">
        <div>
          <h2 className="text-base font-semibold mb-2">市场研究</h2>
          
          <div className="mb-3 px-2 py-1.5 bg-muted/50 rounded-md">
            <Label className="text-[10px] text-muted-foreground">基准指数</Label>
            <div className="font-mono text-sm font-medium">上证综指 (sh.000001)</div>
          </div>

          {/* Stock Search */}
          <div className="space-y-1.5 relative" ref={searchContainerRef}>
            <Label className="text-xs">股票搜索</Label>
            <div className="relative">
              <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input 
                placeholder="输入代码或名称..." 
                className="pl-8"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onFocus={() => setShowSearchResults(true)}
              />
              {searchLoading && (
                <Loader2 className="absolute right-2 top-2.5 h-4 w-4 animate-spin text-muted-foreground" />
              )}
            </div>
            
            {/* Search Results Dropdown */}
            {showSearchResults && (searchResults.length > 0 || (searchQuery === '' && popularStocks.length > 0)) && (
              <div className="absolute z-50 w-full mt-1 bg-popover border rounded-md shadow-md max-h-60 overflow-y-auto">
                {searchQuery === '' && searchResults.length === 0 && (
                  <div className="px-3 py-1.5 text-xs text-muted-foreground border-b">热门股票</div>
                )}
                {(searchResults.length > 0 ? searchResults : popularStocks).map((item) => (
                  <div 
                    key={item.code}
                    className="px-3 py-1.5 hover:bg-accent cursor-pointer text-sm flex justify-between items-center"
                    onClick={() => handleSelectStock(item)}
                  >
                    <span>{item.name}</span>
                    <span className="text-muted-foreground font-mono text-xs">{item.code}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Selected Stock Info */}
          {selectedStock && (
            <div className="mt-2 px-2 py-1.5 border rounded-md bg-accent/20">
              <div className="font-bold text-sm">{selectedStock.name}</div>
              <div className="text-xs font-mono text-muted-foreground">{selectedStock.code}</div>
              {stockContext && (
                <div className="mt-1 text-[10px] space-y-0.5 text-muted-foreground">
                  <div>行业: {stockContext.sw_industry_l1 || '-'}</div>
                  <div>指数: {stockContext.industry_index.name || '-'}</div>
                </div>
              )}
            </div>
          )}
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
              max={250} 
              step={5}
              onValueChange={(vals) => setRollingWindow(vals[0])} 
            />
          </div>
        </div>

        <Button 
          className="w-full mt-auto h-8 text-sm" 
          onClick={runAnalysis}
          disabled={!selectedStock || loading}
        >
          {loading ? <Loader2 className="mr-2 h-3 w-3 animate-spin" /> : null}
          开始分析
        </Button>
      </div>

      {/* Main Workspace */}
      <div className="flex-1 overflow-hidden flex flex-col">
        {bundleData && currentItem ? (
          <Tabs value={activeTab} onValueChange={setActiveTab} className="flex-1 flex flex-col min-h-0">
            <div className="border-b px-4 pt-2 shrink-0">
              <TabsList>
                <TabsTrigger value="overview">概览</TabsTrigger>
                <TabsTrigger value="rolling">滚动相关性</TabsTrigger>
                <TabsTrigger value="regression">Beta / Alpha</TabsTrigger>
                <TabsTrigger value="residuals">残差分析</TabsTrigger>
                <TabsTrigger value="returns">涨跌幅分析</TabsTrigger>
              </TabsList>
            </div>

            <div className="flex-1 overflow-y-auto p-3 pb-8 min-h-0">
              <TabsContent value="overview" className="space-y-3 mt-0">
                <div className="grid grid-cols-3 md:grid-cols-5 gap-2">
                  {renderMetricCard("相关性 (vs 基准)", currentItem.pearson.stock_vs_base)}
                  {renderMetricCard("相关性 (vs 行业)", currentItem.pearson.stock_vs_industry)}
                  {renderMetricCard("行业 vs 基准", currentItem.pearson.industry_vs_base)}
                  {renderMetricCard("Beta (vs 基准)", currentItem.regression.stock_on_base?.beta, `R²: ${currentItem.regression.stock_on_base?.r_squared.toFixed(4)}`)}
                  {renderMetricCard("Beta (vs 行业)", currentItem.regression.stock_on_industry?.beta, `R²: ${currentItem.regression.stock_on_industry?.r_squared.toFixed(4)}`)}
                </div>

                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">累计收益率对比 (归一化)</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[340px] px-2 pb-2">
                    <ReactECharts 
                      option={getOverviewChartOption(currentItem, bundleData.base_index_name)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="rolling" className="space-y-3 mt-0">
                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">{rollingWindow}日滚动相关系数</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100vh-12rem)] px-2 pb-2">
                    <ReactECharts 
                      option={getRollingCorrChartOption(currentItem, bundleData.base_index_name)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="regression" className="space-y-3 mt-0">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  <Card>
                    <CardHeader className="py-2 px-3">
                      <CardTitle className="text-sm">回归: 个股 vs 基准</CardTitle>
                    </CardHeader>
                    <CardContent className="h-[320px] px-2 pb-2">
                      <ReactECharts 
                        option={getScatterOption(
                          currentItem.returns.base, 
                          currentItem.returns.stock, 
                          bundleData.base_index_name, 
                          currentItem.stock_name,
                          currentItem.regression.stock_on_base
                        )} 
                        style={{ height: '100%', width: '100%' }}
                        theme="dark"
                      />
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="py-2 px-3">
                      <CardTitle className="text-sm">回归: 个股 vs 行业</CardTitle>
                    </CardHeader>
                    <CardContent className="h-[320px] px-2 pb-2">
                      <ReactECharts 
                        option={getScatterOption(
                          currentItem.returns.industry, 
                          currentItem.returns.stock, 
                          currentItem.industry_l1_name || '行业', 
                          currentItem.stock_name,
                          currentItem.regression.stock_on_industry
                        )} 
                        style={{ height: '100%', width: '100%' }}
                        theme="dark"
                      />
                    </CardContent>
                  </Card>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                  {renderMetricCard("Alpha (年化)", currentItem.regression.stock_on_base ? currentItem.regression.stock_on_base.alpha * 252 : null)}
                  {renderMetricCard("Beta", currentItem.regression.stock_on_base?.beta)}
                  {renderMetricCard("R²", currentItem.regression.stock_on_base?.r_squared)}
                  {renderMetricCard("残差波动率", currentItem.residual_vol.stock_on_base?.annualized)}
                </div>
              </TabsContent>

              <TabsContent value="residuals" className="space-y-3 mt-0">
                <Card>
                  <CardHeader className="py-2 px-3">
                    <CardTitle className="text-sm">累计超额收益 (残差)</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[calc(100vh-16rem)] px-2 pb-2">
                    <ReactECharts 
                      option={getResidualsChartOption(currentItem)} 
                      style={{ height: '100%', width: '100%' }}
                      theme="dark"
                    />
                  </CardContent>
                </Card>

                <div className="grid grid-cols-2 gap-2">
                  {renderMetricCard("残差波动率 (vs 基准)", currentItem.residual_vol.stock_on_base?.annualized, "年化")}
                  {renderMetricCard("残差波动率 (vs 行业)", currentItem.residual_vol.stock_on_industry?.annualized, "年化")}
                </div>
              </TabsContent>

              <TabsContent value="returns" className="space-y-3 mt-0">
                {currentItem.return_distribution ? (
                  <>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
                      {renderMetricCard("偏度 (Skew)", currentItem.return_distribution.stock.skew.slice(-1)[0], "↗右偏=上涨极值多")}
                      {renderMetricCard("峰度 (Kurtosis)", currentItem.return_distribution.stock.kurtosis.slice(-1)[0], "↑高=极端行情风险")}
                      {renderMetricCard("波动率 (Std)", currentItem.return_distribution.stock.std.slice(-1)[0], "↑高=恐慌 ↓低=冷静")}
                      {renderMetricCard("正收益占比", currentItem.return_distribution.stock.positive_pct.slice(-1)[0], "市场广度(涨家比例)")}
                      <Card className="p-2">
                        <div className="text-[10px] text-muted-foreground truncate">当前状态</div>
                        <div className={`text-lg font-bold mt-0.5 ${
                          currentItem.return_distribution.regime.signal.slice(-1)[0] === 'buy' ? 'text-green-500' :
                          currentItem.return_distribution.regime.signal.slice(-1)[0] === 'sell' ? 'text-red-500' :
                          currentItem.return_distribution.regime.signal.slice(-1)[0] === 'caution' ? 'text-amber-500' :
                          'text-muted-foreground'
                        }`}>
                          {({'buy': '买入', 'sell': '卖出', 'caution': '谨慎', 'neutral': '观望'} as Record<string, string>)[currentItem.return_distribution.regime.signal.slice(-1)[0]] || currentItem.return_distribution.regime.signal.slice(-1)[0]}
                        </div>
                        <p className="text-[10px] text-muted-foreground">右偏低波→买 左偏高波→卖</p>
                      </Card>
                    </div>

                    <Card>
                      <CardHeader className="py-2 px-3">
                        <CardTitle className="text-sm">日收益率分布</CardTitle>
                      </CardHeader>
                      <CardContent className="h-[200px] px-2 pb-2">
                        <ReactECharts 
                          option={getDailyReturnsChartOption(currentItem, bundleData.base_index_name)} 
                          style={{ height: '100%', width: '100%' }}
                          theme="dark"
                        />
                      </CardContent>
                    </Card>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                      <Card>
                        <CardHeader className="py-2 px-3">
                          <CardTitle className="text-sm">滚动偏度 & 峰度</CardTitle>
                        </CardHeader>
                        <CardContent className="h-[250px] px-2 pb-2">
                          <ReactECharts 
                            option={getRollingDistChartOption(currentItem.return_distribution.stock, currentItem.return_distribution.stock.dates)}
                            style={{ height: '100%', width: '100%' }}
                            theme="dark"
                          />
                        </CardContent>
                      </Card>

                      <Card>
                        <CardHeader className="py-2 px-3">
                          <CardTitle className="text-sm">滚动波动率 (Std)</CardTitle>
                        </CardHeader>
                        <CardContent className="h-[250px] px-2 pb-2">
                          <ReactECharts 
                            option={getRollingVolChartOption(currentItem.return_distribution, currentItem.return_distribution.stock.dates, bundleData.base_index_name)}
                            style={{ height: '100%', width: '100%' }}
                            theme="dark"
                          />
                        </CardContent>
                      </Card>
                    </div>

                    <Card>
                      <CardHeader className="py-2 px-3">
                        <CardTitle className="text-sm">市场状态信号 (Regime)</CardTitle>
                      </CardHeader>
                      <CardContent className="h-[140px] px-2 pb-2">
                        <ReactECharts 
                          option={getRegimeChartOption(currentItem.return_distribution.regime)}
                          style={{ height: '100%', width: '100%' }}
                          theme="dark"
                        />
                      </CardContent>
                    </Card>
                  </>
                ) : (
                  <div className="flex items-center justify-center h-40 text-muted-foreground">
                    暂无分布分析数据
                  </div>
                )}
              </TabsContent>
            </div>
          </Tabs>
        ) : (
          <div className="flex-1 flex flex-col items-center justify-center text-muted-foreground">
            <AlertCircle className="h-8 w-8 mb-2 opacity-20" />
            <p className="text-sm">请在左侧选择股票并点击"开始分析"</p>
          </div>
        )}
      </div>
    </div>
  )
}
