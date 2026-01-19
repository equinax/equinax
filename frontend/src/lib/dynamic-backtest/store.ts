/**
 * 动态回测状态管理 (Zustand Store)
 */

import { create } from 'zustand'
import { nanoid } from 'nanoid'
import {
  DynamicBacktestState,
  StockData,
  Trade,
  TradeType,
  TradeMode,
  BenchmarkData,
  TempConfig,
  BENCHMARK_OPTIONS,
} from './types'
import {
  executeBuyTrade,
  executeSellTrade,
  calculateEquityCurve,
  calculateBenchmarkReturns,
  calculateMetrics,
} from './engine'

interface DynamicBacktestActions {
  // 配置
  setInitialCapital: (value: number) => void
  setDateRange: (start: string, end: string) => void
  setBenchmark: (code: string) => void
  
  // 配置锁定
  startEditConfig: () => void
  updateTempConfig: (config: Partial<TempConfig>) => void
  confirmEditConfig: () => void
  cancelEditConfig: () => void
  
  // 股票管理
  addStock: (stock: StockData) => void
  removeStock: (code: string) => void
  selectStock: (code: string | null) => void
  
  // 基准数据
  setBenchmarkData: (data: BenchmarkData) => void
  
  // 交易操作
  addTrade: (params: {
    stockCode: string
    type: TradeType
    date: string
    price: number
    mode: TradeMode
    inputValue: number
  }) => boolean
  removeTrade: (tradeId: string) => void
  
  // 计算
  recalculate: () => void
  
  // 重置
  reset: () => void
}

type DynamicBacktestStore = DynamicBacktestState & DynamicBacktestActions

// 默认日期范围：过去一年
const getDefaultDates = () => {
  const end = new Date()
  const start = new Date()
  start.setFullYear(start.getFullYear() - 1)
  
  return {
    startDate: start.toISOString().split('T')[0],
    endDate: end.toISOString().split('T')[0],
  }
}

const initialState: DynamicBacktestState = {
  initialCapital: 100000,
  ...getDefaultDates(),
  benchmarkCode: BENCHMARK_OPTIONS[0].code,
  
  stocks: new Map(),
  benchmark: null,
  trades: [],
  
  positions: new Map(),
  equityCurve: [],
  metrics: null,
  currentCash: 100000,
  
  selectedStockCode: null,
  isCalculating: false,
  
  isConfigLocked: true,
  tempConfig: null,
}

export const useDynamicBacktestStore = create<DynamicBacktestStore>((set, get) => ({
  ...initialState,
  
  setInitialCapital: (value) => {
    set({ initialCapital: value, currentCash: value })
    get().recalculate()
  },
  
  setDateRange: (start, end) => {
    set({ startDate: start, endDate: end })
    get().recalculate()
  },
  
  setBenchmark: (code) => {
    set({ benchmarkCode: code })
    get().recalculate()
  },
  
  // 配置锁定相关
  startEditConfig: () => {
    const state = get()
    set({
      isConfigLocked: false,
      tempConfig: {
        initialCapital: state.initialCapital,
        startDate: state.startDate,
        endDate: state.endDate,
      },
    })
  },
  
  updateTempConfig: (config) => {
    const current = get().tempConfig
    if (current) {
      set({ tempConfig: { ...current, ...config } })
    }
  },
  
  confirmEditConfig: () => {
    const temp = get().tempConfig
    if (!temp) return
    
    // 应用新配置，清空交易记录和持仓
    set({
      initialCapital: temp.initialCapital,
      startDate: temp.startDate,
      endDate: temp.endDate,
      currentCash: temp.initialCapital,
      trades: [],
      positions: new Map(),
      equityCurve: [],
      metrics: null,
      isConfigLocked: true,
      tempConfig: null,
      // 清空股票数据，触发重新加载
      stocks: new Map(),
      selectedStockCode: null,
      benchmark: null,
    })
  },
  
  cancelEditConfig: () => {
    set({
      isConfigLocked: true,
      tempConfig: null,
    })
  },
  
  addStock: (stock) => {
    const stocks = new Map(get().stocks)
    stocks.set(stock.code, stock)
    set({ stocks })
    
    // 如果是第一只股票，自动选中
    if (stocks.size === 1) {
      set({ selectedStockCode: stock.code })
    }
  },
  
  removeStock: (code) => {
    const stocks = new Map(get().stocks)
    stocks.delete(code)
    
    // 同时删除该股票的所有交易
    const trades = get().trades.filter(t => t.stockCode !== code)
    
    // 如果删除的是当前选中的股票，选择另一只
    let selectedStockCode = get().selectedStockCode
    if (selectedStockCode === code) {
      const firstKey = stocks.keys().next()
      selectedStockCode = stocks.size > 0 && !firstKey.done ? firstKey.value : null
    }
    
    set({ stocks, trades, selectedStockCode })
    get().recalculate()
  },
  
  selectStock: (code) => {
    set({ selectedStockCode: code })
  },
  
  setBenchmarkData: (data) => {
    set({ benchmark: data })
    get().recalculate()
  },
  
  addTrade: (params) => {
    const state = get()
    const stock = state.stocks.get(params.stockCode)
    
    if (!stock) return false
    
    // 构建交易记录
    const baseTrade = {
      id: nanoid(),
      stockCode: params.stockCode,
      type: params.type,
      date: params.date,
      price: params.price,
      mode: params.mode,
      inputValue: params.inputValue,
    }
    
    let trade: Trade
    
    if (params.type === 'BUY') {
      // 计算当天开盘前的现金（不包含当天交易）
      const prevDayTrades = state.trades.filter(t => t.date < params.date)
      const { finalCash: cashAtDayOpen } = calculateEquityCurve(
        state.initialCapital,
        state.stocks,
        prevDayTrades,
        state.startDate,
        params.date
      )
      
      // 计算当天已执行的买入总额
      const todayBuys = state.trades.filter(t => t.date === params.date && t.type === 'BUY')
      const todayBuyAmount = todayBuys.reduce((sum, t) => sum + t.totalCost, 0)
      
      // 计算当天已执行的卖出收入
      const todaySells = state.trades.filter(t => t.date === params.date && t.type === 'SELL')
      const todaySellIncome = todaySells.reduce((sum, t) => sum + (t.executedAmount - t.totalCost), 0)
      
      // 实际可用现金 = 当天开盘现金 - 当天已买入 + 当天已卖出收入
      const availableCash = cashAtDayOpen - todayBuyAmount + todaySellIncome
      
      trade = executeBuyTrade(baseTrade, availableCash)
    } else {
      // 卖出: 需要计算到当天的持仓状态（包含当天所有已执行的交易）
      const allTradesUpToToday = state.trades.filter(t => t.date <= params.date)
      const { finalPositions } = calculateEquityCurve(
        state.initialCapital,
        state.stocks,
        allTradesUpToToday,
        state.startDate,
        params.date
      )
      
      const position = finalPositions.get(params.stockCode)
      if (!position || position.totalShares <= 0) {
        return false // 无持仓可卖
      }
      
      trade = executeSellTrade(baseTrade, position)
    }
    
    if (trade.executedShares === 0) {
      return false // 交易无法执行
    }
    
    // 按日期排序插入
    const trades = [...state.trades, trade].sort((a, b) => a.date.localeCompare(b.date))
    set({ trades })
    
    get().recalculate()
    return true
  },
  
  removeTrade: (tradeId) => {
    const trades = get().trades.filter(t => t.id !== tradeId)
    set({ trades })
    get().recalculate()
  },
  
  recalculate: () => {
    const state = get()
    
    if (state.stocks.size === 0) {
      set({
        positions: new Map(),
        equityCurve: [],
        metrics: null,
        currentCash: state.initialCapital,
      })
      return
    }
    
    set({ isCalculating: true })
    
    try {
      // 计算权益曲线
      const { equityCurve, finalPositions, finalCash } = calculateEquityCurve(
        state.initialCapital,
        state.stocks,
        state.trades,
        state.startDate,
        state.endDate
      )
      
      // 计算基准收益
      const benchmarkReturns = state.benchmark
        ? calculateBenchmarkReturns(state.benchmark, state.startDate, state.endDate)
        : []
      
      // 计算统计指标
      const metrics = calculateMetrics(
        equityCurve,
        state.trades,
        benchmarkReturns,
        state.initialCapital
      )
      
      set({
        equityCurve,
        positions: finalPositions,
        currentCash: finalCash,
        metrics,
        isCalculating: false,
      })
    } catch (error) {
      console.error('Recalculate error:', error)
      set({ isCalculating: false })
    }
  },
  
  reset: () => {
    set({
      ...initialState,
      ...getDefaultDates(),
    })
  },
}))
