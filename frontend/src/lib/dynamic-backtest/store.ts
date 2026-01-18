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
      // 计算当前可用现金（需要先计算到交易日期之前的状态）
      const prevTrades = state.trades.filter(t => t.date < params.date)
      const { finalCash } = calculateEquityCurve(
        state.initialCapital,
        state.stocks,
        prevTrades,
        state.startDate,
        params.date
      )
      
      trade = executeBuyTrade(baseTrade, finalCash)
    } else {
      // 计算当前持仓
      const prevTrades = state.trades.filter(t => t.date < params.date)
      const { finalPositions } = calculateEquityCurve(
        state.initialCapital,
        state.stocks,
        prevTrades,
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
