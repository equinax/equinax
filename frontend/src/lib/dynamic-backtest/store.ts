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
  AddTradeResult,
  BENCHMARK_OPTIONS,
} from './types'
import {
  executeBuyTrade,
  executeSellTrade,
  calculateEquityCurve,
  calculateStateAtDate,
  calculateBenchmarkReturns,
  calculateMetrics,
  generateOptimalTrades as generateOptimalTradesEngine,
  generateWorstTrades as generateWorstTradesEngine,
} from './engine'
import { Position } from './types'

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
  reorderStocks: (fromIndex: number, toIndex: number) => void
  
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
  }) => AddTradeResult
  removeTrade: (tradeId: string) => void
  
  // 配对交易（买入+卖出同时提交）
  addTradePair: (params: {
    stockCode: string
    buyDate: string
    buyPrice: number
    sellDate: string
    sellPrice: number
    shares: number
  }) => AddTradeResult
  
  // 查询指定日期的状态
  getStateAtDate: (targetDate: string) => { availableCash: number; positions: Map<string, Position> }
  
  // 生成最优交易
  generateOptimalTrades: () => AddTradeResult
  
  // 生成最差交易（最大亏损）
  generateWorstTrades: () => AddTradeResult
  
  // 交易高亮
  setHighlightedTrade: (tradeId: string | null) => void
  
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
  stockOrder: [],
  benchmark: null,
  trades: [],
  
  positions: new Map(),
  equityCurve: [],
  metrics: null,
  currentCash: 100000,
  
  selectedStockCode: null,
  isCalculating: false,
  highlightedTradeId: null,
  
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
      stockOrder: [],
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
    const stockOrder = [...get().stockOrder]
    
    // 只有新股票才添加到顺序列表
    if (!stocks.has(stock.code)) {
      stockOrder.push(stock.code)
    }
    stocks.set(stock.code, stock)
    set({ stocks, stockOrder })
    
    // 如果是第一只股票，自动选中
    if (stocks.size === 1) {
      set({ selectedStockCode: stock.code })
    }
  },
  
  removeStock: (code) => {
    const stocks = new Map(get().stocks)
    stocks.delete(code)
    
    // 从顺序列表中移除
    const stockOrder = get().stockOrder.filter(c => c !== code)
    
    // 同时删除该股票的所有交易
    const trades = get().trades.filter(t => t.stockCode !== code)
    
    // 如果删除的是当前选中的股票，选择另一只
    let selectedStockCode = get().selectedStockCode
    if (selectedStockCode === code) {
      const firstKey = stocks.keys().next()
      selectedStockCode = stocks.size > 0 && !firstKey.done ? firstKey.value : null
    }
    
    set({ stocks, stockOrder, trades, selectedStockCode })
    get().recalculate()
  },
  
  selectStock: (code) => {
    set({ selectedStockCode: code })
  },
  
  reorderStocks: (fromIndex, toIndex) => {
    const stockOrder = [...get().stockOrder]
    const [removed] = stockOrder.splice(fromIndex, 1)
    stockOrder.splice(toIndex, 0, removed)
    set({ stockOrder })
  },
  
  setBenchmarkData: (data) => {
    set({ benchmark: data })
    get().recalculate()
  },
  
  addTrade: (params) => {
    const state = get()
    const stock = state.stocks.get(params.stockCode)
    
    if (!stock) return { success: false, error: '请先选择股票' }
    
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
      
      if (availableCash <= 0) {
        return { success: false, error: '可用资金不足' }
      }
      
      trade = executeBuyTrade(baseTrade, availableCash)
      
      if (trade.executedShares === 0) {
        return { success: false, error: '资金不足，无法买入一手' }
      }
      
      // 检查是否在中间插入交易，以及是否会导致后续交易无效
      const laterTrades = state.trades.filter(t => t.date > params.date)
      if (laterTrades.length > 0) {
        // 计算插入新交易后，后续日期的资金状态
        const newTrades = [...state.trades, trade].sort((a, b) => a.date.localeCompare(b.date))
        
        // 模拟执行所有交易，检查是否会出现资金不足
        let simulatedCash = state.initialCapital
        for (const t of newTrades) {
          if (t.type === 'BUY') {
            simulatedCash -= t.totalCost
          } else {
            simulatedCash += t.executedAmount - t.totalCost
          }
          
          if (simulatedCash < 0) {
            return { 
              success: false, 
              error: `在此日期插入交易会导致 ${t.date} 的交易资金不足，请先删除后续交易` 
            }
          }
        }
      }
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
        return { success: false, error: '无持仓可卖' }
      }
      
      trade = executeSellTrade(baseTrade, position)
      
      if (trade.executedShares === 0) {
        return { success: false, error: '卖出数量无效' }
      }
    }
    
    // 按日期排序插入
    const trades = [...state.trades, trade].sort((a, b) => a.date.localeCompare(b.date))
    set({ trades })
    
    get().recalculate()
    return { success: true }
  },
  
  removeTrade: (tradeId) => {
    const state = get()
    const tradeToRemove = state.trades.find(t => t.id === tradeId)
    if (!tradeToRemove) return
    
    // 如果交易有配对ID，同时删除配对交易
    let trades: Trade[]
    if (tradeToRemove.pairId) {
      trades = state.trades.filter(t => t.id !== tradeId && t.pairId !== tradeToRemove.pairId)
    } else {
      trades = state.trades.filter(t => t.id !== tradeId)
    }
    
    // 完整验证剩余交易的有效性（资金 + 持仓）
    // 需要循环验证，因为删除一笔交易可能导致后续多笔交易无效
    let hasInvalidTrades = true
    while (hasInvalidTrades) {
      hasInvalidTrades = false
      const sortedTrades = [...trades].sort((a, b) => a.date.localeCompare(b.date))
      const positions = new Map<string, number>()  // stockCode -> shares
      const invalidTradeIds = new Set<string>()
      let cash = state.initialCapital
      
      for (const trade of sortedTrades) {
        const currentShares = positions.get(trade.stockCode) || 0
        
        if (trade.type === 'BUY') {
          // 买入：检查资金是否足够
          if (cash < trade.totalCost) {
            // 资金不足，标记为无效
            invalidTradeIds.add(trade.id)
            // 如果有配对ID，也标记配对交易
            if (trade.pairId) {
              trades.filter(t => t.pairId === trade.pairId).forEach(t => invalidTradeIds.add(t.id))
            }
          } else {
            cash -= trade.totalCost
            positions.set(trade.stockCode, currentShares + trade.executedShares)
          }
        } else {
          // 卖出：检查持仓是否足够
          if (currentShares < trade.executedShares) {
            // 持仓不足，标记为无效
            invalidTradeIds.add(trade.id)
            // 如果有配对ID，也标记配对交易
            if (trade.pairId) {
              trades.filter(t => t.pairId === trade.pairId).forEach(t => invalidTradeIds.add(t.id))
            }
          } else {
            cash += trade.executedAmount - trade.totalCost
            positions.set(trade.stockCode, currentShares - trade.executedShares)
          }
        }
      }
      
      // 过滤掉无效交易
      if (invalidTradeIds.size > 0) {
        trades = trades.filter(t => !invalidTradeIds.has(t.id))
        hasInvalidTrades = true  // 需要再次验证
      }
    }
    
    set({ trades })
    get().recalculate()
  },
  
  addTradePair: (params) => {
    const state = get()
    const stock = state.stocks.get(params.stockCode)
    
    if (!stock) return { success: false, error: '请先选择股票' }
    
    if (params.sellDate <= params.buyDate) {
      return { success: false, error: '卖出日期必须晚于买入日期' }
    }
    
    // 计算买入日期的可用现金
    const prevDayTrades = state.trades.filter(t => t.date < params.buyDate)
    const { finalCash: cashAtBuyDayOpen } = calculateEquityCurve(
      state.initialCapital,
      state.stocks,
      prevDayTrades,
      state.startDate,
      params.buyDate
    )
    
    // 计算买入当天已执行的交易
    const todayBuys = state.trades.filter(t => t.date === params.buyDate && t.type === 'BUY')
    const todayBuyAmount = todayBuys.reduce((sum, t) => sum + t.totalCost, 0)
    const todaySells = state.trades.filter(t => t.date === params.buyDate && t.type === 'SELL')
    const todaySellIncome = todaySells.reduce((sum, t) => sum + (t.executedAmount - t.totalCost), 0)
    
    const availableCash = cashAtBuyDayOpen - todayBuyAmount + todaySellIncome
    
    if (availableCash <= 0) {
      return { success: false, error: '买入日期可用资金不足' }
    }
    
    // 生成配对ID
    const pairId = nanoid()
    
    // 创建买入交易
    const buyTrade = executeBuyTrade({
      id: nanoid(),
      stockCode: params.stockCode,
      type: 'BUY',
      date: params.buyDate,
      price: params.buyPrice,
      mode: 'shares',
      inputValue: params.shares,
      pairId,  // 设置配对ID
    }, availableCash)
    
    if (buyTrade.executedShares === 0) {
      return { success: false, error: '资金不足，无法买入' }
    }
    
    // 创建卖出交易（卖出买入的全部股数）
    const sellTrade = executeSellTrade({
      id: nanoid(),
      stockCode: params.stockCode,
      type: 'SELL',
      date: params.sellDate,
      price: params.sellPrice,
      mode: 'shares',
      inputValue: buyTrade.executedShares,
      pairId,  // 设置相同的配对ID
    }, {
      stockCode: params.stockCode,
      stockName: stock.name,
      lots: [],
      totalShares: buyTrade.executedShares,
      avgCost: buyTrade.price,
      currentPrice: params.sellPrice,
      currentValue: buyTrade.executedShares * params.sellPrice,
      unrealizedPnL: 0,
      unrealizedPnLPercent: 0,
    })
    
    // 模拟加入这两笔交易后的所有交易，验证资金不会变负
    const newTrades = [...state.trades, buyTrade, sellTrade].sort((a, b) => a.date.localeCompare(b.date))
    
    let simulatedCash = state.initialCapital
    for (const t of newTrades) {
      if (t.type === 'BUY') {
        simulatedCash -= t.totalCost
      } else {
        simulatedCash += t.executedAmount - t.totalCost
      }
      
      if (simulatedCash < 0) {
        return { 
          success: false, 
          error: `插入配对交易会导致 ${t.date} 资金不足，请调整交易` 
        }
      }
    }
    
    // 验证通过，添加交易
    set({ trades: newTrades })
    get().recalculate()
    return { success: true }
  },
  
  getStateAtDate: (targetDate) => {
    const state = get()
    return calculateStateAtDate(
      state.initialCapital,
      state.stocks,
      state.trades,
      state.startDate,
      targetDate
    )
  },
  
  generateOptimalTrades: () => {
    const state = get()
    
    if (state.stocks.size === 0) {
      return { success: false, error: '请先添加股票到股票池' }
    }
    
    // 生成最优交易序列
    const optimalTrades = generateOptimalTradesEngine(
      state.stocks,
      state.startDate,
      state.endDate,
      state.initialCapital
    )
    
    if (optimalTrades.length === 0) {
      return { success: false, error: '未找到正收益的交易机会' }
    }
    
    // 清空现有交易
    set({ trades: [] })
    
    // 依次添加配对交易，使用当前可用资金计算股数
    let successCount = 0
    let currentCash = state.initialCapital
    
    for (const trade of optimalTrades) {
      // 计算可买股数（全仓）
      const maxShares = Math.floor(currentCash / trade.buyPrice / 100) * 100
      
      if (maxShares <= 0) continue
      
      const result = get().addTradePair({
        stockCode: trade.stockCode,
        buyDate: trade.buyDate,
        buyPrice: trade.buyPrice,
        sellDate: trade.sellDate,
        sellPrice: trade.sellPrice,
        shares: maxShares,
      })
      
      if (result.success) {
        successCount++
        // 更新可用现金：卖出金额（简化计算，不考虑手续费）
        currentCash = maxShares * trade.sellPrice
      }
    }
    
    if (successCount === 0) {
      return { success: false, error: '添加交易失败' }
    }
    
    return { success: true }
  },
  
  generateWorstTrades: () => {
    const state = get()
    
    if (state.stocks.size === 0) {
      return { success: false, error: '请先添加股票到股票池' }
    }
    
    const worstTrades = generateWorstTradesEngine(
      state.stocks,
      state.startDate,
      state.endDate,
      state.initialCapital
    )
    
    if (worstTrades.length === 0) {
      return { success: false, error: '未找到负收益的交易机会' }
    }
    
    set({ trades: [] })
    
    let successCount = 0
    let currentCash = state.initialCapital
    
    for (const trade of worstTrades) {
      const maxShares = Math.floor(currentCash / trade.buyPrice / 100) * 100
      
      if (maxShares <= 0) continue
      
      const result = get().addTradePair({
        stockCode: trade.stockCode,
        buyDate: trade.buyDate,
        buyPrice: trade.buyPrice,
        sellDate: trade.sellDate,
        sellPrice: trade.sellPrice,
        shares: maxShares,
      })
      
      if (result.success) {
        successCount++
        currentCash = maxShares * trade.sellPrice
      }
    }
    
    if (successCount === 0) {
      return { success: false, error: '添加交易失败' }
    }
    
    return { success: true }
  },
  
  setHighlightedTrade: (tradeId) => {
    set({ highlightedTradeId: tradeId })
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
