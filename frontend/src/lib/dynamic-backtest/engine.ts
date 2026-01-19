/**
 * 动态回测计算引擎
 * 前端实时计算，模拟后端 backtrader 逻辑
 */

import {
  Trade,
  Position,
  EquityPoint,
  BacktestMetrics,
  StockData,
  BenchmarkData,
  LotRecord,
  CN_STOCK_FEES,
  KLineDataPoint,
} from './types'

/**
 * 计算佣金（A股规则）
 */
export function calculateCommission(amount: number): number {
  const commission = amount * CN_STOCK_FEES.COMMISSION_RATE
  return Math.max(commission, CN_STOCK_FEES.MIN_COMMISSION)
}

/**
 * 计算印花税（仅卖出）
 */
export function calculateStampDuty(amount: number): number {
  return amount * CN_STOCK_FEES.STAMP_DUTY_RATE
}

/**
 * 将股数取整到100股（A股一手）
 */
export function roundToLot(shares: number): number {
  return Math.floor(shares / CN_STOCK_FEES.LOT_SIZE) * CN_STOCK_FEES.LOT_SIZE
}

/**
 * 获取指定日期的K线数据
 */
export function getKlineOnDate(kline: KLineDataPoint[], date: string): KLineDataPoint | undefined {
  return kline.find(k => k.date === date)
}

/**
 * 获取指定日期或之前最近的K线数据
 */
export function getKlineOnOrBefore(kline: KLineDataPoint[], date: string): KLineDataPoint | undefined {
  const sorted = [...kline].sort((a, b) => b.date.localeCompare(a.date))
  return sorted.find(k => k.date <= date)
}

/**
 * 获取日期范围内的交易日列表
 */
export function getTradingDays(
  stocks: Map<string, StockData>,
  startDate: string,
  endDate: string
): string[] {
  const allDates = new Set<string>()
  
  stocks.forEach(stock => {
    stock.kline.forEach(k => {
      if (k.date >= startDate && k.date <= endDate) {
        allDates.add(k.date)
      }
    })
  })
  
  return Array.from(allDates).sort()
}

/**
 * 计算买入交易
 */
export function executeBuyTrade(
  trade: Omit<Trade, 'executedShares' | 'executedAmount' | 'commission' | 'stampDuty' | 'totalCost'>,
  availableCash: number
): Trade {
  let targetAmount: number
  
  switch (trade.mode) {
    case 'amount':
      targetAmount = Math.min(trade.inputValue, availableCash)
      break
    case 'percent':
      targetAmount = Math.min(availableCash * (trade.inputValue / 100), availableCash)
      break
    case 'shares':
      // 股数模式：也需要检查可用资金
      targetAmount = Math.min(trade.inputValue * trade.price, availableCash)
      break
    default:
      targetAmount = 0
  }
  
  // 预估佣金，确保总成本不超过可用资金
  // 先按 targetAmount 计算可买股数
  let rawShares = targetAmount / trade.price
  let executedShares = roundToLot(rawShares)
  
  if (executedShares === 0) {
    return {
      ...trade,
      executedShares: 0,
      executedAmount: 0,
      commission: 0,
      stampDuty: 0,
      totalCost: 0,
    }
  }
  
  // 计算实际成本（含佣金），如果超过可用资金则减少股数
  let executedAmount = executedShares * trade.price
  let commission = calculateCommission(executedAmount)
  let totalCost = executedAmount + commission
  
  // 如果总成本超过可用资金，减少一手再试
  while (totalCost > availableCash && executedShares >= CN_STOCK_FEES.LOT_SIZE) {
    executedShares -= CN_STOCK_FEES.LOT_SIZE
    executedAmount = executedShares * trade.price
    commission = calculateCommission(executedAmount)
    totalCost = executedAmount + commission
  }
  
  if (executedShares === 0) {
    return {
      ...trade,
      executedShares: 0,
      executedAmount: 0,
      commission: 0,
      stampDuty: 0,
      totalCost: 0,
    }
  }
  
  const stampDuty = 0 // 买入无印花税
  
  return {
    ...trade,
    executedShares,
    executedAmount,
    commission,
    stampDuty,
    totalCost,
  }
}

/**
 * 计算卖出交易
 */
export function executeSellTrade(
  trade: Omit<Trade, 'executedShares' | 'executedAmount' | 'commission' | 'stampDuty' | 'totalCost'>,
  position: Position
): Trade {
  let targetShares: number
  
  switch (trade.mode) {
    case 'shares':
      targetShares = Math.min(trade.inputValue, position.totalShares)
      break
    case 'percent':
      targetShares = Math.floor(position.totalShares * (trade.inputValue / 100))
      break
    case 'amount':
      targetShares = Math.floor(trade.inputValue / trade.price)
      break
    default:
      targetShares = 0
  }
  
  // A股卖出可以不是100股整数倍（清仓时）
  const executedShares = Math.min(targetShares, position.totalShares)
  
  if (executedShares === 0) {
    return {
      ...trade,
      executedShares: 0,
      executedAmount: 0,
      commission: 0,
      stampDuty: 0,
      totalCost: 0,
    }
  }
  
  const executedAmount = executedShares * trade.price
  const commission = calculateCommission(executedAmount)
  const stampDuty = calculateStampDuty(executedAmount)
  const totalCost = commission + stampDuty
  
  return {
    ...trade,
    executedShares,
    executedAmount,
    commission,
    stampDuty,
    totalCost,
  }
}

/**
 * 更新持仓（买入）
 */
export function updatePositionOnBuy(
  position: Position | undefined,
  trade: Trade,
  stockName: string
): Position {
  if (!position) {
    // 新建持仓
    return {
      stockCode: trade.stockCode,
      stockName,
      lots: [{
        buyDate: trade.date,
        shares: trade.executedShares,
        costPrice: trade.price,
        remainingShares: trade.executedShares,
      }],
      totalShares: trade.executedShares,
      avgCost: trade.price,
      currentPrice: trade.price,
      currentValue: trade.executedShares * trade.price,
      unrealizedPnL: 0,
      unrealizedPnLPercent: 0,
    }
  }
  
  // 加仓：更新加权平均成本
  const newLot: LotRecord = {
    buyDate: trade.date,
    shares: trade.executedShares,
    costPrice: trade.price,
    remainingShares: trade.executedShares,
  }
  
  const totalCost = position.avgCost * position.totalShares + trade.executedAmount
  const totalShares = position.totalShares + trade.executedShares
  const avgCost = totalCost / totalShares
  
  return {
    ...position,
    lots: [...position.lots, newLot],
    totalShares,
    avgCost,
    currentPrice: trade.price,
    currentValue: totalShares * trade.price,
    unrealizedPnL: (trade.price - avgCost) * totalShares,
    unrealizedPnLPercent: (trade.price - avgCost) / avgCost,
  }
}

/**
 * 更新持仓（卖出，FIFO方式）
 */
export function updatePositionOnSell(
  position: Position,
  trade: Trade
): Position | null {
  let sharesToSell = trade.executedShares
  const updatedLots: LotRecord[] = []
  
  // FIFO: 从最早的lot开始卖出
  for (const lot of position.lots) {
    if (sharesToSell <= 0) {
      updatedLots.push(lot)
      continue
    }
    
    if (lot.remainingShares <= sharesToSell) {
      // 整个lot都卖掉
      sharesToSell -= lot.remainingShares
    } else {
      // 部分卖出
      updatedLots.push({
        ...lot,
        remainingShares: lot.remainingShares - sharesToSell,
      })
      sharesToSell = 0
    }
  }
  
  const totalShares = position.totalShares - trade.executedShares
  
  if (totalShares <= 0) {
    return null // 清仓
  }
  
  // 重新计算加权平均成本
  let totalCost = 0
  let totalRemainingShares = 0
  for (const lot of updatedLots) {
    totalCost += lot.costPrice * lot.remainingShares
    totalRemainingShares += lot.remainingShares
  }
  const avgCost = totalRemainingShares > 0 ? totalCost / totalRemainingShares : 0
  
  return {
    ...position,
    lots: updatedLots,
    totalShares: totalRemainingShares,
    avgCost,
    currentPrice: trade.price,
    currentValue: totalRemainingShares * trade.price,
    unrealizedPnL: (trade.price - avgCost) * totalRemainingShares,
    unrealizedPnLPercent: avgCost > 0 ? (trade.price - avgCost) / avgCost : 0,
  }
}

/**
 * 计算持仓市值
 */
export function calculatePositionValue(
  positions: Map<string, Position>,
  stocks: Map<string, StockData>,
  date: string
): number {
  let totalValue = 0
  
  positions.forEach((position, code) => {
    const stock = stocks.get(code)
    if (!stock) return
    
    const kline = getKlineOnOrBefore(stock.kline, date)
    if (kline) {
      totalValue += position.totalShares * kline.close
    }
  })
  
  return totalValue
}

/**
 * 更新持仓当前价格
 */
export function updatePositionPrices(
  positions: Map<string, Position>,
  stocks: Map<string, StockData>,
  date: string
): Map<string, Position> {
  const updated = new Map<string, Position>()
  
  positions.forEach((position, code) => {
    const stock = stocks.get(code)
    if (!stock) {
      updated.set(code, position)
      return
    }
    
    const kline = getKlineOnOrBefore(stock.kline, date)
    if (kline) {
      const currentPrice = kline.close
      const currentValue = position.totalShares * currentPrice
      const unrealizedPnL = (currentPrice - position.avgCost) * position.totalShares
      const unrealizedPnLPercent = position.avgCost > 0 
        ? (currentPrice - position.avgCost) / position.avgCost 
        : 0
      
      updated.set(code, {
        ...position,
        currentPrice,
        currentValue,
        unrealizedPnL,
        unrealizedPnLPercent,
      })
    } else {
      updated.set(code, position)
    }
  })
  
  return updated
}

/**
 * 计算权益曲线
 */
export function calculateEquityCurve(
  initialCapital: number,
  stocks: Map<string, StockData>,
  trades: Trade[],
  startDate: string,
  endDate: string
): { equityCurve: EquityPoint[]; finalPositions: Map<string, Position>; finalCash: number } {
  const tradingDays = getTradingDays(stocks, startDate, endDate)
  const sortedTrades = [...trades].sort((a, b) => a.date.localeCompare(b.date))
  
  let cash = initialCapital
  const positions = new Map<string, Position>()
  const equityCurve: EquityPoint[] = []
  let prevEquity = initialCapital
  let peakEquity = initialCapital
  
  for (const date of tradingDays) {
    // 处理当日所有交易
    const todayTrades = sortedTrades.filter(t => t.date === date)
    
    for (const trade of todayTrades) {
      const stock = stocks.get(trade.stockCode)
      if (!stock) continue
      
      if (trade.type === 'BUY') {
        cash -= trade.totalCost
        const position = positions.get(trade.stockCode)
        const updated = updatePositionOnBuy(position, trade, stock.name)
        positions.set(trade.stockCode, updated)
      } else {
        cash += trade.executedAmount - trade.totalCost
        const position = positions.get(trade.stockCode)
        if (position) {
          const updated = updatePositionOnSell(position, trade)
          if (updated) {
            positions.set(trade.stockCode, updated)
          } else {
            positions.delete(trade.stockCode)
          }
        }
      }
    }
    
    // 更新持仓价格
    const updatedPositions = updatePositionPrices(positions, stocks, date)
    updatedPositions.forEach((pos, code) => positions.set(code, pos))
    
    // 计算当日权益
    const positionValue = calculatePositionValue(positions, stocks, date)
    const totalEquity = cash + positionValue
    const dailyReturn = prevEquity > 0 ? (totalEquity - prevEquity) / prevEquity : 0
    const cumulativeReturn = (totalEquity - initialCapital) / initialCapital
    
    // 更新峰值和回撤
    peakEquity = Math.max(peakEquity, totalEquity)
    const drawdown = peakEquity > 0 ? (peakEquity - totalEquity) / peakEquity : 0
    
    equityCurve.push({
      date,
      cash,
      positionValue,
      totalEquity,
      dailyReturn,
      cumulativeReturn,
      drawdown,
    })
    
    prevEquity = totalEquity
  }
  
  return { equityCurve, finalPositions: positions, finalCash: cash }
}

/**
 * 计算指定日期的可用现金和持仓状态
 * 用于交易弹窗显示当前可用资金
 */
export function calculateStateAtDate(
  initialCapital: number,
  stocks: Map<string, StockData>,
  trades: Trade[],
  startDate: string,
  targetDate: string
): { availableCash: number; positions: Map<string, Position> } {
  // 获取目标日期之前的所有交易
  const prevDayTrades = trades.filter(t => t.date < targetDate)
  
  // 计算到目标日期开盘前的状态
  const { finalPositions, finalCash: cashAtDayOpen } = calculateEquityCurve(
    initialCapital,
    stocks,
    prevDayTrades,
    startDate,
    targetDate
  )
  
  // 计算目标日期当天已执行的买入总额
  const todayBuys = trades.filter(t => t.date === targetDate && t.type === 'BUY')
  const todayBuyAmount = todayBuys.reduce((sum, t) => sum + t.totalCost, 0)
  
  // 计算目标日期当天已执行的卖出收入
  const todaySells = trades.filter(t => t.date === targetDate && t.type === 'SELL')
  const todaySellIncome = todaySells.reduce((sum, t) => sum + (t.executedAmount - t.totalCost), 0)
  
  // 可用现金 = 当天开盘现金 - 当天已买入 + 当天已卖出收入
  const availableCash = cashAtDayOpen - todayBuyAmount + todaySellIncome
  
  // 更新持仓状态（包含当天交易）
  const todayTrades = trades.filter(t => t.date === targetDate)
  for (const trade of todayTrades) {
    const stock = stocks.get(trade.stockCode)
    if (!stock) continue
    
    if (trade.type === 'BUY') {
      const position = finalPositions.get(trade.stockCode)
      const updated = updatePositionOnBuy(position, trade, stock.name)
      finalPositions.set(trade.stockCode, updated)
    } else {
      const position = finalPositions.get(trade.stockCode)
      if (position) {
        const updated = updatePositionOnSell(position, trade)
        if (updated) {
          finalPositions.set(trade.stockCode, updated)
        } else {
          finalPositions.delete(trade.stockCode)
        }
      }
    }
  }
  
  return { availableCash, positions: finalPositions }
}

/**
 * 计算基准收益曲线（归一化）
 */
export function calculateBenchmarkReturns(
  benchmark: BenchmarkData,
  startDate: string,
  endDate: string
): number[] {
  const filtered = benchmark.kline.filter(k => k.date >= startDate && k.date <= endDate)
  if (filtered.length === 0) return []
  
  const basePrice = filtered[0].close
  return filtered.map(k => (k.close - basePrice) / basePrice)
}

/**
 * 计算回测统计指标
 */
export function calculateMetrics(
  equityCurve: EquityPoint[],
  trades: Trade[],
  benchmarkReturns: number[],
  initialCapital: number
): BacktestMetrics {
  if (equityCurve.length === 0) {
    return {
      totalReturn: 0,
      annualReturn: 0,
      maxDrawdown: 0,
      sharpeRatio: 0,
      volatility: 0,
      winRate: 0,
      profitFactor: 0,
      totalTrades: 0,
      winningTrades: 0,
      losingTrades: 0,
      benchmarkReturn: 0,
      excessReturn: 0,
      alpha: 0,
      beta: 0,
    }
  }
  
  const finalEquity = equityCurve[equityCurve.length - 1].totalEquity
  const totalReturn = (finalEquity - initialCapital) / initialCapital
  
  // 年化收益率（假设252个交易日）
  const days = equityCurve.length
  const annualReturn = days > 0 ? Math.pow(1 + totalReturn, 252 / days) - 1 : 0
  
  // 最大回撤
  const maxDrawdown = Math.max(...equityCurve.map(e => e.drawdown))
  
  // 波动率（日收益率标准差年化）
  const dailyReturns = equityCurve.map(e => e.dailyReturn)
  const meanReturn = dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length
  const variance = dailyReturns.reduce((sum, r) => sum + Math.pow(r - meanReturn, 2), 0) / dailyReturns.length
  const volatility = Math.sqrt(variance) * Math.sqrt(252)
  
  // 夏普比率（假设无风险利率2%）
  const riskFreeRate = 0.02
  const sharpeRatio = volatility > 0 ? (annualReturn - riskFreeRate) / volatility : 0
  
  // 交易统计
  const sellTrades = trades.filter(t => t.type === 'SELL' && t.executedShares > 0)
  const totalTrades = sellTrades.length
  
  // 简化的胜率计算：比较卖出价和买入成本
  // 实际应该跟踪每笔交易的盈亏
  let winningTrades = 0
  let grossProfit = 0
  let grossLoss = 0
  
  // 这里简化处理，实际应该配对买卖交易
  for (const trade of sellTrades) {
    // 找到对应的买入交易
    const buyTrades = trades.filter(t => 
      t.type === 'BUY' && 
      t.stockCode === trade.stockCode && 
      t.date < trade.date
    )
    
    if (buyTrades.length > 0) {
      const avgBuyPrice = buyTrades.reduce((sum, t) => sum + t.price * t.executedShares, 0) /
        buyTrades.reduce((sum, t) => sum + t.executedShares, 0)
      const pnl = (trade.price - avgBuyPrice) * trade.executedShares
      
      if (pnl > 0) {
        winningTrades++
        grossProfit += pnl
      } else {
        grossLoss += Math.abs(pnl)
      }
    }
  }
  
  const losingTrades = totalTrades - winningTrades
  const winRate = totalTrades > 0 ? winningTrades / totalTrades : 0
  const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Infinity : 0
  
  // 基准对比
  const benchmarkReturn = benchmarkReturns.length > 0 
    ? benchmarkReturns[benchmarkReturns.length - 1] 
    : 0
  const excessReturn = totalReturn - benchmarkReturn
  
  // Alpha 和 Beta（简化计算）
  // 实际应该用回归分析
  const alpha = excessReturn
  const beta = 1.0 // 简化
  
  return {
    totalReturn,
    annualReturn,
    maxDrawdown,
    sharpeRatio,
    volatility,
    winRate,
    profitFactor,
    totalTrades,
    winningTrades,
    losingTrades,
    benchmarkReturn,
    excessReturn,
    alpha,
    beta,
  }
}
