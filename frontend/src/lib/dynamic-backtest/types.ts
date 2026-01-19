/**
 * 动态回测类型定义
 */

// K线数据点
export interface KLineDataPoint {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount?: number
  pctChg?: number
  turn?: number
}

// 股票数据（包含后复权K线）
export interface StockData {
  code: string
  name: string
  kline: KLineDataPoint[]
  adjustFactors?: AdjustFactor[]
}

// 复权因子
export interface AdjustFactor {
  date: string
  backAdjustFactor: number
  foreAdjustFactor: number
}

// 交易类型
export type TradeType = 'BUY' | 'SELL'

// 交易方式
export type TradeMode = 'amount' | 'percent' | 'shares'

// 交易记录
export interface Trade {
  id: string
  stockCode: string
  type: TradeType
  date: string
  price: number              // 成交价格
  mode: TradeMode            // 交易方式
  inputValue: number         // 输入值（金额/比例/股数）
  // 计算结果
  executedShares: number     // 实际成交股数
  executedAmount: number     // 实际成交金额
  commission: number         // 佣金
  stampDuty: number          // 印花税（仅卖出）
  totalCost: number          // 总费用
}

// 分批持仓记录（用于FIFO计算）
export interface LotRecord {
  buyDate: string
  shares: number
  costPrice: number
  remainingShares: number
}

// 持仓
export interface Position {
  stockCode: string
  stockName: string
  lots: LotRecord[]          // 分批持仓记录
  totalShares: number        // 总持仓股数
  avgCost: number            // 加权平均成本
  currentPrice: number       // 当前价格
  currentValue: number       // 当前市值
  unrealizedPnL: number      // 浮动盈亏
  unrealizedPnLPercent: number // 浮动盈亏百分比
}

// 权益曲线点
export interface EquityPoint {
  date: string
  cash: number               // 现金
  positionValue: number      // 持仓市值
  totalEquity: number        // 总权益
  dailyReturn: number        // 日收益率
  cumulativeReturn: number   // 累计收益率
  drawdown: number           // 回撤
}

// 基准数据
export interface BenchmarkData {
  code: string
  name: string
  kline: KLineDataPoint[]
  returns: number[]          // 归一化收益率
}

// 常用基准（使用跟踪指数的ETF）
export const BENCHMARK_OPTIONS = [
  { code: 'sh.510300', name: '沪深300ETF' },
  { code: 'sh.510500', name: '中证500ETF' },
  { code: 'sh.510050', name: '上证50ETF' },
  { code: 'sz.159915', name: '创业板ETF' },
  { code: 'sh.510880', name: '红利ETF' },
] as const

// 统计指标
export interface BacktestMetrics {
  totalReturn: number        // 总收益率
  annualReturn: number       // 年化收益率
  maxDrawdown: number        // 最大回撤
  sharpeRatio: number        // 夏普比率
  volatility: number         // 波动率
  winRate: number            // 胜率
  profitFactor: number       // 盈亏比
  totalTrades: number        // 总交易次数
  winningTrades: number      // 盈利交易次数
  losingTrades: number       // 亏损交易次数
  // 与基准对比
  benchmarkReturn: number    // 基准收益率
  excessReturn: number       // 超额收益
  alpha: number              // Alpha
  beta: number               // Beta
}

// 临时配置（编辑模式使用）
export interface TempConfig {
  initialCapital: number
  startDate: string
  endDate: string
}

// 动态回测状态
export interface DynamicBacktestState {
  // 配置
  initialCapital: number
  startDate: string
  endDate: string
  benchmarkCode: string
  
  // 数据
  stocks: Map<string, StockData>
  benchmark: BenchmarkData | null
  trades: Trade[]
  
  // 计算结果
  positions: Map<string, Position>
  equityCurve: EquityPoint[]
  metrics: BacktestMetrics | null
  currentCash: number
  
  // UI状态
  selectedStockCode: string | null
  isCalculating: boolean
  
  // 配置锁定状态
  isConfigLocked: boolean
  tempConfig: TempConfig | null  // 编辑模式下的临时配置
}

// A股费用常量
export const CN_STOCK_FEES = {
  COMMISSION_RATE: 0.00025,    // 佣金万2.5
  MIN_COMMISSION: 5,           // 最低佣金5元
  STAMP_DUTY_RATE: 0.0005,     // 印花税千0.5（仅卖出）
  LOT_SIZE: 100,               // 一手100股
} as const
