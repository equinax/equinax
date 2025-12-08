/**
 * Technical indicators calculation functions
 * All calculations are done on the frontend using K-line data
 */

export interface KLinePoint {
  date: string
  close: number
}

/**
 * Simple Moving Average (SMA)
 * MA(N) = sum of last N closing prices / N
 */
export function calcMA(data: KLinePoint[], period: number): Map<string, number> {
  const result = new Map<string, number>()
  if (data.length < period) return result

  for (let i = period - 1; i < data.length; i++) {
    let sum = 0
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close
    }
    result.set(data[i].date, sum / period)
  }

  return result
}

/**
 * Exponential Moving Average (EMA)
 * EMA = price * k + EMA_prev * (1 - k), where k = 2 / (N + 1)
 */
export function calcEMA(data: KLinePoint[], period: number): Map<string, number> {
  const result = new Map<string, number>()
  if (data.length === 0) return result

  const k = 2 / (period + 1)
  let ema = data[0].close

  for (let i = 0; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k)
    // Only output after we have enough data points for a meaningful EMA
    if (i >= period - 1) {
      result.set(data[i].date, ema)
    }
  }

  return result
}

/**
 * Bollinger Bands
 * Middle = MA(20)
 * Upper = Middle + 2 * STD
 * Lower = Middle - 2 * STD
 */
export function calcBOLL(
  data: KLinePoint[],
  period = 20,
  mult = 2
): {
  upper: Map<string, number>
  middle: Map<string, number>
  lower: Map<string, number>
} {
  const upper = new Map<string, number>()
  const middle = new Map<string, number>()
  const lower = new Map<string, number>()

  if (data.length < period) return { upper, middle, lower }

  for (let i = period - 1; i < data.length; i++) {
    // Calculate MA
    let sum = 0
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close
    }
    const ma = sum / period

    // Calculate Standard Deviation
    let sumSquaredDiff = 0
    for (let j = 0; j < period; j++) {
      const diff = data[i - j].close - ma
      sumSquaredDiff += diff * diff
    }
    const std = Math.sqrt(sumSquaredDiff / period)

    const date = data[i].date
    middle.set(date, ma)
    upper.set(date, ma + mult * std)
    lower.set(date, ma - mult * std)
  }

  return { upper, middle, lower }
}

/**
 * MACD (Moving Average Convergence Divergence)
 * DIF = EMA(12) - EMA(26)
 * DEA = EMA(DIF, 9)
 * MACD Histogram = 2 * (DIF - DEA)
 */
export function calcMACD(data: KLinePoint[]): {
  dif: Map<string, number>
  dea: Map<string, number>
  hist: Map<string, number>
} {
  const dif = new Map<string, number>()
  const dea = new Map<string, number>()
  const hist = new Map<string, number>()

  if (data.length < 26) return { dif, dea, hist }

  // Calculate EMA12 and EMA26
  const k12 = 2 / 13
  const k26 = 2 / 27
  const k9 = 2 / 10

  let ema12 = data[0].close
  let ema26 = data[0].close
  let deaValue = 0
  let firstDif = true

  for (let i = 0; i < data.length; i++) {
    ema12 = data[i].close * k12 + ema12 * (1 - k12)
    ema26 = data[i].close * k26 + ema26 * (1 - k26)

    // Only calculate DIF after enough data for EMA26
    if (i >= 25) {
      const difValue = ema12 - ema26
      dif.set(data[i].date, difValue)

      // Calculate DEA (EMA of DIF)
      if (firstDif) {
        deaValue = difValue
        firstDif = false
      } else {
        deaValue = difValue * k9 + deaValue * (1 - k9)
      }

      // Only output DEA and HIST after a few more periods
      if (i >= 33) {
        dea.set(data[i].date, deaValue)
        hist.set(data[i].date, 2 * (difValue - deaValue))
      }
    }
  }

  return { dif, dea, hist }
}

/**
 * Relative Strength Index (RSI)
 * RSI = 100 - 100 / (1 + RS)
 * RS = Average Gain / Average Loss over N periods
 */
export function calcRSI(data: KLinePoint[], period: number): Map<string, number> {
  const result = new Map<string, number>()
  if (data.length < period + 1) return result

  // Calculate price changes
  const changes: number[] = []
  for (let i = 1; i < data.length; i++) {
    changes.push(data[i].close - data[i - 1].close)
  }

  // Initial average gain and loss
  let avgGain = 0
  let avgLoss = 0

  for (let i = 0; i < period; i++) {
    if (changes[i] > 0) {
      avgGain += changes[i]
    } else {
      avgLoss += Math.abs(changes[i])
    }
  }

  avgGain /= period
  avgLoss /= period

  // First RSI value
  let rsi = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss)
  result.set(data[period].date, rsi)

  // Subsequent RSI values using smoothed averages
  for (let i = period; i < changes.length; i++) {
    const change = changes[i]
    const gain = change > 0 ? change : 0
    const loss = change < 0 ? Math.abs(change) : 0

    avgGain = (avgGain * (period - 1) + gain) / period
    avgLoss = (avgLoss * (period - 1) + loss) / period

    rsi = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss)
    result.set(data[i + 1].date, rsi)
  }

  return result
}
