/**
 * Type definitions for backtest result details.
 * These provide stricter typing than the auto-generated API types.
 */

export interface EquityCurvePoint {
  date: string;
  value: number;
  drawdown?: number;
}

export interface TradeRecord {
  id?: string;
  // Frontend-compatible fields
  entry_date?: string;
  exit_date?: string;
  type?: 'LONG' | 'SHORT' | 'long' | 'short';
  entry_price?: number;
  exit_price?: number;
  // Backend original fields (for compatibility)
  open_datetime?: string;
  close_datetime?: string;
  direction?: 'long' | 'short';
  open_price?: number;
  close_price?: number;
  // Common fields
  size: number;
  pnl: number;
  pnl_percent: number;
  net_pnl?: number;
  commission?: number;
  bars_held?: number;
  duration_days?: number;
  stock_code?: string;
}

export interface MonthlyReturns {
  [yearMonth: string]: number; // e.g., "2024-01": 0.05
}

export interface MetricGroup {
  title: string;
  items: MetricItem[];
}

export interface MetricItem {
  label: string;
  value: number | string | null | undefined;
  format: 'percent' | 'currency' | 'number' | 'ratio' | 'days';
  colorize?: boolean; // Apply profit/loss colors
}
