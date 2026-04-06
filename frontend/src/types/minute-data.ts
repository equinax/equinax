/**
 * Type definitions for intraday minute candle data.
 * These provide stricter typing than the auto-generated API types.
 */

export interface MinuteCandle {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface MinuteDataResponse {
  ts_code: string;
  trade_date: string;
  frequency: string;
  candles: MinuteCandle[];
  fetched_at: string;
}
