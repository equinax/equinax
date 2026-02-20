"""
TuShare 数据源

使用 TuShare Pro API 获取 A 股市场数据。
优点：一次 API 调用获取全市场数据，效率高，不易被封禁。
"""

import logging
import os
from datetime import date
from decimal import Decimal
from typing import List, Optional

import pandas as pd

from .base import BaseDataSource, convert_tushare_code_to_standard

logger = logging.getLogger(__name__)

# 核心指数代码 (TuShare 格式)
# 日常同步只更新这些核心指数的 OHLCV，保证速度
CORE_INDEX_CODES_TUSHARE = [
    "000001.SH",  # 上证综指
    "000016.SH",  # 上证50
    "000300.SH",  # 沪深300 (CSI 300)
    "000905.SH",  # 中证500
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
]


class TuShareDataSource(BaseDataSource):
    """
    TuShare Pro 数据源

    需要 5000+ 积分的 TuShare 账户才能使用全部功能。
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        初始化 TuShare 数据源

        Args:
            api_key: TuShare API Key，如果不提供则从环境变量 TUSHARE_API_KEY 获取
        """
        import tushare as ts

        self._api_key = api_key or os.environ.get("TUSHARE_API_KEY")
        if not self._api_key:
            raise ValueError(
                "TuShare API key not provided. Set TUSHARE_API_KEY environment variable."
            )

        ts.set_token(self._api_key)
        self._pro = ts.pro_api()
        logger.info("TuShare data source initialized")

    @property
    def name(self) -> str:
        return "tushare"

    def _date_to_str(self, d: date) -> str:
        """将 date 转换为 TuShare 格式 YYYYMMDD"""
        return d.strftime("%Y%m%d")

    def _safe_decimal(self, value, default=None) -> Optional[Decimal]:
        """安全转换为 Decimal"""
        if pd.isna(value) or value == "" or value == "-":
            return default
        try:
            return Decimal(str(value))
        except:
            return default

    def _safe_int(self, value, default=None) -> Optional[int]:
        """安全转换为 int"""
        if pd.isna(value) or value == "" or value == "-":
            return default
        try:
            f = float(value)
            if pd.isna(f):  # Handle float NaN
                return default
            return int(f)
        except:
            return default

    def fetch_stock_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场股票日线数据

        使用 pro.daily(trade_date='YYYYMMDD') 一次获取全部股票。
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching stock daily for {date_str}...")

        try:
            df = self._pro.daily(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No stock data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} stocks for {date_str}")

            # 转换为标准格式
            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "open": self._safe_decimal(row.get("open")),
                        "high": self._safe_decimal(row.get("high")),
                        "low": self._safe_decimal(row.get("low")),
                        "close": self._safe_decimal(row.get("close")),
                        "pre_close": self._safe_decimal(row.get("pre_close")),
                        # TuShare vol 单位是手，转为股
                        "volume": self._safe_int(row.get("vol", 0)) * 100
                        if self._safe_int(row.get("vol"))
                        else None,
                        # TuShare amount 单位是千元，转为元
                        "amount": self._safe_decimal(row.get("amount", 0)) * 1000
                        if self._safe_decimal(row.get("amount"))
                        else None,
                        "pct_chg": self._safe_decimal(row.get("pct_chg")),
                        "change": self._safe_decimal(row.get("change")),  # 涨跌额
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching stock daily: {e}")
            raise

    def fetch_etf_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场 ETF 日线数据

        使用 pro.fund_daily(trade_date='YYYYMMDD') 一次获取全部 ETF。
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching ETF daily for {date_str}...")

        try:
            df = self._pro.fund_daily(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No ETF data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} ETFs for {date_str}")

            # 转换为标准格式
            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "open": self._safe_decimal(row.get("open")),
                        "high": self._safe_decimal(row.get("high")),
                        "low": self._safe_decimal(row.get("low")),
                        "close": self._safe_decimal(row.get("close")),
                        "pre_close": self._safe_decimal(row.get("pre_close")),
                        "volume": self._safe_int(row.get("vol", 0)) * 100
                        if self._safe_int(row.get("vol"))
                        else None,
                        "amount": self._safe_decimal(row.get("amount", 0)) * 1000
                        if self._safe_decimal(row.get("amount"))
                        else None,
                        "pct_chg": self._safe_decimal(row.get("pct_chg")),
                        "change": self._safe_decimal(row.get("change")),
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching ETF daily: {e}")
            raise

    def fetch_index_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的核心指数 OHLCV 数据

        使用 index_daily 逐个获取核心指数（6个）的 OHLCV 数据。
        日常同步只更新核心指数，保证速度（6 次 API 调用）。
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching core index OHLCV for {date_str}...")

        records = []
        for ts_code in CORE_INDEX_CODES_TUSHARE:
            try:
                df = self._pro.index_daily(ts_code=ts_code, trade_date=date_str)

                if df is None or df.empty:
                    continue

                row = df.iloc[0]
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "open": self._safe_decimal(row.get("open")),
                        "high": self._safe_decimal(row.get("high")),
                        "low": self._safe_decimal(row.get("low")),
                        "close": self._safe_decimal(row.get("close")),
                        "pre_close": self._safe_decimal(row.get("pre_close")),
                        "volume": self._safe_int(row.get("vol", 0)) * 100
                        if self._safe_int(row.get("vol"))
                        else None,
                        "amount": self._safe_decimal(row.get("amount", 0)) * 1000
                        if self._safe_decimal(row.get("amount"))
                        else None,
                        "pct_chg": self._safe_decimal(row.get("pct_chg")),
                        "change": self._safe_decimal(row.get("change")),
                    }
                )

            except Exception as e:
                logger.warning(f"[TuShare] Failed to fetch index {ts_code}: {e}")
                continue

        logger.info(f"[TuShare] Fetched {len(records)} core indices for {date_str}")
        return pd.DataFrame(records) if records else pd.DataFrame()

    def fetch_index_daily_by_code(
        self, ts_code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        start_str = self._date_to_str(start_date)
        end_str = self._date_to_str(end_date)

        try:
            df = self._pro.index_daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            if df is None or df.empty:
                return pd.DataFrame()

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "trade_date": pd.to_datetime(str(row["trade_date"])).date(),
                        "open": self._safe_decimal(row.get("open")),
                        "high": self._safe_decimal(row.get("high")),
                        "low": self._safe_decimal(row.get("low")),
                        "close": self._safe_decimal(row.get("close")),
                        "pre_close": self._safe_decimal(row.get("pre_close")),
                        "volume": self._safe_int(row.get("vol", 0)) * 100
                        if self._safe_int(row.get("vol"))
                        else None,
                        "amount": self._safe_decimal(row.get("amount", 0)) * 1000
                        if self._safe_decimal(row.get("amount"))
                        else None,
                        "pct_chg": self._safe_decimal(row.get("pct_chg")),
                        "change": self._safe_decimal(row.get("change")),
                    }
                )
            return pd.DataFrame(records) if records else pd.DataFrame()
        except Exception as e:
            logger.warning(
                f"[TuShare] Failed to fetch index {ts_code} range {start_str}-{end_str}: {e}"
            )
            return pd.DataFrame()

    def fetch_stock_adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的股票复权因子

        使用 pro.adj_factor(trade_date='YYYYMMDD')
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching stock adj_factor for {date_str}...")

        try:
            df = self._pro.adj_factor(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No adj_factor data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} adj_factors for {date_str}")

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "adj_factor": self._safe_decimal(row.get("adj_factor")),
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching adj_factor: {e}")
            raise

    def fetch_etf_adj_factor(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        获取 ETF 复权因子（日期范围）

        使用 pro.fund_adj(start_date='YYYYMMDD', end_date='YYYYMMDD')
        """
        start_str = self._date_to_str(start_date)
        end_str = self._date_to_str(end_date)
        logger.info(f"[TuShare] Fetching ETF adj_factor {start_str} to {end_str}...")

        try:
            df = self._pro.fund_adj(start_date=start_str, end_date=end_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No ETF adj_factor data")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} ETF adj_factors")

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                trade_date = pd.to_datetime(row["trade_date"]).date()
                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "adj_factor": self._safe_decimal(row.get("adj_factor")),
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching ETF adj_factor: {e}")
            raise

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """
        获取日期范围内的交易日列表

        使用 pro.trade_cal()
        """
        start_str = self._date_to_str(start_date)
        end_str = self._date_to_str(end_date)

        try:
            df = self._pro.trade_cal(
                exchange="SSE",  # 上交所
                start_date=start_str,
                end_date=end_str,
                is_open="1",  # 只返回交易日
            )

            if df is None or df.empty:
                return []

            trading_days = [pd.to_datetime(d).date() for d in df["cal_date"].tolist()]

            return sorted(trading_days)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching trading days: {e}")
            return []

    def fetch_valuation_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的估值数据

        使用 pro.daily_basic(trade_date='YYYYMMDD')
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching valuation for {date_str}...")

        try:
            df = self._pro.daily_basic(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No valuation data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} valuation records for {date_str}")

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                name = str(row.get("name", ""))

                # TuShare total_mv/circ_mv 单位是万元，转为亿元
                total_mv = self._safe_decimal(row.get("total_mv"))
                circ_mv = self._safe_decimal(row.get("circ_mv"))

                records.append(
                    {
                        "code": code,
                        "trade_date": trade_date,
                        "close": self._safe_decimal(row.get("close")),
                        "turnover_rate": self._safe_decimal(row.get("turnover_rate")),
                        "turnover_rate_f": self._safe_decimal(row.get("turnover_rate_f")),
                        "volume_ratio": self._safe_decimal(row.get("volume_ratio")),
                        "pe": self._safe_decimal(row.get("pe")),
                        "pe_ttm": self._safe_decimal(row.get("pe_ttm")),
                        "pb_mrq": self._safe_decimal(row.get("pb")),
                        "ps": self._safe_decimal(row.get("ps")),
                        "ps_ttm": self._safe_decimal(row.get("ps_ttm")),
                        "dv_ratio": self._safe_decimal(row.get("dv_ratio")),
                        "dv_ttm": self._safe_decimal(row.get("dv_ttm")),
                        "total_mv": Decimal(str(float(total_mv) / 10000)) if total_mv else None,
                        "circ_mv": Decimal(str(float(circ_mv) / 10000)) if circ_mv else None,
                        "total_share": self._safe_decimal(row.get("total_share")),
                        "float_share": self._safe_decimal(row.get("float_share")),
                        "free_share": self._safe_decimal(row.get("free_share")),
                        "is_st": 1 if "ST" in name else 0,
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching valuation: {e}")
            raise

    def fetch_all_index_daily(
        self,
        index_codes: List[str],
        start_date: date,
        end_date: date,
        rate_limit_delay: float = 0.15,
    ) -> pd.DataFrame:
        """
        批量获取所有指数的 OHLCV 数据（日期范围）。

        低频使用：每个指数一次调用可获取日期范围内所有数据。
        适合定期全量更新（如每周一次）。

        Args:
            index_codes: 指数代码列表（标准格式 sh.000001）
            start_date: 开始日期
            end_date: 结束日期
            rate_limit_delay: 每次调用间隔（秒），默认 0.15s = 400次/分钟
        """
        import time

        start_str = self._date_to_str(start_date)
        end_str = self._date_to_str(end_date)

        logger.info(f"[TuShare] Fetching {len(index_codes)} indices from {start_str} to {end_str}")

        all_records = []
        success_count = 0

        for i, code in enumerate(index_codes):
            ts_code = self._convert_to_tushare_code(code)
            if not ts_code:
                continue

            try:
                df = self._pro.index_daily(
                    ts_code=ts_code,
                    start_date=start_str,
                    end_date=end_str,
                )

                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        trade_date = pd.to_datetime(row["trade_date"]).date()
                        all_records.append(
                            {
                                "code": code,
                                "trade_date": trade_date,
                                "open": self._safe_decimal(row.get("open")),
                                "high": self._safe_decimal(row.get("high")),
                                "low": self._safe_decimal(row.get("low")),
                                "close": self._safe_decimal(row.get("close")),
                                "pre_close": self._safe_decimal(row.get("pre_close")),
                                "volume": self._safe_int(row.get("vol", 0)) * 100
                                if self._safe_int(row.get("vol"))
                                else None,
                                "amount": self._safe_decimal(row.get("amount", 0)) * 1000
                                if self._safe_decimal(row.get("amount"))
                                else None,
                                "pct_chg": self._safe_decimal(row.get("pct_chg")),
                                "change": self._safe_decimal(row.get("change")),
                            }
                        )
                    success_count += 1

                if (i + 1) % 50 == 0:
                    logger.info(f"[TuShare] Processed {i + 1}/{len(index_codes)} indices...")

                time.sleep(rate_limit_delay)

            except Exception as e:
                logger.warning(f"[TuShare] Failed to fetch index {code}: {e}")
                continue

        logger.info(f"[TuShare] Fetched {len(all_records)} records from {success_count} indices")
        return pd.DataFrame(all_records) if all_records else pd.DataFrame()

    def fetch_moneyflow_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取个股资金流向

        使用 pro.moneyflow(trade_date='YYYYMMDD')
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching moneyflow for {date_str}...")

        try:
            df = self._pro.moneyflow(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No moneyflow data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} moneyflow records for {date_str}")

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "date": trade_date,
                        "buy_sm_amount": self._safe_decimal(row.get("buy_sm_amount")),
                        "buy_md_amount": self._safe_decimal(row.get("buy_md_amount")),
                        "buy_lg_amount": self._safe_decimal(row.get("buy_lg_amount")),
                        "buy_elg_amount": self._safe_decimal(row.get("buy_elg_amount")),
                        "sell_sm_amount": self._safe_decimal(row.get("sell_sm_amount")),
                        "sell_md_amount": self._safe_decimal(row.get("sell_md_amount")),
                        "sell_lg_amount": self._safe_decimal(row.get("sell_lg_amount")),
                        "sell_elg_amount": self._safe_decimal(row.get("sell_elg_amount")),
                        "net_mf_amount": self._safe_decimal(row.get("net_mf_amount")),
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching moneyflow: {e}")
            raise

    def fetch_limit_list_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取每日涨跌停统计

        使用 pro.limit_list_d(trade_date='YYYYMMDD')
        """
        date_str = self._date_to_str(trade_date)
        logger.info(f"[TuShare] Fetching limit_list_d for {date_str}...")

        try:
            df = self._pro.limit_list_d(trade_date=date_str)

            if df is None or df.empty:
                logger.warning(f"[TuShare] No limit_list_d data for {date_str}")
                return pd.DataFrame()

            logger.info(f"[TuShare] Fetched {len(df)} limit_list records for {date_str}")

            records = []
            for _, row in df.iterrows():
                code = convert_tushare_code_to_standard(row["ts_code"])
                records.append(
                    {
                        "code": code,
                        "date": trade_date,
                        "name": row.get("name") if pd.notna(row.get("name")) else None,
                        "close": self._safe_decimal(row.get("close")),
                        "pct_chg": self._safe_decimal(row.get("pct_chg")),
                        "fd_amount": self._safe_decimal(row.get("fd_amount")),
                        "first_time": row.get("first_time")
                        if pd.notna(row.get("first_time"))
                        else None,
                        "last_time": row.get("last_time")
                        if pd.notna(row.get("last_time"))
                        else None,
                        "open_times": self._safe_int(row.get("open_times")),
                        "up_stat": row.get("up_stat") if pd.notna(row.get("up_stat")) else None,
                        "limit_times": self._safe_int(row.get("limit_times")),
                        "limit_type": row.get("limit") if pd.notna(row.get("limit")) else None,
                    }
                )

            return pd.DataFrame(records)

        except Exception as e:
            logger.error(f"[TuShare] Error fetching limit_list_d: {e}")
            raise

    def _convert_to_tushare_code(self, code: str) -> Optional[str]:
        """sh.000001 -> 000001.SH"""
        if code.startswith("sh."):
            return code[3:] + ".SH"
        elif code.startswith("sz."):
            return code[3:] + ".SZ"
        return None
