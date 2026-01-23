"""
AkShare 数据源

使用 AkShare 获取 A 股市场数据。
注意：AkShare 需要逐个资产获取历史数据，效率较低，容易触发限流。
"""

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

import pandas as pd

from .base import BaseDataSource, convert_akshare_code_to_standard

logger = logging.getLogger(__name__)


class AkShareDataSource(BaseDataSource):
    """
    AkShare 数据源
    
    主要用于备用，当 TuShare 不可用时切换。
    注意：部分接口会触发限流，建议使用 TuShare。
    """
    
    def __init__(self):
        """初始化 AkShare 数据源"""
        import akshare as ak
        self._ak = ak
        logger.info("AkShare data source initialized")
    
    @property
    def name(self) -> str:
        return "akshare"
    
    def _safe_decimal(self, value, default=None) -> Optional[Decimal]:
        """安全转换为 Decimal"""
        if pd.isna(value) or value == '' or value == '-':
            return default
        try:
            return Decimal(str(value))
        except:
            return default
    
    def _safe_int(self, value, default=None) -> Optional[int]:
        """安全转换为 int"""
        if pd.isna(value) or value == '' or value == '-':
            return default
        try:
            return int(float(value))
        except:
            return default
    
    def fetch_stock_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场股票日线数据
        
        使用 stock_zh_a_spot_em() 获取实时/当日数据。
        注意：此接口只能获取当日数据，历史数据需要逐个股票获取。
        """
        logger.info(f"[AkShare] Fetching stock spot data...")
        
        try:
            df = self._ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                logger.warning("[AkShare] No stock data available")
                return pd.DataFrame()
            
            logger.info(f"[AkShare] Fetched {len(df)} stocks")
            
            # 转换为标准格式
            records = []
            for _, row in df.iterrows():
                code = convert_akshare_code_to_standard(row.get('代码', row.get('股票代码', '')))
                
                if not code or code in ('sh.', 'sz.', 'bj.'):
                    continue
                
                records.append({
                    'code': code,
                    'trade_date': trade_date,
                    'open': self._safe_decimal(row.get('今开')),
                    'high': self._safe_decimal(row.get('最高')),
                    'low': self._safe_decimal(row.get('最低')),
                    'close': self._safe_decimal(row.get('最新价')),
                    'pre_close': self._safe_decimal(row.get('昨收')),
                    # 成交量: akshare 返回的是"手"，转为"股" (*100)
                    'volume': self._safe_int(row.get('成交量', 0)) * 100 if self._safe_int(row.get('成交量')) else None,
                    'amount': self._safe_decimal(row.get('成交额')),
                    'pct_chg': self._safe_decimal(row.get('涨跌幅')),
                    'turn': self._safe_decimal(row.get('换手率')),
                })
            
            return pd.DataFrame(records)
            
        except Exception as e:
            logger.error(f"[AkShare] Error fetching stock data: {e}")
            raise
    
    def fetch_etf_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场 ETF 日线数据
        
        使用 fund_etf_spot_em() 获取实时/当日数据。
        """
        logger.info(f"[AkShare] Fetching ETF spot data...")
        
        try:
            df = self._ak.fund_etf_spot_em()
            
            if df is None or df.empty:
                logger.warning("[AkShare] No ETF data available")
                return pd.DataFrame()
            
            logger.info(f"[AkShare] Fetched {len(df)} ETFs")
            
            records = []
            for _, row in df.iterrows():
                code = convert_akshare_code_to_standard(row.get('代码', row.get('基金代码', '')))
                
                if not code:
                    continue
                
                records.append({
                    'code': code,
                    'trade_date': trade_date,
                    'open': self._safe_decimal(row.get('今开')),
                    'high': self._safe_decimal(row.get('最高')),
                    'low': self._safe_decimal(row.get('最低')),
                    'close': self._safe_decimal(row.get('最新价')),
                    'pre_close': self._safe_decimal(row.get('昨收')),
                    'volume': self._safe_int(row.get('成交量', 0)) * 100 if self._safe_int(row.get('成交量')) else None,
                    'amount': self._safe_decimal(row.get('成交额')),
                    'pct_chg': self._safe_decimal(row.get('涨跌幅')),
                    'turn': self._safe_decimal(row.get('换手率')),
                })
            
            return pd.DataFrame(records)
            
        except Exception as e:
            logger.error(f"[AkShare] Error fetching ETF data: {e}")
            raise
    
    def fetch_index_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的指数日线数据
        
        AkShare 没有批量指数接口，返回空 DataFrame。
        指数数据建议使用 TuShare。
        """
        logger.warning("[AkShare] Index daily batch fetch not supported, returning empty")
        return pd.DataFrame()
    
    def fetch_stock_adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的股票复权因子
        
        AkShare 没有批量复权因子接口，返回空 DataFrame。
        """
        logger.warning("[AkShare] Stock adj_factor batch fetch not supported, returning empty")
        return pd.DataFrame()
    
    def fetch_etf_adj_factor(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        获取 ETF 复权因子
        
        AkShare 没有批量 ETF 复权因子接口，返回空 DataFrame。
        """
        logger.warning("[AkShare] ETF adj_factor batch fetch not supported, returning empty")
        return pd.DataFrame()
    
    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """
        获取日期范围内的交易日列表
        
        使用 baostock 获取交易日历。
        """
        import baostock as bs
        
        try:
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"baostock login failed: {lg.error_msg}")
                return []
            
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            rs = bs.query_trade_dates(start_date=start_str, end_date=end_str)
            
            trading_days = []
            while rs.next():
                row = rs.get_row_data()
                if row[1] == '1':  # is_trading_day
                    trading_days.append(date.fromisoformat(row[0]))
            
            bs.logout()
            return trading_days
            
        except Exception as e:
            logger.error(f"[AkShare] Error fetching trading days: {e}")
            return []
    
    def fetch_valuation_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的估值数据
        
        从 stock_zh_a_spot_em() 提取 PE/PB/市值数据。
        """
        logger.info(f"[AkShare] Fetching valuation from spot data...")
        
        try:
            df = self._ak.stock_zh_a_spot_em()
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            records = []
            for _, row in df.iterrows():
                code = convert_akshare_code_to_standard(row.get('代码', row.get('股票代码', '')))
                name = str(row.get('名称', ''))
                
                if not code or code in ('sh.', 'sz.', 'bj.'):
                    continue
                
                # 市值单位转换: akshare 返回的是元，转为亿元
                total_mv = self._safe_decimal(row.get('总市值'))
                circ_mv_raw = self._safe_decimal(row.get('流通市值'))
                
                # 优先使用 akshare 返回的流通市值
                amount = self._safe_decimal(row.get('成交额'))
                turn = self._safe_decimal(row.get('换手率'))
                if circ_mv_raw:
                    circ_mv = Decimal(str(float(circ_mv_raw) / 100000000))  # 转为亿元
                elif amount and turn and float(turn) > 0:
                    circ_mv = Decimal(str((float(amount) * 100 / float(turn)) / 100000000))
                else:
                    circ_mv = None
                
                records.append({
                    'code': code,
                    'trade_date': trade_date,
                    'pe_ttm': self._safe_decimal(row.get('市盈率-动态')),
                    'pb_mrq': self._safe_decimal(row.get('市净率')),
                    'total_mv': Decimal(str(float(total_mv) / 100000000)) if total_mv else None,
                    'circ_mv': circ_mv,
                    'is_st': 1 if 'ST' in name else 0,
                    'turn': self._safe_decimal(row.get('换手率')),
                })
            
            return pd.DataFrame(records)
            
        except Exception as e:
            logger.error(f"[AkShare] Error fetching valuation: {e}")
            raise
