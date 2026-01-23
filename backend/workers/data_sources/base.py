"""
数据源抽象层

提供统一的数据获取接口，支持多个数据源切换。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import pandas as pd


@dataclass
class DailyBar:
    """标准化的日线数据结构"""
    code: str           # 标准化代码: sh.600000, sz.000001
    trade_date: date
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    pre_close: Optional[Decimal]
    volume: Optional[int]       # 股数
    amount: Optional[Decimal]   # 成交额（元）
    pct_chg: Optional[Decimal]  # 涨跌幅（%）
    turn: Optional[Decimal]     # 换手率（%）


@dataclass
class ValuationData:
    """估值数据结构"""
    code: str
    trade_date: date
    pe_ttm: Optional[Decimal]
    pb_mrq: Optional[Decimal]
    total_mv: Optional[Decimal]   # 总市值（亿元）
    circ_mv: Optional[Decimal]    # 流通市值（亿元）
    is_st: int


@dataclass
class AdjFactor:
    """复权因子数据结构"""
    code: str
    trade_date: date
    adj_factor: Decimal


class BaseDataSource(ABC):
    """
    数据源抽象基类
    
    所有数据源必须实现这些方法，返回标准化的数据结构。
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        pass
    
    @abstractmethod
    def fetch_stock_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场股票日线数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            DataFrame with columns:
            - code: str (sh.600000 格式)
            - trade_date: date
            - open, high, low, close, pre_close: Decimal
            - volume: int (股数)
            - amount: Decimal (元)
            - pct_chg: Decimal (%)
        """
        pass
    
    @abstractmethod
    def fetch_etf_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的全市场 ETF 日线数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            DataFrame，格式同 fetch_stock_daily_by_date
        """
        pass
    
    @abstractmethod
    def fetch_index_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的指数日线数据
        
        Args:
            trade_date: 交易日期
            
        Returns:
            DataFrame，格式同 fetch_stock_daily_by_date
        """
        pass
    
    @abstractmethod
    def fetch_stock_adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的股票复权因子
        
        Args:
            trade_date: 交易日期
            
        Returns:
            DataFrame with columns:
            - code: str
            - trade_date: date
            - adj_factor: Decimal
        """
        pass
    
    @abstractmethod
    def fetch_etf_adj_factor(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        获取 ETF 复权因子（日期范围）
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame with columns:
            - code: str
            - trade_date: date  
            - adj_factor: Decimal
        """
        pass
    
    @abstractmethod
    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """
        获取日期范围内的交易日列表
        
        Args:
            start_date: 开始日期（包含）
            end_date: 结束日期（包含）
            
        Returns:
            交易日期列表
        """
        pass
    
    def fetch_valuation_by_date(self, trade_date: date) -> pd.DataFrame:
        """
        获取指定日期的估值数据（可选实现）
        
        默认返回空 DataFrame，子类可覆盖。
        
        Returns:
            DataFrame with columns:
            - code: str
            - trade_date: date
            - pe_ttm, pb_mrq: Decimal
            - total_mv, circ_mv: Decimal (亿元)
            - is_st: int
        """
        return pd.DataFrame()


def convert_tushare_code_to_standard(ts_code: str) -> str:
    """
    将 TuShare 代码格式转换为标准格式
    
    TuShare: 000001.SZ, 600000.SH
    标准: sz.000001, sh.600000
    """
    if not ts_code or '.' not in ts_code:
        return ts_code
    
    code, exchange = ts_code.split('.')
    exchange = exchange.lower()
    return f"{exchange}.{code}"


def convert_standard_code_to_tushare(std_code: str) -> str:
    """
    将标准代码格式转换为 TuShare 格式
    
    标准: sz.000001, sh.600000
    TuShare: 000001.SZ, 600000.SH
    """
    if not std_code or '.' not in std_code:
        return std_code
    
    exchange, code = std_code.split('.')
    exchange = exchange.upper()
    return f"{code}.{exchange}"


def convert_akshare_code_to_standard(code: str) -> str:
    """
    将 AkShare 代码格式转换为标准格式
    
    AkShare: 600000, 000001
    标准: sh.600000, sz.000001
    """
    code = str(code).zfill(6)
    
    if code.startswith('6'):
        return f"sh.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"sz.{code}"
    elif code.startswith('4') or code.startswith('8'):
        return f"bj.{code}"
    elif code.startswith('5'):  # ETF 上交所
        return f"sh.{code}"
    elif code.startswith('1'):  # ETF 深交所
        return f"sz.{code}"
    else:
        return f"sh.{code}"
