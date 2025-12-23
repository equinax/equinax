"""
数据源抽象基类

定义所有数据源必须实现的接口。
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import date
import pandas as pd


class DataSource(ABC):
    """数据源抽象基类"""

    # 数据源元信息
    name: str = "base"
    description: str = "Abstract data source"
    supports_concurrent: bool = False
    rate_limit: float = 0.1  # 请求间隔(秒)

    # 支持的数据类型
    supported_data_types: List[str] = []

    def __init__(self):
        """初始化数据源"""
        self._connected = False

    def connect(self) -> bool:
        """连接数据源（如需要）"""
        self._connected = True
        return True

    def disconnect(self):
        """断开数据源连接"""
        self._connected = False

    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()

    # ========== 股票列表 ==========

    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """
        获取股票列表

        Returns:
            DataFrame with columns: code, name, market, list_date
            - code: 股票代码 (sh.600000, sz.000001)
            - name: 股票名称
            - market: 市场 (sh/sz/bj)
            - list_date: 上市日期
        """
        pass

    # ========== 日线数据 ==========

    @abstractmethod
    def get_daily_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        adjust: str = "hfq"
    ) -> pd.DataFrame:
        """
        获取股票日线数据

        Args:
            code: 股票代码 (sh.600000 或 600000)
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjust: 复权类型 ('qfq'前复权, 'hfq'后复权, 'none'不复权)

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount, ...
        """
        pass

    def get_daily_data_batch(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "hfq"
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取日线数据（子类可覆盖以优化）

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            adjust: 复权类型

        Returns:
            Dict[code -> DataFrame]
        """
        result = {}
        for code in codes:
            try:
                df = self.get_daily_data(code, start_date, end_date, adjust)
                if df is not None and not df.empty:
                    result[code] = df
            except Exception as e:
                print(f"  获取 {code} 数据失败: {e}")
        return result

    # ========== 市值数据 ==========

    def get_market_cap(self, code: str, target_date: str) -> Optional[Dict[str, Any]]:
        """
        获取单只股票市值数据

        Args:
            code: 股票代码
            target_date: 目标日期 (YYYY-MM-DD)

        Returns:
            Dict with keys: code, date, total_mv, circ_mv, pe, pb
            或 None 如果不支持
        """
        raise NotImplementedError(f"{self.name} 不支持单股市值查询")

    def get_market_cap_batch(self, codes: List[str], target_date: str) -> pd.DataFrame:
        """
        批量获取市值数据

        Args:
            codes: 股票代码列表
            target_date: 目标日期

        Returns:
            DataFrame with columns: code, date, total_mv, circ_mv, pe, pb
        """
        raise NotImplementedError(f"{self.name} 不支持批量市值查询")

    def get_market_cap_all(self, target_date: str = None) -> pd.DataFrame:
        """
        获取全市场市值数据（实时或历史）

        Args:
            target_date: 目标日期，None 表示实时

        Returns:
            DataFrame with columns: code, date, total_mv, circ_mv, pe, pb
        """
        raise NotImplementedError(f"{self.name} 不支持全市场市值查询")

    # ========== ETF 数据 ==========

    def get_etf_list(self) -> pd.DataFrame:
        """
        获取 ETF 列表

        Returns:
            DataFrame with columns: code, name, list_date
        """
        raise NotImplementedError(f"{self.name} 不支持 ETF 列表")

    def get_etf_daily_data(
        self,
        code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取 ETF 日线数据

        Args:
            code: ETF 代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount
        """
        raise NotImplementedError(f"{self.name} 不支持 ETF 日线数据")

    # ========== 指数数据 ==========

    def get_index_constituents(self, index_code: str) -> pd.DataFrame:
        """
        获取指数成分股

        Args:
            index_code: 指数代码 (000300, 000905, etc)

        Returns:
            DataFrame with columns: index_code, stock_code, stock_name, weight
        """
        raise NotImplementedError(f"{self.name} 不支持指数成分股")

    # ========== 行业分类 ==========

    def get_industry_classification(self, system: str = "em") -> pd.DataFrame:
        """
        获取行业分类

        Args:
            system: 分类体系 ('em'东方财富, 'sw'申万)

        Returns:
            DataFrame with columns: code, name, industry_code, industry_name
        """
        raise NotImplementedError(f"{self.name} 不支持行业分类")

    # ========== 北向资金 ==========

    def get_northbound_holdings(self, target_date: str = None) -> pd.DataFrame:
        """
        获取北向持仓数据

        Args:
            target_date: 目标日期，None 表示最新

        Returns:
            DataFrame with columns: code, name, hold_shares, hold_ratio, hold_value
        """
        raise NotImplementedError(f"{self.name} 不支持北向持仓")

    # ========== 机构持仓 ==========

    def get_institutional_holdings(self, quarter: str) -> pd.DataFrame:
        """
        获取机构持仓数据

        Args:
            quarter: 季度 (如 '20240930')

        Returns:
            DataFrame with columns: code, name, institution, hold_shares, hold_ratio
        """
        raise NotImplementedError(f"{self.name} 不支持机构持仓")

    # ========== 工具方法 ==========

    @staticmethod
    def normalize_code(code: str) -> str:
        """
        标准化股票代码为 sh.XXXXXX 或 sz.XXXXXX 格式

        Args:
            code: 原始代码 (600000, sh600000, SH.600000, etc)

        Returns:
            标准化代码 (sh.600000)
        """
        code = str(code).strip().lower()

        # 移除可能存在的前缀
        if code.startswith(('sh', 'sz', 'bj')):
            prefix = code[:2]
            code_num = code[2:]
            if code_num.startswith('.'):
                code_num = code_num[1:]
            return f"{prefix}.{code_num}"

        # 根据代码判断市场
        if code.startswith('6'):
            return f"sh.{code}"
        elif code.startswith(('0', '3')):
            return f"sz.{code}"
        elif code.startswith(('4', '8')):
            return f"bj.{code}"
        else:
            return f"sz.{code}"

    @staticmethod
    def extract_code_number(code: str) -> str:
        """
        提取纯数字代码

        Args:
            code: 标准化代码 (sh.600000)

        Returns:
            纯数字代码 (600000)
        """
        code = str(code).strip()
        if '.' in code:
            return code.split('.')[-1]
        if code.startswith(('sh', 'sz', 'bj', 'SH', 'SZ', 'BJ')):
            return code[2:]
        return code
