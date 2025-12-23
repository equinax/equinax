"""
BaoStock 数据源实现

提供 A 股日线数据、股票列表等功能。
优点：免费、稳定、数据质量好
缺点：不支持 ETF、需要登录
"""

import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import date

from . import register_source
from .base import DataSource


@register_source('baostock')
class BaoStockSource(DataSource):
    """BaoStock 数据源"""

    name = "baostock"
    description = "BaoStock - 免费 A 股数据源，支持日线、复权"
    supports_concurrent = False
    rate_limit = 0.02  # 50 req/s

    supported_data_types = ['stock_list', 'stock_daily']

    def __init__(self):
        super().__init__()
        self._bs = None

    def connect(self) -> bool:
        """登录 BaoStock"""
        if self._connected:
            return True

        try:
            import baostock as bs
            self._bs = bs
            lg = bs.login()
            if lg.error_code != '0':
                print(f"BaoStock 登录失败: {lg.error_msg}")
                return False
            self._connected = True
            print("BaoStock 登录成功")
            return True
        except ImportError:
            print("baostock 未安装，请运行: pip install baostock")
            return False
        except Exception as e:
            print(f"BaoStock 连接失败: {e}")
            return False

    def disconnect(self):
        """登出 BaoStock"""
        if self._connected and self._bs:
            self._bs.logout()
            self._connected = False
            print("BaoStock 已登出")

    def get_stock_list(self) -> pd.DataFrame:
        """
        获取 A 股列表

        Returns:
            DataFrame with columns: code, name, market, list_date
        """
        if not self._connected:
            self.connect()

        # 查询当前所有股票
        rs = self._bs.query_stock_basic()
        if rs.error_code != '0':
            print(f"获取股票列表失败: {rs.error_msg}")
            return pd.DataFrame()

        data = []
        while rs.next():
            row = rs.get_row_data()
            data.append(row)

        df = pd.DataFrame(data, columns=rs.fields)

        # 只保留 A 股 (type=1 股票, status=1 上市)
        df = df[(df['type'] == '1') & (df['status'] == '1')]

        # 标准化列名
        result = pd.DataFrame({
            'code': df['code'],  # 已是 sh.600000 格式
            'name': df['code_name'],
            'market': df['code'].str[:2],
            'list_date': df['ipoDate'],
        })

        return result

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
            adjust: 复权类型 ('qfq', 'hfq', 'none')

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount, ...
        """
        if not self._connected:
            self.connect()

        # 标准化代码
        code = self.normalize_code(code)

        # BaoStock adjustflag: 1=后复权, 2=前复权, 3=不复权
        adjust_map = {'hfq': '1', 'qfq': '2', 'none': '3'}
        adjustflag = adjust_map.get(adjust, '1')

        # 查询日线数据
        fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"

        rs = self._bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjustflag
        )

        if rs.error_code != '0':
            print(f"获取 {code} 日线数据失败: {rs.error_msg}")
            return pd.DataFrame()

        data = []
        while rs.next():
            data.append(rs.get_row_data())

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=rs.fields)

        # 转换数据类型
        numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def get_daily_data_batch(
        self,
        codes: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "hfq"
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取日线数据

        BaoStock 不支持真正的批量查询，但可以复用连接
        """
        if not self._connected:
            self.connect()

        result = {}
        for i, code in enumerate(codes):
            try:
                df = self.get_daily_data(code, start_date, end_date, adjust)
                if df is not None and not df.empty:
                    result[code] = df

                # 进度提示（每100个）
                if (i + 1) % 100 == 0:
                    print(f"  已处理 {i + 1}/{len(codes)} 只股票")

            except Exception as e:
                print(f"  获取 {code} 数据失败: {e}")

        return result

    def get_adjust_factors(self, code: str) -> pd.DataFrame:
        """
        获取复权因子

        Args:
            code: 股票代码

        Returns:
            DataFrame with columns: dividOperateDate, foreAdjustFactor, backAdjustFactor, ...
        """
        if not self._connected:
            self.connect()

        code = self.normalize_code(code)

        rs_list = []
        rs_factor = self._bs.query_adjust_factor(
            code=code,
            start_date="1990-01-01",
            end_date=date.today().strftime("%Y-%m-%d")
        )

        while rs_factor.next():
            rs_list.append(rs_factor.get_row_data())

        if not rs_list:
            return pd.DataFrame()

        return pd.DataFrame(rs_list, columns=rs_factor.fields)

    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日历

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        if not self._connected:
            self.connect()

        rs = self._bs.query_trade_dates(start_date=start_date, end_date=end_date)

        dates = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == '1':  # is_trading_day
                dates.append(row[0])

        return dates
