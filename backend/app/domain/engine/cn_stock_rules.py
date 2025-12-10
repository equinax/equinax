"""
A股交易规则实现

包含:
- CNStockCommission: A股佣金模型（佣金 + 印花税 + 最低佣金）
- AShareSizer: 整手交易（100股倍数）
- PriceLimitChecker: 涨跌停检查
- TPlus1Filter: T+1 交易过滤器
"""

import backtrader as bt
from datetime import datetime, date
from typing import Optional, Dict, Any


class CNStockCommission(bt.CommInfoBase):
    """
    A股佣金模型

    费用结构:
    - 佣金: 默认万2.5 (买卖双向)，可自定义
    - 印花税: 0.05% (仅卖出)
    - 最低佣金: 5元

    使用方式:
        cn_commission = CNStockCommission(commission=0.00025)
        cerebro.broker.addcommissioninfo(cn_commission)
    """

    params = (
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('percabs', True),
        ('stamp_duty', 0.0005),      # 印花税 0.05% (仅卖出)
        ('commission', 0.00025),     # 佣金 万2.5 (可自定义)
        ('min_commission', 5.0),     # 最低佣金 5元
    )

    def _getcommission(self, size, price, pseudoexec):
        """
        计算实际佣金

        Args:
            size: 交易数量 (正数买入, 负数卖出)
            price: 成交价格
            pseudoexec: 是否为伪执行 (用于检查)

        Returns:
            float: 佣金金额
        """
        trade_value = abs(size * price)

        # 基础佣金
        commission = trade_value * self.p.commission

        # 最低佣金限制
        if commission < self.p.min_commission:
            commission = self.p.min_commission

        # 卖出时加收印花税
        if size < 0:  # 卖出
            commission += trade_value * self.p.stamp_duty

        return commission


class AShareSizer(bt.Sizer):
    """
    A股整手交易 Sizer

    规则:
    - 买入: 必须是100股的整数倍，向下取整
    - 卖出: 可卖全部持仓 (含零股)

    使用方式:
        cerebro.addsizer(AShareSizer, percents=95)
    """

    params = (
        ('percents', 95),      # 使用现金的百分比
        ('lot_size', 100),     # 一手股数
    )

    def _getsizing(self, comminfo, cash, data, isbuy):
        """
        计算交易数量

        Args:
            comminfo: 佣金信息
            cash: 可用现金
            data: 数据源
            isbuy: 是否买入

        Returns:
            int: 交易股数 (100的整数倍)
        """
        if isbuy:
            # 计算可用资金
            available = cash * self.p.percents / 100

            # 获取当前价格
            price = data.close[0]
            if price <= 0:
                return 0

            # 预留佣金空间 (约0.1%)
            available *= 0.999

            # 计算整手数量
            shares = int(available / price)
            lots = shares // self.p.lot_size

            return lots * self.p.lot_size
        else:
            # 卖出全部持仓
            return self.broker.getposition(data).size


class PriceLimitChecker:
    """
    涨跌停检查器

    规则:
    - 普通股票: +-10%
    - ST/ST*股票: +-5%
    - 科创板(688xxx)/创业板(300xxx, 301xxx): +-20%

    使用方式:
        checker = PriceLimitChecker()
        result = checker.check_price_limit(current_price, preclose, 'sz.000001')
    """

    @staticmethod
    def get_limit_ratio(stock_code: str, is_st: bool = False) -> float:
        """
        获取涨跌停幅度

        Args:
            stock_code: 股票代码 (支持 sh.xxx, sz.xxx 或纯数字格式)
            is_st: 是否为 ST 股票

        Returns:
            float: 涨跌停幅度 (0.05, 0.10, 0.20)
        """
        if is_st:
            return 0.05

        # 标准化代码
        code = stock_code.replace('sh.', '').replace('sz.', '')

        # 科创板 688xxx
        if code.startswith('688'):
            return 0.20

        # 创业板 300xxx, 301xxx
        if code.startswith('300') or code.startswith('301'):
            return 0.20

        # 普通股票
        return 0.10

    @staticmethod
    def calculate_limit_prices(preclose: float, stock_code: str, is_st: bool = False) -> Dict[str, float]:
        """
        计算涨跌停价格

        Args:
            preclose: 前收盘价
            stock_code: 股票代码
            is_st: 是否为 ST 股票

        Returns:
            dict: {upper_limit, lower_limit, limit_ratio}
        """
        limit_ratio = PriceLimitChecker.get_limit_ratio(stock_code, is_st)

        # A股价格精度为分 (0.01)
        upper_limit = round(preclose * (1 + limit_ratio), 2)
        lower_limit = round(preclose * (1 - limit_ratio), 2)

        # 跌停价不能低于0.01
        lower_limit = max(lower_limit, 0.01)

        return {
            'upper_limit': upper_limit,
            'lower_limit': lower_limit,
            'limit_ratio': limit_ratio,
        }

    @staticmethod
    def check_price_limit(
        current_price: float,
        preclose: float,
        stock_code: str,
        is_st: bool = False
    ) -> Dict[str, Any]:
        """
        检查是否涨跌停

        Args:
            current_price: 当前价格
            preclose: 前收盘价
            stock_code: 股票代码
            is_st: 是否为 ST 股票

        Returns:
            dict: {
                at_upper_limit: bool,  # 是否涨停
                at_lower_limit: bool,  # 是否跌停
                upper_limit: float,    # 涨停价
                lower_limit: float,    # 跌停价
                can_buy: bool,         # 是否可买入
                can_sell: bool,        # 是否可卖出
            }
        """
        limits = PriceLimitChecker.calculate_limit_prices(preclose, stock_code, is_st)

        at_upper_limit = current_price >= limits['upper_limit']
        at_lower_limit = current_price <= limits['lower_limit']

        return {
            'at_upper_limit': at_upper_limit,
            'at_lower_limit': at_lower_limit,
            'upper_limit': limits['upper_limit'],
            'lower_limit': limits['lower_limit'],
            'can_buy': not at_upper_limit,   # 涨停不能买
            'can_sell': not at_lower_limit,  # 跌停不能卖
        }


class TPlus1Filter:
    """
    T+1 交易过滤器

    A股规则: 当日买入的股票不能当日卖出，需要等到下一个交易日

    使用方式 (在策略中混入):
        class MyStrategy(bt.Strategy, TPlus1Filter):
            def __init__(self):
                super().__init__()
                TPlus1Filter.__init__(self)

            def next(self):
                if self.position.size > 0:
                    if self.can_sell(self.data, self.datetime.datetime()):
                        self.close()

            def notify_order(self, order):
                if order.status == order.Completed and order.isbuy():
                    self.record_buy(self.data, self.datetime.datetime())
    """

    def __init__(self):
        self._buy_dates: Dict[str, date] = {}  # data._name -> buy_date

    def record_buy(self, data, dt: datetime):
        """
        记录买入日期

        Args:
            data: Backtrader 数据源
            dt: 买入时间
        """
        data_name = getattr(data, '_name', str(id(data)))
        self._buy_dates[data_name] = dt.date() if isinstance(dt, datetime) else dt

    def can_sell(self, data, current_dt: datetime) -> bool:
        """
        检查是否可以卖出 (T+1 规则)

        Args:
            data: Backtrader 数据源
            current_dt: 当前时间

        Returns:
            bool: True 表示可以卖出
        """
        data_name = getattr(data, '_name', str(id(data)))
        buy_date = self._buy_dates.get(data_name)

        if buy_date is None:
            return True  # 没有买入记录，可能是初始持仓

        # 比较日期部分
        current_date = current_dt.date() if isinstance(current_dt, datetime) else current_dt
        return current_date > buy_date

    def clear_position(self, data):
        """
        清除持仓记录 (卖出后调用)

        Args:
            data: Backtrader 数据源
        """
        data_name = getattr(data, '_name', str(id(data)))
        self._buy_dates.pop(data_name, None)

    def get_buy_date(self, data) -> Optional[date]:
        """
        获取买入日期

        Args:
            data: Backtrader 数据源

        Returns:
            date or None: 买入日期
        """
        data_name = getattr(data, '_name', str(id(data)))
        return self._buy_dates.get(data_name)
