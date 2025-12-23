"""
AKShare 数据源实现

提供 ETF、指数成分、行业分类、北向资金、机构持仓等功能。
优点：功能全面、支持 ETF、各种市场数据
缺点：部分接口不稳定、速度较慢
"""

import pandas as pd
from typing import List, Optional
from datetime import date, datetime, timedelta

from . import register_source
from .base import DataSource


@register_source('akshare')
class AKShareSource(DataSource):
    """AKShare 数据源"""

    name = "akshare"
    description = "AKShare - 功能全面的金融数据源，支持 ETF、指数、行业等"
    supports_concurrent = False
    rate_limit = 0.1  # 10 req/s (保守估计)

    supported_data_types = [
        'stock_list', 'stock_daily', 'etf_list', 'etf_daily',
        'index_constituents', 'industry', 'northbound', 'institutional', 'market_cap'
    ]

    def __init__(self):
        super().__init__()
        self._ak = None

    def connect(self) -> bool:
        """初始化 AKShare（无需登录）"""
        if self._connected:
            return True

        try:
            import akshare as ak
            self._ak = ak
            self._connected = True
            return True
        except ImportError:
            print("akshare 未安装，请运行: pip install akshare")
            return False

    def get_stock_list(self) -> pd.DataFrame:
        """
        获取 A 股列表

        Returns:
            DataFrame with columns: code, name, market, list_date
        """
        if not self._connected:
            self.connect()

        try:
            # 使用 stock_info_a_code_name 获取股票列表
            df = self._ak.stock_info_a_code_name()

            result = pd.DataFrame({
                'code': df['code'].apply(self.normalize_code),
                'name': df['name'],
                'market': df['code'].apply(lambda x: 'sh' if x.startswith('6') else 'sz'),
                'list_date': None,  # AKShare 此接口不提供上市日期
            })

            return result

        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()

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
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjust: 复权类型 ('qfq', 'hfq', 'none')

        Returns:
            DataFrame with columns: date, open, high, low, close, volume, amount
        """
        if not self._connected:
            self.connect()

        try:
            # 提取纯数字代码
            code_num = self.extract_code_number(code)

            # AKShare adjust 参数: "qfq", "hfq", ""
            ak_adjust = "" if adjust == "none" else adjust

            df = self._ak.stock_zh_a_hist(
                symbol=code_num,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust=ak_adjust
            )

            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': 'pctChg',
                '涨跌额': 'change',
                '换手率': 'turn',
            })

            df['code'] = self.normalize_code(code)

            return df

        except Exception as e:
            print(f"获取 {code} 日线数据失败: {e}")
            return pd.DataFrame()

    def get_etf_list(self) -> pd.DataFrame:
        """
        获取 ETF 列表

        Returns:
            DataFrame with columns: code, name, list_date
        """
        if not self._connected:
            self.connect()

        try:
            # 获取场内 ETF 列表
            df = self._ak.fund_etf_spot_em()

            result = pd.DataFrame({
                'code': df['代码'].apply(self.normalize_code),
                'name': df['名称'],
                'list_date': None,
            })

            return result

        except Exception as e:
            print(f"获取 ETF 列表失败: {e}")
            return pd.DataFrame()

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
        if not self._connected:
            self.connect()

        try:
            code_num = self.extract_code_number(code)

            df = self._ak.fund_etf_hist_em(
                symbol=code_num,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )

            if df is None or df.empty:
                return pd.DataFrame()

            # 标准化列名
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
            })

            df['code'] = self.normalize_code(code)

            return df

        except Exception as e:
            print(f"获取 ETF {code} 日线数据失败: {e}")
            return pd.DataFrame()

    def get_market_cap_batch(self, codes: List[str], target_date: str) -> pd.DataFrame:
        """
        批量获取市值数据（使用 stock_zh_a_spot_em 全量获取）

        Args:
            codes: 股票代码列表（会忽略，直接全量获取）
            target_date: 目标日期

        Returns:
            DataFrame with columns: code, date, total_mv, circ_mv, pe, pb
        """
        if not self._connected:
            self.connect()

        try:
            # 获取全市场实时数据
            df = self._ak.stock_zh_a_spot_em()

            if df is None or df.empty:
                return pd.DataFrame()

            # 提取需要的字段
            result = pd.DataFrame({
                'code': df['代码'].apply(self.normalize_code),
                'name': df['名称'],
                'date': target_date,
                'close': pd.to_numeric(df['最新价'], errors='coerce'),
                'total_mv': pd.to_numeric(df['总市值'], errors='coerce') / 1e8,  # 转换为亿元
                'circ_mv': pd.to_numeric(df['流通市值'], errors='coerce') / 1e8,
                'pe': pd.to_numeric(df['市盈率-动态'], errors='coerce'),
                'pb': pd.to_numeric(df['市净率'], errors='coerce'),
            })

            # 如果指定了代码列表，过滤
            if codes:
                codes_normalized = [self.normalize_code(c) for c in codes]
                result = result[result['code'].isin(codes_normalized)]

            return result

        except Exception as e:
            print(f"获取市值数据失败: {e}")
            return pd.DataFrame()

    def get_market_cap_all(self, target_date: str = None) -> pd.DataFrame:
        """
        获取全市场市值数据

        Args:
            target_date: 目标日期（仅用于标记，实际获取实时数据）

        Returns:
            DataFrame with columns: code, date, total_mv, circ_mv, pe, pb
        """
        if target_date is None:
            target_date = date.today().strftime('%Y-%m-%d')

        return self.get_market_cap_batch([], target_date)

    def get_index_constituents(self, index_code: str) -> pd.DataFrame:
        """
        获取指数成分股

        Args:
            index_code: 指数代码 (000300, 000905, etc)

        Returns:
            DataFrame with columns: index_code, stock_code, stock_name, weight
        """
        if not self._connected:
            self.connect()

        try:
            df = self._ak.index_stock_cons(symbol=index_code)

            if df is None or df.empty:
                return pd.DataFrame()

            # 解析股票代码列
            code_col = None
            for col in ['品种代码', '证券代码', '成份券代码', 'code', '代码']:
                if col in df.columns:
                    code_col = col
                    break

            if code_col is None:
                print(f"无法找到代码列: {df.columns.tolist()}")
                return pd.DataFrame()

            # 解析股票名称列
            name_col = None
            for col in ['品种名称', '证券名称', '成份券名称', 'name', '名称']:
                if col in df.columns:
                    name_col = col
                    break

            result = pd.DataFrame({
                'index_code': index_code,
                'stock_code': df[code_col].apply(self.normalize_code),
                'stock_name': df[name_col] if name_col else None,
                'weight': None,  # 基础接口可能不含权重
            })

            return result

        except Exception as e:
            print(f"获取指数 {index_code} 成分股失败: {e}")
            return pd.DataFrame()

    def get_industry_classification(self, system: str = "em") -> pd.DataFrame:
        """
        获取行业分类

        Args:
            system: 分类体系 ('em'东方财富, 'sw'申万)

        Returns:
            DataFrame with columns: code, name, industry_code, industry_name
        """
        if not self._connected:
            self.connect()

        try:
            all_data = []

            if system == "em":
                # 东方财富行业分类
                boards = self._ak.stock_board_industry_name_em()

                for _, row in boards.iterrows():
                    board_name = row['板块名称']
                    try:
                        stocks = self._ak.stock_board_industry_cons_em(symbol=board_name)
                        if stocks is not None and not stocks.empty:
                            for _, stock in stocks.iterrows():
                                all_data.append({
                                    'code': self.normalize_code(stock['代码']),
                                    'name': stock['名称'],
                                    'industry_code': board_name,
                                    'industry_name': board_name,
                                    'system': 'em',
                                })
                    except Exception:
                        continue

            elif system == "sw":
                # 申万行业分类
                boards = self._ak.sw_index_first_info()

                for _, row in boards.iterrows():
                    board_code = row['行业代码']
                    board_name = row['行业名称']
                    try:
                        stocks = self._ak.sw_index_cons(symbol=board_code)
                        if stocks is not None and not stocks.empty:
                            for _, stock in stocks.iterrows():
                                all_data.append({
                                    'code': self.normalize_code(stock['股票代码']),
                                    'name': stock['股票名称'],
                                    'industry_code': board_code,
                                    'industry_name': board_name,
                                    'system': 'sw',
                                })
                    except Exception:
                        continue

            return pd.DataFrame(all_data)

        except Exception as e:
            print(f"获取行业分类失败: {e}")
            return pd.DataFrame()

    def get_northbound_holdings(self, target_date: str = None) -> pd.DataFrame:
        """
        获取北向持仓数据

        Args:
            target_date: 目标日期，None 表示最新

        Returns:
            DataFrame with columns: code, name, hold_shares, hold_ratio, hold_value
        """
        if not self._connected:
            self.connect()

        try:
            # 获取沪股通持仓
            df_sh = self._ak.stock_hsgt_hold_stock_em(market="沪股通")
            # 获取深股通持仓
            df_sz = self._ak.stock_hsgt_hold_stock_em(market="深股通")

            df = pd.concat([df_sh, df_sz], ignore_index=True)

            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame({
                'code': df['代码'].apply(self.normalize_code),
                'name': df['名称'],
                'hold_shares': pd.to_numeric(df['持股数量'], errors='coerce'),
                'hold_ratio': pd.to_numeric(df['持股比例'], errors='coerce'),
                'hold_value': pd.to_numeric(df['持股市值'], errors='coerce'),
                'date': target_date or date.today().strftime('%Y-%m-%d'),
            })

            return result

        except Exception as e:
            print(f"获取北向持仓失败: {e}")
            return pd.DataFrame()

    def get_institutional_holdings(self, quarter: str) -> pd.DataFrame:
        """
        获取机构持仓数据（基金重仓股）

        Args:
            quarter: 季度 (如 '20240930')

        Returns:
            DataFrame with columns: code, name, institution, hold_shares, hold_ratio
        """
        if not self._connected:
            self.connect()

        try:
            # 获取基金重仓股
            df = self._ak.stock_report_fund_hold_detail(date=quarter)

            if df is None or df.empty:
                return pd.DataFrame()

            result = pd.DataFrame({
                'code': df['代码'].apply(self.normalize_code) if '代码' in df.columns else df['股票代码'].apply(self.normalize_code),
                'name': df.get('名称', df.get('股票简称')),
                'fund_count': pd.to_numeric(df.get('基金家数', df.get('持股基金家数')), errors='coerce'),
                'hold_shares': pd.to_numeric(df.get('持股总数', df.get('持股数量')), errors='coerce'),
                'hold_value': pd.to_numeric(df.get('持股市值', 0), errors='coerce'),
                'quarter': quarter,
            })

            return result

        except Exception as e:
            print(f"获取机构持仓失败: {e}")
            return pd.DataFrame()
