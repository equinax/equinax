"""
EasyQuotation 数据源实现

专门用于快速获取实时市值数据。
优点：极快（200ms 获取全市场），数据准确
缺点：仅支持实时数据，不支持历史
"""

import pandas as pd
from typing import List, Optional
from datetime import date

from . import register_source
from .base import DataSource


@register_source('easyquotation')
class EasyQuotationSource(DataSource):
    """EasyQuotation 数据源 - 快速实时行情"""

    name = "easyquotation"
    description = "EasyQuotation - 极速实时行情，200ms 获取全市场市值"
    supports_concurrent = True  # 内部已并发
    rate_limit = 0.01  # 可高频调用

    supported_data_types = ['market_cap']

    def __init__(self):
        super().__init__()
        self._quotation = None

    def connect(self) -> bool:
        """初始化 easyquotation"""
        if self._connected:
            return True

        try:
            import easyquotation
            self._eq = easyquotation
            self._quotation = easyquotation.use('sina')
            self._connected = True
            return True
        except ImportError:
            print("easyquotation 未安装，请运行: pip install easyquotation")
            return False
        except Exception as e:
            print(f"EasyQuotation 初始化失败: {e}")
            return False

    def get_stock_list(self) -> pd.DataFrame:
        """EasyQuotation 不支持股票列表，需要配合其他数据源"""
        raise NotImplementedError("EasyQuotation 不支持股票列表，请使用 BaoStock 或 AKShare")

    def get_daily_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        adjust: str = "hfq"
    ) -> pd.DataFrame:
        """EasyQuotation 不支持历史日线数据"""
        raise NotImplementedError("EasyQuotation 不支持历史日线数据，请使用 BaoStock 或 AKShare")

    def get_market_cap_all(self, target_date: str = None) -> pd.DataFrame:
        """
        获取全市场市值数据（实时）

        Args:
            target_date: 目标日期（仅用于标记，实际获取实时数据）

        Returns:
            DataFrame with columns: code, date, close, total_mv, circ_mv, pe, pb
        """
        if not self._connected:
            self.connect()

        if target_date is None:
            target_date = date.today().strftime('%Y-%m-%d')

        try:
            # 使用 sina 行情源，market_snapshot 获取全市场数据
            # prefix=True 会返回带 sh/sz 前缀的代码
            data = self._quotation.market_snapshot(prefix=True)

            if not data:
                print("未获取到行情数据")
                return pd.DataFrame()

            rows = []
            for code, quote in data.items():
                # 跳过指数、基金等非股票
                code_num = code[2:] if code.startswith(('sh', 'sz')) else code
                if not code_num.isdigit():
                    continue

                # 只保留 A 股（6开头上海、0/3开头深圳）
                if not (code_num.startswith('6') or code_num.startswith('0') or code_num.startswith('3')):
                    continue

                # 计算市值（如果有）
                # easyquotation sina 源返回的字段:
                # - now: 现价
                # - 市值相关字段可能需要从其他字段计算或直接获取

                row = {
                    'code': self.normalize_code(code),
                    'name': quote.get('name', ''),
                    'date': target_date,
                    'close': quote.get('now', 0),
                    'open': quote.get('open', 0),
                    'high': quote.get('high', 0),
                    'low': quote.get('low', 0),
                    'volume': quote.get('volume', 0),
                    'amount': quote.get('turnover', 0),
                }

                # 尝试获取市值字段（不同源可能字段不同）
                # 如果 sina 源没有市值，可以尝试 tencent 源
                if 'mktcap' in quote:
                    row['total_mv'] = quote['mktcap'] / 1e8  # 转换为亿元
                if 'lt_mktcap' in quote or 'lt_mcap' in quote:
                    row['circ_mv'] = quote.get('lt_mktcap', quote.get('lt_mcap', 0)) / 1e8

                rows.append(row)

            df = pd.DataFrame(rows)

            # 如果 sina 源没有市值数据，尝试用 tencent 源补充
            if df.empty or 'total_mv' not in df.columns:
                return self._get_market_cap_tencent(target_date)

            return df

        except Exception as e:
            print(f"EasyQuotation 获取市值失败: {e}")
            # 回退到 tencent 源
            return self._get_market_cap_tencent(target_date)

    def _get_market_cap_tencent(self, target_date: str) -> pd.DataFrame:
        """
        使用腾讯源获取市值数据（备用）

        腾讯源提供更完整的市值数据
        """
        try:
            quotation_tx = self._eq.use('tencent')
            data = quotation_tx.market_snapshot(prefix=True)

            if not data:
                return pd.DataFrame()

            rows = []
            for code, quote in data.items():
                code_num = code[2:] if code.startswith(('sh', 'sz')) else code
                if not code_num.isdigit():
                    continue

                if not (code_num.startswith('6') or code_num.startswith('0') or code_num.startswith('3')):
                    continue

                row = {
                    'code': self.normalize_code(code),
                    'name': quote.get('name', ''),
                    'date': target_date,
                    'close': quote.get('now', 0),
                    'total_mv': quote.get('w52', 0) / 1e8 if quote.get('w52') else None,  # 腾讯源市值字段
                    'circ_mv': quote.get('w51', 0) / 1e8 if quote.get('w51') else None,
                }

                rows.append(row)

            return pd.DataFrame(rows)

        except Exception as e:
            print(f"腾讯源获取市值失败: {e}")
            return pd.DataFrame()

    def get_market_cap_batch(self, codes: List[str], target_date: str) -> pd.DataFrame:
        """
        批量获取市值数据

        EasyQuotation 直接获取全市场然后过滤，比逐个获取快得多
        """
        df = self.get_market_cap_all(target_date)

        if codes and not df.empty:
            codes_normalized = [self.normalize_code(c) for c in codes]
            df = df[df['code'].isin(codes_normalized)]

        return df

    def get_realtime_quotes(self, codes: List[str] = None) -> pd.DataFrame:
        """
        获取实时行情

        Args:
            codes: 股票代码列表，None 表示全市场

        Returns:
            DataFrame with realtime quote data
        """
        if not self._connected:
            self.connect()

        try:
            if codes:
                # 指定代码列表
                codes_clean = [self.extract_code_number(c) for c in codes]
                data = self._quotation.stocks(codes_clean)
            else:
                # 全市场
                data = self._quotation.market_snapshot(prefix=True)

            if not data:
                return pd.DataFrame()

            rows = []
            for code, quote in data.items():
                row = {
                    'code': self.normalize_code(code),
                    'name': quote.get('name', ''),
                    'now': quote.get('now', 0),
                    'open': quote.get('open', 0),
                    'high': quote.get('high', 0),
                    'low': quote.get('low', 0),
                    'close': quote.get('close', 0),  # 昨收
                    'volume': quote.get('volume', 0),
                    'amount': quote.get('turnover', 0),
                    'bid1': quote.get('bid1', 0),
                    'ask1': quote.get('ask1', 0),
                }
                rows.append(row)

            return pd.DataFrame(rows)

        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
