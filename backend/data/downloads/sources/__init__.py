"""
多数据源架构

支持在不同数据源之间切换，提供统一的数据获取接口。

Usage:
    from data.downloads.sources import get_source, DataSource

    # 获取默认数据源
    source = get_source('baostock')
    df = source.get_stock_list()

    # 按数据类型获取推荐数据源
    source = get_source_for('stock_daily')  # -> BaoStock
    source = get_source_for('market_cap')   # -> easyquotation (if available)
"""

import os
from typing import Dict, Type, Optional

from .base import DataSource

# 数据源注册表
_SOURCES: Dict[str, Type[DataSource]] = {}


def register_source(name: str):
    """装饰器：注册数据源"""
    def decorator(cls):
        _SOURCES[name] = cls
        return cls
    return decorator


def get_source(name: str = None) -> DataSource:
    """
    获取数据源实例

    Args:
        name: 数据源名称，默认从环境变量 DATA_SOURCE 读取，否则使用 'baostock'

    Returns:
        DataSource 实例
    """
    if name is None:
        name = os.environ.get('DATA_SOURCE', 'baostock')

    if name not in _SOURCES:
        available = list(_SOURCES.keys())
        raise ValueError(f"Unknown data source: {name}. Available: {available}")

    return _SOURCES[name]()


def get_source_for(data_type: str) -> DataSource:
    """
    根据数据类型获取推荐的数据源

    Args:
        data_type: 数据类型 ('stock_daily', 'etf_daily', 'market_cap', 'index', 'industry')

    Returns:
        推荐的 DataSource 实例
    """
    # 数据类型到推荐数据源的映射
    recommendations = {
        'stock_daily': 'baostock',      # 股票日线 → BaoStock (最快、免费)
        'etf_daily': 'akshare',         # ETF日线 → AKShare
        'market_cap': 'easyquotation',  # 市值 → easyquotation (快速实时)
        'index': 'akshare',             # 指数成分 → AKShare
        'industry': 'akshare',          # 行业分类 → AKShare
        'northbound': 'akshare',        # 北向持仓 → AKShare
        'institutional': 'akshare',     # 机构持仓 → AKShare
    }

    source_name = recommendations.get(data_type, 'akshare')

    # 如果推荐的数据源不可用，回退到 akshare
    if source_name not in _SOURCES:
        source_name = 'akshare' if 'akshare' in _SOURCES else list(_SOURCES.keys())[0]

    return _SOURCES[source_name]()


def list_sources() -> Dict[str, str]:
    """列出所有可用的数据源"""
    return {name: cls.description for name, cls in _SOURCES.items()}


# 导入并注册各数据源
try:
    from .baostock_source import BaoStockSource
except ImportError:
    pass

try:
    from .akshare_source import AKShareSource
except ImportError:
    pass

try:
    from .easyquotation_source import EasyQuotationSource
except ImportError:
    pass


# 导入并发工具
from .concurrent import (
    ConcurrentDownloader,
    DownloadResult,
    download_batch_sync,
    download_batch_async,
    ProgressTracker,
)

__all__ = [
    # 数据源基类和注册
    'DataSource',
    'register_source',
    'get_source',
    'get_source_for',
    'list_sources',

    # 并发工具
    'ConcurrentDownloader',
    'DownloadResult',
    'download_batch_sync',
    'download_batch_async',
    'ProgressTracker',
]
