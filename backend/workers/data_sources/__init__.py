"""
数据源工厂模块

提供统一的数据源获取接口。
TuShare Pro 是唯一支持的数据源。
"""

import logging
from typing import Optional

from .base import BaseDataSource

logger = logging.getLogger(__name__)

# 全局单例缓存
_data_source_cache: Optional[BaseDataSource] = None


def get_data_source(source_name: Optional[str] = None) -> BaseDataSource:
    """
    获取数据源实例

    Args:
        source_name: 数据源名称（已废弃，保留参数仅为兼容性）。
            TuShare Pro 是唯一支持的数据源。

    Returns:
        TuShare 数据源实例
    """
    global _data_source_cache

    # 如果有缓存，直接返回
    if _data_source_cache is not None:
        return _data_source_cache

    # 创建 TuShare 实例
    from .tushare_source import TuShareDataSource

    _data_source_cache = TuShareDataSource()
    logger.info("Using TuShare data source")

    return _data_source_cache


def clear_data_source_cache():
    """清除数据源缓存，下次调用 get_data_source() 时会创建新实例"""
    global _data_source_cache
    _data_source_cache = None


__all__ = [
    "BaseDataSource",
    "get_data_source",
    "clear_data_source_cache",
]
