"""
数据源工厂模块

提供统一的数据源获取接口，根据配置返回对应的数据源实例。
"""

import logging
import os
from typing import Optional

from .base import BaseDataSource

logger = logging.getLogger(__name__)

# 全局单例缓存
_data_source_cache: Optional[BaseDataSource] = None


def get_data_source(source_name: Optional[str] = None) -> BaseDataSource:
    """
    获取数据源实例
    
    Args:
        source_name: 数据源名称，可选值：
            - 'tushare': TuShare Pro（默认）
            - 'akshare': AkShare
            如果不指定，从环境变量 DATA_SOURCE 读取，默认为 'tushare'
    
    Returns:
        数据源实例
    
    Raises:
        ValueError: 不支持的数据源类型
    """
    global _data_source_cache
    
    # 确定数据源名称
    if source_name is None:
        source_name = os.environ.get('DATA_SOURCE', 'tushare').lower()
    
    # 如果有缓存且类型匹配，直接返回
    if _data_source_cache is not None and _data_source_cache.name == source_name:
        return _data_source_cache
    
    # 创建新实例
    if source_name == 'tushare':
        from .tushare_source import TuShareDataSource
        _data_source_cache = TuShareDataSource()
        logger.info("Using TuShare data source")
    elif source_name == 'akshare':
        from .akshare_source import AkShareDataSource
        _data_source_cache = AkShareDataSource()
        logger.info("Using AkShare data source")
    else:
        raise ValueError(f"Unsupported data source: {source_name}. Use 'tushare' or 'akshare'.")
    
    return _data_source_cache


def clear_data_source_cache():
    """清除数据源缓存，下次调用 get_data_source() 时会创建新实例"""
    global _data_source_cache
    _data_source_cache = None


__all__ = [
    'BaseDataSource',
    'get_data_source',
    'clear_data_source_cache',
]
