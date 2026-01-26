"""
涨停聚焦 API 端点

提供涨停股票列表、统计等功能。
"""

from datetime import date
from typing import List, Optional
from enum import Enum

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.limit_up_service import get_limit_up_stocks, get_limit_up_summary

router = APIRouter()


# ============================================
# Enums
# ============================================

class TimeRange(str, Enum):
    """时间范围筛选"""
    WEEK = "7"
    MONTH = "30"
    QUARTER = "90"
    YEAR = "365"


class ConsecutiveFilter(str, Enum):
    """连板筛选"""
    ALL = "0"           # 全部
    FIRST = "1"         # 首板
    TWO_PLUS = "2"      # 2连板+
    THREE_PLUS = "3"    # 3连板+
    FIVE_PLUS = "5"     # 5连板+


class CountFilter(str, Enum):
    """涨停次数筛选"""
    ALL = "0"
    THREE_PLUS = "3"
    FIVE_PLUS = "5"
    TEN_PLUS = "10"


# ============================================
# Pydantic Schemas
# ============================================

class LimitUpStockItem(BaseModel):
    """涨停股票信息"""
    code: str = Field(..., description="股票代码")
    name: str = Field(..., description="股票名称")
    latest_date: date = Field(..., description="最近涨停日期")
    pct_chg: float = Field(..., description="涨停当天涨幅")
    volume: int = Field(..., description="成交量(股)")
    amount: float = Field(..., description="成交额(元)")
    turn: float = Field(..., description="换手率(%)")
    close: float = Field(..., description="收盘价")
    total_count: int = Field(..., description="近期涨停总次数")
    consecutive: int = Field(..., description="当前连板数(0=首板)")
    consecutive_label: str = Field(..., description="连板标签(如'2连板')")


class LimitUpListResponse(BaseModel):
    """涨停股票列表响应"""
    items: List[LimitUpStockItem]
    total: int = Field(..., description="总数量")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页数量")
    filters: dict = Field(default_factory=dict, description="当前筛选条件")


class LimitUpSummaryResponse(BaseModel):
    """涨停统计摘要"""
    total_limit_up: int = Field(..., description="涨停总次数")
    unique_stocks: int = Field(..., description="涨停股票数")
    date_range: dict = Field(..., description="统计日期范围")


# ============================================
# API Endpoints
# ============================================

@router.get("/limit-up", response_model=LimitUpListResponse)
async def get_limit_up_list(
    days: int = Query(default=365, ge=1, le=365, description="查询天数范围"),
    min_consecutive: int = Query(default=0, ge=0, description="最小连板数(0=全部)"),
    min_count: int = Query(default=0, ge=0, description="最小涨停次数"),
    industry: Optional[str] = Query(default=None, description="行业筛选"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=50, ge=1, le=200, description="每页数量"),
    db: AsyncSession = Depends(get_db),
):
    """
    获取涨停股票列表
    
    返回指定时间范围内有涨停记录的股票，按最近涨停日期倒序排列。
    
    涨停判定标准：
    - 主板/创业板: 涨幅 >= 9.9%
    - 科创板 (688xxx): 涨幅 >= 19.8%
    - 北交所 (8xxxxx): 涨幅 >= 29.8%
    """
    offset = (page - 1) * page_size
    
    items, total = await get_limit_up_stocks(
        session=db,
        days=days,
        min_consecutive=min_consecutive,
        min_total_count=min_count,
        industry=industry,
        limit=page_size,
        offset=offset,
    )
    
    return LimitUpListResponse(
        items=[LimitUpStockItem(**item) for item in items],
        total=total,
        page=page,
        page_size=page_size,
        filters={
            "days": days,
            "min_consecutive": min_consecutive,
            "min_count": min_count,
            "industry": industry,
        }
    )


@router.get("/limit-up/summary", response_model=LimitUpSummaryResponse)
async def get_limit_up_stats(
    days: int = Query(default=30, ge=1, le=365, description="统计天数范围"),
    db: AsyncSession = Depends(get_db),
):
    """
    获取涨停统计摘要
    
    返回指定时间范围内的涨停统计数据。
    """
    summary = await get_limit_up_summary(db, days=days)
    return LimitUpSummaryResponse(**summary)
