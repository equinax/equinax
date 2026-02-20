"""
涨停聚焦服务

提供涨停股票统计、连板计算等功能。
"""

import logging
from datetime import date, timedelta
from typing import List, Optional, Tuple
from decimal import Decimal

from sqlalchemy import select, func, and_, or_, desc, case, literal_column
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.db.models.asset import AssetMeta, MarketDaily, IndicatorValuation

logger = logging.getLogger(__name__)


def is_limit_up(code: str, pct_chg: Decimal) -> bool:
    """
    判断是否涨停

    - 主板/创业板: 涨幅 >= 9.9%
    - 科创板 (688xxx): 涨幅 >= 19.8%
    - 北交所 (8xxxxx): 涨幅 >= 29.8% (实际上北交所涨跌幅限制为30%)
    """
    if pct_chg is None:
        return False

    # 科创板 688xxx
    if code.startswith("sh.688"):
        return float(pct_chg) >= 19.8
    # 北交所 8xxxxx (code format: bj.8xxxxx)
    elif code.startswith("bj.8") or code.startswith("bj.4"):
        return float(pct_chg) >= 29.8
    # 主板/创业板
    else:
        return float(pct_chg) >= 9.9


async def get_limit_up_stocks(
    session: AsyncSession,
    days: int = 365,
    min_consecutive: int = 0,
    min_total_count: int = 0,
    industry: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> Tuple[List[dict], int]:
    """
    获取涨停股票列表

    Args:
        session: 数据库会话
        days: 查询天数范围（默认365天）
        min_consecutive: 最小连板数筛选
        min_total_count: 最小涨停次数筛选
        industry: 行业筛选
        limit: 每页数量
        offset: 偏移量

    Returns:
        (涨停股票列表, 总数)
    """
    # 计算日期范围
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    # 涨停判定条件
    # 主板/创业板: >= 9.9%, 科创板: >= 19.8%, 北交所: >= 29.8%
    limit_up_condition = or_(
        and_(
            ~MarketDaily.code.like("sh.688%"),
            ~MarketDaily.code.like("bj.%"),
            MarketDaily.pct_chg >= 9.9,
        ),
        and_(MarketDaily.code.like("sh.688%"), MarketDaily.pct_chg >= 19.8),
        and_(MarketDaily.code.like("bj.%"), MarketDaily.pct_chg >= 29.8),
    )

    # Step 1: 获取每只股票的涨停统计
    # 子查询：所有涨停记录
    limit_up_records = (
        select(
            MarketDaily.code,
            MarketDaily.date,
            MarketDaily.pct_chg,
            MarketDaily.volume,
            MarketDaily.amount,
            IndicatorValuation.turnover_rate.label("turn"),
            MarketDaily.close,
        )
        .outerjoin(
            IndicatorValuation,
            and_(
                MarketDaily.code == IndicatorValuation.code,
                MarketDaily.date == IndicatorValuation.date,
            ),
        )
        .where(
            and_(
                MarketDaily.date >= start_date,
                MarketDaily.date <= end_date,
                limit_up_condition,
            )
        )
        .subquery()
    )

    # 统计每只股票的涨停次数和最近涨停日期
    stats_query = select(
        limit_up_records.c.code,
        func.count().label("total_count"),
        func.max(limit_up_records.c.date).label("latest_date"),
    ).group_by(limit_up_records.c.code)

    # 添加涨停次数筛选
    if min_total_count > 0:
        stats_query = stats_query.having(func.count() >= min_total_count)

    stats_subquery = stats_query.subquery()

    # Step 2: 获取最近一次涨停的详细信息
    latest_limit_up = (
        select(
            limit_up_records.c.code,
            limit_up_records.c.date,
            limit_up_records.c.pct_chg,
            limit_up_records.c.volume,
            limit_up_records.c.amount,
            limit_up_records.c.turn,
            limit_up_records.c.close,
        )
        .join(
            stats_subquery,
            and_(
                limit_up_records.c.code == stats_subquery.c.code,
                limit_up_records.c.date == stats_subquery.c.latest_date,
            ),
        )
        .subquery()
    )

    # Step 3: 关联资产信息
    main_query = (
        select(
            latest_limit_up.c.code,
            latest_limit_up.c.date.label("latest_date"),
            latest_limit_up.c.pct_chg,
            latest_limit_up.c.volume,
            latest_limit_up.c.amount,
            latest_limit_up.c.turn,
            latest_limit_up.c.close,
            stats_subquery.c.total_count,
            AssetMeta.name,
        )
        .join(stats_subquery, latest_limit_up.c.code == stats_subquery.c.code)
        .outerjoin(AssetMeta, latest_limit_up.c.code == AssetMeta.code)
        .order_by(desc(latest_limit_up.c.date), desc(stats_subquery.c.total_count))
    )

    # 计算总数
    count_query = select(func.count()).select_from(main_query.subquery())
    total_result = await session.execute(count_query)
    total = total_result.scalar() or 0

    # 分页查询
    paginated_query = main_query.limit(limit).offset(offset)
    result = await session.execute(paginated_query)
    rows = result.all()

    # 转换结果
    items = []
    for row in rows:
        items.append(
            {
                "code": row.code,
                "name": row.name or "",
                "latest_date": row.latest_date,
                "pct_chg": float(row.pct_chg) if row.pct_chg else 0,
                "volume": int(row.volume) if row.volume else 0,
                "amount": float(row.amount) if row.amount else 0,
                "turn": float(row.turn) if row.turn else 0,
                "close": float(row.close) if row.close else 0,
                "total_count": row.total_count,
                "consecutive": 0,  # 需要单独计算连板
                "consecutive_label": "首板",  # 默认
            }
        )

    # Step 4: 计算连板数（对当前页的股票）
    if items:
        codes = [item["code"] for item in items]
        consecutive_map = await _calculate_consecutive_limits(session, codes, end_date)

        for item in items:
            cons = consecutive_map.get(item["code"], 0)
            item["consecutive"] = cons
            if cons == 0:
                item["consecutive_label"] = "首板"
            elif cons == 1:
                item["consecutive_label"] = "2连板"
            else:
                item["consecutive_label"] = f"{cons + 1}连板"

    # 连板筛选（后处理，因为需要先计算连板数）
    if min_consecutive > 0:
        items = [item for item in items if item["consecutive"] >= min_consecutive - 1]

    return items, total


async def _calculate_consecutive_limits(
    session: AsyncSession,
    codes: List[str],
    reference_date: date,
    lookback_days: int = 30,
) -> dict:
    """
    计算连板数

    查看最近涨停日期之前的连续涨停天数
    """
    result = {}

    # 获取最近30天的交易日数据
    start_date = reference_date - timedelta(days=lookback_days)

    query = (
        select(
            MarketDaily.code,
            MarketDaily.date,
            MarketDaily.pct_chg,
        )
        .where(
            and_(
                MarketDaily.code.in_(codes),
                MarketDaily.date >= start_date,
                MarketDaily.date <= reference_date,
            )
        )
        .order_by(MarketDaily.code, desc(MarketDaily.date))
    )

    rows = await session.execute(query)

    # 按股票分组
    stock_data = {}
    for row in rows:
        if row.code not in stock_data:
            stock_data[row.code] = []
        stock_data[row.code].append(
            {
                "date": row.date,
                "pct_chg": float(row.pct_chg) if row.pct_chg else 0,
            }
        )

    # 计算每只股票的连板数
    for code, records in stock_data.items():
        consecutive = 0

        # records 已按日期倒序排列
        for i, record in enumerate(records):
            if is_limit_up(code, Decimal(str(record["pct_chg"]))):
                consecutive += 1
            else:
                # 遇到非涨停日，停止计数
                break

        # 连板数 = 连续涨停天数 - 1（首板不算连板）
        result[code] = max(0, consecutive - 1)

    return result


async def get_limit_up_summary(
    session: AsyncSession,
    days: int = 30,
) -> dict:
    """
    获取涨停统计摘要

    Returns:
        {
            'total_limit_up': 涨停总次数,
            'unique_stocks': 涨停股票数,
            'consecutive_2plus': 2连板以上数量,
            'consecutive_3plus': 3连板以上数量,
        }
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    limit_up_condition = or_(
        and_(
            ~MarketDaily.code.like("sh.688%"),
            ~MarketDaily.code.like("bj.%"),
            MarketDaily.pct_chg >= 9.9,
        ),
        and_(MarketDaily.code.like("sh.688%"), MarketDaily.pct_chg >= 19.8),
        and_(MarketDaily.code.like("bj.%"), MarketDaily.pct_chg >= 29.8),
    )

    query = select(
        func.count().label("total_limit_up"),
        func.count(func.distinct(MarketDaily.code)).label("unique_stocks"),
    ).where(
        and_(
            MarketDaily.date >= start_date,
            MarketDaily.date <= end_date,
            limit_up_condition,
        )
    )

    result = await session.execute(query)
    row = result.one()

    return {
        "total_limit_up": row.total_limit_up,
        "unique_stocks": row.unique_stocks,
        "date_range": {
            "start": str(start_date),
            "end": str(end_date),
        },
    }
