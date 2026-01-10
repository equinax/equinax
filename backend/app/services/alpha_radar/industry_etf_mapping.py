"""
Industry-ETF Mapping Service

Maps Shenwan L1 industries to related ETF sub-categories,
enabling lookup of relevant ETFs when hovering over industry cells.
"""

from datetime import date
from typing import Any, Dict, List, Optional

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.etf_classifier import ETFClassifier


# Static mapping: SW L1 industry name -> related ETF sub-categories
SW_INDUSTRY_TO_ETF_SUBCATEGORY: Dict[str, List[str]] = {
    # 金融类
    "银行": ["银行"],
    "非银金融": ["证券", "保险", "金融", "券商"],

    # 消费类
    "食品饮料": ["消费", "食品", "白酒", "酒"],
    "家用电器": ["家电", "消费"],
    "美容护理": ["消费", "医美"],
    "商贸零售": ["消费", "零售"],
    "社会服务": ["消费", "旅游"],
    "纺织服饰": ["消费"],
    "轻工制造": ["消费", "家居"],
    "农林牧渔": ["农业", "养殖"],

    # 医药类
    "医药生物": ["医药", "医疗", "生物", "创新药", "医疗器械"],

    # TMT类
    "电子": ["半导体", "芯片", "电子"],
    "计算机": ["软件", "信创", "云计算", "计算机", "人工智能", "AI"],
    "通信": ["通信", "5G"],
    "传媒": ["传媒", "游戏", "影视", "文化", "动漫"],

    # 周期类
    "有色金属": ["有色", "稀土", "锂电"],
    "钢铁": ["钢铁"],
    "基础化工": ["化工"],
    "煤炭": ["煤炭"],
    "石油石化": ["能源", "石油"],
    "建筑材料": ["建材"],
    "建筑装饰": ["建筑", "基建"],

    # 制造类
    "机械设备": ["机械", "机器人", "工业母机", "高端装备"],
    "国防军工": ["军工", "国防"],
    "汽车": ["汽车", "新能源车", "智能汽车"],
    "电力设备": ["新能源", "光伏", "风电", "储能", "电池"],

    # 公用类
    "公用事业": ["公用", "电力", "水务", "环保"],
    "交通运输": ["交通", "物流", "航运", "航空"],

    # 地产类
    "房地产": ["地产", "房地产"],

    # 其他
    "环保": ["环保", "碳中和"],
    "综合": [],
}


class IndustryETFMappingService:
    """Service for mapping industries to related ETFs."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.classifier = ETFClassifier()

    async def get_related_etfs(
        self,
        industry: str,
        target_date: Optional[date] = None,
        limit: int = 5,
    ) -> Dict[str, Any]:
        """
        Get ETFs related to a given SW L1 industry.

        Args:
            industry: SW L1 industry name (e.g., "银行", "传媒")
            target_date: Date to fetch ETF data for (defaults to latest)
            limit: Maximum number of ETFs to return

        Returns:
            Dict with:
              - industry: The queried industry name
              - sub_categories: Matched ETF sub-categories
              - etfs: List of related ETFs with code, name, change_pct, amount
        """
        # Get sub-categories for this industry
        sub_categories = SW_INDUSTRY_TO_ETF_SUBCATEGORY.get(industry, [])

        if not sub_categories:
            return {
                "industry": industry,
                "sub_categories": [],
                "etfs": [],
            }

        # Build keyword pattern for matching ETF names
        # Match any sub-category keyword in the ETF name
        keyword_pattern = "|".join(sub_categories)

        # Get the target date
        if target_date is None:
            date_query = text("""
                SELECT MAX(trade_date) as max_date
                FROM etf_daily
                WHERE trade_date <= CURRENT_DATE
            """)
            result = await self.db.execute(date_query)
            row = result.fetchone()
            if row and row.max_date:
                target_date = row.max_date
            else:
                return {
                    "industry": industry,
                    "sub_categories": sub_categories,
                    "etfs": [],
                }

        # Query ETFs matching the keywords
        query = text("""
            SELECT
                e.code,
                e.name,
                d.close,
                d.pre_close,
                d.amount,
                CASE
                    WHEN d.pre_close > 0
                    THEN ROUND(((d.close - d.pre_close) / d.pre_close * 100)::numeric, 2)
                    ELSE NULL
                END as change_pct
            FROM etf e
            JOIN etf_daily d ON e.code = d.code
            WHERE d.trade_date = :target_date
              AND e.name ~* :keyword_pattern
              AND d.amount > 10000000  -- Filter out low-volume ETFs
            ORDER BY d.amount DESC
            LIMIT :limit
        """)

        result = await self.db.execute(
            query,
            {
                "target_date": target_date,
                "keyword_pattern": keyword_pattern,
                "limit": limit,
            }
        )
        rows = result.fetchall()

        etfs = []
        for row in rows:
            etfs.append({
                "code": row.code,
                "name": row.name,
                "change_pct": float(row.change_pct) if row.change_pct is not None else None,
                "amount": float(row.amount) if row.amount is not None else None,
            })

        return {
            "industry": industry,
            "sub_categories": sub_categories,
            "etfs": etfs,
            "date": str(target_date),
        }

    async def get_all_mappings(self) -> Dict[str, List[str]]:
        """Return all industry to ETF sub-category mappings."""
        return SW_INDUSTRY_TO_ETF_SUBCATEGORY.copy()
