"""ETF Heatmap service for Alpha Radar.

Provides ETF heatmap data grouped by category (broad, sector, theme, cross_border, commodity, bond).
Each row shows representative ETFs with their change percentages.
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.etf_classifier import ETFClassifier


class ETFHeatmapService:
    """
    Service for generating ETF heatmap data grouped by category.

    Features:
    1. Classifies ETFs by name keywords
    2. Groups by category and sub-category
    3. Selects representative ETF (highest volume) per sub-category
    4. Returns 6 rows: broad, sector, theme, cross_border, commodity, bond
    5. Top movers row: top 10 ETFs by daily change (bubble-up mechanism)
    """

    # Maximum items per category row
    MAX_ITEMS_PER_CATEGORY = 10

    # Top movers settings
    TOP_MOVERS_LIMIT = 10
    MIN_AMOUNT_FOR_TOP_MOVERS = 10000000  # 1000万成交额过滤僵尸ETF

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_etf_heatmap(
        self,
        target_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Get ETF heatmap data grouped by category.

        Args:
            target_date: Target date (default: latest trading date)

        Returns:
            Dict with date, top_movers, and categories (6 rows of ETF data)
        """
        # Resolve date
        if target_date is None:
            target_date = await self._get_latest_trading_date()
        if target_date is None:
            return self._empty_response()

        # Load ETF data
        df = await self._load_etf_data(target_date)
        if df.is_empty():
            return self._empty_response(target_date)

        # Get top movers (by change_pct, not limited by category)
        top_movers = self._get_top_movers(df)

        # Classify all ETFs
        df = self._classify_etfs(df)

        # Group by category and select representatives
        categories = self._build_category_data(df)

        return {
            "date": target_date,
            "top_movers": top_movers,
            "categories": categories,
        }

    async def _get_latest_trading_date(self) -> Optional[date]:
        """Get the latest trading date with ETF data."""
        result = await self.db.execute(
            text("""
                SELECT MAX(md.date)
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                WHERE am.asset_type = 'ETF'
            """)
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None

    async def _load_etf_data(self, target_date: date) -> pl.DataFrame:
        """Load ETF market data for the target date."""
        query = text("""
            SELECT
                am.code,
                am.name,
                md.close AS price,
                md.pct_chg AS change_pct,
                md.amount,
                md.volume
            FROM asset_meta am
            JOIN market_daily md ON am.code = md.code AND md.date = :target_date
            WHERE am.asset_type = 'ETF'
              AND am.status = 1
              AND md.amount > 0
            ORDER BY md.amount DESC NULLS LAST
        """)

        result = await self.db.execute(query, {"target_date": target_date})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame([dict(zip(columns, row)) for row in rows])

    def _classify_etfs(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add classification columns to the dataframe."""
        # Apply classifier to each ETF
        classifications = []
        for row in df.iter_rows(named=True):
            category, sub_category = ETFClassifier.classify(row["name"])
            classifications.append({
                "code": row["code"],
                "category": category,
                "sub_category": sub_category,
            })

        # Create classification dataframe
        class_df = pl.DataFrame(classifications)

        # Join back to main dataframe
        df = df.join(class_df, on="code", how="left")

        return df

    def _get_top_movers(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """
        Get top movers by daily change percentage.

        Filters:
        - amount > MIN_AMOUNT_FOR_TOP_MOVERS (filter zombie ETFs)

        Returns:
        - Top N ETFs sorted by change_pct descending
        """
        # Filter by minimum amount
        filtered = df.filter(pl.col("amount") > self.MIN_AMOUNT_FOR_TOP_MOVERS)

        if filtered.is_empty():
            return []

        # Sort by change_pct descending
        sorted_df = filtered.sort("change_pct", descending=True, nulls_last=True)

        # Take top N
        top_df = sorted_df.head(self.TOP_MOVERS_LIMIT)

        # Classify and build items
        items = []
        for row in top_df.iter_rows(named=True):
            category, sub_category = ETFClassifier.classify(row["name"])
            items.append({
                "code": row["code"],
                "name": sub_category,  # Simplified name
                "full_name": row["name"],
                "change_pct": self._to_decimal(row["change_pct"]),
                "amount": self._to_decimal(row["amount"]),
                "category": category,
                "is_representative": True,
            })

        return items

    def _build_category_data(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """
        Build category data structure for heatmap.

        Returns list of categories, each with:
        - category: str (broad, sector, theme, cross_border, commodity, bond)
        - label: str (宽基, 行业, 赛道, 跨境, 商品, 债券)
        - items: List[EtfHeatmapItem]
        - avg_change_pct: Decimal
        """
        categories = []

        for cat_key, cat_label in ETFClassifier.get_all_categories():
            # Filter ETFs for this category
            cat_df = df.filter(pl.col("category") == cat_key)

            if cat_df.is_empty():
                # Add empty category
                categories.append({
                    "category": cat_key,
                    "label": cat_label,
                    "items": [],
                    "avg_change_pct": Decimal("0"),
                })
                continue

            # Group by sub_category and select representative (highest volume)
            grouped = cat_df.group_by("sub_category").agg([
                pl.col("code").first().alias("code"),
                pl.col("name").first().alias("full_name"),
                pl.col("price").first().alias("price"),
                pl.col("change_pct").first().alias("change_pct"),
                pl.col("amount").first().alias("amount"),
                pl.col("amount").sum().alias("total_amount"),  # For sorting
            ])

            # Sort by total_amount descending, limit to MAX_ITEMS_PER_CATEGORY
            grouped = grouped.sort("total_amount", descending=True)
            grouped = grouped.head(self.MAX_ITEMS_PER_CATEGORY)

            # Build items list
            items = []
            for row in grouped.iter_rows(named=True):
                items.append({
                    "code": row["code"],
                    "name": row["sub_category"],  # Simplified name
                    "full_name": row["full_name"],
                    "change_pct": self._to_decimal(row["change_pct"]),
                    "amount": self._to_decimal(row["amount"]),
                    "is_representative": True,
                })

            # Calculate average change_pct for the category
            avg_change = cat_df.select(pl.col("change_pct").mean()).item()

            categories.append({
                "category": cat_key,
                "label": cat_label,
                "items": items,
                "avg_change_pct": self._to_decimal(avg_change),
            })

        return categories

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        """Convert value to Decimal, handling None and NaN."""
        if value is None:
            return None
        try:
            if isinstance(value, float):
                import math
                if math.isnan(value) or math.isinf(value):
                    return None
            return Decimal(str(round(float(value), 4)))
        except (ValueError, TypeError):
            return None

    def _empty_response(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """Return empty response structure."""
        return {
            "date": target_date,
            "top_movers": [],
            "categories": [
                {
                    "category": cat_key,
                    "label": cat_label,
                    "items": [],
                    "avg_change_pct": Decimal("0"),
                }
                for cat_key, cat_label in ETFClassifier.get_all_categories()
            ],
        }
