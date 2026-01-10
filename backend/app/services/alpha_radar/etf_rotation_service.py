"""ETF Rotation service for Alpha Radar.

Provides ETF rotation matrix with category aggregation and expandable sub-categories.
Supports hybrid view: 6 main categories that expand to show sub-categories.
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.etf_classifier import ETFClassifier


# Category display order
ETF_CATEGORY_ORDER = ["broad", "sector", "theme", "cross_border", "commodity", "bond"]


class ETFRotationService:
    """
    Service for generating ETF rotation matrix data.

    Creates a date × category matrix for analyzing ETF rotation patterns.
    Supports hybrid view with expandable sub-categories.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_rotation_matrix(
        self,
        days: int = 30,
        end_date: Optional[date] = None,
        page: int = 1,
        page_size: int = 30,
    ) -> Dict[str, Any]:
        """
        Get ETF rotation matrix (category summary view).

        Args:
            days: Number of trading days to include
            end_date: End date (default: latest trading day)
            page: Page number
            page_size: Items per page (rows/dates)

        Returns:
            Dict with rows (date × category data), pagination info
        """
        # Resolve end date
        if end_date is None:
            end_date = await self._get_latest_trading_date()
        if end_date is None:
            return self._empty_response(page, page_size)

        # Get trading days
        trading_days = await self._get_trading_days(end_date, days)
        if not trading_days:
            return self._empty_response(page, page_size)

        # Apply pagination to trading days
        total = len(trading_days)
        pages = (total + page_size - 1) // page_size if total > 0 else 0
        offset = (page - 1) * page_size
        paginated_days = trading_days[offset:offset + page_size]

        if not paginated_days:
            return self._empty_response(page, page_size)

        # Load ETF data for the paginated days
        df = await self._load_etf_data(paginated_days)
        if df.is_empty():
            return self._empty_response(page, page_size)

        # Classify ETFs
        df = self._classify_etfs(df)

        # Aggregate by date and category
        matrix_df = self._aggregate_by_category(df)

        # Build rows
        rows = self._build_rows(matrix_df, paginated_days)

        return {
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "categories": [
                {"key": cat, "label": ETFClassifier.get_category_label(cat)}
                for cat in ETF_CATEGORY_ORDER
            ],
        }

    async def get_category_detail(
        self,
        category: str,
        days: int = 30,
        end_date: Optional[date] = None,
        page: int = 1,
        page_size: int = 30,
    ) -> Dict[str, Any]:
        """
        Get expanded sub-category detail for a specific category.

        Args:
            category: Category to expand (broad, sector, theme, etc.)
            days: Number of trading days to include
            end_date: End date
            page: Page number
            page_size: Items per page

        Returns:
            Dict with rows (date × sub-category data), pagination info
        """
        if category not in ETF_CATEGORY_ORDER:
            return self._empty_response(page, page_size)

        # Resolve end date
        if end_date is None:
            end_date = await self._get_latest_trading_date()
        if end_date is None:
            return self._empty_response(page, page_size)

        # Get trading days
        trading_days = await self._get_trading_days(end_date, days)
        if not trading_days:
            return self._empty_response(page, page_size)

        # Apply pagination
        total = len(trading_days)
        pages = (total + page_size - 1) // page_size if total > 0 else 0
        offset = (page - 1) * page_size
        paginated_days = trading_days[offset:offset + page_size]

        if not paginated_days:
            return self._empty_response(page, page_size)

        # Load ETF data
        df = await self._load_etf_data(paginated_days)
        if df.is_empty():
            return self._empty_response(page, page_size)

        # Classify and filter to category
        df = self._classify_etfs(df)
        df = df.filter(pl.col("category") == category)

        if df.is_empty():
            return self._empty_response(page, page_size)

        # Aggregate by date and sub_category
        matrix_df = self._aggregate_by_sub_category(df)

        # Get unique sub-categories sorted by volume
        sub_categories = self._get_sorted_sub_categories(matrix_df)

        # Build detail rows
        rows = self._build_detail_rows(matrix_df, paginated_days, sub_categories)

        return {
            "category": category,
            "category_label": ETFClassifier.get_category_label(category),
            "rows": rows,
            "sub_categories": sub_categories,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
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

    async def _get_trading_days(
        self, end_date: date, days: int
    ) -> List[date]:
        """Get the most recent N trading days ending at end_date."""
        result = await self.db.execute(
            text("""
                SELECT DISTINCT date
                FROM market_daily
                WHERE date <= :end_date
                ORDER BY date DESC
                LIMIT :days
            """),
            {"end_date": end_date, "days": days}
        )
        return [row[0] for row in result.fetchall()]

    async def _load_etf_data(self, trading_days: List[date]) -> pl.DataFrame:
        """Load ETF market data for the given trading days."""
        query = text("""
            SELECT
                am.code,
                am.name,
                md.date,
                md.pct_chg AS change_pct,
                md.amount
            FROM asset_meta am
            JOIN market_daily md ON am.code = md.code
            WHERE am.asset_type = 'ETF'
              AND am.status = 1
              AND md.date = ANY(:dates)
              AND md.amount > 0
        """)

        result = await self.db.execute(query, {"dates": trading_days})
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame([dict(zip(columns, row)) for row in rows])

    def _classify_etfs(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add classification columns to the dataframe."""
        classifications = []
        for row in df.iter_rows(named=True):
            category, sub_category = ETFClassifier.classify(row["name"])
            classifications.append({
                "code": row["code"],
                "date": row["date"],
                "category": category,
                "sub_category": sub_category,
            })

        class_df = pl.DataFrame(classifications)
        df = df.join(class_df, on=["code", "date"], how="left")
        return df

    def _aggregate_by_category(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate data by date × category."""
        return df.group_by(["date", "category"]).agg([
            pl.col("change_pct").mean().alias("avg_change_pct"),
            pl.col("amount").sum().alias("total_amount"),
            pl.len().alias("etf_count"),
            # Top performer info
            pl.col("change_pct").max().alias("top_change_pct"),
        ])

    def _aggregate_by_sub_category(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aggregate data by date × sub_category."""
        # First, select representative ETF per sub_category (highest amount)
        df = df.sort(["date", "sub_category", "amount"], descending=[True, True, True])

        # Get representative per sub-category per day
        representative = (
            df.group_by(["date", "sub_category"])
            .first()
            .select([
                "date",
                "sub_category",
                pl.col("code").alias("rep_code"),
                pl.col("name").alias("rep_name"),
                pl.col("change_pct"),
                pl.col("amount"),
            ])
        )

        return representative

    def _get_sorted_sub_categories(self, matrix_df: pl.DataFrame) -> List[str]:
        """Get sub-categories sorted by total volume."""
        return (
            matrix_df.group_by("sub_category")
            .agg(pl.col("amount").sum().alias("total"))
            .sort("total", descending=True)
            .select("sub_category")
            .to_series()
            .to_list()
        )

    def _build_rows(
        self,
        matrix_df: pl.DataFrame,
        trading_days: List[date],
    ) -> List[Dict[str, Any]]:
        """Build rotation matrix rows (one row per date)."""
        rows = []

        for day in sorted(trading_days, reverse=True):
            day_data = matrix_df.filter(pl.col("date") == day)

            categories = {}
            for cat in ETF_CATEGORY_ORDER:
                cat_data = day_data.filter(pl.col("category") == cat)

                if cat_data.is_empty():
                    categories[cat] = {
                        "avg_change_pct": None,
                        "total_amount": None,
                        "etf_count": 0,
                        "top_change_pct": None,
                    }
                else:
                    row = cat_data.row(0, named=True)
                    categories[cat] = {
                        "avg_change_pct": self._to_decimal(row["avg_change_pct"]),
                        "total_amount": self._to_decimal(row["total_amount"]),
                        "etf_count": row["etf_count"],
                        "top_change_pct": self._to_decimal(row["top_change_pct"]),
                    }

            rows.append({
                "date": day,
                "categories": categories,
            })

        return rows

    def _build_detail_rows(
        self,
        matrix_df: pl.DataFrame,
        trading_days: List[date],
        sub_categories: List[str],
    ) -> List[Dict[str, Any]]:
        """Build detail rows for expanded category view."""
        rows = []

        for day in sorted(trading_days, reverse=True):
            day_data = matrix_df.filter(pl.col("date") == day)

            sub_cats = {}
            for sub_cat in sub_categories:
                sub_data = day_data.filter(pl.col("sub_category") == sub_cat)

                if sub_data.is_empty():
                    sub_cats[sub_cat] = {
                        "change_pct": None,
                        "amount": None,
                        "rep_code": None,
                        "rep_name": None,
                    }
                else:
                    row = sub_data.row(0, named=True)
                    sub_cats[sub_cat] = {
                        "change_pct": self._to_decimal(row["change_pct"]),
                        "amount": self._to_decimal(row["amount"]),
                        "rep_code": row["rep_code"],
                        "rep_name": row["rep_name"],
                    }

            rows.append({
                "date": day,
                "sub_categories": sub_cats,
            })

        return rows

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

    def _empty_response(
        self, page: int = 1, page_size: int = 30
    ) -> Dict[str, Any]:
        """Return empty response structure."""
        return {
            "rows": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "pages": 0,
            "categories": [
                {"key": cat, "label": ETFClassifier.get_category_label(cat)}
                for cat in ETF_CATEGORY_ORDER
            ],
        }

    # ============================================
    # Flat Rotation Matrix (SVG-based view)
    # ============================================

    async def get_flat_rotation_matrix(
        self,
        days: int = 60,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        获取扁平化 ETF 轮动矩阵 (所有子品类作为列).

        用于 SVG 矩阵渲染，类似行业轮动矩阵的数据结构。

        Args:
            days: 交易日数量
            end_date: 截止日期 (用于无限滚动分页)

        Returns:
            Dict with:
              - trading_days: List[date] - Y轴日期
              - sub_categories: List[column] - X轴子品类列
              - category_order: List[str] - 大类顺序
              - category_labels: Dict[str, str] - 大类标签
              - days: int - 请求天数
        """
        # Resolve end date
        if end_date is None:
            end_date = await self._get_latest_trading_date()
        if end_date is None:
            return self._empty_flat_response(days)

        # Get trading days
        trading_days = await self._get_trading_days(end_date, days)
        if not trading_days:
            return self._empty_flat_response(days)

        # Load ETF data for all trading days
        df = await self._load_etf_data(trading_days)
        if df.is_empty():
            return self._empty_flat_response(days)

        # Classify ETFs
        df = self._classify_etfs(df)

        # Build columns (one per sub-category)
        columns = self._build_flat_columns(df, trading_days)

        return {
            "trading_days": sorted(trading_days, reverse=True),
            "sub_categories": columns,
            "category_order": ETF_CATEGORY_ORDER,
            "category_labels": {
                cat: ETFClassifier.get_category_label(cat)
                for cat in ETF_CATEGORY_ORDER
            },
            "days": days,
        }

    def _build_flat_columns(
        self,
        df: pl.DataFrame,
        trading_days: List[date],
    ) -> List[Dict[str, Any]]:
        """
        构建扁平化的列数据 (每个子品类一列).

        按大类分组，每类内按成交额排序。
        """
        # Get representative ETF per sub-category per day
        df = df.sort(["date", "sub_category", "amount"], descending=[True, True, True])
        representative = (
            df.group_by(["date", "category", "sub_category"])
            .first()
            .select([
                "date",
                "category",
                "sub_category",
                pl.col("code").alias("rep_code"),
                pl.col("name").alias("rep_name"),
                pl.col("change_pct"),
                pl.col("amount"),
            ])
        )

        # Get unique sub-categories with their category and total volume
        sub_cat_info = (
            representative.group_by(["category", "sub_category"])
            .agg([
                pl.col("amount").sum().alias("total_amount"),
            ])
        )

        # Sort by category order, then by volume within category
        columns = []

        for category in ETF_CATEGORY_ORDER:
            cat_subs = sub_cat_info.filter(pl.col("category") == category)
            if cat_subs.is_empty():
                continue

            cat_subs = cat_subs.sort("total_amount", descending=True)

            for row in cat_subs.iter_rows(named=True):
                sub_category = row["sub_category"]

                # Get cells for this sub-category
                sub_data = representative.filter(pl.col("sub_category") == sub_category)

                cells = []
                today_change = None
                period_changes = []

                for day in sorted(trading_days, reverse=True):
                    day_data = sub_data.filter(pl.col("date") == day)

                    if day_data.is_empty():
                        cells.append({
                            "date": day,
                            "change_pct": None,
                            "amount": None,
                            "rep_code": None,
                            "rep_name": None,
                        })
                    else:
                        cell_row = day_data.row(0, named=True)
                        change_pct = self._to_decimal(cell_row["change_pct"])
                        cells.append({
                            "date": day,
                            "change_pct": change_pct,
                            "amount": self._to_decimal(cell_row["amount"]),
                            "rep_code": cell_row["rep_code"],
                            "rep_name": cell_row["rep_name"],
                        })

                        # Track for aggregation
                        if change_pct is not None:
                            period_changes.append(float(change_pct))
                            if today_change is None:
                                today_change = change_pct

                # Calculate period change (simple sum for now)
                period_change = (
                    self._to_decimal(sum(period_changes))
                    if period_changes else None
                )

                columns.append({
                    "name": sub_category,
                    "category": category,
                    "category_label": ETFClassifier.get_category_label(category),
                    "cells": cells,
                    "today_change": today_change,
                    "period_change": period_change,
                    "total_amount": self._to_decimal(row["total_amount"]),
                })

        return columns

    def _empty_flat_response(self, days: int = 60) -> Dict[str, Any]:
        """Return empty flat response structure."""
        return {
            "trading_days": [],
            "sub_categories": [],
            "category_order": ETF_CATEGORY_ORDER,
            "category_labels": {
                cat: ETFClassifier.get_category_label(cat)
                for cat in ETF_CATEGORY_ORDER
            },
            "days": days,
        }

    # ============================================
    # ETF Subcategory List (for Tooltip)
    # ============================================

    async def get_etfs_by_subcategory(
        self,
        category: str,
        sub_category: str,
        target_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        获取指定子品类下所有 ETF 列表 (用于 Tooltip 动态加载).

        Args:
            category: 大类 (broad, sector, theme, cross_border, commodity, bond)
            sub_category: 子品类名 (如 '沪深300', '银行')
            target_date: 目标日期 (默认最新交易日)

        Returns:
            Dict with category, sub_category, date, etfs (sorted by change_pct desc)
        """
        # Resolve target date
        if target_date is None:
            target_date = await self._get_latest_trading_date()

        if target_date is None:
            return {
                "category": category,
                "sub_category": sub_category,
                "date": None,
                "etfs": [],
            }

        # Load all ETF data for the target date
        df = await self._load_etf_data([target_date])
        if df.is_empty():
            return {
                "category": category,
                "sub_category": sub_category,
                "date": target_date,
                "etfs": [],
            }

        # Classify ETFs
        df = self._classify_etfs(df)

        # Filter by category and sub_category
        df = df.filter(
            (pl.col("category") == category) &
            (pl.col("sub_category") == sub_category)
        )

        if df.is_empty():
            return {
                "category": category,
                "sub_category": sub_category,
                "date": target_date,
                "etfs": [],
            }

        # Filter by minimum amount (> 1000万 = 10,000,000)
        df = df.filter(pl.col("amount") > 10_000_000)

        # Sort by change_pct descending
        df = df.sort("change_pct", descending=True)

        # Build ETF list
        etfs = []
        for row in df.iter_rows(named=True):
            etfs.append({
                "code": row["code"],
                "name": row["name"],
                "change_pct": self._to_decimal(row["change_pct"]),
            })

        return {
            "category": category,
            "sub_category": sub_category,
            "date": target_date,
            "etfs": etfs,
        }
