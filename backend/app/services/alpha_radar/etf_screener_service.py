"""ETF screener service for Alpha Radar.

Provides ETF discovery with representative ETF logic, premium/discount monitoring,
and category-based filtering.
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class ETFScreenerService:
    """
    Service for ETF screening with representative ETF logic.

    Features:
    1. Category filtering (broad, sector, cross_border, commodity, bond)
    2. Representative ETF selection (highest volume per underlying index)
    3. Premium/discount monitoring with alerts
    4. Comprehensive scoring
    """

    # ETF type mapping from category to database values
    CATEGORY_TO_ETF_TYPES = {
        "broad": ["BROAD_BASED"],
        "sector": ["SECTOR", "THEME"],
        "cross_border": ["CROSS_BORDER"],
        "commodity": ["COMMODITY"],
        "bond": ["BOND"],
    }

    # ETFs that support T+0 trading
    T_PLUS_ZERO_TYPES = ["CROSS_BORDER", "COMMODITY", "BOND", "CURRENCY"]

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_etf_screener_results(
        self,
        category: Optional[str] = None,
        target_date: Optional[date] = None,
        representative_only: bool = True,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "amount",
        sort_order: str = "desc",
    ) -> Dict[str, Any]:
        """
        Get ETF screener results with representative logic and scoring.

        Args:
            category: ETF category filter (broad, sector, cross_border, commodity, bond)
            target_date: Target date (default: latest)
            representative_only: Only show representative ETF per underlying index
            page: Page number
            page_size: Items per page
            sort_by: Sort field (amount, discount_rate, score)
            sort_order: Sort order (asc, desc)

        Returns:
            Dict with items, total, page, pages, date
        """
        # Resolve date
        if target_date is None:
            target_date = await self._get_latest_trading_date()
        if target_date is None:
            return self._empty_response()

        # Load ETF data
        df = await self._load_etf_data(target_date, category)
        if df.is_empty():
            return self._empty_response(target_date)

        # Apply representative ETF logic
        if representative_only:
            df = self._select_representative_etfs(df)

        # Calculate score and labels
        df = self._calculate_score_and_labels(df)

        # Get total before pagination
        total = df.height

        # Sort
        df = self._apply_sorting(df, sort_by, sort_order)

        # Paginate
        offset = (page - 1) * page_size
        df = df.slice(offset, page_size)

        # Convert to response items
        items = self._convert_to_items(df)

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": (total + page_size - 1) // page_size if total > 0 else 0,
            "date": target_date,
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

    async def _load_etf_data(
        self,
        target_date: date,
        category: Optional[str] = None,
    ) -> pl.DataFrame:
        """Load ETF data with market and profile info."""
        # Build category filter
        category_filter = ""
        params = {"target_date": target_date}

        if category and category in self.CATEGORY_TO_ETF_TYPES:
            etf_types = self.CATEGORY_TO_ETF_TYPES[category]
            category_filter = "AND ep.etf_type = ANY(:etf_types)"
            params["etf_types"] = etf_types

        query = text(f"""
            SELECT
                am.code,
                am.name,
                ep.etf_type,
                ep.underlying_index_code,
                ep.underlying_index_name,
                ep.fund_company,
                ep.management_fee,
                ep.custody_fee,
                md.close AS price,
                md.pct_chg AS change_pct,
                md.amount,
                md.turn,
                md.volume,
                ie.discount_rate,
                ie.unit_total,
                ie.tracking_error,
                ie.iopv
            FROM asset_meta am
            JOIN etf_profile ep ON am.code = ep.code
            LEFT JOIN market_daily md ON am.code = md.code AND md.date = :target_date
            LEFT JOIN indicator_etf ie ON am.code = ie.code AND ie.date = :target_date
            WHERE am.asset_type = 'ETF'
              AND am.status = 1
              AND md.amount > 0
              {category_filter}
            ORDER BY md.amount DESC NULLS LAST
        """)

        result = await self.db.execute(query, params)
        rows = result.fetchall()
        columns = result.keys()

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(
            [dict(zip(columns, row)) for row in rows]
        )

    def _select_representative_etfs(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Select representative ETF per underlying index.

        For ETFs tracking the same index, keep only the one with highest volume.
        ETFs without underlying_index_code are kept as-is.
        """
        # Separate ETFs with and without underlying index
        with_index = df.filter(pl.col("underlying_index_code").is_not_null())
        without_index = df.filter(pl.col("underlying_index_code").is_null())

        if with_index.is_empty():
            return without_index

        # Rank by amount within each underlying index, keep rank 1
        with_index = with_index.with_columns([
            pl.col("amount")
            .rank(method="ordinal", descending=True)
            .over("underlying_index_code")
            .alias("_rank")
        ])

        representative = with_index.filter(pl.col("_rank") == 1).drop("_rank")

        # Combine with ETFs that have no underlying index
        return pl.concat([representative, without_index])

    def _calculate_score_and_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate composite score and generate labels."""
        # Normalize amount for score calculation (30% weight)
        df = df.with_columns([
            (pl.col("amount").rank() / pl.len() * 30).alias("amount_score"),
        ])

        # Tracking efficiency score (20% weight) - lower error is better
        if "tracking_error" in df.columns:
            df = df.with_columns([
                pl.when(pl.col("tracking_error").is_not_null())
                .then(20 - (pl.col("tracking_error").rank() / pl.len() * 20))
                .otherwise(10)
                .alias("tracking_score")
            ])
        else:
            df = df.with_columns([pl.lit(10).alias("tracking_score")])

        # Fee score (15% weight) - lower fee is better
        if "management_fee" in df.columns:
            df = df.with_columns([
                pl.when(pl.col("management_fee").is_not_null())
                .then(15 - (pl.col("management_fee").rank() / pl.len() * 15))
                .otherwise(7.5)
                .alias("fee_score")
            ])
        else:
            df = df.with_columns([pl.lit(7.5).alias("fee_score")])

        # Scale score (15% weight) - unit_total * price
        if "unit_total" in df.columns and "price" in df.columns:
            df = df.with_columns([
                (pl.col("unit_total") * pl.col("price")).alias("_scale")
            ])
            df = df.with_columns([
                pl.when(pl.col("_scale").is_not_null())
                .then(pl.col("_scale").rank() / pl.len() * 15)
                .otherwise(7.5)
                .alias("scale_score")
            ]).drop("_scale")
        else:
            df = df.with_columns([pl.lit(7.5).alias("scale_score")])

        # Momentum score (20% weight) - recent return
        if "change_pct" in df.columns:
            df = df.with_columns([
                pl.when(pl.col("change_pct").is_not_null())
                .then(pl.col("change_pct").rank() / pl.len() * 20)
                .otherwise(10)
                .alias("momentum_score")
            ])
        else:
            df = df.with_columns([pl.lit(10).alias("momentum_score")])

        # Composite score
        df = df.with_columns([
            (
                pl.col("amount_score") +
                pl.col("tracking_score") +
                pl.col("fee_score") +
                pl.col("scale_score") +
                pl.col("momentum_score")
            ).alias("score")
        ])

        # Generate labels
        df = self._generate_labels(df)

        return df

    def _generate_labels(self, df: pl.DataFrame) -> pl.DataFrame:
        """Generate ETF labels based on metrics."""
        # Liquidity king - highest amount in category
        df = df.with_columns([
            (pl.col("amount") == pl.col("amount").max().over("etf_type"))
            .alias("is_liquidity_king")
        ])

        # High premium warning (> 5%)
        df = df.with_columns([
            pl.when(pl.col("discount_rate").is_not_null())
            .then(pl.col("discount_rate") > 5)
            .otherwise(False)
            .alias("is_high_premium")
        ])

        # Medium premium warning (> 3%)
        df = df.with_columns([
            pl.when(pl.col("discount_rate").is_not_null())
            .then((pl.col("discount_rate") > 3) & (pl.col("discount_rate") <= 5))
            .otherwise(False)
            .alias("is_medium_premium")
        ])

        # Discount opportunity (< -3%)
        df = df.with_columns([
            pl.when(pl.col("discount_rate").is_not_null())
            .then(pl.col("discount_rate") < -3)
            .otherwise(False)
            .alias("is_discount")
        ])

        # T+0 ETF
        df = df.with_columns([
            pl.col("etf_type").is_in(self.T_PLUS_ZERO_TYPES).alias("is_t_plus_zero")
        ])

        return df

    def _apply_sorting(
        self,
        df: pl.DataFrame,
        sort_by: str,
        sort_order: str,
    ) -> pl.DataFrame:
        """Apply sorting to the dataframe."""
        descending = sort_order == "desc"

        sort_map = {
            "amount": "amount",
            "discount_rate": "discount_rate",
            "score": "score",
            "change": "change_pct",
            "turn": "turn",
        }

        col = sort_map.get(sort_by, "amount")

        if col not in df.columns:
            col = "amount"

        return df.sort(col, descending=descending, nulls_last=True)

    def _convert_to_items(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Convert Polars DataFrame to list of ETF items."""
        items = []

        for row in df.iter_rows(named=True):
            # Build labels list
            labels = []
            if row.get("is_liquidity_king"):
                labels.append("liquidity_king")
            if row.get("is_high_premium"):
                labels.append("high_premium")
            elif row.get("is_medium_premium"):
                labels.append("medium_premium")
            if row.get("is_discount"):
                labels.append("discount")
            if row.get("is_t_plus_zero"):
                labels.append("t_plus_zero")

            item = {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "etf_type": row.get("etf_type"),
                "underlying_index_code": row.get("underlying_index_code"),
                "underlying_index_name": row.get("underlying_index_name"),
                "price": self._to_decimal(row.get("price")),
                "change_pct": self._to_decimal(row.get("change_pct")),
                "amount": self._to_decimal(row.get("amount")),
                "turn": self._to_decimal(row.get("turn")),
                "discount_rate": self._to_decimal(row.get("discount_rate")),
                "unit_total": self._to_decimal(row.get("unit_total")),
                "fund_company": row.get("fund_company"),
                "management_fee": self._to_decimal(row.get("management_fee")),
                "score": self._to_decimal(row.get("score")),
                "is_representative": True,  # All returned are representative
                "labels": labels,
            }
            items.append(item)

        return items

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
            "items": [],
            "total": 0,
            "page": 1,
            "page_size": 50,
            "pages": 0,
            "date": target_date,
        }
