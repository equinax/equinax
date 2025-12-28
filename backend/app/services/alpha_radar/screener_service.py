"""Screener service for Alpha Radar.

Orchestrates data loading, scoring, and filtering for the intelligent screener.
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine


class ScreenerService:
    """
    Service for intelligent stock screening.

    Orchestrates:
    1. Data loading via PolarsEngine
    2. Score calculation via ScoringEngine
    3. Filtering and pagination
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.polars_engine = PolarsEngine(db)
        self.scoring_engine = ScoringEngine()

    async def get_screener_results(
        self,
        tab: str,
        mode: str,
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "score",
        sort_order: str = "desc",
    ) -> Dict[str, Any]:
        """
        Get screener results with scoring and filtering.

        Args:
            tab: Screener tab (panorama, smart, value, trend)
            mode: Time mode (snapshot, period)
            target_date: Target date for snapshot mode
            start_date: Start date for period mode
            end_date: End date for period mode
            filters: Filter criteria (board, size_category, industry_l1, min_score)
            page: Page number (1-indexed)
            page_size: Items per page
            sort_by: Sort field (score, change, volume, valuation, main_strength, code)
            sort_order: Sort order (asc, desc)

        Returns:
            Dict with items, total, page, page_size, pages, tab, time_mode, dates
        """
        filters = filters or {}

        # Resolve dates
        if mode == "snapshot":
            if target_date is None:
                target_date = await self.polars_engine.get_latest_trading_date()
            if target_date is None:
                return self._empty_response(tab, mode)
        else:
            # Period mode
            if end_date is None:
                end_date = await self.polars_engine.get_latest_trading_date()
            if start_date is None:
                # Default to 20 trading days
                start_date = end_date  # Simplified - would need proper calculation

        # Load data
        df = await self.polars_engine.load_market_data(
            target_date=target_date if mode == "snapshot" else None,
            start_date=start_date if mode == "period" else None,
            end_date=end_date if mode == "period" else None,
        )

        if df.is_empty():
            return self._empty_response(tab, mode, target_date, start_date, end_date)

        # Calculate technical indicators
        df = self.polars_engine.calculate_technical_indicators(df)

        # Get latest data point per stock
        if mode == "snapshot":
            df = df.filter(pl.col("date") == target_date)
        else:
            # Period mode - aggregate
            df = self.polars_engine.calculate_period_metrics(df, start_date, end_date)

        # Load and join valuation data
        valuation_df = await self.polars_engine.load_valuation_data(
            target_date if mode == "snapshot" else end_date
        )
        if not valuation_df.is_empty():
            # Calculate PE percentile
            valuation_df = valuation_df.with_columns([
                (pl.col("pe_ttm").rank() / pl.len()).alias("pe_percentile"),
                (pl.col("volatility_20d").rank() / pl.len()).alias("vol_percentile")
                if "volatility_20d" in valuation_df.columns
                else pl.lit(0.5).alias("vol_percentile"),
            ])
            df = df.join(valuation_df, on="code", how="left")

        # Load and join style factors
        style_df = await self.polars_engine.load_style_factors(
            target_date if mode == "snapshot" else end_date
        )
        if not style_df.is_empty():
            # Calculate vol_percentile from volatility_20d
            if "volatility_20d" in style_df.columns:
                style_df = style_df.with_columns([
                    (pl.col("volatility_20d").rank() / pl.len()).alias("vol_percentile"),
                ])

            # Build select columns based on what exists
            style_select_cols = ["code", "size_category", "momentum_20d", "momentum_60d"]
            if "vol_percentile" in style_df.columns:
                style_select_cols.append("vol_percentile")

            df = df.join(
                style_df.select(style_select_cols),
                on="code",
                how="left",
                suffix="_style"
            )
            # Merge vol_percentile if it came from style_df
            if "vol_percentile_style" in df.columns:
                df = df.with_columns([
                    pl.coalesce([pl.col("vol_percentile"), pl.col("vol_percentile_style")])
                    .alias("vol_percentile")
                ]).drop("vol_percentile_style")

        # Load and join stock profiles for industry
        profile_df = await self.polars_engine.load_stock_profiles()
        if not profile_df.is_empty():
            df = df.join(profile_df, on="code", how="left")

        # Calculate scores based on tab
        if tab == "panorama":
            df = self.scoring_engine.calculate_panorama_score(df)
            score_col = "panorama_score"
        elif tab == "smart":
            df = self.scoring_engine.calculate_smart_accumulation_score(df)
            score_col = "smart_score"
        elif tab == "value":
            df = self.scoring_engine.calculate_deep_value_score(df)
            score_col = "value_score"
        elif tab == "trend":
            df = self.scoring_engine.calculate_super_trend_score(df)
            score_col = "trend_score"
        else:
            df = self.scoring_engine.calculate_panorama_score(df)
            score_col = "panorama_score"

        # Generate quant labels
        df = self.scoring_engine.generate_quant_labels(df)

        # Apply filters
        df = self._apply_filters(df, filters, score_col)

        # Get total count before pagination
        total = df.height

        # Sort
        df = self._apply_sorting(df, sort_by, sort_order, score_col)

        # Paginate
        offset = (page - 1) * page_size
        df = df.slice(offset, page_size)

        # Convert to response items
        items = self._convert_to_items(df, score_col, mode)

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": (total + page_size - 1) // page_size if total > 0 else 0,
            "tab": tab,
            "time_mode": mode,
            "date": target_date if mode == "snapshot" else None,
            "start_date": start_date if mode == "period" else None,
            "end_date": end_date if mode == "period" else None,
        }

    def _apply_filters(
        self,
        df: pl.DataFrame,
        filters: Dict[str, Any],
        score_col: str,
    ) -> pl.DataFrame:
        """Apply filters to the dataframe."""
        # Board filter
        if filters.get("board"):
            boards = filters["board"].split(",")
            df = df.filter(pl.col("board").is_in(boards))

        # Size category filter
        if filters.get("size_category"):
            sizes = filters["size_category"].split(",")
            df = df.filter(pl.col("size_category").is_in(sizes))

        # Industry L1 filter
        if filters.get("industry_l1"):
            df = df.filter(pl.col("sw_industry_l1") == filters["industry_l1"])

        # Minimum score filter
        if filters.get("min_score") is not None:
            df = df.filter(pl.col(score_col) >= filters["min_score"])

        return df

    def _apply_sorting(
        self,
        df: pl.DataFrame,
        sort_by: str,
        sort_order: str,
        score_col: str,
    ) -> pl.DataFrame:
        """Apply sorting to the dataframe."""
        descending = sort_order == "desc"

        sort_map = {
            "score": score_col,
            "change": "pct_chg",
            "volume": "volume",
            "valuation": "pe_percentile",
            "main_strength": "main_strength_proxy",
            "code": "code",
        }

        col = sort_map.get(sort_by, score_col)

        # Handle missing columns
        if col not in df.columns:
            col = score_col

        return df.sort(col, descending=descending, nulls_last=True)

    def _convert_to_items(
        self,
        df: pl.DataFrame,
        score_col: str,
        mode: str,
    ) -> List[Dict[str, Any]]:
        """Convert Polars DataFrame to list of screener items."""
        items = []

        # Define label columns
        label_cols = [
            "label_main_accumulation",
            "label_undervalued",
            "label_oversold",
            "label_high_volatility",
            "label_breakout",
            "label_volume_surge",
        ]

        for row in df.iter_rows(named=True):
            # Collect active labels
            quant_labels = []
            label_map = {
                "label_main_accumulation": "main_accumulation",
                "label_undervalued": "undervalued",
                "label_oversold": "oversold",
                "label_high_volatility": "high_volatility",
                "label_breakout": "breakout",
                "label_volume_surge": "volume_surge",
            }
            for col, label in label_map.items():
                if row.get(col, False):
                    quant_labels.append(label)

            # Determine valuation level
            pe_pct = row.get("pe_percentile", 0.5)
            if pe_pct is not None:
                if pe_pct < 0.25:
                    val_level = "LOW"
                elif pe_pct < 0.75:
                    val_level = "MEDIUM"
                elif pe_pct < 0.9:
                    val_level = "HIGH"
                else:
                    val_level = "EXTREME"
            else:
                val_level = None

            item = {
                "code": row.get("code", ""),
                "name": row.get("name", ""),
                "asset_type": row.get("asset_type", "stock").lower(),
                "price": self._to_decimal(row.get("close")),
                "change_pct": self._to_decimal(row.get("pct_chg")) if mode == "snapshot" else None,
                "composite_score": self._to_decimal(row.get(score_col, 50)),
                "quant_labels": quant_labels,
                "main_strength_proxy": self._to_decimal(row.get("main_strength_proxy")),
                "valuation_level": val_level,
                "valuation_percentile": self._to_decimal(pe_pct * 100 if pe_pct is not None else None),
                "size_category": row.get("size_category"),
                "industry_l1": row.get("sw_industry_l1"),
            }

            # Period mode specific fields
            if mode == "period":
                item["period_return"] = self._to_decimal(row.get("period_return"))
                item["max_drawdown"] = self._to_decimal(row.get("max_drawdown"))
                item["avg_turnover"] = self._to_decimal(row.get("avg_turnover"))

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

    def _empty_response(
        self,
        tab: str,
        mode: str,
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Return empty response structure."""
        return {
            "items": [],
            "total": 0,
            "page": 1,
            "page_size": 50,
            "pages": 0,
            "tab": tab,
            "time_mode": mode,
            "date": target_date,
            "start_date": start_date,
            "end_date": end_date,
        }
