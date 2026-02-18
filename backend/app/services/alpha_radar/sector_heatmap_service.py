"""Sector Heatmap service for Alpha Radar.

Provides industry-level aggregation for treemap visualization.
Includes panorama scoring logic used exclusively by this service.
"""

from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.polars_engine import PolarsEngine


# --- Panorama scoring (self-contained, used only by heatmap) ---

# Columns that panorama scoring needs, with their defaults
_PANORAMA_REQUIRED_COLUMNS = {
    "accumulation_score": 0.0,
    "ma_alignment_score": 50.0,
    "momentum_percentile": 0.5,
    "trend_quality_20d": 50.0,
    "volume_buildup_quality": 40.0,
    "climax_score": 0.0,
    "post_spike_consolidation": 50.0,
    "recent_vol_spike_max": 0.0,
}


def _calculate_panorama_score(df: pl.DataFrame, market_regime_score: float = 50.0) -> pl.DataFrame:
    """Calculate panorama (全景) composite score for heatmap visualization.

    Iter 11: Low-VBQ penalty from cross-date failure analysis (12-01, 12-22).

    Observation: On mixed-result dates, losers have significantly lower VBQ
    than winners.  12-01: loser group avg VBQ=67.3 vs winner group avg VBQ=78.3
    (delta +11.6).  11-10: delta +14.8.  Low VBQ indicates sloppy/inconsistent
    volume buildup — distribution rather than accumulation.

    Implementation: penalty fires only when VBQ < 65.
    Formula: ((65 - VBQ).clip(0, 25) / 25) * 100.  Weight: 0.05.

    Result: Panorama WR 80.0% → 82.9% (+2.9pp).  Other tabs unchanged.

    Note: Quadratic spike penalty was also tested (Attempt A) but REJECTED —
    it reduced penalty on low-spike losers (荣昌生物 spike=5.5, T+5=-8.64%),
    promoting them into top 5 and causing panorama regression to 74.3%.
    Linear spike * 0.15 retained.
    """
    if df.is_empty():
        return df

    # Ensure required columns exist with defaults
    for col, default in _PANORAMA_REQUIRED_COLUMNS.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(default).alias(col))

    df = df.with_columns(
        [
            (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
            (pl.col("ma_alignment_score").fill_null(50.0)).alias("alignment_component"),
            (pl.col("momentum_percentile").fill_null(0.5) * 100).alias("momentum_component"),
            (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
            (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
            ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
            (pl.col("post_spike_consolidation").fill_null(50.0)).alias("consolidation_component"),
            (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
            # Iter 11: low-VBQ penalty — fires only when VBQ < 65
            (
                (65.0 - pl.col("volume_buildup_quality").fill_null(40.0)).clip(0.0, 25.0)
                / 25.0
                * 100
            ).alias("low_vbq_penalty"),
        ]
    )

    raw_score = (
        pl.col("accumulation_component") * 0.15
        + pl.col("alignment_component") * 0.10
        + pl.col("momentum_component") * 0.05
        + pl.col("trend_quality_component") * 0.10
        + pl.col("buildup_component") * 0.10
        + pl.col("anti_climax_component") * 0.20
        + pl.col("consolidation_component") * 0.15
        - pl.col("recent_spike_penalty") * 0.15
        - pl.col("low_vbq_penalty") * 0.05
    )

    # Apply market regime discount
    rs = market_regime_score
    if rs >= 60:
        discount = min(1.25, 1.0 + (rs - 60.0) / 40.0 * 0.25)
    elif rs >= 40:
        discount = max(0.85, min(1.0, 1.0 + (rs - 50.0) / 50.0 * 0.15))
    elif rs >= 30:
        discount = 0.85 - (40.0 - rs) / 10.0 * 0.10
    else:
        discount = max(0.60, 0.75 - (30.0 - rs) / 30.0 * 0.15)

    df = df.with_columns(
        [(raw_score.clip(0.0, 100.0) * discount).clip(0.0, 100.0).alias("panorama_score")]
    )

    return df


class SectorHeatmapService:
    """
    Service for generating sector heatmap data.

    Aggregates market data by industry (SW申万 L1/L2) for treemap visualization.
    Supports multiple metrics: change_pct, amount, main_strength, score.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.polars_engine = PolarsEngine(db)

    async def get_sector_heatmap(
        self,
        metric: str = "change",
        mode: str = "snapshot",
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Get sector heatmap data aggregated by industry.

        Args:
            metric: Metric to use for coloring (change, amount, main_strength, score)
            mode: Time mode (snapshot, period)
            target_date: Target date for snapshot mode
            start_date: Start date for period mode
            end_date: End date for period mode

        Returns:
            Dict with sectors (nested L1 -> L2), min_value, max_value, market_avg
        """
        # Resolve dates
        if mode == "snapshot":
            if target_date is None:
                target_date = await self.polars_engine.get_latest_trading_date()
            if target_date is None:
                return self._empty_response(metric, mode)
        else:
            # Period mode
            if end_date is None:
                end_date = await self.polars_engine.get_latest_trading_date()
            if start_date is None:
                start_date = end_date

        # Load market data
        df = await self.polars_engine.load_market_data(
            target_date=target_date if mode == "snapshot" else None,
            start_date=start_date if mode == "period" else None,
            end_date=end_date if mode == "period" else None,
            lookback_days=20,  # Less lookback needed for heatmap
        )

        if df.is_empty():
            return self._empty_response(metric, mode, target_date, start_date, end_date)

        # Calculate technical indicators for main_strength
        df = self.polars_engine.calculate_technical_indicators(df)

        # Get latest data point per stock for snapshot mode
        if mode == "snapshot":
            df = df.filter(pl.col("date") == target_date)
        else:
            # Period mode - get end date data with period metrics
            df = df.filter(pl.col("date") == end_date)

        # Load and join stock profiles for industry classification
        profile_df = await self.polars_engine.load_stock_profiles()
        if profile_df.is_empty():
            return self._empty_response(metric, mode, target_date, start_date, end_date)

        df = df.join(profile_df, on="code", how="left")

        # Filter out stocks without industry classification
        df = df.filter(pl.col("sw_industry_l1").is_not_null())

        if df.is_empty():
            return self._empty_response(metric, mode, target_date, start_date, end_date)

        # Calculate panorama score for score metric
        if metric == "score":
            # Load valuation data
            valuation_df = await self.polars_engine.load_valuation_data(
                target_date if mode == "snapshot" else end_date
            )
            if not valuation_df.is_empty():
                valuation_df = valuation_df.with_columns(
                    [
                        (pl.col("pe_ttm").rank() / pl.len()).alias("pe_percentile"),
                    ]
                )
                df = df.join(valuation_df, on="code", how="left")

            # Load style factors
            style_df = await self.polars_engine.load_style_factors(
                target_date if mode == "snapshot" else end_date
            )
            if not style_df.is_empty():
                df = df.join(
                    style_df.select(["code", "size_category", "momentum_20d", "momentum_60d"]),
                    on="code",
                    how="left",
                    suffix="_style",
                )

            df = _calculate_panorama_score(df)

        # Aggregate by L2 first, then by L1
        sectors = self._aggregate_sectors(df, metric)

        # Calculate min/max/avg
        all_values = []
        for sector in sectors:
            all_values.append(float(sector["value"]))
            for child in sector.get("children", []):
                all_values.append(float(child["value"]))

        min_value = min(all_values) if all_values else 0
        max_value = max(all_values) if all_values else 0
        market_avg = sum(all_values) / len(all_values) if all_values else 0

        return {
            "time_mode": mode,
            "date": target_date if mode == "snapshot" else None,
            "start_date": start_date if mode == "period" else None,
            "end_date": end_date if mode == "period" else None,
            "metric": metric,
            "sectors": sectors,
            "min_value": self._to_decimal(min_value),
            "max_value": self._to_decimal(max_value),
            "market_avg": self._to_decimal(market_avg),
        }

    def _aggregate_sectors(
        self,
        df: pl.DataFrame,
        metric: str,
    ) -> List[Dict[str, Any]]:
        """
        Aggregate data by industry L1 and L2.

        Returns nested structure:
        [
            {
                name: "银行",
                stock_count: 42,
                value: 1.23,  # based on metric
                size_value: 1234567890,  # amount for area sizing
                children: [
                    { name: "国有大行", stock_count: 6, value: 0.85, ... },
                    { name: "股份制银行", stock_count: 12, value: 1.56, ... },
                    ...
                ],
                ...
            },
            ...
        ]
        """
        # First aggregate by L2
        l2_agg = df.group_by(["sw_industry_l1", "sw_industry_l2"]).agg(
            [
                pl.len().alias("stock_count"),
                pl.col("pct_chg").mean().alias("avg_change_pct"),
                pl.col("amount").sum().alias("total_amount"),
                pl.col("main_strength_proxy").mean().alias("avg_main_strength"),
                (
                    pl.col("panorama_score").mean()
                    if "panorama_score" in df.columns
                    else pl.lit(50.0)
                ).alias("avg_score"),
                (pl.col("pct_chg") > 0).sum().alias("up_count"),
                (pl.col("pct_chg") < 0).sum().alias("down_count"),
                (pl.col("pct_chg") == 0).sum().alias("flat_count"),
            ]
        )

        # Determine value column based on metric
        value_col = {
            "change": "avg_change_pct",
            "amount": "total_amount",
            "main_strength": "avg_main_strength",
            "score": "avg_score",
        }.get(metric, "avg_change_pct")

        # Build nested structure
        sectors_dict: Dict[str, Dict[str, Any]] = {}

        for row in l2_agg.iter_rows(named=True):
            l1_name = row["sw_industry_l1"]
            l2_name = row["sw_industry_l2"]

            if l1_name not in sectors_dict:
                sectors_dict[l1_name] = {
                    "name": l1_name,
                    "stock_count": 0,
                    "value": 0.0,
                    "size_value": 0.0,
                    "avg_change_pct": 0.0,
                    "total_amount": 0.0,
                    "avg_main_strength": 0.0,
                    "avg_score": 0.0,
                    "up_count": 0,
                    "down_count": 0,
                    "flat_count": 0,
                    "children": [],
                    "_child_count": 0,
                    "_sum_change": 0.0,
                    "_sum_strength": 0.0,
                    "_sum_score": 0.0,
                }

            # Add L2 child
            l2_value = row.get(value_col) or 0
            l2_amount = row.get("total_amount") or 0
            l2_item = {
                "name": l2_name or l1_name,
                "stock_count": row["stock_count"] or 0,
                "value": self._to_decimal(l2_value),
                "size_value": self._to_decimal(l2_amount),
                "avg_change_pct": self._to_decimal(row.get("avg_change_pct")),
                "total_amount": self._to_decimal(row.get("total_amount")),
                "avg_main_strength": self._to_decimal(row.get("avg_main_strength")),
                "avg_score": self._to_decimal(row.get("avg_score")),
                "up_count": row.get("up_count") or 0,
                "down_count": row.get("down_count") or 0,
                "flat_count": row.get("flat_count") or 0,
            }
            sectors_dict[l1_name]["children"].append(l2_item)

            # Accumulate L1 totals
            sector = sectors_dict[l1_name]
            sector["stock_count"] += row["stock_count"] or 0
            sector["total_amount"] = float(sector["total_amount"]) + (row.get("total_amount") or 0)
            sector["up_count"] += row.get("up_count") or 0
            sector["down_count"] += row.get("down_count") or 0
            sector["flat_count"] += row.get("flat_count") or 0
            sector["_child_count"] += 1
            sector["_sum_change"] += (row.get("avg_change_pct") or 0) * (row["stock_count"] or 1)
            sector["_sum_strength"] += (row.get("avg_main_strength") or 0) * (
                row["stock_count"] or 1
            )
            sector["_sum_score"] += (row.get("avg_score") or 0) * (row["stock_count"] or 1)

        # Finalize L1 aggregations
        sectors = []
        for l1_name, sector in sectors_dict.items():
            if sector["stock_count"] > 0:
                sector["avg_change_pct"] = self._to_decimal(
                    sector["_sum_change"] / sector["stock_count"]
                )
                sector["avg_main_strength"] = self._to_decimal(
                    sector["_sum_strength"] / sector["stock_count"]
                )
                sector["avg_score"] = self._to_decimal(sector["_sum_score"] / sector["stock_count"])

            # Set value based on metric
            if metric == "change":
                sector["value"] = sector["avg_change_pct"]
            elif metric == "amount":
                sector["value"] = self._to_decimal(sector["total_amount"])
            elif metric == "main_strength":
                sector["value"] = sector["avg_main_strength"]
            elif metric == "score":
                sector["value"] = sector["avg_score"]
            else:
                sector["value"] = sector["avg_change_pct"]

            sector["size_value"] = self._to_decimal(sector["total_amount"])
            sector["total_amount"] = self._to_decimal(sector["total_amount"])

            # Clean up temp fields
            del sector["_child_count"]
            del sector["_sum_change"]
            del sector["_sum_strength"]
            del sector["_sum_score"]

            # Sort children by size_value (amount) descending
            sector["children"].sort(key=lambda x: float(x.get("size_value") or 0), reverse=True)

            sectors.append(sector)

        # Sort sectors by size_value (amount) descending
        sectors.sort(key=lambda x: float(x.get("size_value") or 0), reverse=True)

        return sectors

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
        metric: str,
        mode: str,
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Return empty response structure."""
        return {
            "time_mode": mode,
            "date": target_date,
            "start_date": start_date,
            "end_date": end_date,
            "metric": metric,
            "sectors": [],
            "min_value": Decimal("0"),
            "max_value": Decimal("0"),
            "market_avg": Decimal("0"),
        }
