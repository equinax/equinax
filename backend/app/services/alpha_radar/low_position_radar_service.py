"""Low Position Radar service — single-day mainline sector + low-position stock scoring."""

from __future__ import annotations

from datetime import date as dt_date, datetime
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.alpha_radar.engine.config_loader import (
    load_strategy_config,
    score_from_config,
)
from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine


_SECTOR_WEIGHTS = {
    "limit_up_count": 0.25,
    "median_change_pct": 0.15,
    "volume_expansion": 0.20,
    "main_inflow_yi": 0.20,
    "max_board_height": 0.10,
    "promotion_rate": 0.10,
}


def _parse_date(date_str: str) -> dt_date:
    s = date_str.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date format: {date_str!r} (expected YYYY-MM-DD or YYYYMMDD)")


def _minmax(series: pl.Series) -> pl.Series:
    if series.is_empty():
        return series
    lo_v = series.min()
    hi_v = series.max()
    if lo_v is None or hi_v is None:
        return pl.Series(name=series.name, values=[50.0] * len(series), dtype=pl.Float64)
    lo = float(lo_v)  # type: ignore[arg-type]
    hi = float(hi_v)  # type: ignore[arg-type]
    if hi == lo:
        return pl.Series(name=series.name, values=[50.0] * len(series), dtype=pl.Float64)
    return ((series.cast(pl.Float64) - lo) / (hi - lo) * 100.0).alias(series.name)


class LowPositionRadarService:
    """Single-day主线低位 radar — sector heat + low-position stock candidates.

    Reuses YAML-config scoring engine (no new engine class).
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.polars_engine = PolarsEngine(db)

    async def compute(self, date: str) -> dict[str, Any]:
        target_date = _parse_date(date)

        market_df_full = await self.polars_engine.load_market_data(
            target_date=target_date, lookback_days=60
        )
        if market_df_full.is_empty():
            return self._empty(target_date)

        df = self.polars_engine.calculate_technical_indicators(market_df_full)

        profile_df = await self.polars_engine.load_stock_profiles()
        if not profile_df.is_empty():
            df = df.join(profile_df, on="code", how="left")

        valuation_df = await self.polars_engine.load_valuation_data(target_date)
        if not valuation_df.is_empty():
            df = df.join(
                valuation_df.select(["code", "is_st", "total_mv", "circ_mv"]),
                on="code",
                how="left",
            )

        moneyflow_df = await self.polars_engine.load_moneyflow_data(target_date)
        if not moneyflow_df.is_empty():
            df = df.join(moneyflow_df, on="code", how="left")

        net_mf_df = await self._load_net_moneyflow(target_date)
        if not net_mf_df.is_empty():
            df = df.join(net_mf_df, on="code", how="left")

        limit_df = await self._load_limit_list(target_date)
        prev_limit_df = await self._load_prev_limit_list(target_date)

        if not limit_df.is_empty():
            df = df.join(limit_df, on="code", how="left")
        else:
            df = df.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("limit_type"),
                pl.lit(0, dtype=pl.Int64).alias("limit_times"),
            )

        sector_mom = PolarsEngine.compute_sector_momentum(market_df_full, profile_df, target_date)
        if not sector_mom.is_empty():
            df = df.join(
                sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
                on="sw_industry_l1",
                how="left",
            ).with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

        snapshot = df.filter(pl.col("date") == target_date)
        if snapshot.is_empty():
            return self._empty(target_date)

        snapshot = snapshot.with_columns(
            (
                (
                    (
                        pl.col("return_5d").fill_null(0.0)
                        - pl.col("sector_momentum_5d").fill_null(0.0)
                    ).clip(-15.0, 15.0)
                    + 15.0
                )
                / 30.0
                * 100.0
            ).alias("rs_vs_sector"),
            (
                pl.when(
                    (pl.col("limit_type") == "U")
                    & (pl.col("limit_times").fill_null(0) <= 1)
                    & (pl.col("price_position_60d").fill_null(1.0) < 0.4)
                )
                .then(pl.lit(100.0))
                .otherwise(pl.lit(0.0))
            ).alias("low_first_board_flag"),
            (
                (
                    (pl.lit(1.0) - pl.col("price_position_60d").fill_null(1.0)).clip(0.0, 1.0)
                    * pl.col("sector_momentum_5d").fill_null(0.0).clip(0.0, 10.0)
                    * 10.0
                ).clip(0.0, 100.0)
            ).alias("catchup_score"),
        )

        scoring_engine = ScoringEngine()
        regime = await self.polars_engine.load_market_regime(target_date)
        if regime:
            scoring_engine = ScoringEngine(
                market_regime_score=regime.get("market_regime_score", 50.0),
                ici_20d=regime.get("ici_20d", 0.0),
                dispersion_std=regime.get("dispersion_std", 0.0),
            )

        snapshot = scoring_engine._ensure_columns(snapshot)
        config = load_strategy_config("mainline", version="1")
        snapshot = score_from_config(config, snapshot, scoring_engine._apply_regime_discount)

        sectors_payload = self._build_sectors(snapshot)
        top_sectors = [s["name"] for s in sectors_payload[:10]]
        stocks_payload = self._build_stocks(snapshot, top_sectors)

        return {
            "date": target_date.isoformat(),
            "sectors": sectors_payload[:31],
            "stocks": stocks_payload[:100],
        }

    async def _load_net_moneyflow(self, target_date: dt_date) -> pl.DataFrame:
        result = await self.db.execute(
            text("SELECT code, net_mf_amount FROM moneyflow_daily WHERE date = :d"),
            {"d": target_date},
        )
        rows = result.fetchall()
        if not rows:
            return pl.DataFrame(schema={"code": pl.Utf8, "net_mf_amount": pl.Float64})
        return pl.DataFrame(
            {
                "code": [r[0] for r in rows],
                "net_mf_amount": [float(r[1]) if r[1] is not None else 0.0 for r in rows],
            }
        )

    async def _load_limit_list(self, target_date: dt_date) -> pl.DataFrame:
        result = await self.db.execute(
            text("SELECT code, limit_type, limit_times FROM limit_list_daily WHERE date = :d"),
            {"d": target_date},
        )
        rows = result.fetchall()
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(
            {
                "code": [r[0] for r in rows],
                "limit_type": [r[1] for r in rows],
                "limit_times": [int(r[2]) if r[2] is not None else 0 for r in rows],
            }
        )

    async def _load_prev_limit_list(self, target_date: dt_date) -> pl.DataFrame:
        result = await self.db.execute(
            text(
                """
                SELECT code, limit_times FROM limit_list_daily
                WHERE limit_type = 'U' AND date = (
                    SELECT MAX(date) FROM limit_list_daily WHERE date < :d
                )
                """
            ),
            {"d": target_date},
        )
        rows = result.fetchall()
        if not rows:
            return pl.DataFrame(schema={"code": pl.Utf8, "prev_limit_times": pl.Int64})
        return pl.DataFrame(
            {
                "code": [r[0] for r in rows],
                "prev_limit_times": [int(r[1]) if r[1] is not None else 1 for r in rows],
            }
        )

    def _build_sectors(self, snap: pl.DataFrame) -> list[dict[str, Any]]:
        if "sw_industry_l1" not in snap.columns:
            return []

        df = snap.filter(pl.col("sw_industry_l1").is_not_null())
        if df.is_empty():
            return []

        agg = df.group_by("sw_industry_l1").agg(
            [
                (pl.col("limit_type") == "U").sum().alias("limit_up_count"),
                pl.col("pct_chg").fill_null(0.0).median().alias("median_change_pct"),
                pl.col("vol_ramp_5v20").fill_null(0.0).median().alias("volume_expansion"),
                (pl.col("net_mf_amount").fill_null(0.0).sum() / 10000.0).alias("main_inflow_yi"),
                pl.col("limit_times").fill_null(0).max().alias("max_board_height"),
                (
                    (pl.col("limit_times").fill_null(0) >= 2).sum().cast(pl.Float64)
                    / pl.max_horizontal(
                        [(pl.col("limit_type") == "U").sum().cast(pl.Float64), pl.lit(1.0)]
                    )
                ).alias("promotion_rate"),
                pl.len().alias("stock_count"),
            ]
        )

        agg = agg.with_columns(
            [
                _minmax(agg["limit_up_count"].cast(pl.Float64)).alias("_n_lu"),
                _minmax(agg["median_change_pct"].cast(pl.Float64)).alias("_n_med"),
                _minmax(agg["volume_expansion"].cast(pl.Float64)).alias("_n_vol"),
                _minmax(agg["main_inflow_yi"].cast(pl.Float64)).alias("_n_mf"),
                _minmax(agg["max_board_height"].cast(pl.Float64)).alias("_n_bh"),
                _minmax(agg["promotion_rate"].cast(pl.Float64)).alias("_n_pr"),
            ]
        )

        agg = agg.with_columns(
            (
                pl.col("_n_lu") * _SECTOR_WEIGHTS["limit_up_count"]
                + pl.col("_n_med") * _SECTOR_WEIGHTS["median_change_pct"]
                + pl.col("_n_vol") * _SECTOR_WEIGHTS["volume_expansion"]
                + pl.col("_n_mf") * _SECTOR_WEIGHTS["main_inflow_yi"]
                + pl.col("_n_bh") * _SECTOR_WEIGHTS["max_board_height"]
                + pl.col("_n_pr") * _SECTOR_WEIGHTS["promotion_rate"]
            )
            .clip(0.0, 100.0)
            .alias("heat_score")
        ).sort("heat_score", descending=True)

        out: list[dict[str, Any]] = []
        for row in agg.iter_rows(named=True):
            out.append(
                {
                    "name": row["sw_industry_l1"],
                    "heat_score": round(float(row["heat_score"]), 2),
                    "limit_up_count": int(row["limit_up_count"]),
                    "median_change_pct": round(float(row["median_change_pct"]), 2),
                    "volume_expansion": round(float(row["volume_expansion"]), 3),
                    "main_inflow_yi": round(float(row["main_inflow_yi"]), 2),
                    "max_board_height": int(row["max_board_height"]),
                    "promotion_rate": round(float(row["promotion_rate"]), 3),
                    "stock_count": int(row["stock_count"]),
                }
            )
        return out

    def _build_stocks(self, snap: pl.DataFrame, top_sectors: list[str]) -> list[dict[str, Any]]:
        if not top_sectors or "mainline_score" not in snap.columns:
            return []

        df = snap.filter(pl.col("sw_industry_l1").is_in(top_sectors))
        if df.is_empty():
            return []

        df = df.sort("mainline_score", descending=True, nulls_last=True)
        out: list[dict[str, Any]] = []
        for row in df.iter_rows(named=True):
            tags = self._build_tags(row)
            out.append(
                {
                    "code": row.get("code", ""),
                    "name": row.get("name", ""),
                    "industry_l1": row.get("sw_industry_l1"),
                    "close": _to_float(row.get("close")),
                    "change_pct": _to_float(row.get("pct_chg")),
                    "radar_score": round(float(row.get("mainline_score") or 0.0), 2),
                    "price_position_60d": _to_float(row.get("price_position_60d")),
                    "rs_vs_sector": _to_float(row.get("rs_vs_sector")),
                    "elg_net_percentile": _to_float(row.get("elg_net_percentile")),
                    "ma_alignment_score": _to_float(row.get("ma_alignment_score")),
                    "volume_buildup_quality": _to_float(row.get("volume_buildup_quality")),
                    "tags": tags,
                }
            )
        return out

    @staticmethod
    def _build_tags(row: dict[str, Any]) -> list[str]:
        tags: list[str] = []
        if (row.get("low_first_board_flag") or 0) >= 50:
            tags.append("首板低位")
        if (row.get("price_position_60d") or 1.0) < 0.3:
            tags.append("深度低位")
        if (row.get("rs_vs_sector") or 50.0) > 65:
            tags.append("强相对")
        if (row.get("elg_net_percentile") or 50.0) > 80:
            tags.append("主力净流入")
        if (row.get("volume_buildup_quality") or 0.0) > 70:
            tags.append("蓄势充分")
        if (row.get("ma_alignment_score") or 0.0) > 70:
            tags.append("均线多头")
        if (row.get("catchup_score") or 0.0) > 60:
            tags.append("补涨候选")
        return tags

    @staticmethod
    def _empty(target_date: dt_date) -> dict[str, Any]:
        return {"date": target_date.isoformat(), "sectors": [], "stocks": []}


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    import math

    if math.isnan(f) or math.isinf(f):
        return None
    return round(f, 4)
