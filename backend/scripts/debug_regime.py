"""Debug regime scores for specific dates.

Usage:
    docker compose exec api python -m scripts.debug_regime
"""

import asyncio
import datetime
import logging

import polars as pl
from sqlalchemy import text

from app.db.session import async_session_maker
from scripts.alpha_radar_backtest import compute_regime_score

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


HOSTILE_DATES = [
    datetime.date(2025, 3, 24),
    datetime.date(2025, 9, 15),
    datetime.date(2025, 10, 13),
    datetime.date(2026, 2, 5),
]

# Also include some known-good dates for comparison
GOOD_DATES = [
    datetime.date(2025, 4, 7),
    datetime.date(2025, 7, 7),
    datetime.date(2025, 8, 4),
    datetime.date(2025, 8, 18),
    datetime.date(2026, 1, 5),
]


async def main():
    all_dates = sorted(HOSTILE_DATES + GOOD_DATES)
    earliest = min(all_dates) - datetime.timedelta(days=90)
    latest = max(all_dates)

    async with async_session_maker() as db:
        # Load index data
        result = await db.execute(
            text("""
                SELECT code, date, close, pct_chg
                FROM market_daily
                WHERE code = 'sh.000001'
                AND date >= :start AND date <= :end
                ORDER BY date
            """),
            {"start": earliest, "end": latest},
        )
        rows = result.fetchall()
        columns = list(result.keys())
        index_df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if not index_df.is_empty():
            index_df = index_df.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.col("close").cast(pl.Float64),
                ]
            )

        # Load market data for breadth calculation
        result = await db.execute(
            text("""
                SELECT md.code, md.date, md.pct_chg
                FROM market_daily md
                JOIN asset_meta am ON md.code = am.code
                WHERE md.date >= :start AND md.date <= :end
                AND am.asset_type = 'STOCK'
            """),
            {"start": earliest, "end": latest},
        )
        rows = result.fetchall()
        columns = list(result.keys())
        market_df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        if not market_df.is_empty():
            market_df = market_df.with_columns(
                [
                    pl.col("date").cast(pl.Date),
                    pl.col("pct_chg").cast(pl.Float64),
                ]
            )

    log.info(f"Index data: {index_df.height} rows")
    log.info(f"Market data: {market_df.height} rows")
    log.info("")

    # Compute detailed regime info for each date
    log.info("=" * 100)
    log.info(
        f"{'Date':>12} {'Type':>8} {'Regime':>7} {'Weak':>5} {'Abstain':>8}  | "
        f"{'Idx5dRet':>9} {'IdxComp':>8} {'BrMod':>7} {'BrToday':>8} {'Fragil':>7} | "
        f"{'BrTodayV':>9} {'Br5dAvg':>8} {'Panic':>6}"
    )
    log.info("-" * 100)

    for d in sorted(all_dates):
        label = "HOSTILE" if d in HOSTILE_DATES else "GOOD"

        # Replicate the compute_regime_score internals to extract components
        idx = index_df.filter(pl.col("date") <= d).sort("date")
        if idx.height < 10:
            log.info(f"{d} {label:>8}  INSUFFICIENT INDEX DATA")
            continue

        closes = idx["close"].to_list()
        close_today = closes[-1]
        close_5d_ago = closes[-5] if len(closes) >= 5 else close_today
        ret_5d = (close_today / close_5d_ago - 1) * 100 if close_5d_ago else 0.0

        # Index component
        if ret_5d > 3.0:
            index_component = max(10.0, 30.0 - (ret_5d - 3.0) * 5)
        elif ret_5d > 1.0:
            index_component = 50.0 - (ret_5d - 1.0) * 10
        elif ret_5d > -1.0:
            index_component = 60.0 + (-ret_5d) * 10
        elif ret_5d > -3.0:
            index_component = 70.0 + (-ret_5d - 1.0) * 7.5
        else:
            index_component = max(25.0, 55.0 + ret_5d * 5)
        index_component = max(0.0, min(100.0, index_component))

        # Breadth
        breadth_values = []
        breadth_today_val = 50.0
        breadth_5d_avg = 50.0

        if not market_df.is_empty():
            all_market_dates = (
                market_df.select("date")
                .unique()
                .filter(pl.col("date") <= d)
                .sort("date", descending=True)
                .head(5)
            )
            date_list = all_market_dates["date"].to_list()
            if date_list:
                stocks = market_df.filter(
                    (pl.col("date").is_in(date_list))
                    & (~pl.col("code").str.starts_with("sh.000"))
                    & (~pl.col("code").str.starts_with("sz.399"))
                )
                if not stocks.is_empty():
                    breadth_per_day = (
                        stocks.group_by("date")
                        .agg((pl.col("pct_chg").fill_null(0.0) > 0).mean().alias("breadth_pct"))
                        .sort("date", descending=True)
                        .with_columns(pl.col("breadth_pct") * 100)
                    )
                    if breadth_per_day.height > 0:
                        breadth_values = breadth_per_day["breadth_pct"].to_list()
                        breadth_today_val = breadth_values[0]
                        breadth_5d_avg = sum(breadth_values) / len(breadth_values)

        panic_days = sum(1 for v in breadth_values if v < 40)
        weak_days = sum(1 for v in breadth_values if v < 35)

        # Breadth modifier
        breadth_modifier = 0.0
        if ret_5d > 0 and breadth_5d_avg < 45:
            divergence_gap = ret_5d * (45 - breadth_5d_avg) / 45.0
            panic_penalty = panic_days * 3.0
            breadth_modifier = -(divergence_gap * 5.0 + panic_penalty)
            breadth_modifier = max(-25.0, breadth_modifier)
            if breadth_today_val > 60:
                attenuation = min(1.0, (breadth_today_val - 60) / 20.0)
                breadth_modifier *= 1.0 - attenuation * 0.7
        elif ret_5d <= 0 and breadth_5d_avg < 35:
            capitulation_depth = (35 - breadth_5d_avg) / 35.0
            breadth_modifier = capitulation_depth * 8.0
        elif breadth_5d_avg > 55:
            breadth_modifier = min(10.0, (breadth_5d_avg - 55) * 0.5)
        elif breadth_5d_avg < 45 and panic_days >= 3:
            breadth_modifier = -panic_days * 2.0

        breadth_today_modifier = 0.0
        if breadth_today_val > 60:
            breadth_today_modifier = min(20.0, (breadth_today_val - 60) * 1.0)
        elif breadth_today_val < 35:
            breadth_today_modifier = max(-15.0, (breadth_today_val - 35) * 0.5)

        fragility_penalty = 0.0
        if weak_days >= 2:
            fragility_penalty = weak_days * 5.0
        elif weak_days == 1 and panic_days >= 2:
            fragility_penalty = 5.0

        if fragility_penalty > 0 and breadth_today_val > 65:
            frag_attenuation = min(1.0, (breadth_today_val - 65) / 15.0)
            fragility_penalty *= 1.0 - frag_attenuation * 0.7

        score = index_component + breadth_modifier + breadth_today_modifier - fragility_penalty
        score = max(0.0, min(100.0, round(score, 1)))

        should_abstain = score < 45 and weak_days >= 3
        abstain_str = "ABSTAIN" if should_abstain else ""

        log.info(
            f"{d} {label:>8} {score:>7.1f} {weak_days:>5} {abstain_str:>8}  | "
            f"{ret_5d:>+9.2f}% {index_component:>7.1f} {breadth_modifier:>+7.1f} "
            f"{breadth_today_modifier:>+7.1f} {fragility_penalty:>7.1f} | "
            f"{breadth_today_val:>8.1f}% {breadth_5d_avg:>7.1f}% {panic_days:>5}"
        )

    log.info("")
    log.info("Breadth details for hostile dates:")
    log.info("-" * 80)
    for d in HOSTILE_DATES:
        all_market_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") <= d)
            .sort("date", descending=True)
            .head(5)
        )
        date_list = all_market_dates["date"].to_list()
        if date_list:
            stocks = market_df.filter(
                (pl.col("date").is_in(date_list))
                & (~pl.col("code").str.starts_with("sh.000"))
                & (~pl.col("code").str.starts_with("sz.399"))
            )
            breadth_per_day = (
                stocks.group_by("date")
                .agg(
                    [
                        (pl.col("pct_chg").fill_null(0.0) > 0).mean().alias("breadth_pct"),
                        pl.col("pct_chg").fill_null(0.0).mean().alias("avg_return"),
                        pl.len().alias("n_stocks"),
                    ]
                )
                .sort("date")
                .with_columns(
                    [
                        pl.col("breadth_pct") * 100,
                        pl.col("avg_return").round(3),
                    ]
                )
            )
            log.info(f"\n{d} (HOSTILE):")
            for row in breadth_per_day.iter_rows(named=True):
                marker = " <-- TARGET" if row["date"] == d else ""
                log.info(
                    f"  {row['date']}  breadth={row['breadth_pct']:5.1f}%  "
                    f"avg_ret={row['avg_return']:+.3f}%  n={row['n_stocks']}{marker}"
                )


if __name__ == "__main__":
    asyncio.run(main())
