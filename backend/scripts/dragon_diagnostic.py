"""Dragon 龙头涨停 diagnostic script.

Deep analysis of dragon strategy: what factors distinguish winners from losers,
which stocks hit limit-up in evaluation window, and what pre-conditions predict
limit-up potential.

Usage:
    docker compose exec api python -m scripts.dragon_diagnostic
    docker compose exec api python -m scripts.dragon_diagnostic --dates 2025-10-13,2025-11-24
"""

import argparse
import asyncio
import datetime
import logging
import time

import polars as pl
from sqlalchemy import text

from app.db.session import async_session_maker
from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine
from app.services.alpha_radar.engine import score_tab
from scripts.alpha_radar_backtest import (
    load_all_data,
    compute_regime_score,
    compute_scores_for_date,
    sample_trading_days,
)

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


async def analyze_limit_up_overlap(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """Analyze how many recommended stocks actually hit limit-up in T+20 window."""

    log.info("\n" + "=" * 100)
    log.info("PART 1: LIMIT-UP OVERLAP ANALYSIS")
    log.info("How many dragon recommendations actually hit limit-up in T+20?")
    log.info("=" * 100)

    total_recs = 0
    total_with_limit_up = 0
    total_winning = 0
    total_losing = 0
    winners_with_limit_up = 0
    losers_with_limit_up = 0

    for d in test_dates:
        recs = compute_scores_for_date(
            d,
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            "dragon",
            5,
            moneyflow_df=moneyflow_df,
            limit_df=limit_df,
        )
        if not recs:
            continue

        # Get T+20 evaluation window
        future_dates = (
            market_df.select("date").unique().filter(pl.col("date") > d).sort("date").head(20)
        )
        if future_dates.height < 20:
            continue

        eval_dates = future_dates["date"].to_list()
        eval_end = eval_dates[-1]

        codes = [r["code"] for r in recs]

        # Check limit-up in window
        lu = limit_df.filter(
            (pl.col("code").is_in(codes))
            & (pl.col("date").is_in(eval_dates))
            & (pl.col("limit_type") == "U")
        )

        # Get T+20 returns
        ref_prices = market_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes))).select(
            ["code", pl.col("close").alias("ref_close")]
        )

        eval_prices = market_df.filter(
            (pl.col("date") == eval_end) & (pl.col("code").is_in(codes))
        ).select(["code", pl.col("close").alias("eval_close")])

        perf = ref_prices.join(eval_prices, on="code", how="inner")
        perf = perf.with_columns(
            ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
            .round(2)
            .alias("return_pct")
        )

        log.info(f"\n  {d}:")
        for r in recs:
            code = r["code"]
            ret_row = perf.filter(pl.col("code") == code)
            ret_val = ret_row["return_pct"][0] if not ret_row.is_empty() else None

            lu_code = lu.filter(pl.col("code") == code)
            lu_count = lu_code.height
            lu_dates_str = (
                ", ".join(str(d) for d in lu_code["date"].to_list()) if lu_count > 0 else "none"
            )

            is_winner = ret_val is not None and ret_val > 0
            marker = "✅" if is_winner else "❌" if ret_val is not None else "?"
            lu_marker = f"🔥 {lu_count}次涨停 ({lu_dates_str})" if lu_count > 0 else "无涨停"

            total_recs += 1
            if lu_count > 0:
                total_with_limit_up += 1
            if is_winner:
                total_winning += 1
                if lu_count > 0:
                    winners_with_limit_up += 1
            elif ret_val is not None:
                total_losing += 1
                if lu_count > 0:
                    losers_with_limit_up += 1

            ret_str = f"{ret_val:.2f}%" if ret_val is not None else "N/A"
            log.info(
                f"    {code} {r['name']:<8} score={r['score']:.1f} T+20={ret_str} {marker} | {lu_marker}"
            )

    log.info(f"\n{'=' * 80}")
    log.info(f"SUMMARY:")
    log.info(f"  Total recommendations: {total_recs}")
    log.info(
        f"  With limit-up in T+20: {total_with_limit_up} ({total_with_limit_up / max(total_recs, 1) * 100:.1f}%)"
    )
    log.info(f"  Winners: {total_winning} ({total_winning / max(total_recs, 1) * 100:.1f}%)")
    log.info(f"    ├ with limit-up: {winners_with_limit_up}")
    log.info(f"    └ without limit-up: {total_winning - winners_with_limit_up}")
    log.info(f"  Losers: {total_losing} ({total_losing / max(total_recs, 1) * 100:.1f}%)")
    log.info(f"    ├ with limit-up: {losers_with_limit_up}")
    log.info(f"    └ without limit-up: {total_losing - losers_with_limit_up}")


async def analyze_missed_dragons(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """Find stocks that hit limit-up but were NOT recommended — missed opportunities."""

    log.info("\n" + "=" * 100)
    log.info("PART 2: MISSED DRAGON ANALYSIS")
    log.info("Stocks that hit limit-up in T+20 but were NOT recommended")
    log.info("=" * 100)

    for d in test_dates:
        # Get recommendations
        recs = compute_scores_for_date(
            d,
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            "dragon",
            5,
            moneyflow_df=moneyflow_df,
            limit_df=limit_df,
        )
        rec_codes = {r["code"] for r in recs} if recs else set()

        # Get T+20 window
        future_dates = (
            market_df.select("date").unique().filter(pl.col("date") > d).sort("date").head(20)
        )
        if future_dates.height < 20:
            continue
        eval_dates = future_dates["date"].to_list()

        # Find ALL stocks that hit limit-up in window (first time)
        all_lu = limit_df.filter(
            (pl.col("date").is_in(eval_dates)) & (pl.col("limit_type") == "U")
        ).sort(["code", "date"])

        # Get first limit-up date per stock
        first_lu = all_lu.group_by("code").agg(
            pl.col("date").min().alias("first_lu_date"),
            pl.len().alias("lu_count"),
        )

        # Get the universe of stocks that had data on target_date
        universe = market_df.filter(
            (pl.col("date") == d)
            & (~pl.col("code").str.starts_with("sh.000"))
            & (~pl.col("code").str.starts_with("sz.399"))
        ).select(["code", "name", "close", "pct_chg"])

        # Join to find limit-up stocks that were in our universe
        lu_in_universe = first_lu.join(universe, on="code", how="inner")
        lu_in_universe = lu_in_universe.sort("lu_count", descending=True)

        missed = lu_in_universe.filter(~pl.col("code").is_in(list(rec_codes)))
        caught = lu_in_universe.filter(pl.col("code").is_in(list(rec_codes)))

        log.info(f"\n  {d}: {lu_in_universe.height} stocks hit limit-up in T+20 window")
        log.info(f"    Caught: {caught.height} | Missed: {missed.height}")

        # Show top missed stocks (most limit-ups)
        if missed.height > 0:
            top_missed = missed.head(5)
            log.info(f"    Top missed (by limit-up count):")
            for row in top_missed.iter_rows(named=True):
                log.info(
                    f"      {row['code']} {row.get('name', ''):<8} close={row.get('close', 0):.2f} lu_count={row['lu_count']} first_lu={row['first_lu_date']}"
                )


async def analyze_factor_distribution(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """Analyze factor distributions for winners vs losers on worst dates."""

    log.info("\n" + "=" * 100)
    log.info("PART 3: FACTOR ANALYSIS ON WORST DATES")
    log.info("Comparing factor values between winners and losers")
    log.info("=" * 100)

    # Focus on worst dates: WR <= 40%
    worst_dates = [
        datetime.date(2025, 4, 7),  # WR=40%, AR=-3.08%
        datetime.date(2025, 4, 21),  # WR=20%, AR=-3.65%
        datetime.date(2025, 10, 13),  # WR=0%, AR=-2.46%
        datetime.date(2025, 10, 27),  # WR=40%, AR=-1.95%
        datetime.date(2025, 11, 24),  # WR=20%, AR=-8.24%
    ]

    # Also analyze best dates for contrast
    best_dates = [
        datetime.date(2025, 2, 10),  # WR=100%, AR=6.37%
        datetime.date(2025, 6, 23),  # WR=100%, AR=17.11%
        datetime.date(2025, 7, 21),  # WR=100%, AR=3.14%
        datetime.date(2025, 12, 22),  # WR=80%, AR=10.05%
    ]

    factor_cols = [
        "main_strength_proxy",
        "accumulation_score",
        "volume_consistency_score",
        "trend_quality_20d",
        "volume_buildup_quality",
        "climax_score",
        "elg_net_percentile",
        "recent_vol_spike_max",
        "price_position_60d",
        "price_range_position_20d",
        "momentum_quality_ratio",
        "resistance_proximity_penalty",
        "sector_momentum_5d",
        "pct_chg",
        "return_5d",
        "vol_ramp_5v20",
        "turn_ramp_5v20",
        "ma_alignment_score",
        "stability_score",
        "days_since_vol_peak_20d",
        "post_spike_consolidation",
    ]

    engine = PolarsEngine.__new__(PolarsEngine)

    all_winner_data = []
    all_loser_data = []

    for d in worst_dates + best_dates:
        is_worst = d in worst_dates
        label = "WORST" if is_worst else "BEST"

        regime_score, weak_days, _ = compute_regime_score(
            d, index_df, market_df, moneyflow_df, limit_df
        )

        # Build full scored df (top 50 for analysis)
        all_dates = market_df.select("date").unique().sort("date")
        dates_before = all_dates.filter(pl.col("date") <= d)
        if dates_before.height < 20:
            continue
        lookback_dates = dates_before.tail(60)
        lookback_start = lookback_dates["date"][0]

        df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= d))
        df = engine.calculate_technical_indicators(df)
        df = df.filter(pl.col("date") == d)

        if not valuation_df.is_empty():
            val_day = valuation_df.filter(pl.col("date") == d)
            if not val_day.is_empty():
                val_day = val_day.with_columns(
                    [(pl.col("pe_ttm").rank() / pl.len()).alias("pe_percentile")]
                )
                df = df.join(val_day, on="code", how="left", suffix="_val")
                if "is_st" in df.columns:
                    df = df.filter(pl.col("is_st").fill_null(0) != 1)
                elif "is_st_val" in df.columns:
                    df = df.filter(pl.col("is_st_val").fill_null(0) != 1)

        if not style_df.is_empty():
            style_day = style_df.filter(pl.col("date") == d)
            if not style_day.is_empty():
                style_select_cols = ["code", "size_category", "momentum_20d", "momentum_60d"]
                for col in [
                    "value_percentile",
                    "momentum_percentile",
                    "turnover_percentile",
                    "size_percentile",
                    "ep_ratio",
                    "bp_ratio",
                ]:
                    if col in style_day.columns:
                        style_select_cols.append(col)
                df = df.join(
                    style_day.select(style_select_cols), on="code", how="left", suffix="_style"
                )

        if not profile_df.is_empty():
            df = df.join(profile_df, on="code", how="left")

        sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, d)
        if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
            df = df.join(
                sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
                on="sw_industry_l1",
                how="left",
            )
            df = df.with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

        if moneyflow_df is not None and not moneyflow_df.is_empty():
            mf_today = moneyflow_df.filter(pl.col("date") == d)
            if not mf_today.is_empty():
                mf_features = mf_today.with_columns(
                    [(pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw")]
                ).select(["code", "net_mf_amount", "elg_net_raw"])
                mf_features = mf_features.with_columns(
                    [
                        (pl.col("net_mf_amount").rank() / pl.len() * 100).alias(
                            "mf_net_percentile"
                        ),
                        (pl.col("elg_net_raw").rank() / pl.len() * 100).alias("elg_net_percentile"),
                    ]
                ).select(["code", "mf_net_percentile", "elg_net_percentile"])
                df = df.join(mf_features, on="code", how="left")
                df = df.with_columns(
                    [
                        pl.col("mf_net_percentile").fill_null(50.0),
                        pl.col("elg_net_percentile").fill_null(50.0),
                    ]
                )

        # Filter like the real pipeline
        if "near_limit_up" in df.columns:
            df = df.filter(pl.col("near_limit_up") == False)
        if "pct_chg" in df.columns:
            df = df.filter(pl.col("pct_chg").fill_null(0.0).abs() <= 5.0)

        # Score
        df, score_col = score_tab("dragon", df, market_regime_score=regime_score)  # type: ignore[arg-type]
        df = df.sort(score_col, descending=True)

        # Get top 5 (with sector diversification)
        if "sw_industry_l1" in df.columns:
            selected_indices = []
            sector_counts = {}
            for i, row in enumerate(df.iter_rows(named=True)):
                sector = row.get("sw_industry_l1", "")
                if sector and sector_counts.get(sector, 0) >= 2:
                    continue
                selected_indices.append(i)
                sector_counts[sector] = sector_counts.get(sector, 0) + 1
                if len(selected_indices) >= 5:
                    break
            top5 = df[selected_indices] if selected_indices else df.head(5)
        else:
            top5 = df.head(5)

        # Get T+20 returns
        future_dates = (
            market_df.select("date").unique().filter(pl.col("date") > d).sort("date").head(20)
        )
        if future_dates.height < 20:
            continue
        eval_date = future_dates["date"][-1]

        codes = top5["code"].to_list()
        ref_prices = market_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes))).select(
            ["code", pl.col("close").alias("ref_close")]
        )
        eval_prices = market_df.filter(
            (pl.col("date") == eval_date) & (pl.col("code").is_in(codes))
        ).select(["code", pl.col("close").alias("eval_close")])
        perf = ref_prices.join(eval_prices, on="code", how="inner")
        perf = perf.with_columns(
            ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
            .round(2)
            .alias("return_pct")
        )

        log.info(f"\n  [{label}] {d} (regime={regime_score:.1f}):")

        for row in top5.iter_rows(named=True):
            code = row["code"]
            ret_row = perf.filter(pl.col("code") == code)
            ret_val = ret_row["return_pct"][0] if not ret_row.is_empty() else None

            is_winner = ret_val is not None and ret_val > 0
            marker = "✅" if is_winner else "❌" if ret_val is not None else "?"
            ret_str = f"{ret_val:.2f}%" if ret_val is not None else "N/A"

            # Print key factors
            score = row.get(score_col, 0)
            ms = row.get("main_strength_proxy", 0)
            acc = row.get("accumulation_score", 0)
            vcs = row.get("volume_consistency_score", 0)
            tq = row.get("trend_quality_20d", 0)
            vbq = row.get("volume_buildup_quality", 0)
            cs = row.get("climax_score", 0)
            elg = row.get("elg_net_percentile", 0)
            pp60 = row.get("price_position_60d", 0)
            pp20 = row.get("price_range_position_20d", 0)
            mqr = row.get("momentum_quality_ratio", 0)
            rpp = row.get("resistance_proximity_penalty", 0)
            sm5 = row.get("sector_momentum_5d", 0)
            pchg = row.get("pct_chg", 0)
            r5d = row.get("return_5d", 0)
            ma_align = row.get("ma_alignment_score", 0)
            dsvp = row.get("days_since_vol_peak_20d", 0)
            psc = row.get("post_spike_consolidation", 0)

            log.info(
                f"    {code} {row.get('name', ''):<8} T+20={ret_str} {marker} score={score:.1f}"
            )
            log.info(f"      MS={ms:.1f} ACC={acc:.2f} VCS={vcs:.1f} TQ={tq:.1f} VBQ={vbq:.1f}")
            log.info(
                f"      CS={cs:.2f} ELG={elg:.1f} PP60={pp60:.2f} PP20={pp20:.2f} MA={ma_align:.1f}"
            )
            log.info(
                f"      MQR={mqr:.1f} RPP={rpp:.1f} SM5={sm5:.2f} pchg={pchg:.2f} r5d={r5d:.2f}"
            )
            log.info(f"      DSVP={dsvp:.0f} PSC={psc:.1f}")

            factor_data = {
                "date": str(d),
                "code": code,
                "is_winner": is_winner,
                "return": ret_val,
                "main_strength_proxy": ms,
                "accumulation_score": acc,
                "volume_consistency_score": vcs,
                "trend_quality_20d": tq,
                "volume_buildup_quality": vbq,
                "climax_score": cs,
                "elg_net_percentile": elg,
                "price_position_60d": pp60,
                "price_range_position_20d": pp20,
                "momentum_quality_ratio": mqr,
                "resistance_proximity_penalty": rpp,
                "sector_momentum_5d": sm5,
                "pct_chg": pchg,
                "return_5d": r5d,
                "ma_alignment_score": ma_align,
                "days_since_vol_peak_20d": dsvp,
                "post_spike_consolidation": psc,
            }

            if is_winner:
                all_winner_data.append(factor_data)
            elif ret_val is not None:
                all_loser_data.append(factor_data)

    # Aggregate comparison
    if all_winner_data and all_loser_data:
        log.info(f"\n{'=' * 80}")
        log.info(f"FACTOR AVERAGES: WINNERS vs LOSERS (across worst+best dates)")
        log.info(f"{'=' * 80}")
        log.info(f"{'Factor':<35} {'Winners':>10} {'Losers':>10} {'Delta':>10} {'Signal':>8}")
        log.info("-" * 80)

        compare_factors = [
            "main_strength_proxy",
            "accumulation_score",
            "volume_consistency_score",
            "trend_quality_20d",
            "volume_buildup_quality",
            "climax_score",
            "elg_net_percentile",
            "price_position_60d",
            "price_range_position_20d",
            "momentum_quality_ratio",
            "resistance_proximity_penalty",
            "sector_momentum_5d",
            "pct_chg",
            "return_5d",
            "ma_alignment_score",
            "days_since_vol_peak_20d",
            "post_spike_consolidation",
        ]

        for f in compare_factors:
            w_vals = [d[f] for d in all_winner_data if d.get(f) is not None]
            l_vals = [d[f] for d in all_loser_data if d.get(f) is not None]
            if w_vals and l_vals:
                w_avg = sum(w_vals) / len(w_vals)
                l_avg = sum(l_vals) / len(l_vals)
                delta = w_avg - l_avg
                signal = "🟢" if abs(delta) > 3 else "🟡" if abs(delta) > 1 else "⚪"
                log.info(f"  {f:<33} {w_avg:>10.2f} {l_avg:>10.2f} {delta:>+10.2f} {signal}")

        log.info(f"\n  Winners: {len(all_winner_data)} | Losers: {len(all_loser_data)}")


async def analyze_what_real_dragons_look_like(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """Reverse-engineer: look at stocks that DID hit consecutive limit-ups,
    and analyze their factor profiles on the recommendation date."""

    log.info("\n" + "=" * 100)
    log.info("PART 4: REVERSE-ENGINEER REAL DRAGONS")
    log.info("Stocks with 2+ limit-ups in T+20: what did they look like at recommendation time?")
    log.info("=" * 100)

    engine = PolarsEngine.__new__(PolarsEngine)

    for d in test_dates:
        # Get T+20 window
        future_dates = (
            market_df.select("date").unique().filter(pl.col("date") > d).sort("date").head(20)
        )
        if future_dates.height < 20:
            continue
        eval_dates = future_dates["date"].to_list()

        # Find stocks with 2+ limit-ups
        lu = limit_df.filter((pl.col("date").is_in(eval_dates)) & (pl.col("limit_type") == "U"))
        lu_counts = (
            lu.group_by("code").agg(pl.len().alias("lu_count")).filter(pl.col("lu_count") >= 2)
        )

        if lu_counts.height == 0:
            continue

        # Get their factor profiles on target_date
        all_dates = market_df.select("date").unique().sort("date")
        dates_before = all_dates.filter(pl.col("date") <= d)
        if dates_before.height < 20:
            continue
        lookback_dates = dates_before.tail(60)
        lookback_start = lookback_dates["date"][0]

        df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= d))
        df = engine.calculate_technical_indicators(df)
        df = df.filter(pl.col("date") == d)

        # Join profile for names
        if not profile_df.is_empty():
            df = df.join(profile_df, on="code", how="left")

        # Join sector momentum
        sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, d)
        if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
            df = df.join(
                sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
                on="sw_industry_l1",
                how="left",
            )

        # Join moneyflow
        if moneyflow_df is not None and not moneyflow_df.is_empty():
            mf_today = moneyflow_df.filter(pl.col("date") == d)
            if not mf_today.is_empty():
                mf_features = mf_today.with_columns(
                    [(pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw")]
                ).select(["code", "net_mf_amount", "elg_net_raw"])
                mf_features = mf_features.with_columns(
                    [
                        (pl.col("net_mf_amount").rank() / pl.len() * 100).alias(
                            "mf_net_percentile"
                        ),
                        (pl.col("elg_net_raw").rank() / pl.len() * 100).alias("elg_net_percentile"),
                    ]
                ).select(["code", "mf_net_percentile", "elg_net_percentile"])
                df = df.join(mf_features, on="code", how="left")

        dragons = df.filter(pl.col("code").is_in(lu_counts["code"].to_list()))

        if dragons.height == 0:
            continue

        # Join lu_count
        dragons = dragons.join(lu_counts, on="code", how="left")
        dragons = dragons.sort("lu_count", descending=True)

        # Get T+20 return
        eval_end = eval_dates[-1]

        log.info(f"\n  {d}: {dragons.height} stocks had 2+ limit-ups in next 20 days")

        for row in dragons.head(8).iter_rows(named=True):
            code = row["code"]
            lu_count = row.get("lu_count", 0)

            ref_close = row.get("close", 0)
            eval_row = market_df.filter((pl.col("date") == eval_end) & (pl.col("code") == code))
            eval_close = eval_row["close"][0] if not eval_row.is_empty() else None
            ret = (
                (eval_close / ref_close - 1) * 100
                if ref_close and eval_close and ref_close > 0
                else 0
            )

            ms = row.get("main_strength_proxy", 0) or 0
            acc = row.get("accumulation_score", 0) or 0
            vcs = row.get("volume_consistency_score", 0) or 0
            tq = row.get("trend_quality_20d", 0) or 0
            vbq = row.get("volume_buildup_quality", 0) or 0
            cs = row.get("climax_score", 0) or 0
            pp60 = row.get("price_position_60d", 0) or 0
            pp20 = row.get("price_range_position_20d", 0) or 0
            ma = row.get("ma_alignment_score", 0) or 0
            r5d = row.get("return_5d", 0) or 0
            pchg = row.get("pct_chg", 0) or 0
            elg = row.get("elg_net_percentile", 0) or 0
            sm5 = row.get("sector_momentum_5d", 0) or 0

            log.info(f"    {code} {row.get('name', ''):<8} lu={lu_count} T+20={ret:.1f}%")
            log.info(f"      MS={ms:.1f} ACC={acc:.2f} VCS={vcs:.1f} TQ={tq:.1f} VBQ={vbq:.1f}")
            log.info(f"      CS={cs:.2f} PP60={pp60:.2f} PP20={pp20:.2f} MA={ma:.1f} ELG={elg:.1f}")
            log.info(f"      r5d={r5d:.2f} pchg={pchg:.2f} SM5={sm5:.2f}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", type=str, default=None)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for date sampling (default: random each run)",
    )
    args = parser.parse_args()

    if args.dates:
        test_dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    else:
        test_dates = sample_trading_days(n=25, seed=args.seed)

    earliest = min(test_dates)
    latest = max(test_dates)
    end_date = latest + datetime.timedelta(days=55)

    log.info("Loading data...")
    async with async_session_maker() as db:
        (
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
        ) = await load_all_data(db, earliest, end_date, lookback_days=60)

    await analyze_limit_up_overlap(
        test_dates, market_df, valuation_df, style_df, profile_df, index_df, moneyflow_df, limit_df
    )
    await analyze_missed_dragons(
        test_dates, market_df, valuation_df, style_df, profile_df, index_df, moneyflow_df, limit_df
    )
    await analyze_factor_distribution(
        test_dates, market_df, valuation_df, style_df, profile_df, index_df, moneyflow_df, limit_df
    )
    await analyze_what_real_dragons_look_like(
        test_dates, market_df, valuation_df, style_df, profile_df, index_df, moneyflow_df, limit_df
    )


if __name__ == "__main__":
    asyncio.run(main())
