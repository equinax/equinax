"""Rally 大盘主升 diagnostic script.

Deep analysis of rally strategy: factor discrimination (winners vs losers),
market-cap distribution of picks, size-return correlation, and per-date
qualitative breakdown.

Usage:
    docker compose exec api python -m scripts.rally_diagnostic
    docker compose exec api python -m scripts.rally_diagnostic --seed 42
    docker compose exec api python -m scripts.rally_diagnostic --dates 2025-03-26,2025-11-24
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
from app.services.alpha_radar.engine.config_loader import load_strategy_config
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

TAB = "rally"
_rally_config = load_strategy_config(TAB)
EVAL_PERIOD = _rally_config.eval_period
TOP_N = _rally_config.backtest_top_n


async def analyze_market_cap_distribution(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """PART 1: What market cap are we actually selecting? Are they large-cap?"""

    log.info("\n" + "=" * 100)
    log.info("PART 1: MARKET CAP DISTRIBUTION OF RALLY PICKS")
    log.info("Are we selecting large-cap stocks as intended?")
    log.info("=" * 100)

    all_picks_data = []
    size_category_counts: dict[str, int] = {}

    for d in test_dates:
        recs = compute_scores_for_date(
            d,
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            TAB,
            TOP_N,
            moneyflow_df=moneyflow_df,
            limit_df=limit_df,
        )
        if not recs:
            continue

        codes = [r["code"] for r in recs]

        val_day = valuation_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes)))

        # Get style data for size category
        sty_day = (
            style_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes)))
            if not style_df.is_empty()
            else pl.DataFrame()
        )

        # Get T+10 returns
        future_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") > d)
            .sort("date")
            .head(EVAL_PERIOD)
        )
        if future_dates.height < EVAL_PERIOD:
            continue
        eval_date = future_dates["date"][-1]

        ref_prices = market_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes))).select(
            ["code", pl.col("close").alias("ref_close")]
        )
        eval_prices = market_df.filter(
            (pl.col("date") == eval_date) & (pl.col("code").is_in(codes))
        ).select(["code", pl.col("close").alias("eval_close")])
        perf = ref_prices.join(eval_prices, on="code", how="inner").with_columns(
            ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
            .round(2)
            .alias("return_pct")
        )

        for r in recs:
            code = r["code"]
            # Market cap
            val_row = val_day.filter(pl.col("code") == code)
            total_mv = (
                val_row["total_mv"][0]
                if not val_row.is_empty() and "total_mv" in val_row.columns
                else None
            )
            circ_mv = (
                val_row["circ_mv"][0]
                if not val_row.is_empty() and "circ_mv" in val_row.columns
                else None
            )

            # Size category
            sty_row = (
                sty_day.filter(pl.col("code") == code) if not sty_day.is_empty() else pl.DataFrame()
            )
            size_cat = (
                sty_row["size_category"][0]
                if not sty_row.is_empty() and "size_category" in sty_row.columns
                else "UNKNOWN"
            )
            size_pct = (
                sty_row["size_percentile"][0]
                if not sty_row.is_empty() and "size_percentile" in sty_row.columns
                else None
            )

            # Return
            ret_row = perf.filter(pl.col("code") == code)
            ret_val = ret_row["return_pct"][0] if not ret_row.is_empty() else None

            size_category_counts[size_cat] = size_category_counts.get(size_cat, 0) + 1

            all_picks_data.append(
                {
                    "date": str(d),
                    "code": code,
                    "name": r["name"],
                    "score": r["score"],
                    "total_mv": total_mv,
                    "circ_mv": circ_mv,
                    "size_category": size_cat,
                    "size_percentile": size_pct,
                    "return_pct": ret_val,
                }
            )

    # Summary
    total = len(all_picks_data)
    log.info(f"\n  Total picks analyzed: {total}")
    log.info(f"\n  SIZE CATEGORY DISTRIBUTION:")
    for cat in ["MEGA", "LARGE", "MID", "SMALL", "MICRO", "UNKNOWN"]:
        count = size_category_counts.get(cat, 0)
        pct = count / max(total, 1) * 100
        bar = "█" * int(pct / 2)
        log.info(f"    {cat:<8}: {count:>3} ({pct:>5.1f}%) {bar}")

    # Market cap stats
    mvs = [p["total_mv"] for p in all_picks_data if p["total_mv"] is not None]
    if mvs:
        mvs_sorted = sorted(mvs)
        log.info(f"\n  TOTAL MARKET VALUE (万元) STATS:")
        log.info(f"    Min:    {min(mvs):>12,.0f}")
        log.info(f"    P25:    {mvs_sorted[len(mvs_sorted) // 4]:>12,.0f}")
        log.info(f"    Median: {mvs_sorted[len(mvs_sorted) // 2]:>12,.0f}")
        log.info(f"    P75:    {mvs_sorted[3 * len(mvs_sorted) // 4]:>12,.0f}")
        log.info(f"    Max:    {max(mvs):>12,.0f}")
        # Convert to 亿元 for readability
        log.info(f"\n  IN 亿元:")
        log.info(f"    Min:    {min(mvs) / 10000:>8.1f}亿")
        log.info(f"    P25:    {mvs_sorted[len(mvs_sorted) // 4] / 10000:>8.1f}亿")
        log.info(f"    Median: {mvs_sorted[len(mvs_sorted) // 2] / 10000:>8.1f}亿")
        log.info(f"    P75:    {mvs_sorted[3 * len(mvs_sorted) // 4] / 10000:>8.1f}亿")
        log.info(f"    Max:    {max(mvs) / 10000:>8.1f}亿")

    # Size percentile stats
    size_pcts = [p["size_percentile"] for p in all_picks_data if p["size_percentile"] is not None]
    if size_pcts:
        size_pcts_sorted = sorted(size_pcts)
        log.info(f"\n  SIZE PERCENTILE STATS (0=smallest, 1=largest):")
        log.info(f"    Min:    {min(size_pcts):.3f}")
        log.info(f"    P25:    {size_pcts_sorted[len(size_pcts_sorted) // 4]:.3f}")
        log.info(f"    Median: {size_pcts_sorted[len(size_pcts_sorted) // 2]:.3f}")
        log.info(f"    P75:    {size_pcts_sorted[3 * len(size_pcts_sorted) // 4]:.3f}")
        log.info(f"    Max:    {max(size_pcts):.3f}")

    # WR by size category
    log.info(f"\n  WIN RATE BY SIZE CATEGORY:")
    for cat in ["MEGA", "LARGE", "MID", "SMALL", "MICRO"]:
        cat_picks = [
            p for p in all_picks_data if p["size_category"] == cat and p["return_pct"] is not None
        ]
        if cat_picks:
            wins = sum(1 for p in cat_picks if p["return_pct"] > 0)
            avg_ret = sum(p["return_pct"] for p in cat_picks) / len(cat_picks)
            wr = wins / len(cat_picks) * 100
            log.info(f"    {cat:<8}: WR={wr:>5.1f}% AR={avg_ret:>+6.2f}% (n={len(cat_picks)})")

    return all_picks_data


async def analyze_factor_discrimination(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """PART 2: Factor discrimination — which factors separate winners from losers?"""

    log.info("\n" + "=" * 100)
    log.info("PART 2: FACTOR DISCRIMINATION (WINNERS vs LOSERS)")
    log.info("Which factors distinguish rally winners from losers?")
    log.info("=" * 100)

    engine = PolarsEngine.__new__(PolarsEngine)

    all_winner_data = []
    all_loser_data = []

    factor_cols = [
        # Scoring formula components
        "ma_alignment_score",
        "trend_quality_20d",
        "volume_buildup_quality",
        "accumulation_score",
        "post_spike_consolidation",
        "momentum_quality_ratio",
        "climax_score",
        "recent_vol_spike_max",
        "resistance_proximity_penalty",
        # Additional factors for analysis
        "main_strength_proxy",
        "volume_consistency_score",
        "price_position_60d",
        "price_range_position_20d",
        "sector_momentum_5d",
        "pct_chg",
        "return_5d",
        "stability_score",
        "vol_ramp_5v20",
        "days_since_vol_peak_20d",
        # Size factors (key for this strategy)
        "total_mv",
        "circ_mv",
        "size_percentile",
    ]

    for d in test_dates:
        regime_score, weak_days, regime_details = compute_regime_score(
            d, index_df, market_df, moneyflow_df, limit_df
        )

        # Build full scored df
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

        # Filter same as pipeline
        if "near_limit_up" in df.columns:
            df = df.filter(pl.col("near_limit_up") == False)  # noqa: E712

        # Score
        df, score_col = score_tab(TAB, df, market_regime_score=regime_score)  # type: ignore[arg-type]
        df = df.sort(score_col, descending=True)

        # Sector diversification (same as backtest)
        if "sw_industry_l1" in df.columns:
            selected_indices = []
            sector_counts = {}
            for i, row in enumerate(df.iter_rows(named=True)):
                sector = row.get("sw_industry_l1", "")
                if sector and sector_counts.get(sector, 0) >= 2:
                    continue
                selected_indices.append(i)
                sector_counts[sector] = sector_counts.get(sector, 0) + 1
                if len(selected_indices) >= TOP_N:
                    break
            top_df = df[selected_indices] if selected_indices else df.head(TOP_N)
        else:
            top_df = df.head(TOP_N)

        # Get T+10 returns
        future_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") > d)
            .sort("date")
            .head(EVAL_PERIOD)
        )
        if future_dates.height < EVAL_PERIOD:
            continue
        eval_date = future_dates["date"][-1]

        codes = top_df["code"].to_list()
        ref_prices = market_df.filter((pl.col("date") == d) & (pl.col("code").is_in(codes))).select(
            ["code", pl.col("close").alias("ref_close")]
        )
        eval_prices = market_df.filter(
            (pl.col("date") == eval_date) & (pl.col("code").is_in(codes))
        ).select(["code", pl.col("close").alias("eval_close")])
        perf = ref_prices.join(eval_prices, on="code", how="inner").with_columns(
            ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
            .round(2)
            .alias("return_pct")
        )

        log.info(f"\n  {d} (regime={regime_score:.1f}):")

        for row in top_df.iter_rows(named=True):
            code = row["code"]
            ret_row = perf.filter(pl.col("code") == code)
            ret_val = ret_row["return_pct"][0] if not ret_row.is_empty() else None

            is_winner = ret_val is not None and ret_val > 0
            marker = "✅" if is_winner else "❌" if ret_val is not None else "?"
            ret_str = f"{ret_val:.2f}%" if ret_val is not None else "N/A"

            score = row.get(score_col, 0)
            total_mv = row.get("total_mv", 0) or 0
            circ_mv = row.get("circ_mv", 0) or 0
            size_cat = row.get("size_category", "?")
            size_pct = row.get("size_percentile", None)
            size_pct_str = f"{size_pct:.3f}" if size_pct is not None else "N/A"

            ma_align = row.get("ma_alignment_score", 0) or 0
            tq = row.get("trend_quality_20d", 0) or 0
            vbq = row.get("volume_buildup_quality", 0) or 0
            acc = row.get("accumulation_score", 0) or 0
            psc = row.get("post_spike_consolidation", 0) or 0
            mqr = row.get("momentum_quality_ratio", 0) or 0
            cs = row.get("climax_score", 0) or 0
            rvs = row.get("recent_vol_spike_max", 0) or 0
            rpp = row.get("resistance_proximity_penalty", 0) or 0
            sm5 = row.get("sector_momentum_5d", 0) or 0
            pp60 = row.get("price_position_60d", 0) or 0

            log.info(
                f"    {code} {row.get('name', ''):<8} T+{EVAL_PERIOD}={ret_str} {marker} "
                f"score={score:.1f} mv={total_mv / 10000:.1f}亿 {size_cat}({size_pct_str})"
            )
            log.info(
                f"      MA={ma_align:.1f} TQ={tq:.1f} VBQ={vbq:.1f} ACC={acc:.2f} "
                f"PSC={psc:.1f} MQR={mqr:.1f} CS={cs:.2f} SM5={sm5:.2f}"
            )
            log.info(f"      spike={rvs:.1f} resist={rpp:.1f} PP60={pp60:.2f}")

            factor_data = {
                "date": str(d),
                "code": code,
                "name": row.get("name", ""),
                "is_winner": is_winner,
                "return": ret_val,
                "score": score,
                "total_mv": total_mv,
                "circ_mv": circ_mv,
                "size_category": size_cat,
                "size_percentile": size_pct,
            }
            for f in factor_cols:
                factor_data[f] = row.get(f, None)

            if is_winner:
                all_winner_data.append(factor_data)
            elif ret_val is not None:
                all_loser_data.append(factor_data)

    # Aggregate comparison
    if all_winner_data and all_loser_data:
        log.info(f"\n{'=' * 90}")
        log.info(f"FACTOR AVERAGES: WINNERS vs LOSERS (all dates)")
        log.info(f"{'=' * 90}")
        log.info(f"{'Factor':<35} {'Winners':>10} {'Losers':>10} {'Delta':>10} {'Signal':>8}")
        log.info("-" * 90)

        compare_factors = [
            "ma_alignment_score",
            "trend_quality_20d",
            "volume_buildup_quality",
            "accumulation_score",
            "post_spike_consolidation",
            "momentum_quality_ratio",
            "climax_score",
            "recent_vol_spike_max",
            "resistance_proximity_penalty",
            "main_strength_proxy",
            "volume_consistency_score",
            "price_position_60d",
            "price_range_position_20d",
            "sector_momentum_5d",
            "pct_chg",
            "return_5d",
            "stability_score",
            "vol_ramp_5v20",
            "days_since_vol_peak_20d",
            "total_mv",
            "circ_mv",
            "size_percentile",
        ]

        for f in compare_factors:
            w_vals = [d[f] for d in all_winner_data if d.get(f) is not None]
            l_vals = [d[f] for d in all_loser_data if d.get(f) is not None]
            if w_vals and l_vals:
                w_avg = sum(w_vals) / len(w_vals)
                l_avg = sum(l_vals) / len(l_vals)
                delta = w_avg - l_avg
                # Scale-aware signal: use relative delta for large-scale factors
                if f in ("total_mv", "circ_mv"):
                    rel_delta = abs(delta) / max(abs(w_avg), abs(l_avg), 1) * 100
                    signal = "🟢" if rel_delta > 20 else "🟡" if rel_delta > 10 else "⚪"
                    log.info(
                        f"  {f:<33} {w_avg / 10000:>10.1f}亿 {l_avg / 10000:>10.1f}亿 "
                        f"{delta / 10000:>+10.1f}亿 {signal}"
                    )
                elif f == "size_percentile":
                    signal = "🟢" if abs(delta) > 0.1 else "🟡" if abs(delta) > 0.05 else "⚪"
                    log.info(f"  {f:<33} {w_avg:>10.3f} {l_avg:>10.3f} {delta:>+10.3f} {signal}")
                else:
                    signal = "🟢" if abs(delta) > 3 else "🟡" if abs(delta) > 1 else "⚪"
                    log.info(f"  {f:<33} {w_avg:>10.2f} {l_avg:>10.2f} {delta:>+10.2f} {signal}")

        log.info(f"\n  Winners: {len(all_winner_data)} | Losers: {len(all_loser_data)}")

        # WR by return bucket
        all_data = all_winner_data + all_loser_data
        log.info(f"\n  RETURN DISTRIBUTION:")
        buckets = [
            ("<-10%", lambda r: r < -10),
            ("-10~-5%", lambda r: -10 <= r < -5),
            ("-5~0%", lambda r: -5 <= r < 0),
            ("0~5%", lambda r: 0 <= r < 5),
            ("5~10%", lambda r: 5 <= r < 10),
            (">10%", lambda r: r >= 10),
        ]
        for label, pred in buckets:
            count = sum(1 for d in all_data if d["return"] is not None and pred(d["return"]))
            log.info(f"    {label:<10}: {count:>3} ({count / max(len(all_data), 1) * 100:.1f}%)")

    return all_winner_data, all_loser_data


async def analyze_size_return_correlation(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
):
    """PART 3: If we HAD selected large-cap stocks, would they have performed better?

    For each test date, look at the top-scoring stocks in each size category
    and compare their T+10 returns. This tells us if there's an inherent
    size-return relationship for rally-type stocks.
    """

    log.info("\n" + "=" * 100)
    log.info("PART 3: SIZE-RETURN ANALYSIS (COUNTERFACTUAL)")
    log.info("What if we filtered for large-cap? How would returns differ?")
    log.info("=" * 100)

    engine = PolarsEngine.__new__(PolarsEngine)

    # Track returns by size bucket across all dates
    size_bucket_returns: dict[str, list[float]] = {
        "MEGA": [],
        "LARGE": [],
        "MID": [],
        "SMALL": [],
        "MICRO": [],
    }

    for d in test_dates:
        regime_score, _, _ = compute_regime_score(d, index_df, market_df, moneyflow_df, limit_df)

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

        if "near_limit_up" in df.columns:
            df = df.filter(pl.col("near_limit_up") == False)  # noqa: E712

        # Score all stocks
        df, score_col = score_tab(TAB, df, market_regime_score=regime_score)  # type: ignore[arg-type]
        df = df.sort(score_col, descending=True)

        # Get T+10 returns for ALL scored stocks (not just top 5)
        future_dates = (
            market_df.select("date")
            .unique()
            .filter(pl.col("date") > d)
            .sort("date")
            .head(EVAL_PERIOD)
        )
        if future_dates.height < EVAL_PERIOD:
            continue
        eval_date = future_dates["date"][-1]

        # For top 50 stocks in each size bucket, compute return
        for size_cat in ["MEGA", "LARGE", "MID", "SMALL", "MICRO"]:
            if "size_category" not in df.columns:
                continue
            cat_df = df.filter(pl.col("size_category") == size_cat).head(5)
            if cat_df.is_empty():
                continue

            codes = cat_df["code"].to_list()
            ref_prices = market_df.filter(
                (pl.col("date") == d) & (pl.col("code").is_in(codes))
            ).select(["code", pl.col("close").alias("ref_close")])
            eval_prices = market_df.filter(
                (pl.col("date") == eval_date) & (pl.col("code").is_in(codes))
            ).select(["code", pl.col("close").alias("eval_close")])
            perf = ref_prices.join(eval_prices, on="code", how="inner").with_columns(
                ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100)
                .round(2)
                .alias("return_pct")
            )
            for ret in perf["return_pct"].to_list():
                if ret is not None:
                    size_bucket_returns[size_cat].append(ret)

    # Summary
    log.info(
        f"\n  COUNTERFACTUAL: Top-5 rally-scored stocks per size category, T+{EVAL_PERIOD} returns:"
    )
    log.info(f"  {'Category':<10} {'WR':>8} {'AR':>8} {'Avg Win':>10} {'Avg Loss':>10} {'n':>5}")
    log.info("  " + "-" * 55)
    for cat in ["MEGA", "LARGE", "MID", "SMALL", "MICRO"]:
        rets = size_bucket_returns[cat]
        if rets:
            wins = [r for r in rets if r > 0]
            losses = [r for r in rets if r <= 0]
            wr = len(wins) / len(rets) * 100
            ar = sum(rets) / len(rets)
            avg_win = sum(wins) / len(wins) if wins else 0
            avg_loss = sum(losses) / len(losses) if losses else 0
            log.info(
                f"  {cat:<10} {wr:>7.1f}% {ar:>+7.2f}% {avg_win:>+9.2f}% {avg_loss:>+9.2f}% {len(rets):>5}"
            )
        else:
            log.info(f"  {cat:<10} no data")


async def analyze_universe_size_distribution(
    test_dates: list[datetime.date],
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
):
    """PART 4: What does the overall universe look like by size? Context for interpreting picks."""

    log.info("\n" + "=" * 100)
    log.info("PART 4: UNIVERSE SIZE DISTRIBUTION (CONTEXT)")
    log.info("What's the base rate of each size category in the full stock universe?")
    log.info("=" * 100)

    # Sample a few dates
    sample_dates = test_dates[:3]
    for d in sample_dates:
        if style_df.is_empty():
            log.info("  No style data available")
            break
        sty_day = style_df.filter(pl.col("date") == d)
        if sty_day.is_empty():
            continue
        total = sty_day.height
        log.info(f"\n  {d}: {total} stocks in universe")
        for cat in ["MEGA", "LARGE", "MID", "SMALL", "MICRO"]:
            if "size_category" in sty_day.columns:
                count = sty_day.filter(pl.col("size_category") == cat).height
                log.info(f"    {cat:<8}: {count:>5} ({count / max(total, 1) * 100:>5.1f}%)")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", type=str, default=None)
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--n-dates", type=int, default=25, help="Number of dates to sample")
    args = parser.parse_args()

    if args.dates:
        test_dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    else:
        test_dates = sample_trading_days(n=args.n_dates, seed=args.seed)

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

    log.info(f"Analyzing {len(test_dates)} dates for {TAB} strategy (T+{EVAL_PERIOD})")

    # Part 4 first (lightweight context)
    await analyze_universe_size_distribution(test_dates, market_df, valuation_df, style_df)

    # Part 1: Market cap distribution
    await analyze_market_cap_distribution(
        test_dates,
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
    )

    # Part 2: Factor discrimination
    await analyze_factor_discrimination(
        test_dates,
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
    )

    # Part 3: Size-return counterfactual
    await analyze_size_return_correlation(
        test_dates,
        market_df,
        valuation_df,
        style_df,
        profile_df,
        index_df,
        moneyflow_df,
        limit_df,
    )


if __name__ == "__main__":
    asyncio.run(main())
