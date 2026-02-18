"""Diagnostic script: dump detailed weekly scoring factors for specific dates.

Shows factor components for top-N picks AND successful stocks that were ranked lower,
enabling qualitative comparison between winners and losers.

Usage:
    docker compose exec api python -m scripts.weekly_factor_analysis --dates 2025-02-10
    docker compose exec api python -m scripts.weekly_factor_analysis --dates 2025-02-10,2025-09-01 --top-n 15
"""

import argparse
import asyncio
import datetime
import logging
import time

import polars as pl

from app.db.session import async_session_maker
from app.services.alpha_radar.polars_engine import PolarsEngine
from app.services.alpha_radar.scoring import ScoringEngine
from app.services.alpha_radar.engine import score_tab
from scripts.alpha_radar_backtest import load_all_data, compute_regime_score

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def build_scored_df(
    target_date: datetime.date,
    market_df: pl.DataFrame,
    valuation_df: pl.DataFrame,
    style_df: pl.DataFrame,
    profile_df: pl.DataFrame,
    index_df: pl.DataFrame,
    moneyflow_df: pl.DataFrame,
    limit_df: pl.DataFrame,
    top_n: int = 20,
) -> tuple[pl.DataFrame, float, dict]:
    """Build full scored DataFrame for weekly tab, returning ALL intermediate columns."""

    engine = PolarsEngine.__new__(PolarsEngine)
    regime_score, weak_days, regime_details = compute_regime_score(
        target_date, index_df, market_df, moneyflow_df, limit_df
    )

    # Get lookback
    all_dates = market_df.select("date").unique().sort("date")
    dates_before = all_dates.filter(pl.col("date") <= target_date)
    if dates_before.height < 20:
        return pl.DataFrame(), regime_score, regime_details

    lookback_dates = dates_before.tail(60)
    lookback_start = lookback_dates["date"][0]

    df = market_df.filter((pl.col("date") >= lookback_start) & (pl.col("date") <= target_date))
    if df.is_empty():
        return pl.DataFrame(), regime_score, regime_details

    df = engine.calculate_technical_indicators(df)
    df = df.filter(pl.col("date") == target_date)
    if df.is_empty():
        return pl.DataFrame(), regime_score, regime_details

    # Join valuation
    if not valuation_df.is_empty():
        val_day = valuation_df.filter(pl.col("date") == target_date)
        if not val_day.is_empty():
            val_day = val_day.with_columns(
                [(pl.col("pe_ttm").rank() / pl.len()).alias("pe_percentile")]
            )
            df = df.join(val_day, on="code", how="left", suffix="_val")
            if "is_st" in df.columns:
                df = df.filter(pl.col("is_st").fill_null(0) != 1)
            elif "is_st_val" in df.columns:
                df = df.filter(pl.col("is_st_val").fill_null(0) != 1)

    # Join style
    if not style_df.is_empty():
        style_day = style_df.filter(pl.col("date") == target_date)
        if not style_day.is_empty():
            if "volatility_20d" in style_day.columns:
                style_day = style_day.with_columns(
                    [(pl.col("volatility_20d").rank() / pl.len()).alias("vol_percentile")]
                )
            style_select_cols = ["code", "size_category", "momentum_20d", "momentum_60d"]
            for col in [
                "vol_percentile",
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
            for scol in ["vol_percentile", "value_percentile", "momentum_percentile"]:
                suffixed = f"{scol}_style"
                if suffixed in df.columns:
                    df = df.with_columns(
                        pl.coalesce([pl.col(scol), pl.col(suffixed)]).alias(scol)
                    ).drop(suffixed)

    # Join profiles
    if not profile_df.is_empty():
        df = df.join(profile_df, on="code", how="left")

    # Sector momentum
    sector_mom = PolarsEngine.compute_sector_momentum(market_df, profile_df, target_date)
    if not sector_mom.is_empty() and "sw_industry_l1" in df.columns:
        df = df.join(
            sector_mom.select(["sw_industry_l1", "sector_momentum_5d"]),
            on="sw_industry_l1",
            how="left",
        )
        df = df.with_columns(pl.col("sector_momentum_5d").fill_null(0.0))

    # Moneyflow
    if moneyflow_df is not None and not moneyflow_df.is_empty():
        mf_today = moneyflow_df.filter(pl.col("date") == target_date)
        if not mf_today.is_empty():
            mf_features = mf_today.with_columns(
                [(pl.col("buy_elg_amount") - pl.col("sell_elg_amount")).alias("elg_net_raw")]
            ).select(["code", "net_mf_amount", "elg_net_raw"])
            mf_features = mf_features.with_columns(
                [
                    (pl.col("net_mf_amount").rank() / pl.len() * 100).alias("mf_net_percentile"),
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

    # Hard filters for weekly
    if "near_limit_up" in df.columns:
        df = df.filter(pl.col("near_limit_up") == False)  # noqa: E712
    if "pct_chg" in df.columns:
        df = df.filter(pl.col("pct_chg").fill_null(0.0).abs() <= 5.0)

    # Score using the engine
    df, score_col = score_tab("weekly", df, market_regime_score=regime_score)  # type: ignore[arg-type]

    # Sort by score
    df = df.sort([score_col, "code"], descending=[True, False], nulls_last=True)

    return df.head(top_n), regime_score, regime_details


def get_t6_returns(
    codes: list[str],
    target_date: datetime.date,
    market_df: pl.DataFrame,
) -> dict[str, float | None]:
    """Get T+6 return for each code."""
    future_dates = (
        market_df.select("date").unique().filter(pl.col("date") > target_date).sort("date")
    )
    if future_dates.height < 6:
        return {c: None for c in codes}

    eval_date = future_dates["date"][5]  # T+6

    ref = (
        market_df.filter((pl.col("date") == target_date) & (pl.col("code").is_in(codes)))
        .select(["code", "close"])
        .rename({"close": "ref_close"})
    )
    ev = (
        market_df.filter((pl.col("date") == eval_date) & (pl.col("code").is_in(codes)))
        .select(["code", "close"])
        .rename({"close": "eval_close"})
    )
    perf = ref.join(ev, on="code", how="left")
    perf = perf.with_columns(
        [
            pl.when(pl.col("eval_close").is_not_null())
            .then(
                ((pl.col("eval_close") - pl.col("ref_close")) / pl.col("ref_close") * 100).round(2)
            )
            .otherwise(None)
            .alias("return_pct")
        ]
    )
    return {row["code"]: row["return_pct"] for row in perf.iter_rows(named=True)}


async def analyze_dates(dates: list[datetime.date], top_n: int = 20):
    """Run full factor analysis for given dates."""
    earliest = min(dates)
    latest = max(dates)
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

    # Factor columns to display (weekly components)
    factor_cols = [
        "anti_climax_component",
        "consistency_component",
        "consolidation_component",
        "trend_quality_component",
        "buildup_component",
        "accumulation_component",
        "momentum_quality_component",
        "recent_spike_penalty",
        "resistance_penalty",
        "surge_penalty",
    ]

    # Raw input factors
    raw_cols = [
        "climax_score",
        "volume_consistency_score",
        "post_spike_consolidation",
        "trend_quality_20d",
        "volume_buildup_quality",
        "accumulation_score",
        "momentum_quality_ratio",
        "recent_vol_spike_max",
        "resistance_proximity_penalty",
        "pct_chg",
    ]

    # Context columns
    context_cols = [
        "close",
        "volume_ratio_5d",
        "ma_alignment_score",
        "price_position_60d",
        "return_5d",
        "sector_momentum_5d",
        "mf_net_percentile",
        "elg_net_percentile",
    ]

    for target_date in dates:
        log.info(f"\n{'=' * 120}")
        log.info(f"DATE: {target_date}  |  TOP-{top_n} WEEKLY FACTOR ANALYSIS")
        log.info(f"{'=' * 120}")

        df, regime_score, regime_details = build_scored_df(
            target_date,
            market_df,
            valuation_df,
            style_df,
            profile_df,
            index_df,
            moneyflow_df,
            limit_df,
            top_n=top_n,
        )

        log.info(f"\nRegime: {regime_score:.1f}")
        log.info(
            f"  ret_5d={regime_details.get('ret_5d', 0):.2f}%  "
            f"breadth_today={regime_details.get('breadth_today', 0):.1f}%  "
            f"breadth_5d_avg={regime_details.get('breadth_5d_avg', 0):.1f}%  "
            f"mf_inflow={regime_details.get('mf_pct_inflow', 0):.1f}%  "
            f"limit_up={regime_details.get('limit_up', 0)}  "
            f"limit_down={regime_details.get('limit_down', 0)}"
        )

        if df.is_empty():
            log.info("  ABSTAINED or no data")
            continue

        codes = df["code"].to_list()
        returns = get_t6_returns(codes, target_date, market_df)

        # Print header
        log.info(
            f"\n{'Rank':<5} {'Code':<12} {'Name':<10} {'Score':<7} {'T+6%':<8} {'W/L':<4} "
            f"| {'anti_clm':>8} {'consist':>8} {'consol':>8} {'trend':>8} {'buildup':>8} "
            f"{'accum':>8} {'mom_q':>8} {'spk_pen':>8} {'res_pen':>8} {'srg_pen':>8}"
        )
        log.info("-" * 160)

        for rank, row in enumerate(df.iter_rows(named=True), 1):
            code = row.get("code", "")
            name = row.get("name", "")[:8]
            score = row.get("weekly_score", 0)
            ret = returns.get(code)
            wl = "✅" if ret is not None and ret > 0 else ("❌" if ret is not None else "?")
            ret_str = f"{ret:.2f}" if ret is not None else "N/A"

            components = []
            for fc in factor_cols:
                v = row.get(fc, 0)
                components.append(f"{v:8.1f}")

            log.info(
                f"{rank:<5} {code:<12} {name:<10} {score:6.1f} {ret_str:>7}  {wl}  "
                f"| {''.join(components)}"
            )

        # Also print raw factor values
        log.info(
            f"\n{'Rank':<5} {'Code':<12} {'Name':<10} "
            f"| {'climax':>8} {'vol_con':>8} {'pst_spk':>8} {'trnd_q':>8} {'bld_q':>8} "
            f"{'accum':>8} {'mom_qr':>8} {'spk_max':>8} {'res_prox':>8} {'pct_chg':>8}"
        )
        log.info("-" * 150)

        for rank, row in enumerate(df.iter_rows(named=True), 1):
            code = row.get("code", "")
            name = row.get("name", "")[:8]
            ret = returns.get(code)
            wl = "✅" if ret is not None and ret > 0 else ("❌" if ret is not None else "?")

            raws = []
            for rc in raw_cols:
                v = row.get(rc, 0)
                if v is None:
                    v = 0.0
                raws.append(f"{v:8.3f}")

            log.info(f"{rank:<5} {code:<12} {name:<10} {wl}  | {''.join(raws)}")

        # Context factors
        log.info(
            f"\n{'Rank':<5} {'Code':<12} {'Name':<10} "
            f"| {'close':>8} {'vol_r5d':>8} {'ma_aln':>8} {'pr_p60':>8} {'ret_5d':>8} "
            f"{'sec_mom':>8} {'mf_net%':>8} {'elg_net%':>8} {'sector':<12}"
        )
        log.info("-" * 150)

        for rank, row in enumerate(df.iter_rows(named=True), 1):
            code = row.get("code", "")
            name = row.get("name", "")[:8]
            ret = returns.get(code)
            wl = "✅" if ret is not None and ret > 0 else ("❌" if ret is not None else "?")

            ctx = []
            for cc in context_cols:
                v = row.get(cc, 0)
                if v is None:
                    v = 0.0
                ctx.append(f"{v:8.2f}")

            sector = row.get("sw_industry_l1", "N/A") or "N/A"
            log.info(f"{rank:<5} {code:<12} {name:<10} {wl}  | {''.join(ctx)} {sector:<12}")

        # Summary stats
        winners = [c for c, r in returns.items() if r is not None and r > 0]
        losers = [c for c, r in returns.items() if r is not None and r <= 0]
        log.info(
            f"\n  Summary: {len(winners)} winners, {len(losers)} losers out of top-{min(top_n, len(codes))}"
        )

        # Winner vs Loser factor comparison (top-5 only)
        top5_codes = codes[:5]
        top5_wins = [c for c in top5_codes if returns.get(c) is not None and returns[c] > 0]
        top5_losses = [c for c in top5_codes if returns.get(c) is not None and returns[c] <= 0]

        if top5_wins and top5_losses:
            win_df = df.filter(pl.col("code").is_in(top5_wins))
            loss_df = df.filter(pl.col("code").is_in(top5_losses))

            log.info(f"\n  TOP-5 WINNER vs LOSER FACTOR AVERAGES:")
            log.info(
                f"  {'Factor':<30} {'Winners(n={len(top5_wins)})':>15} {'Losers(n={len(top5_losses)})':>15} {'Delta':>10}"
            )
            log.info(f"  {'-' * 70}")
            for fc in factor_cols + raw_cols + context_cols:
                if fc in win_df.columns and fc in loss_df.columns:
                    w_avg = win_df[fc].mean()
                    l_avg = loss_df[fc].mean()
                    if w_avg is not None and l_avg is not None:
                        delta = w_avg - l_avg
                        log.info(f"  {fc:<30} {w_avg:>15.2f} {l_avg:>15.2f} {delta:>10.2f}")


def main():
    parser = argparse.ArgumentParser(description="Weekly Factor Analysis")
    parser.add_argument("--dates", type=str, required=True, help="Comma-separated dates")
    parser.add_argument(
        "--top-n", type=int, default=20, help="Top N stocks to analyze (default: 20)"
    )
    args = parser.parse_args()

    dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    asyncio.run(analyze_dates(dates, args.top_n))


if __name__ == "__main__":
    main()
