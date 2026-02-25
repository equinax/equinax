"""Comprehensive Alpha Radar backtest report generator.

Runs full backtests across many sample dates and generates structured
Markdown reports (summary + per-date) for evaluating iteration progress.

Usage:
    docker compose exec api python -m scripts.alpha_radar_report
    docker compose exec api python -m scripts.alpha_radar_report --sample light   # ~5 dates, quick
    docker compose exec api python -m scripts.alpha_radar_report --sample heavy   # ~50 dates
    docker compose exec api python -m scripts.alpha_radar_report --sample full    # All trading days 2025-01 ~ 2026-02
    docker compose exec api python -m scripts.alpha_radar_report --dates 2025-03-10,2025-06-09
    docker compose exec api python -m scripts.alpha_radar_report --months 2026-01,2026-02
    docker compose exec api python -m scripts.alpha_radar_report --version 8.0 --tabs overnight
    docker compose exec api python -m scripts.alpha_radar_report --seed 42        # Reproducible random sampling
"""

import argparse
import asyncio
import datetime
import logging
import os
import sys
import time


log = logging.getLogger("alpha_radar_report")

TARGETS: dict[str, dict[str, float]] = {
    "weekly": {"wr": 65.0, "ar": 1.5},
    "rally": {"wr": 60.0, "ar": 2.0},
    "dragon": {"wr": 55.0, "ar": 5.0},
}

SAMPLE_COUNTS = {
    "light": 5,
    "normal": 25,
    "heavy": 50,
}

FULL_RANGE_START = datetime.date(2025, 1, 1)
FULL_RANGE_END = datetime.date(2026, 2, 10)


def _get_full_trading_days() -> list[datetime.date]:
    from scripts.alpha_radar_backtest import get_trading_days

    days = get_trading_days(start=FULL_RANGE_START, end=FULL_RANGE_END)
    log.info(f"Full mode: {len(days)} trading days from {days[0]} to {days[-1]}")
    return days


def _fmt(v: float | None, suffix: str = "%", decimals: int = 2) -> str:
    if v is None:
        return "N/A"
    return f"{v:.{decimals}f}{suffix}"


def _bold_if_above(val: float | None, target: float, formatted: str) -> str:
    if val is not None and val >= target:
        return f"**{formatted}**"
    return formatted


def generate_summary_md(
    all_results: dict,
    confidence_map: dict[datetime.date, float],
    regime_data: dict,
    tabs: list[str],
    top_n: int,
    test_dates: list[datetime.date],
    tab_labels: dict[str, str],
    tab_periods: dict[str, int],
    tab_versions: dict[str, str],
    elapsed: float,
) -> str:
    lines: list[str] = []
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    low_conf_dates = [d for d in test_dates if confidence_map.get(d, 1.0) < 1.0]

    lines.append("# Alpha Radar 综合回测报告")
    lines.append("")
    lines.append(f"- **生成时间**: {now}")
    lines.append(f"- **样本日期数**: {len(test_dates)} ({test_dates[0]} ~ {test_dates[-1]})")
    lines.append(
        f"- **日期数**: {len(test_dates)} (其中 {len(low_conf_dates)} 个低信心日, 权重 < 1.0)"
    )
    lines.append(f"- **Top-N**: {top_n}")
    lines.append(f"- **策略**: {', '.join(f'{tab}({tab_labels.get(tab, tab)})' for tab in tabs)}")
    lines.append(f"- **版本**: {', '.join(f'v{tab_versions.get(tab, "?")}' for tab in tabs)}")
    lines.append(f"- **耗时**: {elapsed:.1f}s")
    lines.append("")

    # --- Per-tab aggregate (weighted) ---
    lines.append("## 策略汇总")
    lines.append("")
    lines.append(
        "| 策略 | 评估周期 | 加权胜率 | 目标 | 加权收益率 | 目标 | 加权盈亏比 | 年化复合 | 日期数 | 达标 |"
    )
    lines.append(
        "|------|----------|----------|------|------------|------|------------|----------|--------|------|"
    )

    for tab in tabs:
        wrs, ars, weights = [], [], []
        plrs: list[tuple[float, float]] = []
        for d in test_dates:
            perf = all_results[d][tab]["performance"]
            conf = confidence_map.get(d, 1.0)
            if perf["win_rate"] is not None:
                wrs.append(perf["win_rate"])
                ars.append(perf["avg_return"])
                weights.append(conf)
                if perf["profit_loss_ratio"] is not None:
                    plrs.append((perf["profit_loss_ratio"], conf))

        if not wrs:
            lines.append(
                f"| {tab} ({tab_labels.get(tab, '')}) | T+{tab_periods.get(tab, '?')} | N/A | | N/A | | N/A | N/A | 0 | |"
            )
            continue

        total_w = sum(weights)
        avg_wr = sum(w * v for w, v in zip(weights, wrs)) / total_w
        avg_ar = sum(w * v for w, v in zip(weights, ars)) / total_w
        if plrs:
            plr_w = sum(c for _, c in plrs)
            avg_plr = sum(v * c for v, c in plrs) / plr_w
        else:
            avg_plr = None

        period = tab_periods.get(tab, 5)
        periods_per_year = 252 / period
        cagr = ((1 + avg_ar / 100) ** periods_per_year - 1) * 100

        t = TARGETS.get(tab, {})
        wr_target = t.get("wr", 0)
        ar_target = t.get("ar", 0)

        wr_str = _bold_if_above(avg_wr, wr_target, _fmt(avg_wr))
        ar_str = _bold_if_above(avg_ar, ar_target, _fmt(avg_ar))
        plr_str = _fmt(avg_plr, "", 2) if avg_plr else "N/A"
        cagr_str = f"{cagr:+.1f}%"

        wr_pass = avg_wr >= wr_target
        ar_pass = avg_ar >= ar_target
        status = "✅" if (wr_pass and ar_pass) else ("⚠️" if (wr_pass or ar_pass) else "❌")

        lines.append(
            f"| {tab} ({tab_labels.get(tab, '')}) | T+{tab_periods.get(tab, '?')} "
            f"| {wr_str} | >{wr_target}% "
            f"| {ar_str} | >{ar_target}% "
            f"| {plr_str} | {cagr_str} | {len(wrs)} | {status} |"
        )

    lines.append("")

    # --- Date × Tab matrix ---
    lines.append("## 逐日明细")
    lines.append("")
    header = "| 日期 | 信心 | " + " | ".join(f"{tab} WR / AR" for tab in tabs) + " |"
    sep = "|------|------|" + "|".join("------|" for _ in tabs)
    lines.append(header)
    lines.append(sep)

    for d in test_dates:
        conf = confidence_map.get(d, 1.0)
        conf_str = f"{conf}" if conf < 1.0 else "1.0"
        cells = []
        for tab in tabs:
            perf = all_results[d][tab]["performance"]
            wr = perf["win_rate"]
            ar = perf["avg_return"]
            if wr is not None:
                t = TARGETS.get(tab, {})
                wr_str = _bold_if_above(wr, t.get("wr", 0), _fmt(wr, "%", 1))
                ar_str = _bold_if_above(ar, t.get("ar", 0), _fmt(ar))
                indicator = " ⚠️" if conf < 1.0 else ""
                cells.append(f"{wr_str} / {ar_str}{indicator}")
            else:
                cells.append("NO_DATA")
        lines.append(f"| {d} | {conf_str} | " + " | ".join(cells) + " |")

    lines.append("")

    # --- Best / Worst analysis ---
    lines.append("## 最佳 / 最差日期")
    lines.append("")
    for tab in tabs:
        date_metrics: list[tuple[datetime.date, float, float]] = []
        for d in test_dates:
            perf = all_results[d][tab]["performance"]
            if perf["win_rate"] is not None and perf["avg_return"] is not None:
                date_metrics.append((d, perf["win_rate"], perf["avg_return"]))

        if not date_metrics:
            continue

        by_ar = sorted(date_metrics, key=lambda x: x[2], reverse=True)
        best = by_ar[0]
        worst = by_ar[-1]
        lines.append(f"### {tab} ({tab_labels.get(tab, '')})")
        lines.append(f"- **最佳**: {best[0]} — WR={best[1]}%, AR={best[2]:.2f}%")
        lines.append(f"- **最差**: {worst[0]} — WR={worst[1]}%, AR={worst[2]:.2f}%")
        lines.append("")

    # --- Low-confidence analysis ---
    if low_conf_dates:
        lines.append("## 低信心日分析")
        lines.append("")
        lines.append(
            "| 日期 | 信心系数 | Regime Score | Weak Days | Breadth | MF Inflow% | MF Avg Net |"
        )
        lines.append(
            "|------|----------|-------------|-----------|---------|------------|------------|"
        )
        for d in sorted(low_conf_dates):
            rd = regime_data.get(d, {})
            conf = confidence_map.get(d, 1.0)
            lines.append(
                f"| {d} "
                f"| {conf} "
                f"| {rd.get('regime_score', 'N/A')} "
                f"| {rd.get('weak_days', 'N/A')} "
                f"| {_fmt(rd.get('breadth_today'), '%', 1)} "
                f"| {_fmt(rd.get('mf_pct_inflow'), '%', 1)} "
                f"| {_fmt(rd.get('mf_avg_net'), '', 0)} |"
            )
        lines.append("")

    lines.append("---")
    lines.append(f"*Generated by alpha_radar_report.py*")

    return "\n".join(lines) + "\n"


def generate_date_md(
    d: datetime.date,
    results_for_date: dict,
    regime: dict,
    confidence: float,
    tabs: list[str],
    tab_labels: dict[str, str],
    tab_periods: dict[str, int],
    top_n: int,
) -> str:
    lines: list[str] = []

    lines.append(f"# {d} 回测报告")
    lines.append("")

    # --- Market regime ---
    lines.append("## 市场环境")
    lines.append("")
    lines.append(f"| 指标 | 值 |")
    lines.append(f"|------|----|")
    lines.append(f"| 信心系数 | {confidence} |")
    lines.append(f"| Regime Score | {regime.get('regime_score', 'N/A')} |")
    lines.append(f"| Weak Days | {regime.get('weak_days', 'N/A')} |")
    lines.append(f"| 5日收益率 | {_fmt(regime.get('ret_5d'), '%')} |")
    lines.append(f"| 指数分量 | {_fmt(regime.get('index_component'), '', 1)} |")
    lines.append(f"| 当日涨跌比 | {_fmt(regime.get('breadth_today'), '%', 1)} |")
    lines.append(f"| 5日均涨跌比 | {_fmt(regime.get('breadth_5d_avg'), '%', 1)} |")
    lines.append(f"| 涨跌比修正 | {_fmt(regime.get('breadth_modifier'), '', 1)} |")
    lines.append(f"| 当日涨跌比修正 | {_fmt(regime.get('breadth_today_modifier'), '', 1)} |")
    lines.append(f"| 脆弱惩罚 | {_fmt(regime.get('fragility_penalty'), '', 1)} |")
    lines.append(f"| 恐慌天数 | {regime.get('panic_days', 'N/A')} |")
    lines.append(f"| 资金流入占比 | {_fmt(regime.get('mf_pct_inflow'), '%', 1)} |")
    lines.append(f"| 平均净流入 | {_fmt(regime.get('mf_avg_net'), '万', 0)} |")
    lines.append(f"| 大单净流入 | {_fmt(regime.get('mf_elg_net'), '万', 0)} |")
    lines.append(f"| 涨停数 | {regime.get('limit_up', 'N/A')} |")
    lines.append(f"| 跌停数 | {regime.get('limit_down', 'N/A')} |")
    lines.append("")

    if confidence < 1.0:
        lines.append(f"> ⚠️ 该日信心系数: {confidence} — 市场环境不利，推荐权重降低")
        lines.append("")

    # --- Per-tab sections ---
    for tab in tabs:
        tab_result = results_for_date.get(tab, {})
        recs = tab_result.get("recommendations", [])
        perf = tab_result.get("performance", {})
        period = tab_periods.get(tab, 5)

        lines.append(f"## {tab} ({tab_labels.get(tab, '')}), T+{period}")
        lines.append("")

        wr = perf.get("win_rate")
        ar = perf.get("avg_return")
        plr = perf.get("profit_loss_ratio")
        avg_win = perf.get("avg_win")
        avg_loss = perf.get("avg_loss")

        lines.append(f"| 指标 | 值 |")
        lines.append(f"|------|----|")
        lines.append(f"| 胜率 | {_fmt(wr)} |")
        lines.append(f"| 平均收益率 | {_fmt(ar)} |")
        lines.append(f"| 盈亏比 | {_fmt(plr, '', 2)} |")
        lines.append(f"| 平均盈利 | {_fmt(avg_win)} |")
        lines.append(f"| 平均亏损 | {_fmt(avg_loss)} |")
        lines.append(f"| 推荐数 | {len(recs)} |")
        lines.append("")

        if recs:
            stocks = perf.get("stocks", [])
            stock_returns = {s["code"]: s.get("return") for s in stocks}
            stock_limit_ups = {s["code"]: s for s in stocks}

            has_limit_data = any(s.get("limit_up_count") is not None for s in stocks)
            if has_limit_data:
                lines.append(
                    "| # | 代码 | 名称 | 评分 | 收盘价 | 当日涨跌 | T+N收益 | 涨停 | 连板 | 结果 |"
                )
                lines.append(
                    "|---|------|------|------|--------|----------|---------|------|------|------|"
                )
            else:
                lines.append("| # | 代码 | 名称 | 评分 | 收盘价 | 当日涨跌 | T+N收益 | 结果 |")
                lines.append("|---|------|------|------|--------|----------|---------|------|")

            for i, r in enumerate(recs, 1):
                code = r.get("code", "")
                name = r.get("name", "")
                score = _fmt(r.get("score"), "", 1)
                close = _fmt(r.get("close"), "", 2)
                pct_chg = _fmt(r.get("pct_chg"), "%")
                ret = stock_returns.get(code)
                ret_str = _fmt(ret)
                if isinstance(ret, (int, float)):
                    marker = "✅" if ret > 0 else "❌"
                else:
                    marker = "?"
                if has_limit_data:
                    s = stock_limit_ups.get(code, {})
                    lu_count = s.get("limit_up_count", 0)
                    lu_consec = s.get("max_consec_limit_up", 0)
                    lines.append(
                        f"| {i} | {code} | {name} | {score} | {close} | {pct_chg} | {ret_str} | {lu_count} | {lu_consec} | {marker} |"
                    )
                else:
                    lines.append(
                        f"| {i} | {code} | {name} | {score} | {close} | {pct_chg} | {ret_str} | {marker} |"
                    )

            lines.append("")

    lines.append("---")
    lines.append(f"*Generated by alpha_radar_report.py*")

    return "\n".join(lines) + "\n"


def generate_reports(
    all_results: dict,
    confidence_map: dict[datetime.date, float],
    regime_data: dict,
    tabs: list[str],
    top_n: int,
    test_dates: list[datetime.date],
    tab_labels: dict[str, str],
    tab_periods: dict[str, int],
    tab_versions: dict[str, str],
    output_dir: str,
    elapsed: float,
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    dates_dir = os.path.join(output_dir, "dates")
    os.makedirs(dates_dir, exist_ok=True)

    summary = generate_summary_md(
        all_results,
        confidence_map,
        regime_data,
        tabs,
        top_n,
        test_dates,
        tab_labels,
        tab_periods,
        tab_versions,
        elapsed,
    )
    summary_path = os.path.join(output_dir, "summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)
    log.info(f"Written: {summary_path}")

    for d in test_dates:
        regime = regime_data.get(d, {})
        conf = confidence_map.get(d, 1.0)
        date_md = generate_date_md(
            d,
            all_results.get(d, {}),
            regime,
            conf,
            tabs,
            tab_labels,
            tab_periods,
            top_n,
        )
        date_path = os.path.join(dates_dir, f"{d}.md")
        with open(date_path, "w", encoding="utf-8") as f:
            f.write(date_md)

    log.info(f"Written {len(test_dates)} date reports to {dates_dir}/")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Alpha Radar comprehensive backtest report generator"
    )
    parser.add_argument("--top-n", type=int, default=5, help="Top N stocks per tab (default: 5)")
    parser.add_argument(
        "--tabs",
        type=str,
        default="weekly,rally,dragon",
        help="Comma-separated tabs to test (default: weekly,rally,dragon)",
    )
    parser.add_argument(
        "--sample",
        type=str,
        default="normal",
        choices=["light", "normal", "heavy", "full"],
        help="Sample size preset: light(~5), normal(25), heavy(~54), full(all trading days 2025-01~2026-02)",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated dates (YYYY-MM-DD). Overrides --sample if provided.",
    )
    parser.add_argument(
        "--months",
        type=str,
        default=None,
        help="Comma-separated months (YYYY-MM). Expands to all trading days in those months. Overrides --sample.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: .reports/YYYY-MM-DD_HHMMSS)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for date sampling (default: random each run)",
    )
    parser.add_argument(
        "--version",
        type=str,
        default=None,
        help="Strategy config version (e.g. 8.0). Default: head version.",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()

    if args.dates:
        test_dates = [datetime.date.fromisoformat(d.strip()) for d in args.dates.split(",")]
    elif args.months:
        from scripts.alpha_radar_backtest import get_trading_days

        months = [m.strip() for m in args.months.split(",")]
        test_dates = []
        for m in months:
            year, month = int(m[:4]), int(m[5:7])
            if month == 12:
                end = datetime.date(year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                end = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
            start = datetime.date(year, month, 1)
            days = get_trading_days(start=start, end=end)
            test_dates.extend(days)
        test_dates = sorted(set(test_dates))
        log.info(f"--months {args.months}: {len(test_dates)} trading days")
    elif args.sample == "full":
        test_dates = _get_full_trading_days()
    else:
        from scripts.alpha_radar_backtest import sample_trading_days

        n = SAMPLE_COUNTS[args.sample]
        test_dates = sample_trading_days(n=n, seed=args.seed)

    tabs = [t.strip() for t in args.tabs.split(",")]

    from scripts.alpha_radar_backtest import run_backtest
    from app.services.alpha_radar.engine.config_loader import load_strategy_config

    tab_configs = {k: load_strategy_config(k, version=args.version) for k in tabs}
    tab_labels = {k: cfg.label_cn for k, cfg in tab_configs.items()}
    tab_periods = {k: cfg.eval_period for k, cfg in tab_configs.items()}
    tab_versions = {k: cfg.version for k, cfg in tab_configs.items()}

    output_dir = args.output_dir
    if not output_dir:
        tz_shanghai = datetime.timezone(datetime.timedelta(hours=8))
        now = datetime.datetime.now(tz=tz_shanghai)
        output_dir = f".reports/{now.strftime('%Y-%m-%d_%H%M%S')}"

    log.info(f"Running backtest: {len(test_dates)} dates × {len(tabs)} tabs, top_n={args.top_n}")
    t_start = time.time()
    all_results, confidence_map, regime_data = asyncio.run(
        run_backtest(test_dates, tabs, args.top_n, verbose=False, version=args.version)
    )
    elapsed = time.time() - t_start

    log.info(f"Backtest complete in {elapsed:.1f}s. Generating reports...")
    generate_reports(
        all_results,
        confidence_map,
        regime_data,
        tabs,
        args.top_n,
        test_dates,
        tab_labels,
        tab_periods,
        tab_versions,
        output_dir,
        elapsed,
    )
    log.info(f"Done. Reports in: {output_dir}/")


if __name__ == "__main__":
    main()
