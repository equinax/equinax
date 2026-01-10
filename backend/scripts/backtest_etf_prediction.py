"""Backtest ETF Prediction Algorithm.

Evaluates prediction accuracy by comparing T-day predictions with T+1 actual returns.

Metrics:
- IC (Information Coefficient): Correlation between prediction scores and next-day returns
- Hit Rate (Top 10): Percentage of Top 10 predictions that went up the next day
- Top-Bottom Spread: Return difference between Top 10 and Bottom 10
- Spearman Rank Correlation: Correlation of score ranks vs return ranks

Usage:
    # Run backtest with current algorithm
    docker compose exec api python -m scripts.backtest_etf_prediction

    # Specify date range
    docker compose exec api python -m scripts.backtest_etf_prediction --start 2025-10-01 --end 2026-01-09

    # Show daily details
    docker compose exec api python -m scripts.backtest_etf_prediction --verbose

    # Compare algorithms
    docker compose exec api python -m scripts.backtest_etf_prediction --compare
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from decimal import Decimal

import numpy as np
import polars as pl
from sqlalchemy import text


def pearsonr(x: list, y: list) -> tuple:
    """Calculate Pearson correlation coefficient using numpy."""
    if len(x) < 3:
        return (0.0, 1.0)
    x_arr = np.array(x)
    y_arr = np.array(y)
    corr = np.corrcoef(x_arr, y_arr)[0, 1]
    if np.isnan(corr):
        return (0.0, 1.0)
    return (float(corr), 0.0)


def spearmanr(x: list, y: list) -> tuple:
    """Calculate Spearman rank correlation using numpy."""
    if len(x) < 3:
        return (0.0, 1.0)
    # Convert to ranks
    x_arr = np.array(x)
    y_arr = np.array(y)
    x_ranks = np.argsort(np.argsort(x_arr))
    y_ranks = np.argsort(np.argsort(y_arr))
    # Pearson on ranks
    corr = np.corrcoef(x_ranks, y_ranks)[0, 1]
    if np.isnan(corr):
        return (0.0, 1.0)
    return (float(corr), 0.0)


from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.services.alpha_radar.etf_prediction_service import (
    ETFPredictionService,
    PredictionConfig,
    DEFAULT_CONFIG,
)


@dataclass
class DailyMetrics:
    """Metrics for a single day's prediction."""
    date: date
    total_subcategories: int
    top10_hit_rate: float  # % of top 10 predictions that went up
    ic: float  # Information coefficient (Pearson correlation)
    spearman: float  # Spearman rank correlation
    top10_avg_return: float  # Average return of top 10 predictions
    bottom10_avg_return: float  # Average return of bottom 10 predictions
    spread: float  # Top 10 return - Bottom 10 return
    max_score: float
    min_score: float
    avg_score: float
    score_std: float  # Standard deviation of scores
    predictions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class BacktestResult:
    """Aggregated backtest results."""
    algorithm: str
    start_date: date
    end_date: date
    trading_days: int

    # Aggregated metrics
    avg_ic: float
    avg_spearman: float
    avg_hit_rate: float
    avg_spread: float
    positive_ic_days: int  # Days with IC > 0

    # Score distribution
    avg_max_score: float
    avg_min_score: float
    avg_score_range: float

    # Daily details
    daily_metrics: List[DailyMetrics] = field(default_factory=list)


class ETFPredictionBacktester:
    """Backtest ETF prediction algorithm."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def run(
        self,
        start_date: date,
        end_date: date,
        algorithm: str = "v1_current",
        verbose: bool = False,
    ) -> BacktestResult:
        """
        Run backtest over the specified date range.

        Args:
            start_date: Backtest start date
            end_date: Backtest end date
            algorithm: Algorithm version to test
            verbose: Print daily details

        Returns:
            BacktestResult with aggregated metrics
        """
        print(f"\n{'='*60}")
        print(f"Running Backtest: {algorithm}")
        print(f"Period: {start_date} to {end_date}")
        print(f"{'='*60}")

        # Get trading days in range
        trading_days = await self._get_trading_days(start_date, end_date)
        print(f"Trading days: {len(trading_days)}")

        if len(trading_days) < 2:
            print("Error: Need at least 2 trading days for backtest")
            return self._empty_result(algorithm, start_date, end_date)

        daily_metrics: List[DailyMetrics] = []

        # Process each day (except last, since we need T+1 returns)
        for i, target_date in enumerate(trading_days[:-1]):
            next_date = trading_days[i + 1]

            # Get predictions for T day
            predictions = await self._get_predictions(target_date, algorithm)

            if not predictions:
                if verbose:
                    print(f"  {target_date}: No predictions")
                continue

            # Get actual returns for T+1
            actual_returns = await self._get_next_day_returns(next_date)

            if not actual_returns:
                if verbose:
                    print(f"  {target_date}: No T+1 returns for {next_date}")
                continue

            # Calculate metrics
            metrics = self._calculate_daily_metrics(
                target_date, predictions, actual_returns
            )
            daily_metrics.append(metrics)

            if verbose:
                self._print_daily_metrics(metrics)

        # Aggregate results
        result = self._aggregate_results(
            algorithm, start_date, end_date, daily_metrics
        )

        # Print summary
        self._print_summary(result)

        return result

    async def _get_trading_days(
        self, start_date: date, end_date: date
    ) -> List[date]:
        """Get trading days in the date range."""
        result = await self.db.execute(
            text("""
                SELECT DISTINCT date
                FROM market_daily
                WHERE date >= :start_date AND date <= :end_date
                ORDER BY date
            """),
            {"start_date": start_date, "end_date": end_date}
        )
        return [row[0] for row in result.fetchall()]

    async def _get_predictions(
        self, target_date: date, algorithm: str
    ) -> List[Dict[str, Any]]:
        """Get predictions for the given date using specified algorithm."""
        if algorithm == "v1_current":
            service = ETFPredictionService(self.db)
            result = await service.get_predictions(target_date=target_date)
            return result.get("predictions", [])
        else:
            # Future algorithms can be added here
            raise ValueError(f"Unknown algorithm: {algorithm}")

    async def _get_next_day_returns(
        self, target_date: date
    ) -> Dict[str, float]:
        """
        Get T+1 returns for each ETF subcategory.

        Returns dict mapping sub_category -> average change percentage
        """
        # Query aggregated subcategory returns
        query = text("""
            WITH etf_returns AS (
                SELECT
                    am.code,
                    am.name,
                    md.pct_chg as change_pct,
                    md.amount
                FROM asset_meta am
                JOIN market_daily md ON am.code = md.code
                WHERE am.asset_type = 'ETF'
                  AND am.status = 1
                  AND md.date = :target_date
                  AND md.amount > 0
            )
            SELECT code, name, change_pct, amount
            FROM etf_returns
        """)

        result = await self.db.execute(query, {"target_date": target_date})
        rows = result.fetchall()

        if not rows:
            return {}

        # Classify ETFs and aggregate by subcategory
        from app.services.alpha_radar.etf_classifier import ETFClassifier

        subcategory_returns: Dict[str, List[float]] = {}

        for code, name, change_pct, amount in rows:
            _, sub_category = ETFClassifier.classify(name)
            if sub_category not in subcategory_returns:
                subcategory_returns[sub_category] = []
            if change_pct is not None:
                subcategory_returns[sub_category].append(float(change_pct))

        # Calculate average return per subcategory
        return {
            sub_cat: sum(returns) / len(returns)
            for sub_cat, returns in subcategory_returns.items()
            if returns
        }

    def _calculate_daily_metrics(
        self,
        target_date: date,
        predictions: List[Dict[str, Any]],
        actual_returns: Dict[str, float],
    ) -> DailyMetrics:
        """Calculate metrics for a single day."""
        # Match predictions with actual returns
        matched = []
        for pred in predictions:
            sub_cat = pred["sub_category"]
            score = float(pred["ambush_score"] or 0)
            actual = actual_returns.get(sub_cat)

            if actual is not None:
                matched.append({
                    "sub_category": sub_cat,
                    "score": score,
                    "actual_return": actual,
                    "category": pred.get("category", ""),
                })

        if len(matched) < 5:
            # Not enough data for meaningful metrics
            return DailyMetrics(
                date=target_date,
                total_subcategories=len(matched),
                top10_hit_rate=0.0,
                ic=0.0,
                spearman=0.0,
                top10_avg_return=0.0,
                bottom10_avg_return=0.0,
                spread=0.0,
                max_score=0.0,
                min_score=0.0,
                avg_score=0.0,
                score_std=0.0,
                predictions=matched,
            )

        # Sort by score descending
        matched.sort(key=lambda x: x["score"], reverse=True)

        scores = [m["score"] for m in matched]
        returns = [m["actual_return"] for m in matched]

        # Calculate IC (Pearson correlation)
        ic, _ = pearsonr(scores, returns) if len(scores) > 2 else (0.0, 1.0)

        # Calculate Spearman rank correlation
        spearman, _ = spearmanr(scores, returns) if len(scores) > 2 else (0.0, 1.0)

        # Top 10 and Bottom 10 analysis
        n = len(matched)
        top_n = min(10, n // 2)

        top_10 = matched[:top_n]
        bottom_10 = matched[-top_n:] if n > top_n else []

        # Hit rate: % of top 10 that went up
        top10_up = sum(1 for m in top_10 if m["actual_return"] > 0)
        top10_hit_rate = top10_up / len(top_10) * 100 if top_10 else 0.0

        # Average returns
        top10_avg = sum(m["actual_return"] for m in top_10) / len(top_10) if top_10 else 0.0
        bottom10_avg = sum(m["actual_return"] for m in bottom_10) / len(bottom_10) if bottom_10 else 0.0

        # Score statistics
        max_score = max(scores)
        min_score = min(scores)
        avg_score = sum(scores) / len(scores)
        score_std = (sum((s - avg_score) ** 2 for s in scores) / len(scores)) ** 0.5

        return DailyMetrics(
            date=target_date,
            total_subcategories=n,
            top10_hit_rate=top10_hit_rate,
            ic=ic if not (ic != ic) else 0.0,  # Handle NaN
            spearman=spearman if not (spearman != spearman) else 0.0,
            top10_avg_return=top10_avg,
            bottom10_avg_return=bottom10_avg,
            spread=top10_avg - bottom10_avg,
            max_score=max_score,
            min_score=min_score,
            avg_score=avg_score,
            score_std=score_std,
            predictions=matched,
        )

    def _aggregate_results(
        self,
        algorithm: str,
        start_date: date,
        end_date: date,
        daily_metrics: List[DailyMetrics],
    ) -> BacktestResult:
        """Aggregate daily metrics into final result."""
        if not daily_metrics:
            return self._empty_result(algorithm, start_date, end_date)

        n = len(daily_metrics)

        return BacktestResult(
            algorithm=algorithm,
            start_date=start_date,
            end_date=end_date,
            trading_days=n,
            avg_ic=sum(m.ic for m in daily_metrics) / n,
            avg_spearman=sum(m.spearman for m in daily_metrics) / n,
            avg_hit_rate=sum(m.top10_hit_rate for m in daily_metrics) / n,
            avg_spread=sum(m.spread for m in daily_metrics) / n,
            positive_ic_days=sum(1 for m in daily_metrics if m.ic > 0),
            avg_max_score=sum(m.max_score for m in daily_metrics) / n,
            avg_min_score=sum(m.min_score for m in daily_metrics) / n,
            avg_score_range=sum(m.max_score - m.min_score for m in daily_metrics) / n,
            daily_metrics=daily_metrics,
        )

    def _empty_result(
        self, algorithm: str, start_date: date, end_date: date
    ) -> BacktestResult:
        """Return empty result for error cases."""
        return BacktestResult(
            algorithm=algorithm,
            start_date=start_date,
            end_date=end_date,
            trading_days=0,
            avg_ic=0.0,
            avg_spearman=0.0,
            avg_hit_rate=0.0,
            avg_spread=0.0,
            positive_ic_days=0,
            avg_max_score=0.0,
            avg_min_score=0.0,
            avg_score_range=0.0,
        )

    def _print_daily_metrics(self, m: DailyMetrics):
        """Print metrics for a single day."""
        print(f"  {m.date}: IC={m.ic:+.3f}, Spearman={m.spearman:+.3f}, "
              f"HitRate={m.top10_hit_rate:.0f}%, Spread={m.spread:+.2f}%, "
              f"Score=[{m.min_score:.0f}-{m.max_score:.0f}]")

    def _print_summary(self, result: BacktestResult):
        """Print backtest summary."""
        print(f"\n{'='*60}")
        print(f"Backtest Summary: {result.algorithm}")
        print(f"{'='*60}")
        print(f"Period: {result.start_date} to {result.end_date}")
        print(f"Trading Days: {result.trading_days}")

        print(f"\n--- Prediction Accuracy ---")
        print(f"  Average IC:              {result.avg_ic:+.4f}")
        print(f"  Average Spearman:        {result.avg_spearman:+.4f}")
        print(f"  Positive IC Days:        {result.positive_ic_days}/{result.trading_days} "
              f"({result.positive_ic_days/result.trading_days*100:.0f}%)")

        print(f"\n--- Top 10 Performance ---")
        print(f"  Average Hit Rate:        {result.avg_hit_rate:.1f}%")
        print(f"  Average Top-Bottom Spread: {result.avg_spread:+.3f}%")

        print(f"\n--- Score Distribution ---")
        print(f"  Average Max Score:       {result.avg_max_score:.1f}")
        print(f"  Average Min Score:       {result.avg_min_score:.1f}")
        print(f"  Average Score Range:     {result.avg_score_range:.1f}")

        # Performance evaluation
        print(f"\n--- Evaluation ---")
        if result.avg_ic > 0.15:
            print("  ✅ IC > 0.15: Excellent predictive power")
        elif result.avg_ic > 0.05:
            print("  ⚠️  IC 0.05-0.15: Moderate predictive power")
        else:
            print("  ❌ IC < 0.05: Weak predictive power")

        if result.avg_hit_rate > 55:
            print("  ✅ Hit Rate > 55%: Good accuracy")
        elif result.avg_hit_rate > 50:
            print("  ⚠️  Hit Rate 50-55%: Marginally better than random")
        else:
            print("  ❌ Hit Rate < 50%: Worse than random")

        if result.avg_spread > 0.3:
            print("  ✅ Spread > 0.3%: Strong alpha signal")
        elif result.avg_spread > 0:
            print("  ⚠️  Spread 0-0.3%: Weak alpha signal")
        else:
            print("  ❌ Spread < 0%: Negative alpha")

        if result.avg_score_range < 30:
            print("  ❌ Score Range < 30: Scores too compressed")
        elif result.avg_score_range < 60:
            print("  ⚠️  Score Range 30-60: Moderate spread")
        else:
            print("  ✅ Score Range > 60: Good score distribution")


async def get_async_session():
    """Create async database session."""
    engine = create_async_engine(
        settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
        echo=False,
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session(), engine


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Backtest ETF prediction algorithm"
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2025-10-01",
        help="Backtest start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Backtest end date (YYYY-MM-DD, default: yesterday)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print daily details"
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare multiple algorithms"
    )
    args = parser.parse_args()

    # Parse dates
    start_date = date.fromisoformat(args.start)
    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = date.today() - timedelta(days=1)

    session, engine = await get_async_session()

    try:
        backtester = ETFPredictionBacktester(session)

        # Run current algorithm
        result = await backtester.run(
            start_date=start_date,
            end_date=end_date,
            algorithm="v1_current",
            verbose=args.verbose,
        )

        # Future: compare with v2 algorithm
        if args.compare:
            print("\n\n" + "="*60)
            print("Algorithm comparison not yet implemented.")
            print("Add v2_percentile algorithm to enable comparison.")
            print("="*60)

    finally:
        await session.close()
        await engine.dispose()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
