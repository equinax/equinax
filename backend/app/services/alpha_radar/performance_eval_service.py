"""Performance evaluation service for Alpha Radar.

Evaluates the performance of recommended stocks after a given date.
Calculates returns for configurable trading day periods (T+1, T+3, T+5, T+10, T+20).
Provides win rate, profit/loss ratio, and comprehensive assessment.
"""

import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class PerformanceEvalService:
    """Service for evaluating screener recommendation performance."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def evaluate_performance(
        self,
        codes: list[str],
        date: datetime.date,
        periods: list[int] = [1, 3, 5, 10, 20],
    ) -> dict:
        """
        Evaluate the performance of recommended stocks after a given date.

        Args:
            codes: List of stock codes to evaluate (e.g. ["sh.600000", "sz.000001"])
            date: Recommendation date (T0)
            periods: Evaluation periods in trading days

        Returns:
            dict with stocks, period_stats, assessment, date, total_stocks
        """
        if not codes:
            return {
                "date": date,
                "total_stocks": 0,
                "stocks": [],
                "period_stats": [],
                "assessment": "无数据",
            }

        # 1. Get stock names from asset_meta
        name_map = await self._get_stock_names(codes)

        # 2. Get reference close prices on the recommendation date
        ref_prices = await self._get_close_prices(codes, date)

        # 3. For each period, find the Nth trading day after date and get close prices
        period_dates = await self._get_future_trading_dates(date, max(periods))
        period_prices: dict[int, dict[str, Decimal]] = {}
        for period in periods:
            if period <= len(period_dates):
                target_date = period_dates[period - 1]
                period_prices[period] = await self._get_close_prices(codes, target_date)
            else:
                period_prices[period] = {}

        # 4. Build per-stock results
        stocks = []
        for code in codes:
            ref_price = ref_prices.get(code)
            returns: dict[int, Optional[Decimal]] = {}
            for period in periods:
                if ref_price and code in period_prices[period]:
                    future_close = period_prices[period][code]
                    ret = (future_close - ref_price) / ref_price * 100
                    returns[period] = Decimal(str(ret)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                else:
                    returns[period] = None

            stocks.append(
                {
                    "code": code,
                    "name": name_map.get(code, code),
                    "ref_price": ref_price,
                    "returns": returns,
                }
            )

        # 5. Calculate aggregate stats per period
        period_stats = []
        for period in periods:
            rets = [s["returns"][period] for s in stocks if s["returns"][period] is not None]
            total = len(rets)
            if total == 0:
                period_stats.append(
                    {
                        "period": period,
                        "win_count": 0,
                        "lose_count": 0,
                        "total": 0,
                        "win_rate": None,
                        "avg_return": None,
                        "avg_win": None,
                        "avg_loss": None,
                        "profit_loss_ratio": None,
                    }
                )
                continue

            wins = [r for r in rets if r > 0]
            losses = [r for r in rets if r <= 0]
            win_count = len(wins)
            lose_count = len(losses)
            win_rate = Decimal(str(win_count / total * 100)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            avg_return = Decimal(str(sum(rets) / total)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            avg_win = (
                Decimal(str(sum(wins) / len(wins))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                if wins
                else None
            )
            avg_loss = (
                Decimal(str(sum(losses) / len(losses))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                if losses
                else None
            )
            profit_loss_ratio = None
            if avg_win is not None and avg_loss is not None and avg_loss != 0:
                profit_loss_ratio = (avg_win / abs(avg_loss)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )

            period_stats.append(
                {
                    "period": period,
                    "win_count": win_count,
                    "lose_count": lose_count,
                    "total": total,
                    "win_rate": win_rate,
                    "avg_return": avg_return,
                    "avg_win": avg_win,
                    "avg_loss": avg_loss,
                    "profit_loss_ratio": profit_loss_ratio,
                }
            )

        # 6. Generate assessment text
        assessment = self._generate_assessment(period_stats, periods)

        return {
            "date": date,
            "total_stocks": len(codes),
            "stocks": stocks,
            "period_stats": period_stats,
            "assessment": assessment,
        }

    async def _get_stock_names(self, codes: list[str]) -> dict[str, str]:
        """Get stock names from asset_meta table."""
        result = await self.db.execute(
            text("SELECT code, name FROM asset_meta WHERE code = ANY(:codes)"),
            {"codes": codes},
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def _get_close_prices(
        self, codes: list[str], target_date: datetime.date
    ) -> dict[str, Decimal]:
        """Get close prices for given codes on a specific date."""
        result = await self.db.execute(
            text(
                "SELECT code, close FROM market_daily "
                "WHERE code = ANY(:codes) AND date = :target_date"
            ),
            {"codes": codes, "target_date": target_date},
        )
        return {row[0]: Decimal(str(row[1])) for row in result.fetchall()}

    async def _get_future_trading_dates(
        self, ref_date: datetime.date, max_period: int
    ) -> list[datetime.date]:
        """
        Get the next N trading dates after ref_date.

        Uses market_daily with Shanghai main board stocks (sh.6%) to determine
        actual trading days.
        """
        result = await self.db.execute(
            text(
                "SELECT DISTINCT date FROM market_daily "
                "WHERE date > :ref_date AND code LIKE 'sh.6%%' "
                "ORDER BY date ASC "
                "LIMIT :period"
            ),
            {"ref_date": ref_date, "period": max_period},
        )
        return [row[0] for row in result.fetchall()]

    def _generate_assessment(self, period_stats: list[dict], periods: list[int]) -> str:
        """Generate assessment text based on period stats.

        Uses T+5 period for assessment if available, otherwise the longest
        available period with data.
        """
        # Try T+5 first, then fall back to longest period with data
        eval_stats = None
        if 5 in periods:
            for ps in period_stats:
                if ps["period"] == 5 and ps["total"] > 0:
                    eval_stats = ps
                    break

        if eval_stats is None:
            # Use longest period with data
            for ps in reversed(period_stats):
                if ps["total"] > 0:
                    eval_stats = ps
                    break

        if eval_stats is None or eval_stats["win_rate"] is None:
            return "数据不足，无法评估"

        win_rate = float(eval_stats["win_rate"])
        avg_return = float(eval_stats["avg_return"]) if eval_stats["avg_return"] is not None else 0

        if win_rate >= 70 and avg_return > 2:
            return "推荐效果优秀 ✅"
        elif win_rate >= 60 and avg_return > 0:
            return "推荐效果良好 👍"
        elif win_rate >= 50:
            return "推荐效果一般 ⚠️"
        else:
            return "推荐效果较差 ❌"
