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
        base_price: str = "t0_close",
    ) -> dict:
        """
        Evaluate the performance of recommended stocks after a given date.

        Args:
            codes: List of stock codes to evaluate (e.g. ["sh.600000", "sz.000001"])
            date: Recommendation date (T0)
            periods: Evaluation periods in trading days
            base_price: 't0_close' for T0 close price, 't1_open' for T+1 open price

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

        name_map = await self._get_stock_names(codes)

        if base_price == "t1_open":
            return await self._evaluate_t1_open(codes, date, periods, name_map)

        ref_prices = await self._get_close_prices(codes, date)

        period_dates = await self._get_future_trading_dates(date, max(periods))
        period_prices: dict[int, dict[str, Decimal]] = {}
        for period in periods:
            if period <= len(period_dates):
                target_date = period_dates[period - 1]
                period_prices[period] = await self._get_close_prices(codes, target_date)
            else:
                period_prices[period] = {}

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

        period_stats = self._calc_period_stats(stocks, periods)
        assessment = self._generate_assessment(period_stats, periods)

        return {
            "date": date,
            "total_stocks": len(codes),
            "stocks": stocks,
            "period_stats": period_stats,
            "assessment": assessment,
        }

    async def _evaluate_t1_open(
        self,
        codes: list[str],
        date: datetime.date,
        periods: list[int],
        name_map: dict[str, str],
    ) -> dict:
        max_needed = max(periods) + 1
        period_dates_list = await self._get_future_trading_dates(date, max_needed)

        if len(period_dates_list) == 0:
            return {
                "date": date,
                "total_stocks": len(codes),
                "stocks": [],
                "period_stats": [],
                "assessment": "无数据",
                "period_dates": {p: None for p in periods},
            }

        t1_date = period_dates_list[0]
        buy_prices = await self._get_open_prices(codes, t1_date)
        stock_details = await self._get_stock_details(codes, date)

        response_period_dates: dict[int, Optional[str]] = {}
        for period in periods:
            idx = period
            if idx < len(period_dates_list):
                response_period_dates[period] = period_dates_list[idx].isoformat()
            else:
                response_period_dates[period] = None

        period_prices: dict[int, dict[str, Decimal]] = {}
        for period in periods:
            idx = period
            if idx < len(period_dates_list):
                target_date = period_dates_list[idx]
                period_prices[period] = await self._get_close_prices(codes, target_date)
            else:
                period_prices[period] = {}

        stocks = []
        for code in codes:
            buy_price_val = buy_prices.get(code)
            returns: dict[int, Optional[Decimal]] = {}
            for period in periods:
                if buy_price_val and code in period_prices[period]:
                    future_close = period_prices[period][code]
                    ret = (future_close - buy_price_val) / buy_price_val * 100
                    returns[period] = Decimal(str(ret)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                else:
                    returns[period] = None

            detail = stock_details.get(code, {})
            stocks.append(
                {
                    "code": code,
                    "name": name_map.get(code, code),
                    "ref_price": buy_price_val,
                    "buy_price": buy_price_val,
                    "buy_date": t1_date,
                    "returns": returns,
                    "total_mv": detail.get("total_mv"),
                    "circ_mv": detail.get("circ_mv"),
                    "volume": detail.get("volume"),
                    "turnover": detail.get("turnover"),
                    "pe_ttm": detail.get("pe_ttm"),
                    "pb_mrq": detail.get("pb_mrq"),
                }
            )

        period_stats = self._calc_period_stats(stocks, periods)
        assessment = self._generate_assessment(period_stats, periods)

        return {
            "date": date,
            "total_stocks": len(codes),
            "stocks": stocks,
            "period_stats": period_stats,
            "assessment": assessment,
            "period_dates": response_period_dates,
        }

    def _calc_period_stats(self, stocks: list[dict], periods: list[int]) -> list[dict]:
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
        return period_stats

    async def _get_stock_names(self, codes: list[str]) -> dict[str, str]:
        """Get stock names from asset_meta table."""
        result = await self.db.execute(
            text("SELECT code, name FROM asset_meta WHERE code = ANY(:codes)"),
            {"codes": codes},
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def _get_stock_details(self, codes: list[str], date: datetime.date) -> dict[str, dict]:
        """Get financial details (valuation + market) for stocks on a given date.

        Returns dict keyed by code with total_mv, circ_mv, pe_ttm, pb_mrq,
        volume, turnover fields.
        """
        details: dict[str, dict] = {}

        # Valuation data from indicator_valuation
        # Use <= date to handle weekends/holidays (fall back to nearest prior trading day)
        val_result = await self.db.execute(
            text(
                "SELECT DISTINCT ON (code) code, total_mv, circ_mv, pe_ttm, pb_mrq "
                "FROM indicator_valuation "
                "WHERE code = ANY(:codes) AND date <= :date "
                "ORDER BY code, date DESC"
            ),
            {"codes": codes, "date": date},
        )
        for row in val_result.fetchall():
            details[row[0]] = {
                "total_mv": Decimal(str(row[1])) if row[1] is not None else None,
                "circ_mv": Decimal(str(row[2])) if row[2] is not None else None,
                "pe_ttm": Decimal(str(row[3])) if row[3] is not None else None,
                "pb_mrq": Decimal(str(row[4])) if row[4] is not None else None,
            }

        # Market data from market_daily (volume, turn -> turnover)
        mkt_result = await self.db.execute(
            text(
                "SELECT DISTINCT ON (code) code, volume, turn "
                "FROM market_daily "
                "WHERE code = ANY(:codes) AND date <= :date "
                "ORDER BY code, date DESC"
            ),
            {"codes": codes, "date": date},
        )
        for row in mkt_result.fetchall():
            entry = details.setdefault(row[0], {})
            entry["volume"] = Decimal(str(row[1])) if row[1] is not None else None
            entry["turnover"] = Decimal(str(row[2])) if row[2] is not None else None

        return details

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

    async def _get_open_prices(
        self, codes: list[str], target_date: datetime.date
    ) -> dict[str, Decimal]:
        """Get open prices for given codes on a specific date."""
        result = await self.db.execute(
            text(
                "SELECT code, open FROM market_daily "
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
