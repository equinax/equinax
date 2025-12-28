"""Dashboard service for Alpha Radar.

Provides market regime dashboard data including:
- Market state (Bull/Bear/Range)
- Market breadth
- Style rotation
- Smart money indicators
"""

from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import text, select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.classification import MarketRegime, MarketRegimeType
from app.services.alpha_radar.polars_engine import PolarsEngine


class DashboardService:
    """Service for computing market regime dashboard metrics."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.polars_engine = PolarsEngine(db)

    async def get_market_state(self, target_date: date) -> dict:
        """
        Get market state from MarketRegime table.

        Returns:
            dict with regime, regime_score, regime_description
        """
        result = await self.db.execute(
            select(MarketRegime).where(MarketRegime.date == target_date)
        )
        regime = result.scalar_one_or_none()

        if regime:
            regime_type = regime.regime
            if isinstance(regime_type, MarketRegimeType):
                regime_str = regime_type.value
            else:
                regime_str = str(regime_type)

            # Map regime to description
            descriptions = {
                "BULL": "强势上涨",
                "BEAR": "弱势下跌",
                "RANGE": "震荡整理",
            }

            return {
                "regime": regime_str,
                "regime_score": regime.regime_score or Decimal("0"),
                "regime_description": descriptions.get(regime_str, "未知状态"),
            }

        # Default if no data
        return {
            "regime": "RANGE",
            "regime_score": Decimal("0"),
            "regime_description": "数据暂无",
        }

    async def get_market_breadth(self, target_date: date) -> dict:
        """
        Get market breadth metrics.

        Uses MarketRegime table for basic breadth, calculates MA20 breadth separately.

        Returns:
            dict with up_count, down_count, flat_count, up_down_ratio,
            above_ma20_ratio, limit_up_count, limit_down_count
        """
        result = await self.db.execute(
            select(MarketRegime).where(MarketRegime.date == target_date)
        )
        regime = result.scalar_one_or_none()

        if regime:
            up_count = regime.up_count or 0
            down_count = regime.down_count or 0
            total = regime.total_stocks or (up_count + down_count)
            flat_count = max(0, total - up_count - down_count)

            # Calculate up/down ratio
            up_down_ratio = Decimal(str(up_count / down_count)) if down_count > 0 else Decimal("999")

            return {
                "up_count": up_count,
                "down_count": down_count,
                "flat_count": flat_count,
                "up_down_ratio": round(up_down_ratio, 2),
                "above_ma20_ratio": Decimal("0.5"),  # Placeholder - would need separate calculation
                "limit_up_count": regime.limit_up_count or 0,
                "limit_down_count": regime.limit_down_count or 0,
            }

        # Default if no data
        return {
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "up_down_ratio": Decimal("1.0"),
            "above_ma20_ratio": Decimal("0.5"),
            "limit_up_count": 0,
            "limit_down_count": 0,
        }

    async def get_style_rotation(self, target_date: date) -> dict:
        """
        Calculate style rotation metrics (large value vs small growth).

        Compares performance of:
        - Large cap value stocks vs
        - Small cap growth stocks

        Returns:
            dict with large_value_strength, small_growth_strength,
            dominant_style, rotation_signal
        """
        # Query style factor performance
        # Large value: MEGA/LARGE size + VALUE style
        # Small growth: SMALL/MICRO size + GROWTH style
        query = text("""
            WITH style_returns AS (
                SELECT
                    sse.size_category,
                    sse.value_category,
                    AVG(md.pct_chg) as avg_return
                FROM stock_style_exposure sse
                JOIN market_daily md ON sse.code = md.code AND sse.date = md.date
                WHERE sse.date = :target_date
                AND sse.size_category IS NOT NULL
                AND sse.value_category IS NOT NULL
                GROUP BY sse.size_category, sse.value_category
            )
            SELECT
                size_category,
                value_category,
                avg_return
            FROM style_returns
        """)

        result = await self.db.execute(query, {"target_date": target_date})
        rows = result.fetchall()

        # Calculate style strengths
        large_value_return = 0.0
        small_growth_return = 0.0
        large_value_count = 0
        small_growth_count = 0

        for row in rows:
            size_cat = row[0]
            value_cat = row[1]
            avg_ret = float(row[2] or 0)

            # Large value
            if size_cat in ("MEGA", "LARGE") and value_cat == "VALUE":
                large_value_return += avg_ret
                large_value_count += 1

            # Small growth
            if size_cat in ("SMALL", "MICRO") and value_cat == "GROWTH":
                small_growth_return += avg_ret
                small_growth_count += 1

        # Normalize to 0-100 scale
        # Use relative strength comparison
        lv_strength = 50.0
        sg_strength = 50.0

        if large_value_count > 0 and small_growth_count > 0:
            lv_avg = large_value_return / large_value_count
            sg_avg = small_growth_return / small_growth_count

            # Convert to relative strength
            total = abs(lv_avg) + abs(sg_avg) + 0.001  # Avoid division by zero
            lv_strength = 50 + (lv_avg / total) * 50
            sg_strength = 50 + (sg_avg / total) * 50

        # Determine dominant style
        if lv_strength > sg_strength + 10:
            dominant_style = "large_value"
        elif sg_strength > lv_strength + 10:
            dominant_style = "small_growth"
        else:
            dominant_style = "balanced"

        # Rotation signal - would need historical comparison
        rotation_signal = "stable"

        return {
            "large_value_strength": Decimal(str(round(lv_strength, 2))),
            "small_growth_strength": Decimal(str(round(sg_strength, 2))),
            "dominant_style": dominant_style,
            "rotation_signal": rotation_signal,
        }

    async def get_smart_money(self, target_date: date) -> dict:
        """
        Calculate smart money proxy indicators.

        Based on turnover and volume patterns across the market.

        Returns:
            dict with market_avg_volume_ratio, high_turnover_count,
            accumulation_signal_count, distribution_signal_count, money_flow_proxy
        """
        # Query market-wide volume and turnover patterns
        query = text("""
            WITH today AS (
                SELECT
                    code,
                    volume,
                    turn,
                    pct_chg
                FROM market_daily
                WHERE date = :target_date
            ),
            history AS (
                SELECT
                    code,
                    AVG(volume) as avg_volume_5d
                FROM market_daily
                WHERE date < :target_date
                AND date >= :start_date
                GROUP BY code
            ),
            combined AS (
                SELECT
                    t.code,
                    t.volume,
                    t.turn,
                    t.pct_chg,
                    CASE WHEN h.avg_volume_5d > 0 THEN t.volume / h.avg_volume_5d ELSE 1.0 END as volume_ratio
                FROM today t
                LEFT JOIN history h ON t.code = h.code
            )
            SELECT
                AVG(volume_ratio) as avg_vol_ratio,
                COUNT(CASE WHEN turn > 5 THEN 1 END) as high_turnover_count,
                COUNT(CASE WHEN volume_ratio > 1.5 AND pct_chg < 0 THEN 1 END) as accumulation_count,
                COUNT(CASE WHEN volume_ratio > 1.5 AND pct_chg > 3 THEN 1 END) as distribution_count,
                COUNT(*) as total
            FROM combined
        """)

        # Calculate start date for 5-day average
        from datetime import timedelta
        start_date = target_date - timedelta(days=10)  # Extra buffer for trading days

        result = await self.db.execute(
            query,
            {"target_date": target_date, "start_date": start_date}
        )
        row = result.fetchone()

        if row:
            avg_vol_ratio = float(row[0] or 1.0)
            high_turnover = int(row[1] or 0)
            accumulation = int(row[2] or 0)
            distribution = int(row[3] or 0)

            # Determine money flow
            if accumulation > distribution * 1.5:
                money_flow = "inflow"
            elif distribution > accumulation * 1.5:
                money_flow = "outflow"
            else:
                money_flow = "neutral"

            return {
                "market_avg_volume_ratio": Decimal(str(round(avg_vol_ratio, 2))),
                "high_turnover_count": high_turnover,
                "accumulation_signal_count": accumulation,
                "distribution_signal_count": distribution,
                "money_flow_proxy": money_flow,
            }

        # Default
        return {
            "market_avg_volume_ratio": Decimal("1.0"),
            "high_turnover_count": 0,
            "accumulation_signal_count": 0,
            "distribution_signal_count": 0,
            "money_flow_proxy": "neutral",
        }

    async def get_full_dashboard(
        self,
        target_date: Optional[date] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> dict:
        """
        Get complete dashboard data.

        For snapshot mode: uses target_date
        For period mode: would aggregate over the period (simplified to use end_date)
        """
        # Resolve date
        if target_date is None:
            if end_date:
                target_date = end_date
            else:
                target_date = await self.polars_engine.get_latest_trading_date()

        if target_date is None:
            raise ValueError("No trading data available")

        # Get all dashboard components
        market_state = await self.get_market_state(target_date)
        market_breadth = await self.get_market_breadth(target_date)
        style_rotation = await self.get_style_rotation(target_date)
        smart_money = await self.get_smart_money(target_date)

        return {
            "date": target_date,
            "market_state": market_state,
            "market_breadth": market_breadth,
            "style_rotation": style_rotation,
            "smart_money": smart_money,
        }
