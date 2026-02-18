"""Strategy configuration for Alpha Radar recommendation engine.

Central registry of all strategy tabs with their metadata and evaluation parameters.
"""

from dataclasses import dataclass
from typing import Literal

ScreenerTabKey = Literal["weekly", "rally", "dragon", "overnight"]


@dataclass(frozen=True)
class StrategyConfig:
    """Configuration for a single strategy tab."""

    tab: ScreenerTabKey
    label_cn: str
    description: str
    score_column: str
    eval_period_trading_days: int
    requires_moneyflow: bool
    backtest_top_n: int = 5


STRATEGIES: dict[ScreenerTabKey, StrategyConfig] = {
    "weekly": StrategyConfig(
        tab="weekly",
        label_cn="周内短线",
        description="稳定T+6, 马上起飞",
        score_column="weekly_score",
        eval_period_trading_days=6,
        requires_moneyflow=False,
    ),
    "rally": StrategyConfig(
        tab="rally",
        label_cn="大盘主升",
        description="MA多头+量能阶梯+趋势质量",
        score_column="rally_score",
        eval_period_trading_days=10,
        requires_moneyflow=False,
    ),
    "dragon": StrategyConfig(
        tab="dragon",
        label_cn="龙头涨停",
        description="主力吸筹+突破蓄力+量价一致",
        score_column="dragon_score",
        eval_period_trading_days=20,
        requires_moneyflow=True,
        backtest_top_n=4,
    ),
    "overnight": StrategyConfig(
        tab="overnight",
        label_cn="隔夜超短",
        description="确定性隔夜机会·T+1买T+2卖",
        score_column="overnight_score",
        eval_period_trading_days=2,
        requires_moneyflow=False,
    ),
}
