"""Dragon Leader (龙头涨停) scoring engine — explosive limit-up potential.

Incremental optimization from baseline. One change per iteration for attribution.

Baseline: WR=56.0%, AR=2.80%, P/L=2.97
Iter 3 (+sector momentum):    WR=57.0%, AR=2.71%, P/L=4.03
Iter 4 (+trend_quality, -VCS): WR=57.0%, AR=3.62%, P/L=4.35
Iter 5 (+return_5d penalty @5%): WR=57.0%, AR=2.55% — REVERTED, too aggressive
Iter 6 (MS 0.15→0.10, SM 0.07→0.12): WR=62.0%, AR=3.67%, P/L=3.59 ✅ NEW BEST
Iter 7 (ACC 0.10→0.05, TQ 0.15→0.20): WR=62.0%, AR=2.75% — REVERTED, AR drop
Iter 8 (VCS 0.10→0.05, SM 0.12→0.17): WR=60.0%, AR=4.41%, P/L=3.23 — KEPT (AR↑, WR↓2pp)
Iter 9 (AC 0.15→0.10, TQ 0.15→0.20): WR=57.0%, AR=4.37% — REVERTED, WR drop
Iter 10 (+graduated return_5d penalty 3-8%, wt 0.05): WR=61%, AR=3.01% — REVERTED, AR killed
Iter 11 (invert MS: reward lower values via (85-MS)/10): WR=54%, AR=1.03% — REVERTED, catastrophic
Iter 12 (cap TQ: saturate above 70 via (TQ-55)/15): WR=55%, AR=1.84% — REVERTED, both worse
Iter 13 (+TQ×SM interaction 0.05, ELG 0.05→0): WR=54%, AR=0.70% — REVERTED, worst yet
Iter 14 (top_n 5→4 in config): WR=61.2%, AR=5.68%, P/L=3.38 — BEST OVERALL ✅
Iter 15 (SM 0.17→0.20, ELG 0.05→0.02): WR=58.8%, AR=2.81% — REVERTED, SM overshoots
Iter 16 (conditional penalty_scale by sector heat): WR=60%, AR=5.31% — REVERTED, no gain
Iter 17 (SM 0.17→0.18, VCS 0.05→0.04): WR=60%, AR=5.40% — REVERTED, no gain
Iter 18 (surge 0.08→0.05): WR=60%, AR=5.65% — REVERTED, WR dropped
Iter 19 (ceiling 0.08→0.05): WR=61.2%, AR=5.85%, P/L=3.45 — KEPT
Iter 20 (ceiling 0.05→0.03): WR=58.8%, AR=5.19% — REVERTED, overshoots
Iter 21 (resistance 0.08→0.05): WR=62.5%, AR=5.90%, P/L=3.82 — BEST OVERALL ✅

CURRENT: Iter 8+14+19+21 (WR=62.5%, AR=5.90%, P/L=3.82)
"""

import polars as pl

from app.services.alpha_radar.engine.config_loader import load_strategy_config, score_from_config
from app.services.alpha_radar.scoring import ScoringEngine


class DragonScoringEngine(ScoringEngine):
    """Scoring engine for the Dragon Leader (龙头涨停) strategy tab."""

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate dragon_score — Iter 21 (current best)."""
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        if self.config_mode:
            cfg = load_strategy_config("dragon")
            return score_from_config(cfg, df, self._apply_regime_discount)

        df = df.with_columns(
            [
                (pl.col("main_strength_proxy").fill_null(50.0)).alias("main_strength_component"),
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
                (pl.col("volume_consistency_score").fill_null(50.0)).alias("consistency_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("elg_net_percentile").fill_null(50.0)).alias("elg_flow_component"),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (((pl.col("pct_chg").fill_null(0.0).abs() - 3.0).clip(0.0, 4.0) / 4.0) * 100).alias(
                    "surge_penalty"
                ),
                (
                    ((pl.col("price_position_60d").fill_null(0.5) - 0.80).clip(0.0, 0.20) / 0.20)
                    * 100
                ).alias("ceiling_penalty"),
                (
                    ((pl.col("sector_momentum_5d").fill_null(0.0) + 5.0) / 15.0).clip(0.0, 1.0)
                    * 100
                ).alias("sector_momentum_positive"),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
            ]
        )

        raw_score = (
            pl.col("main_strength_component") * 0.10
            + pl.col("accumulation_component") * 0.10
            + pl.col("consistency_component") * 0.05
            + pl.col("trend_quality_component") * 0.15
            + pl.col("buildup_component") * 0.05
            + pl.col("anti_climax_component") * 0.15
            + pl.col("elg_flow_component") * 0.05
            + pl.col("sector_momentum_positive") * 0.17
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("surge_penalty") * 0.08
            - pl.col("ceiling_penalty") * 0.05
            - pl.col("resistance_penalty") * 0.05
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("dragon_score")]
        )

        return df
