"""Dragon Leader (龙头先锋) scoring engine — mid-term explosive potential.

VERBATIM copy of ScoringEngine.calculate_dragon_leader_score() from scoring.py.
Do NOT modify weights or logic without updating theory.md and running backtest.
"""

import polars as pl

from app.services.alpha_radar.scoring import ScoringEngine


class DragonScoringEngine(ScoringEngine):
    """Scoring engine for the Dragon Leader (龙头先锋) strategy tab.

    Inherits _ensure_columns() and _apply_regime_discount() from ScoringEngine.
    """

    def score(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate dragon_score for all rows in df.

        Iter 15: Major redesign based on failure analysis of 2025-04-21 and 2025-05-12
        (both 0% WR). Root causes identified:
          1. NO climax penalty → selects stocks at distribution peaks
          2. Same-day moneyflow (elg/mf) acts as momentum chaser, not leading indicator
          3. No price_position_60d penalty → buys near 60d highs
          4. sector_weakness was one-sided (only penalized weak sectors), not hot sectors
          5. resistance_proximity too low weight (0.05)

        Changes from Iter 14:
          - ADD anti_climax_component (0.15) — critical missing penalty
          - ADD ceiling_penalty for price_position_60d > 0.80 (0.08)
          - ADD sector_overheat_penalty for sector_mom > 5% (0.05)
          - REDUCE elg_net_percentile (0.10 → 0.05) — same-day flow is lagging
          - REDUCE mf_net_percentile (0.05 → 0.00) — removed, too noisy
          - REDUCE main_strength (0.20 → 0.15) — was overweighting momentum
          - INCREASE resistance_penalty (0.05 → 0.08)
          - INCREASE surge_penalty (0.05 → 0.08)
        """
        if df.is_empty():
            return df

        df = self._ensure_columns(df)

        df = df.with_columns(
            [
                (pl.col("main_strength_proxy").fill_null(50.0)).alias("main_strength_component"),
                (pl.col("accumulation_score").fill_null(0.0) * 100).alias("accumulation_component"),
                (pl.col("volume_consistency_score").fill_null(50.0)).alias("consistency_component"),
                (pl.col("trend_quality_20d").fill_null(50.0)).alias("trend_quality_component"),
                (pl.col("volume_buildup_quality").fill_null(40.0)).alias("buildup_component"),
                # Iter 15: anti_climax — penalize stocks at distribution peaks (climax_score > 0.3)
                ((1 - pl.col("climax_score").fill_null(0.0)) * 100).alias("anti_climax_component"),
                (pl.col("elg_net_percentile").fill_null(50.0)).alias("elg_flow_component"),
                (pl.col("recent_vol_spike_max").fill_null(0.0)).alias("recent_spike_penalty"),
                (((pl.col("pct_chg").fill_null(0.0).abs() - 3.0).clip(0.0, 4.0) / 4.0) * 100).alias(
                    "surge_penalty"
                ),
                # Iter 15: ceiling penalty — stocks near 60d high are riskier
                (
                    ((pl.col("price_position_60d").fill_null(0.5) - 0.80).clip(0.0, 0.20) / 0.20)
                    * 100
                ).alias("ceiling_penalty"),
                # Iter 15: sector overheat — hot sectors (>5% 5d mom) tend to reverse
                (
                    ((pl.col("sector_momentum_5d").fill_null(0.0) - 5.0).clip(0.0, 5.0) / 5.0) * 100
                ).alias("sector_overheat_penalty"),
                ((-pl.col("sector_momentum_5d").fill_null(0.0).clip(-5.0, 0.0)) / 5.0 * 100)
                .fill_null(0.0)
                .alias("sector_weakness_penalty"),
                (pl.col("resistance_proximity_penalty").fill_null(0.0)).alias("resistance_penalty"),
            ]
        )

        raw_score = (
            pl.col("main_strength_component") * 0.15
            + pl.col("accumulation_component") * 0.10
            + pl.col("consistency_component") * 0.15
            + pl.col("trend_quality_component") * 0.10
            + pl.col("buildup_component") * 0.05
            + pl.col("anti_climax_component") * 0.15
            + pl.col("elg_flow_component") * 0.05
            - pl.col("recent_spike_penalty") * 0.10
            - pl.col("surge_penalty") * 0.08
            - pl.col("ceiling_penalty") * 0.08
            - pl.col("sector_overheat_penalty") * 0.05
            - pl.col("sector_weakness_penalty") * 0.02
            - pl.col("resistance_penalty") * 0.08
        )

        df = df.with_columns(
            [self._apply_regime_discount(raw_score.clip(0.0, 100.0)).alias("dragon_score")]
        )

        return df
