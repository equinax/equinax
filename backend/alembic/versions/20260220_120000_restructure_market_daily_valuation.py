"""restructure_market_daily_and_indicator_valuation

Align DB schema with TuShare API interfaces:
- market_daily: ADD change, DROP turn, DROP trade_status (pure OHLCV table = TuShare daily API)
- indicator_valuation: ADD ~11 daily_basic fields, DROP pcf_ncf_ttm (= TuShare daily_basic API)
- Migrate existing turn data from market_daily to indicator_valuation.turnover_rate

Revision ID: e5f6g7h8i9j0
Revises: d4e5f6g7h8i9
Create Date: 2026-02-20 12:00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e5f6g7h8i9j0"
down_revision: Union[str, Sequence[str], None] = "d4e5f6g7h8i9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # =========================================================================
    # 1. indicator_valuation: ADD new columns from TuShare daily_basic API
    # =========================================================================

    # close - 当日收盘价 (用于计算指标时不必 JOIN market_daily)
    op.add_column(
        "indicator_valuation", sa.Column("close", sa.Numeric(precision=12, scale=4), nullable=True)
    )

    # turnover_rate - 换手率 (%) (从 market_daily.turn 迁移过来)
    op.add_column(
        "indicator_valuation",
        sa.Column("turnover_rate", sa.Numeric(precision=8, scale=4), nullable=True),
    )

    # turnover_rate_f - 自由流通换手率 (%)
    op.add_column(
        "indicator_valuation",
        sa.Column("turnover_rate_f", sa.Numeric(precision=8, scale=4), nullable=True),
    )

    # volume_ratio - 量比
    op.add_column(
        "indicator_valuation",
        sa.Column("volume_ratio", sa.Numeric(precision=8, scale=4), nullable=True),
    )

    # pe - 市盈率 (静态)
    op.add_column(
        "indicator_valuation", sa.Column("pe", sa.Numeric(precision=12, scale=4), nullable=True)
    )

    # ps - 市销率 (静态, 区别于 ps_ttm)
    op.add_column(
        "indicator_valuation", sa.Column("ps", sa.Numeric(precision=12, scale=4), nullable=True)
    )

    # dv_ratio - 股息率 (%)
    op.add_column(
        "indicator_valuation",
        sa.Column("dv_ratio", sa.Numeric(precision=8, scale=4), nullable=True),
    )

    # dv_ttm - 股息率 TTM (%)
    op.add_column(
        "indicator_valuation", sa.Column("dv_ttm", sa.Numeric(precision=8, scale=4), nullable=True)
    )

    # total_share - 总股本 (万股)
    op.add_column(
        "indicator_valuation",
        sa.Column("total_share", sa.Numeric(precision=18, scale=2), nullable=True),
    )

    # float_share - 流通股本 (万股)
    op.add_column(
        "indicator_valuation",
        sa.Column("float_share", sa.Numeric(precision=18, scale=2), nullable=True),
    )

    # free_share - 自由流通股本 (万股)
    op.add_column(
        "indicator_valuation",
        sa.Column("free_share", sa.Numeric(precision=18, scale=2), nullable=True),
    )

    # =========================================================================
    # 2. Migrate turn data: market_daily.turn -> indicator_valuation.turnover_rate
    #    TimescaleDB compressed chunks have a per-DML decompression limit.
    #    Disable it for these bulk UPDATEs, then restore default.
    # =========================================================================
    op.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")

    op.execute("""
        UPDATE indicator_valuation iv
        SET turnover_rate = md.turn
        FROM market_daily md
        WHERE iv.code = md.code
          AND iv.date = md.date
          AND md.turn IS NOT NULL
          AND iv.turnover_rate IS NULL
    """)

    # Also populate close from market_daily for existing records
    op.execute("""
        UPDATE indicator_valuation iv
        SET close = md.close
        FROM market_daily md
        WHERE iv.code = md.code
          AND iv.date = md.date
          AND md.close IS NOT NULL
          AND iv.close IS NULL
    """)

    # Restore default decompression limit
    op.execute("RESET timescaledb.max_tuples_decompressed_per_dml_transaction")

    # =========================================================================
    # 3. market_daily: ADD change column (涨跌额, from TuShare daily API)
    # =========================================================================
    op.add_column(
        "market_daily", sa.Column("change", sa.Numeric(precision=12, scale=4), nullable=True)
    )

    # =========================================================================
    # 4. Drop continuous aggregates that reference market_daily.turn
    #    TimescaleDB continuous aggregates create internal views that block DROP COLUMN.
    # =========================================================================
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_stats_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_daily_monthly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_daily_weekly CASCADE")

    # =========================================================================
    # 5. market_daily: DROP turn and trade_status columns
    # =========================================================================
    op.drop_column("market_daily", "turn")
    op.drop_column("market_daily", "trade_status")

    # =========================================================================
    # 6. Recreate continuous aggregates without avg_turn
    # =========================================================================
    op.execute("""
        CREATE MATERIALIZED VIEW market_daily_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('1 week', date) AS week,
            first(open, date) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, date) AS close,
            sum(volume) AS volume,
            sum(amount) AS amount
        FROM market_daily
        GROUP BY code, time_bucket('1 week', date)
        WITH NO DATA
    """)

    op.execute("""
        CREATE MATERIALIZED VIEW market_daily_monthly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('1 month', date) AS month,
            first(open, date) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, date) AS close,
            sum(volume) AS volume,
            sum(amount) AS amount
        FROM market_daily
        GROUP BY code, time_bucket('1 month', date)
        WITH NO DATA
    """)

    op.execute("""
        CREATE MATERIALIZED VIEW market_stats_daily
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', date) AS date,
            count(*) AS total_stocks,
            sum(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
            sum(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
            sum(CASE WHEN pct_chg = 0 THEN 1 ELSE 0 END) AS flat_count,
            sum(CASE WHEN pct_chg >= 9.9 THEN 1 ELSE 0 END) AS limit_up_count,
            sum(CASE WHEN pct_chg <= -9.9 THEN 1 ELSE 0 END) AS limit_down_count,
            avg(pct_chg) AS avg_pct_chg,
            sum(amount) AS total_amount,
            sum(volume) AS total_volume
        FROM market_daily
        GROUP BY time_bucket('1 day', date)
        WITH NO DATA
    """)

    op.execute("""SELECT add_continuous_aggregate_policy('market_daily_weekly',
        start_offset => INTERVAL '1 month',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    )""")

    op.execute("""SELECT add_continuous_aggregate_policy('market_daily_monthly',
        start_offset => INTERVAL '3 months',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    )""")

    op.execute("""SELECT add_continuous_aggregate_policy('market_stats_daily',
        start_offset => INTERVAL '1 month',
        end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day',
        if_not_exists => TRUE
    )""")

    # =========================================================================
    # 7. indicator_valuation: DROP pcf_ncf_ttm (always empty, not in daily_basic)
    # =========================================================================
    op.drop_column("indicator_valuation", "pcf_ncf_ttm")


def downgrade() -> None:
    op.add_column(
        "indicator_valuation",
        sa.Column("pcf_ncf_ttm", sa.Numeric(precision=12, scale=4), nullable=True),
    )

    # Drop and recreate continuous aggregates to restore turn column
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_stats_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_daily_monthly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_daily_weekly CASCADE")

    op.add_column("market_daily", sa.Column("trade_status", sa.Integer(), nullable=True))
    op.add_column(
        "market_daily", sa.Column("turn", sa.Numeric(precision=8, scale=4), nullable=True)
    )

    op.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
    op.execute("""
        UPDATE market_daily md
        SET turn = iv.turnover_rate
        FROM indicator_valuation iv
        WHERE md.code = iv.code
          AND md.date = iv.date
          AND iv.turnover_rate IS NOT NULL
    """)
    op.execute("RESET timescaledb.max_tuples_decompressed_per_dml_transaction")

    op.drop_column("market_daily", "change")

    # Recreate continuous aggregates with avg_turn
    op.execute("""
        CREATE MATERIALIZED VIEW market_daily_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('1 week', date) AS week,
            first(open, date) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, date) AS close,
            sum(volume) AS volume,
            sum(amount) AS amount,
            avg(turn) AS avg_turn
        FROM market_daily
        GROUP BY code, time_bucket('1 week', date)
        WITH NO DATA
    """)

    op.execute("""
        CREATE MATERIALIZED VIEW market_daily_monthly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('1 month', date) AS month,
            first(open, date) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, date) AS close,
            sum(volume) AS volume,
            sum(amount) AS amount,
            avg(turn) AS avg_turn
        FROM market_daily
        GROUP BY code, time_bucket('1 month', date)
        WITH NO DATA
    """)

    op.execute("""
        CREATE MATERIALIZED VIEW market_stats_daily
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', date) AS date,
            count(*) AS total_stocks,
            sum(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
            sum(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
            sum(CASE WHEN pct_chg = 0 THEN 1 ELSE 0 END) AS flat_count,
            sum(CASE WHEN pct_chg >= 9.9 THEN 1 ELSE 0 END) AS limit_up_count,
            sum(CASE WHEN pct_chg <= -9.9 THEN 1 ELSE 0 END) AS limit_down_count,
            avg(pct_chg) AS avg_pct_chg,
            sum(amount) AS total_amount,
            sum(volume) AS total_volume,
            avg(turn) AS avg_turn
        FROM market_daily
        GROUP BY time_bucket('1 day', date)
        WITH NO DATA
    """)

    op.execute("""SELECT add_continuous_aggregate_policy('market_daily_weekly',
        start_offset => INTERVAL '1 month', end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day', if_not_exists => TRUE
    )""")
    op.execute("""SELECT add_continuous_aggregate_policy('market_daily_monthly',
        start_offset => INTERVAL '3 months', end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day', if_not_exists => TRUE
    )""")
    op.execute("""SELECT add_continuous_aggregate_policy('market_stats_daily',
        start_offset => INTERVAL '1 month', end_offset => INTERVAL '1 day',
        schedule_interval => INTERVAL '1 day', if_not_exists => TRUE
    )""")

    op.drop_column("indicator_valuation", "free_share")
    op.drop_column("indicator_valuation", "float_share")
    op.drop_column("indicator_valuation", "total_share")
    op.drop_column("indicator_valuation", "dv_ttm")
    op.drop_column("indicator_valuation", "dv_ratio")
    op.drop_column("indicator_valuation", "ps")
    op.drop_column("indicator_valuation", "pe")
    op.drop_column("indicator_valuation", "volume_ratio")
    op.drop_column("indicator_valuation", "turnover_rate_f")
    op.drop_column("indicator_valuation", "turnover_rate")
    op.drop_column("indicator_valuation", "close")
