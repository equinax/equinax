"""Drop unused tables: indicator_etf, stock_microstructure, technical_indicators.

These tables were empty and no longer needed.

Revision ID: f6g7h8i9j0k1
Revises: e5f6g7h8i9j0
Create Date: 2026-02-20 13:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "f6g7h8i9j0k1"
down_revision: Union[str, None] = "e5f6g7h8i9j0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop continuous aggregate that depends on indicator_etf first
    op.execute("DROP MATERIALIZED VIEW IF EXISTS etf_indicators_weekly CASCADE")

    # Drop the 3 unused (empty) tables
    op.execute("DROP TABLE IF EXISTS indicator_etf CASCADE")
    op.execute("DROP TABLE IF EXISTS stock_microstructure CASCADE")
    op.execute("DROP TABLE IF EXISTS technical_indicators CASCADE")


def downgrade() -> None:
    # Recreate technical_indicators
    op.create_table(
        "technical_indicators",
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("ma_5", sa.Numeric(12, 4), nullable=True),
        sa.Column("ma_10", sa.Numeric(12, 4), nullable=True),
        sa.Column("ma_20", sa.Numeric(12, 4), nullable=True),
        sa.Column("ma_60", sa.Numeric(12, 4), nullable=True),
        sa.Column("ma_120", sa.Numeric(12, 4), nullable=True),
        sa.Column("ma_250", sa.Numeric(12, 4), nullable=True),
        sa.Column("ema_12", sa.Numeric(12, 4), nullable=True),
        sa.Column("ema_26", sa.Numeric(12, 4), nullable=True),
        sa.Column("macd_dif", sa.Numeric(12, 6), nullable=True),
        sa.Column("macd_dea", sa.Numeric(12, 6), nullable=True),
        sa.Column("macd_hist", sa.Numeric(12, 6), nullable=True),
        sa.Column("rsi_6", sa.Numeric(8, 4), nullable=True),
        sa.Column("rsi_12", sa.Numeric(8, 4), nullable=True),
        sa.Column("rsi_24", sa.Numeric(8, 4), nullable=True),
        sa.Column("kdj_k", sa.Numeric(8, 4), nullable=True),
        sa.Column("kdj_d", sa.Numeric(8, 4), nullable=True),
        sa.Column("kdj_j", sa.Numeric(8, 4), nullable=True),
        sa.Column("boll_upper", sa.Numeric(12, 4), nullable=True),
        sa.Column("boll_middle", sa.Numeric(12, 4), nullable=True),
        sa.Column("boll_lower", sa.Numeric(12, 4), nullable=True),
        sa.Column("vol_ma_5", sa.BigInteger(), nullable=True),
        sa.Column("vol_ma_10", sa.BigInteger(), nullable=True),
        sa.Column("atr_14", sa.Numeric(12, 4), nullable=True),
        sa.Column("obv", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("code", "date"),
    )
    op.create_index("idx_tech_ind_date", "technical_indicators", ["date"])
    op.create_index("idx_tech_ind_code", "technical_indicators", ["code"])
    op.execute(
        "SELECT create_hypertable('technical_indicators', 'date', "
        "chunk_time_interval => INTERVAL '1 month', "
        "migrate_data => true, if_not_exists => true)"
    )

    # Recreate indicator_etf
    op.create_table(
        "indicator_etf",
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("iopv", sa.Numeric(12, 6), nullable=True),
        sa.Column("discount_rate", sa.Numeric(8, 4), nullable=True),
        sa.Column("unit_total", sa.Numeric(18, 2), nullable=True),
        sa.Column("tracking_error", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("code", "date"),
    )
    op.create_index("idx_indicator_etf_date", "indicator_etf", ["date"])
    op.create_index("idx_indicator_etf_code", "indicator_etf", ["code"])
    op.execute(
        "SELECT create_hypertable('indicator_etf', 'date', "
        "chunk_time_interval => INTERVAL '1 month', "
        "migrate_data => true, if_not_exists => true)"
    )

    # Recreate stock_microstructure (post-migration version, without northbound columns)
    op.create_table(
        "stock_microstructure",
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("is_retail_hot", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_main_controlled", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("dragon_tiger_count_20d", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("code", "date"),
    )
    op.create_index("idx_microstructure_date", "stock_microstructure", ["date"])
    op.create_index("idx_microstructure_code", "stock_microstructure", ["code"])
    op.execute(
        "SELECT create_hypertable('stock_microstructure', 'date', "
        "chunk_time_interval => INTERVAL '1 month', "
        "migrate_data => true, if_not_exists => true)"
    )

    # Recreate etf_indicators_weekly continuous aggregate
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS etf_indicators_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('1 week', date) AS week,
            last(iopv, date) AS iopv,
            avg(discount_rate) AS avg_discount_rate,
            last(unit_total, date) AS unit_total,
            avg(tracking_error) AS avg_tracking_error
        FROM indicator_etf
        GROUP BY code, time_bucket('1 week', date)
        WITH NO DATA
    """)
