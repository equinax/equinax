"""add_moneyflow_limit_tables

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6g7h8
Create Date: 2026-02-15 12:00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d4e5f6g7h8i9"
down_revision: Union[str, Sequence[str], None] = "c3d4e5f6g7h8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create moneyflow_daily table
    op.create_table(
        "moneyflow_daily",
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("buy_sm_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("buy_md_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("buy_lg_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("buy_elg_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("sell_sm_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("sell_md_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("sell_lg_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("sell_elg_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("net_mf_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("code", "date"),
    )
    op.create_index("idx_moneyflow_daily_code", "moneyflow_daily", ["code"], unique=False)
    op.create_index("idx_moneyflow_daily_date", "moneyflow_daily", ["date"], unique=False)

    # Create limit_list_daily table
    op.create_table(
        "limit_list_daily",
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("name", sa.String(length=50), nullable=True),
        sa.Column("close", sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column("pct_chg", sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column("fd_amount", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("first_time", sa.String(length=20), nullable=True),
        sa.Column("last_time", sa.String(length=20), nullable=True),
        sa.Column("open_times", sa.Integer(), nullable=True),
        sa.Column("up_stat", sa.String(length=20), nullable=True),
        sa.Column("limit_times", sa.Integer(), nullable=True),
        sa.Column("limit_type", sa.String(length=5), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("code", "date"),
    )
    op.create_index("idx_limit_list_daily_code", "limit_list_daily", ["code"], unique=False)
    op.create_index("idx_limit_list_daily_date", "limit_list_daily", ["date"], unique=False)
    op.create_index("idx_limit_list_daily_type", "limit_list_daily", ["limit_type"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_limit_list_daily_type", table_name="limit_list_daily")
    op.drop_index("idx_limit_list_daily_date", table_name="limit_list_daily")
    op.drop_index("idx_limit_list_daily_code", table_name="limit_list_daily")
    op.drop_table("limit_list_daily")

    op.drop_index("idx_moneyflow_daily_date", table_name="moneyflow_daily")
    op.drop_index("idx_moneyflow_daily_code", table_name="moneyflow_daily")
    op.drop_table("moneyflow_daily")
