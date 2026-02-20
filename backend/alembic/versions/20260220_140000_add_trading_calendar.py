"""Add trading_calendar table.

Stores exchange trading calendar from TuShare trade_cal API.
Used by data map heatmap (filter non-trading days) and gaps detection.

Revision ID: g7h8i9j0k1l2
Revises: f6g7h8i9j0k1
Create Date: 2026-02-20 14:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "g7h8i9j0k1l2"
down_revision: Union[str, None] = "f6g7h8i9j0k1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trading_calendar",
        sa.Column("cal_date", sa.Date(), nullable=False),
        sa.Column("exchange", sa.String(10), nullable=False, server_default="SSE"),
        sa.Column("is_open", sa.SmallInteger(), nullable=False, server_default="0"),
        sa.Column("pretrade_date", sa.Date(), nullable=True),
        sa.PrimaryKeyConstraint("cal_date", "exchange"),
    )
    op.create_index(
        "idx_trading_calendar_open",
        "trading_calendar",
        ["is_open", "cal_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_trading_calendar_open", table_name="trading_calendar")
    op.drop_table("trading_calendar")
