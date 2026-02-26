"""Drop fore_adjust_factor and back_adjust_factor columns.

Only keep adjust_factor (cumulative factor from TuShare).
前复权 and 后复权 are now computed at query time:
  前复权价 = price × (factor / latest_factor)
  后复权价 = price × (factor / base_factor)

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-02-25 09:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "h8i9j0k1l2m3"
down_revision: Union[str, None] = "g7h8i9j0k1l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("adjust_factor", "fore_adjust_factor")
    op.drop_column("adjust_factor", "back_adjust_factor")


def downgrade() -> None:
    op.add_column(
        "adjust_factor",
        sa.Column("back_adjust_factor", sa.Numeric(precision=12, scale=6), nullable=True),
    )
    op.add_column(
        "adjust_factor",
        sa.Column("fore_adjust_factor", sa.Numeric(precision=12, scale=6), nullable=True),
    )
