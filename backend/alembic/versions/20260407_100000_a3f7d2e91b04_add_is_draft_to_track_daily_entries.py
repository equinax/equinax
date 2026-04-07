"""add is_draft to track_daily_entries

Revision ID: a3f7d2e91b04
Revises: 118b694c0126
Create Date: 2026-04-07 10:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a3f7d2e91b04"
down_revision: Union[str, Sequence[str], None] = "118b694c0126"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add is_draft boolean column to track_daily_entries."""
    op.add_column(
        "track_daily_entries",
        sa.Column("is_draft", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )


def downgrade() -> None:
    """Remove is_draft column from track_daily_entries."""
    op.drop_column("track_daily_entries", "is_draft")
