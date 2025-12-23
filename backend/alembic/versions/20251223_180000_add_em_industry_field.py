"""add_em_industry_field

Revision ID: a1b2c3d4e5f6
Revises: ed4a0e4392b1
Create Date: 2025-12-23 18:00:00

Add em_industry (东方财富行业) field to stock_profile table.
EastMoney industry classification is a flat structure (L1 only) with 86 industry categories.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'ed4a0e4392b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add em_industry field to stock_profile."""
    # Add em_industry column
    op.add_column('stock_profile',
                  sa.Column('em_industry', sa.String(100), nullable=True))

    # Create index for em_industry
    op.create_index('idx_stock_profile_em_industry', 'stock_profile', ['em_industry'])


def downgrade() -> None:
    """Remove em_industry field from stock_profile."""
    # Drop index first
    op.drop_index('idx_stock_profile_em_industry', table_name='stock_profile')

    # Drop column
    op.drop_column('stock_profile', 'em_industry')
