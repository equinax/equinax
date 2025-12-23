"""remove_northbound_institutional_fields

Revision ID: ed4a0e4392b1
Revises: 3f56b2b929aa
Create Date: 2025-12-23 16:25:05

Remove northbound holdings and institutional holdings fields from microstructure tables.
These data sources are unreliable (API only returns current day snapshots, not historical data).
We will use turnover-based calculations (circ_mv = amount / turn) instead.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ed4a0e4392b1'
down_revision: Union[str, Sequence[str], None] = '3f56b2b929aa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Remove northbound and institutional holdings fields."""

    # Drop indexes first
    op.drop_index('idx_microstructure_institutional', table_name='stock_microstructure')
    op.drop_index('idx_microstructure_northbound', table_name='stock_microstructure')

    # Drop columns from stock_microstructure
    op.drop_column('stock_microstructure', 'fund_holding_ratio')
    op.drop_column('stock_microstructure', 'fund_holding_change')
    op.drop_column('stock_microstructure', 'northbound_holding_ratio')
    op.drop_column('stock_microstructure', 'northbound_holding_change')
    op.drop_column('stock_microstructure', 'is_institutional')
    op.drop_column('stock_microstructure', 'is_northbound_heavy')

    # Drop columns from stock_classification_snapshot
    op.drop_column('stock_classification_snapshot', 'is_institutional')
    op.drop_column('stock_classification_snapshot', 'is_northbound_heavy')

    # Add new columns to stock_classification_snapshot
    op.add_column('stock_classification_snapshot',
                  sa.Column('is_retail_hot', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('stock_classification_snapshot',
                  sa.Column('is_main_controlled', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    """Restore northbound and institutional holdings fields."""

    # Drop new columns from stock_classification_snapshot
    op.drop_column('stock_classification_snapshot', 'is_retail_hot')
    op.drop_column('stock_classification_snapshot', 'is_main_controlled')

    # Restore columns to stock_classification_snapshot
    op.add_column('stock_classification_snapshot',
                  sa.Column('is_institutional', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('stock_classification_snapshot',
                  sa.Column('is_northbound_heavy', sa.Boolean(), nullable=False, server_default='false'))

    # Restore columns to stock_microstructure
    op.add_column('stock_microstructure',
                  sa.Column('fund_holding_ratio', sa.Numeric(precision=8, scale=4), nullable=True))
    op.add_column('stock_microstructure',
                  sa.Column('fund_holding_change', sa.Numeric(precision=8, scale=4), nullable=True))
    op.add_column('stock_microstructure',
                  sa.Column('northbound_holding_ratio', sa.Numeric(precision=8, scale=4), nullable=True))
    op.add_column('stock_microstructure',
                  sa.Column('northbound_holding_change', sa.Numeric(precision=8, scale=4), nullable=True))
    op.add_column('stock_microstructure',
                  sa.Column('is_institutional', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('stock_microstructure',
                  sa.Column('is_northbound_heavy', sa.Boolean(), nullable=False, server_default='false'))

    # Restore indexes
    op.create_index('idx_microstructure_institutional', 'stock_microstructure', ['is_institutional'])
    op.create_index('idx_microstructure_northbound', 'stock_microstructure', ['is_northbound_heavy'])
