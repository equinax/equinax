"""add_etf_classification_fields

Revision ID: c3d4e5f6g7h8
Revises: b2c3d4e5f6g7
Create Date: 2026-01-10 12:00:00

Add ETF classification fields to etf_profile table:
- etf_sub_category: Sub-category like 银行, 医药, 新能源
- classification_source: AUTO or MANUAL
- classification_confidence: HIGH, MEDIUM, or LOW
- classified_at: Timestamp of classification
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6g7h8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6g7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add ETF classification fields to etf_profile."""
    # Add etf_sub_category column
    op.add_column('etf_profile',
                  sa.Column('etf_sub_category', sa.String(100), nullable=True))

    # Add classification_source column (AUTO/MANUAL)
    op.add_column('etf_profile',
                  sa.Column('classification_source', sa.String(20), nullable=True))

    # Add classification_confidence column (HIGH/MEDIUM/LOW)
    op.add_column('etf_profile',
                  sa.Column('classification_confidence', sa.String(20), nullable=True))

    # Add classified_at timestamp column
    op.add_column('etf_profile',
                  sa.Column('classified_at', sa.DateTime(timezone=True), nullable=True))

    # Create indexes for frequently queried columns
    op.create_index('idx_etf_profile_sub_category', 'etf_profile', ['etf_sub_category'])
    op.create_index('idx_etf_profile_source', 'etf_profile', ['classification_source'])


def downgrade() -> None:
    """Remove ETF classification fields from etf_profile."""
    # Drop indexes first
    op.drop_index('idx_etf_profile_source', table_name='etf_profile')
    op.drop_index('idx_etf_profile_sub_category', table_name='etf_profile')

    # Drop columns
    op.drop_column('etf_profile', 'classified_at')
    op.drop_column('etf_profile', 'classification_confidence')
    op.drop_column('etf_profile', 'classification_source')
    op.drop_column('etf_profile', 'etf_sub_category')
