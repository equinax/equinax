"""add_index_profile_and_sync_history

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2025-12-24 12:00:00

Add index_profile table for storing index metadata and industry composition.
Add sync_history table for tracking data sync operations.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6g7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create index_profile and sync_history tables."""

    # Create index_profile table
    op.create_table(
        'index_profile',
        sa.Column('code', sa.String(20), sa.ForeignKey('asset_meta.code', ondelete='CASCADE'), primary_key=True),
        sa.Column('short_name', sa.String(20), nullable=True),
        sa.Column('index_type', sa.String(50), nullable=True),
        sa.Column('constituent_count', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('industry_composition', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('top_industry_l1', sa.String(50), nullable=True),
        sa.Column('top_industry_weight', sa.Numeric(8, 4), nullable=True),
        sa.Column('herfindahl_index', sa.Numeric(8, 6), nullable=True),
        sa.Column('composition_updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )

    # Create indexes for index_profile
    op.create_index('idx_index_profile_type', 'index_profile', ['index_type'])
    op.create_index('idx_index_profile_top_industry', 'index_profile', ['top_industry_l1'])

    # Create sync_history table
    op.create_table(
        'sync_history',
        sa.Column('id', postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column('sync_type', sa.String(50), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='running'),
        sa.Column('triggered_by', sa.String(50), nullable=True),
        sa.Column('duration_seconds', sa.Numeric(10, 2), nullable=True),
        sa.Column('records_downloaded', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('records_imported', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('records_classified', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('error_details', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('details', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )

    # Create indexes for sync_history
    op.create_index('idx_sync_history_started_at', 'sync_history', ['started_at'], postgresql_ops={'started_at': 'DESC'})
    op.create_index('idx_sync_history_status', 'sync_history', ['status'])
    op.create_index('idx_sync_history_type', 'sync_history', ['sync_type'])


def downgrade() -> None:
    """Drop index_profile and sync_history tables."""

    # Drop sync_history indexes and table
    op.drop_index('idx_sync_history_type', table_name='sync_history')
    op.drop_index('idx_sync_history_status', table_name='sync_history')
    op.drop_index('idx_sync_history_started_at', table_name='sync_history')
    op.drop_table('sync_history')

    # Drop index_profile indexes and table
    op.drop_index('idx_index_profile_top_industry', table_name='index_profile')
    op.drop_index('idx_index_profile_type', table_name='index_profile')
    op.drop_table('index_profile')
