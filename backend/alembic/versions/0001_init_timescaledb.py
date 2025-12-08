"""init_timescaledb

Revision ID: 0001
Revises:
Create Date: 2025-12-08

TimescaleDB native schema with hypertables, compression, and continuous aggregates.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '0001'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create TimescaleDB schema with hypertables."""

    # Ensure TimescaleDB extension is enabled
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")

    # ============================================
    # Non-time-series tables (regular PostgreSQL)
    # ============================================

    op.create_table('stock_basic',
    sa.Column('code', sa.String(length=20), nullable=False),
    sa.Column('code_name', sa.String(length=100), nullable=True),
    sa.Column('ipo_date', sa.Date(), nullable=True),
    sa.Column('out_date', sa.Date(), nullable=True),
    sa.Column('stock_type', sa.Integer(), nullable=True),
    sa.Column('status', sa.Integer(), nullable=True),
    sa.Column('exchange', sa.String(length=10), nullable=True),
    sa.Column('sector', sa.String(length=50), nullable=True),
    sa.Column('industry', sa.String(length=100), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('code')
    )
    op.create_index('idx_stock_basic_exchange', 'stock_basic', ['exchange'], unique=False)
    op.create_index('idx_stock_basic_sector', 'stock_basic', ['sector'], unique=False)

    op.create_table('users',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('email', sa.String(length=255), nullable=False),
    sa.Column('username', sa.String(length=100), nullable=False),
    sa.Column('password_hash', sa.String(length=255), nullable=False),
    sa.Column('salt', sa.String(length=64), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('is_admin', sa.Boolean(), nullable=False),
    sa.Column('api_key_hash', sa.String(length=255), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('last_login', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('username')
    )
    op.create_index(op.f('ix_users_api_key_hash'), 'users', ['api_key_hash'], unique=False)
    op.create_index(op.f('ix_users_email'), 'users', ['email'], unique=True)

    op.create_table('backtest_jobs',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('strategy_ids', postgresql.ARRAY(sa.String()), nullable=False),
    sa.Column('stock_codes', postgresql.ARRAY(sa.String(length=20)), nullable=False),
    sa.Column('stock_filter', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=False),
    sa.Column('end_date', sa.Date(), nullable=False),
    sa.Column('initial_capital', sa.Numeric(precision=18, scale=2), nullable=False),
    sa.Column('commission_rate', sa.Numeric(precision=8, scale=6), nullable=False),
    sa.Column('slippage', sa.Numeric(precision=8, scale=6), nullable=False),
    sa.Column('position_sizing', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('priority', sa.Integer(), nullable=False),
    sa.Column('status', sa.Enum('PENDING', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', name='backteststatus'), nullable=False),
    sa.Column('progress', sa.Numeric(precision=5, scale=2), nullable=False),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('arq_job_id', sa.String(length=255), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('total_backtests', sa.Integer(), nullable=False),
    sa.Column('successful_backtests', sa.Integer(), nullable=False),
    sa.Column('failed_backtests', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_backtest_jobs_created', 'backtest_jobs', ['created_at'], unique=False)
    op.create_index('idx_backtest_jobs_status', 'backtest_jobs', ['status'], unique=False)
    op.create_index('idx_backtest_jobs_user', 'backtest_jobs', ['user_id'], unique=False)

    op.create_table('strategies',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('code', sa.Text(), nullable=False),
    sa.Column('code_hash', sa.String(length=64), nullable=False),
    sa.Column('strategy_type', sa.String(length=50), nullable=True),
    sa.Column('indicators_used', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('is_validated', sa.Boolean(), nullable=False),
    sa.Column('validation_error', sa.Text(), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('is_public', sa.Boolean(), nullable=False),
    sa.Column('execution_mode', sa.String(length=20), nullable=False),
    sa.Column('risk_params', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('broker_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_strategies_active', 'strategies', ['is_active'], unique=False)
    op.create_index('idx_strategies_type', 'strategies', ['strategy_type'], unique=False)
    op.create_index('idx_strategies_user', 'strategies', ['user_id'], unique=False)
    op.create_index('idx_strategies_user_name_version', 'strategies', ['user_id', 'name', 'version'], unique=True)

    op.create_table('backtest_results',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('job_id', sa.UUID(), nullable=False),
    sa.Column('strategy_id', sa.UUID(), nullable=False),
    sa.Column('stock_code', sa.String(length=20), nullable=False),
    sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('total_return', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('annual_return', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('sharpe_ratio', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('sortino_ratio', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('calmar_ratio', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('max_drawdown', sa.Numeric(precision=8, scale=6), nullable=True),
    sa.Column('max_drawdown_duration', sa.Integer(), nullable=True),
    sa.Column('volatility', sa.Numeric(precision=8, scale=6), nullable=True),
    sa.Column('var_95', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('cvar_95', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('beta', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('alpha', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('total_trades', sa.Integer(), nullable=True),
    sa.Column('winning_trades', sa.Integer(), nullable=True),
    sa.Column('losing_trades', sa.Integer(), nullable=True),
    sa.Column('win_rate', sa.Numeric(precision=5, scale=4), nullable=True),
    sa.Column('avg_win', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('avg_loss', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('profit_factor', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('avg_trade_duration', sa.Numeric(precision=8, scale=2), nullable=True),
    sa.Column('final_value', sa.Numeric(precision=18, scale=2), nullable=True),
    sa.Column('peak_value', sa.Numeric(precision=18, scale=2), nullable=True),
    sa.Column('equity_curve', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('trades', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('monthly_returns', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('execution_time_ms', sa.Integer(), nullable=True),
    sa.Column('status', sa.Enum('PENDING', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', name='backteststatus', create_type=False), nullable=False),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['job_id'], ['backtest_jobs.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['strategy_id'], ['strategies.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_backtest_results_job', 'backtest_results', ['job_id'], unique=False)
    op.create_index('idx_backtest_results_job_strategy_stock', 'backtest_results', ['job_id', 'strategy_id', 'stock_code'], unique=True)
    op.create_index('idx_backtest_results_sharpe', 'backtest_results', ['sharpe_ratio'], unique=False)
    op.create_index('idx_backtest_results_stock', 'backtest_results', ['stock_code'], unique=False)
    op.create_index('idx_backtest_results_strategy', 'backtest_results', ['strategy_id'], unique=False)

    op.create_table('strategy_versions',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('strategy_id', sa.UUID(), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('code', sa.Text(), nullable=False),
    sa.Column('code_hash', sa.String(length=64), nullable=False),
    sa.Column('parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('change_notes', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('created_by', sa.UUID(), nullable=True),
    sa.ForeignKeyConstraint(['created_by'], ['users.id'], ),
    sa.ForeignKeyConstraint(['strategy_id'], ['strategies.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_strategy_versions_strategy_version', 'strategy_versions', ['strategy_id', 'version'], unique=True)

    # ============================================
    # TimescaleDB Hypertables (time-series data)
    # ============================================

    # Daily K-line data - primary time-series table
    op.create_table('daily_k_data',
    sa.Column('code', sa.String(length=20), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('open', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('high', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('low', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('close', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('preclose', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('volume', sa.BigInteger(), nullable=True),
    sa.Column('amount', sa.Numeric(precision=18, scale=2), nullable=True),
    sa.Column('turn', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('trade_status', sa.Integer(), nullable=True),
    sa.Column('pct_chg', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('pe_ttm', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('pb_mrq', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ps_ttm', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('pcf_ncf_ttm', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('is_st', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('code', 'date')
    )
    op.create_index('idx_daily_k_date', 'daily_k_data', ['date'], unique=False)
    op.create_index('idx_daily_k_code', 'daily_k_data', ['code'], unique=False)

    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable('daily_k_data', 'date',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => true
        )
    """)

    # Enable compression
    op.execute("""
        ALTER TABLE daily_k_data SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'code',
            timescaledb.compress_orderby = 'date DESC'
        )
    """)

    # Add compression policy (compress data older than 90 days)
    op.execute("""
        SELECT add_compression_policy('daily_k_data', INTERVAL '90 days', if_not_exists => true)
    """)

    # Technical indicators
    op.create_table('technical_indicators',
    sa.Column('code', sa.String(length=20), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('ma_5', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ma_10', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ma_20', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ma_60', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ma_120', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ma_250', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ema_12', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ema_26', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('macd_dif', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('macd_dea', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('macd_hist', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('rsi_6', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('rsi_12', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('rsi_24', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('kdj_k', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('kdj_d', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('kdj_j', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('boll_upper', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('boll_middle', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('boll_lower', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('vol_ma_5', sa.BigInteger(), nullable=True),
    sa.Column('vol_ma_10', sa.BigInteger(), nullable=True),
    sa.Column('atr_14', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('obv', sa.BigInteger(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('code', 'date')
    )
    op.create_index('idx_tech_ind_date', 'technical_indicators', ['date'], unique=False)
    op.create_index('idx_tech_ind_code', 'technical_indicators', ['code'], unique=False)

    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable('technical_indicators', 'date',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => true
        )
    """)

    # Enable compression
    op.execute("""
        ALTER TABLE technical_indicators SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'code',
            timescaledb.compress_orderby = 'date DESC'
        )
    """)

    # Add compression policy
    op.execute("""
        SELECT add_compression_policy('technical_indicators', INTERVAL '90 days', if_not_exists => true)
    """)

    # Adjust factors
    op.create_table('adjust_factor',
    sa.Column('code', sa.String(length=20), nullable=False),
    sa.Column('divid_operate_date', sa.Date(), nullable=False),
    sa.Column('fore_adjust_factor', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('back_adjust_factor', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('adjust_factor', sa.Numeric(precision=12, scale=6), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('code', 'divid_operate_date')
    )
    op.create_index('idx_adjust_factor_code', 'adjust_factor', ['code'], unique=False)
    op.create_index('idx_adjust_factor_date', 'adjust_factor', ['divid_operate_date'], unique=False)

    # Convert to hypertable (yearly chunks since less data)
    op.execute("""
        SELECT create_hypertable('adjust_factor', 'divid_operate_date',
            chunk_time_interval => INTERVAL '1 year',
            if_not_exists => true
        )
    """)

    # No compression for adjust_factor (small table, frequently accessed)

    # Fundamental indicators
    op.create_table('fundamental_indicators',
    sa.Column('code', sa.String(length=20), nullable=False),
    sa.Column('report_date', sa.Date(), nullable=False),
    sa.Column('report_type', sa.String(length=10), nullable=True),
    sa.Column('pe_ratio', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('pb_ratio', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ps_ratio', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('ev_ebitda', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('roe', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('roa', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('gross_margin', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('net_margin', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('operating_margin', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('revenue_growth_yoy', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('profit_growth_yoy', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('eps_growth_yoy', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('operating_cash_flow', sa.Numeric(precision=18, scale=2), nullable=True),
    sa.Column('free_cash_flow', sa.Numeric(precision=18, scale=2), nullable=True),
    sa.Column('cash_flow_per_share', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('debt_to_equity', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('current_ratio', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('quick_ratio', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('eps', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('bvps', sa.Numeric(precision=12, scale=4), nullable=True),
    sa.Column('dividend_yield', sa.Numeric(precision=8, scale=4), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('code', 'report_date')
    )
    op.create_index('idx_fund_ind_code', 'fundamental_indicators', ['code'], unique=False)
    op.create_index('idx_fund_ind_date', 'fundamental_indicators', ['report_date'], unique=False)

    # Convert to hypertable
    op.execute("""
        SELECT create_hypertable('fundamental_indicators', 'report_date',
            chunk_time_interval => INTERVAL '1 year',
            if_not_exists => true
        )
    """)

    # Enable compression for older data
    op.execute("""
        ALTER TABLE fundamental_indicators SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'code',
            timescaledb.compress_orderby = 'report_date DESC'
        )
    """)

    # Compress data older than 1 year
    op.execute("""
        SELECT add_compression_policy('fundamental_indicators', INTERVAL '1 year', if_not_exists => true)
    """)

    # ============================================
    # Continuous Aggregates (pre-computed views)
    # ============================================

    # Weekly K-line continuous aggregate
    op.execute("""
        CREATE MATERIALIZED VIEW daily_k_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('7 days', date) AS week,
            first(open, date) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, date) AS close,
            sum(volume) AS volume,
            sum(amount) AS amount,
            avg(turn) AS avg_turn,
            last(pct_chg, date) AS pct_chg
        FROM daily_k_data
        GROUP BY code, time_bucket('7 days', date)
        WITH NO DATA
    """)

    # Monthly K-line continuous aggregate
    op.execute("""
        CREATE MATERIALIZED VIEW daily_k_monthly
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
            avg(turn) AS avg_turn,
            last(pct_chg, date) AS pct_chg
        FROM daily_k_data
        GROUP BY code, time_bucket('1 month', date)
        WITH NO DATA
    """)

    # Daily market statistics continuous aggregate
    op.execute("""
        CREATE MATERIALIZED VIEW market_daily_stats
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('1 day', date) AS date,
            count(*) AS stock_count,
            count(*) FILTER (WHERE pct_chg > 0) AS gainers,
            count(*) FILTER (WHERE pct_chg < 0) AS losers,
            count(*) FILTER (WHERE pct_chg = 0) AS unchanged,
            avg(pct_chg) AS avg_change,
            max(pct_chg) AS max_change,
            min(pct_chg) AS min_change,
            sum(amount) AS total_amount,
            sum(volume) AS total_volume,
            avg(turn) AS avg_turnover
        FROM daily_k_data
        GROUP BY time_bucket('1 day', date)
        WITH NO DATA
    """)

    # Weekly technical indicators aggregate
    op.execute("""
        CREATE MATERIALIZED VIEW tech_indicators_weekly
        WITH (timescaledb.continuous) AS
        SELECT
            code,
            time_bucket('7 days', date) AS week,
            last(ma_5, date) AS ma_5,
            last(ma_10, date) AS ma_10,
            last(ma_20, date) AS ma_20,
            last(ma_60, date) AS ma_60,
            last(macd_dif, date) AS macd_dif,
            last(macd_dea, date) AS macd_dea,
            last(macd_hist, date) AS macd_hist,
            avg(rsi_6) AS avg_rsi_6,
            avg(rsi_12) AS avg_rsi_12,
            last(kdj_k, date) AS kdj_k,
            last(kdj_d, date) AS kdj_d,
            last(kdj_j, date) AS kdj_j
        FROM technical_indicators
        GROUP BY code, time_bucket('7 days', date)
        WITH NO DATA
    """)

    # Note: Continuous aggregate refresh policies are NOT added here
    # because they require data to exist first (bucket validation).
    # After importing data, run manually:
    #   SELECT add_continuous_aggregate_policy('daily_k_weekly', ...);
    # Or use: CALL refresh_continuous_aggregate('view_name', NULL, NULL);


def downgrade() -> None:
    """Remove all tables and hypertables."""
    # Remove compression policies
    op.execute("SELECT remove_compression_policy('daily_k_data', if_exists => true)")
    op.execute("SELECT remove_compression_policy('technical_indicators', if_exists => true)")
    op.execute("SELECT remove_compression_policy('fundamental_indicators', if_exists => true)")

    # Drop continuous aggregates
    op.execute("DROP MATERIALIZED VIEW IF EXISTS daily_k_weekly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS daily_k_monthly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS market_daily_stats CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS tech_indicators_weekly CASCADE")

    # Drop regular tables
    op.drop_index('idx_strategy_versions_strategy_version', table_name='strategy_versions')
    op.drop_table('strategy_versions')
    op.drop_index('idx_backtest_results_strategy', table_name='backtest_results')
    op.drop_index('idx_backtest_results_stock', table_name='backtest_results')
    op.drop_index('idx_backtest_results_sharpe', table_name='backtest_results')
    op.drop_index('idx_backtest_results_job_strategy_stock', table_name='backtest_results')
    op.drop_index('idx_backtest_results_job', table_name='backtest_results')
    op.drop_table('backtest_results')
    op.drop_index('idx_strategies_user_name_version', table_name='strategies')
    op.drop_index('idx_strategies_user', table_name='strategies')
    op.drop_index('idx_strategies_type', table_name='strategies')
    op.drop_index('idx_strategies_active', table_name='strategies')
    op.drop_table('strategies')
    op.drop_index('idx_backtest_jobs_user', table_name='backtest_jobs')
    op.drop_index('idx_backtest_jobs_status', table_name='backtest_jobs')
    op.drop_index('idx_backtest_jobs_created', table_name='backtest_jobs')
    op.drop_table('backtest_jobs')
    op.drop_index(op.f('ix_users_email'), table_name='users')
    op.drop_index(op.f('ix_users_api_key_hash'), table_name='users')
    op.drop_table('users')

    # Drop hypertables
    op.drop_index('idx_fund_ind_date', table_name='fundamental_indicators')
    op.drop_index('idx_fund_ind_code', table_name='fundamental_indicators')
    op.drop_table('fundamental_indicators')

    op.drop_index('idx_adjust_factor_date', table_name='adjust_factor')
    op.drop_index('idx_adjust_factor_code', table_name='adjust_factor')
    op.drop_table('adjust_factor')

    op.drop_index('idx_tech_ind_code', table_name='technical_indicators')
    op.drop_index('idx_tech_ind_date', table_name='technical_indicators')
    op.drop_table('technical_indicators')

    op.drop_index('idx_daily_k_code', table_name='daily_k_data')
    op.drop_index('idx_daily_k_date', table_name='daily_k_data')
    op.drop_table('daily_k_data')

    op.drop_index('idx_stock_basic_sector', table_name='stock_basic')
    op.drop_index('idx_stock_basic_exchange', table_name='stock_basic')
    op.drop_table('stock_basic')

    # Drop enum type
    op.execute("DROP TYPE IF EXISTS backteststatus")
