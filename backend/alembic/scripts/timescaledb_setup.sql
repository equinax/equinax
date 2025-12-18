-- TimescaleDB Setup Script
-- This script sets up continuous aggregates for hypertables
-- Hypertables are already created in the Alembic migration
--
-- Run this AFTER alembic migrations complete:
-- docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -f /docker-entrypoint-initdb.d/timescaledb_setup.sql

-- ============================================
-- 1. Verify Hypertables (created in migration)
-- ============================================
-- The following tables are configured as hypertables in the migration:
-- - market_daily (chunk_time_interval: 1 month)
-- - indicator_valuation (chunk_time_interval: 1 month)
-- - indicator_etf (chunk_time_interval: 1 month)
-- - adjust_factor (chunk_time_interval: 3 months)
-- - stock_style_exposure (chunk_time_interval: 1 month)
-- - stock_microstructure (chunk_time_interval: 1 month)
-- - market_regime (chunk_time_interval: 1 month)
-- - stock_classification_snapshot (chunk_time_interval: 1 month)
-- - backtest_equity (chunk_time_interval: 1 month)

-- ============================================
-- 2. Create Continuous Aggregates
-- ============================================

-- Weekly K-line aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS market_daily_weekly
WITH (timescaledb.continuous) AS
SELECT
    code,
    time_bucket('1 week', date) AS week,
    first(open, date) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, date) AS close,
    sum(volume) AS volume,
    sum(amount) AS amount,
    avg(turn) AS avg_turn
FROM market_daily
GROUP BY code, time_bucket('1 week', date)
WITH NO DATA;

-- Monthly K-line aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS market_daily_monthly
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
    avg(turn) AS avg_turn
FROM market_daily
GROUP BY code, time_bucket('1 month', date)
WITH NO DATA;

-- Market daily statistics (overall market health)
CREATE MATERIALIZED VIEW IF NOT EXISTS market_stats_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', date) AS date,
    count(*) AS total_stocks,
    sum(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
    sum(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
    sum(CASE WHEN pct_chg = 0 THEN 1 ELSE 0 END) AS flat_count,
    sum(CASE WHEN pct_chg >= 9.9 THEN 1 ELSE 0 END) AS limit_up_count,
    sum(CASE WHEN pct_chg <= -9.9 THEN 1 ELSE 0 END) AS limit_down_count,
    avg(pct_chg) AS avg_pct_chg,
    sum(amount) AS total_amount,
    sum(volume) AS total_volume,
    avg(turn) AS avg_turn
FROM market_daily
GROUP BY time_bucket('1 day', date)
WITH NO DATA;

-- Valuation statistics aggregation (weekly)
CREATE MATERIALIZED VIEW IF NOT EXISTS valuation_weekly
WITH (timescaledb.continuous) AS
SELECT
    code,
    time_bucket('1 week', date) AS week,
    last(pe_ttm, date) AS pe_ttm,
    last(pb_mrq, date) AS pb_mrq,
    last(ps_ttm, date) AS ps_ttm,
    last(total_mv, date) AS total_mv,
    last(circ_mv, date) AS circ_mv,
    avg(pe_ttm) AS avg_pe_ttm,
    avg(pb_mrq) AS avg_pb_mrq
FROM indicator_valuation
GROUP BY code, time_bucket('1 week', date)
WITH NO DATA;

-- ETF indicator aggregation (weekly)
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
WITH NO DATA;

-- ============================================
-- 3. Add Auto-refresh Policies
-- ============================================

SELECT add_continuous_aggregate_policy('market_daily_weekly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('market_daily_monthly',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('market_stats_daily',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('valuation_weekly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('etf_indicators_weekly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================
-- 4. Compression Policies (optional, for older data)
-- ============================================

-- Enable compression on market_daily (compress data older than 6 months)
ALTER TABLE market_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'code'
);

SELECT add_compression_policy('market_daily', INTERVAL '6 months', if_not_exists => TRUE);

-- Enable compression on indicator_valuation
ALTER TABLE indicator_valuation SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'code'
);

SELECT add_compression_policy('indicator_valuation', INTERVAL '6 months', if_not_exists => TRUE);

-- Enable compression on stock_style_exposure
ALTER TABLE stock_style_exposure SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'code'
);

SELECT add_compression_policy('stock_style_exposure', INTERVAL '6 months', if_not_exists => TRUE);

-- ============================================
-- 5. Useful Indexes for Time-Series Queries
-- ============================================

-- Index for efficient time-range queries with code filter
CREATE INDEX IF NOT EXISTS idx_market_daily_code_date ON market_daily (code, date DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_valuation_code_date ON indicator_valuation (code, date DESC);
CREATE INDEX IF NOT EXISTS idx_indicator_etf_code_date ON indicator_etf (code, date DESC);

-- Done
SELECT 'TimescaleDB continuous aggregates and policies configured!' AS status;
