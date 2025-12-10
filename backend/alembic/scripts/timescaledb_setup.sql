-- TimescaleDB Setup Script
-- This script sets up hypertables and continuous aggregates
-- Run this AFTER alembic migrations complete
--
-- Usage: docker-compose exec api psql -U $POSTGRES_USER -d $POSTGRES_DB -f alembic/timescaledb_setup.sql

-- ============================================
-- 1. Convert tables to Hypertables
-- ============================================

-- daily_k_data: Main time-series table for stock price data
SELECT create_hypertable(
    'daily_k_data', 'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- backtest_equity: Backtest equity curve time-series
SELECT create_hypertable(
    'backtest_equity', 'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- technical_indicators: Technical indicators time-series
SELECT create_hypertable(
    'technical_indicators', 'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- ============================================
-- 2. Create Continuous Aggregates
-- ============================================

-- Weekly K-line aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_k_weekly
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
FROM daily_k_data
GROUP BY code, time_bucket('1 week', date)
WITH NO DATA;

-- Monthly K-line aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_k_monthly
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
FROM daily_k_data
GROUP BY code, time_bucket('1 month', date)
WITH NO DATA;

-- Market daily statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS market_daily_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', date) AS date,
    count(*) AS total_stocks,
    sum(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
    sum(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
    sum(CASE WHEN pct_chg = 0 THEN 1 ELSE 0 END) AS flat_count,
    avg(pct_chg) AS avg_pct_chg,
    sum(amount) AS total_amount,
    sum(volume) AS total_volume
FROM daily_k_data
WHERE trade_status = 1
GROUP BY time_bucket('1 day', date)
WITH NO DATA;

-- Weekly technical indicators aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS tech_indicators_weekly
WITH (timescaledb.continuous) AS
SELECT
    code,
    time_bucket('1 week', date) AS week,
    last(ma_5, date) AS ma_5,
    last(ma_20, date) AS ma_20,
    last(ma_60, date) AS ma_60,
    last(macd_dif, date) AS macd_dif,
    last(macd_dea, date) AS macd_dea,
    last(rsi_6, date) AS rsi_6,
    last(rsi_12, date) AS rsi_12,
    avg(atr_14) AS avg_atr_14
FROM technical_indicators
GROUP BY code, time_bucket('1 week', date)
WITH NO DATA;

-- ============================================
-- 3. Add Auto-refresh Policies
-- ============================================

SELECT add_continuous_aggregate_policy('daily_k_weekly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('daily_k_monthly',
    start_offset => INTERVAL '3 months',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('market_daily_stats',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('tech_indicators_weekly',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Done
SELECT 'TimescaleDB setup completed!' AS status;
