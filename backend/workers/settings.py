"""ARQ Worker settings and configuration."""

from arq.connections import RedisSettings
from arq import cron

from app.config import settings
from workers.backtest_tasks import run_backtest_job, run_single_backtest
from workers.indicator_tasks import calculate_indicators
from workers.classification_tasks import (
    calculate_structural_classification,
    calculate_style_factors,
    calculate_market_regime,
    generate_classification_snapshot,
    daily_classification_update,
)
from workers.data_tasks import (
    download_stock_data,
    download_etf_data,
    import_stock_data,
    import_etf_data,
    daily_data_update,
    check_data_status,
    get_download_status,
)
from workers.index_tasks import (
    calculate_index_industry_composition,
    daily_index_update,
)


def parse_redis_url(url: str) -> RedisSettings:
    """Parse Redis URL into RedisSettings."""
    # redis://localhost:6379/0
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        database=int(parsed.path.lstrip("/") or 0),
    )


class WorkerSettings:
    """ARQ Worker settings."""

    # Redis connection
    redis_settings = parse_redis_url(settings.redis_url)

    # Functions to register
    functions = [
        run_backtest_job,
        run_single_backtest,
        calculate_indicators,
        # Classification tasks
        calculate_structural_classification,
        calculate_style_factors,
        calculate_market_regime,
        generate_classification_snapshot,
        daily_classification_update,
        # Data tasks
        download_stock_data,
        download_etf_data,
        import_stock_data,
        import_etf_data,
        daily_data_update,
        check_data_status,
        get_download_status,
        # Index tasks
        calculate_index_industry_composition,
        daily_index_update,
    ]

    # Cron jobs for automated data updates
    # Note: Times are in UTC. For CST (UTC+8), 16:00 CST = 08:00 UTC
    cron_jobs = [
        # Daily data update at 16:30 CST (after market close)
        cron(daily_data_update, hour=8, minute=30),
        # Weekly index composition update on Sunday at 22:00 CST (14:00 UTC)
        # Updates industry weights based on current constituent stocks
        cron(daily_index_update, weekday=6, hour=14, minute=0),
    ]

    # Worker settings
    max_jobs = settings.max_concurrent_backtests
    job_timeout = settings.backtest_timeout_seconds
    keep_result = 3600  # Keep results for 1 hour
    retry_jobs = True
    max_tries = 3

    # Health check
    health_check_interval = 30

    # Logging
    queue_name = "arq:queue"

    @staticmethod
    async def on_startup(ctx: dict) -> None:
        """Called when worker starts."""
        print("ARQ Worker starting up...")
        # Initialize database connection pool if needed
        ctx["db_pool"] = None  # Will be initialized per task

    @staticmethod
    async def on_shutdown(ctx: dict) -> None:
        """Called when worker shuts down."""
        print("ARQ Worker shutting down...")
        # Clean up resources
        if ctx.get("db_pool"):
            await ctx["db_pool"].close()
