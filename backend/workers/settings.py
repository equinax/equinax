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
    ]

    # Optional: Cron jobs
    # cron_jobs = [
    #     cron(calculate_daily_indicators, hour=1, minute=0),  # Run at 1:00 AM
    # ]

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
