"""ARQ Redis connection for task queue."""

from arq import create_pool
from arq.connections import RedisSettings, ArqRedis
from typing import Optional

from app.config import settings

_arq_pool: Optional[ArqRedis] = None


def parse_redis_url(url: str) -> RedisSettings:
    """Parse Redis URL into RedisSettings."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return RedisSettings(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        database=int(parsed.path.lstrip("/") or 0),
    )


async def get_arq_pool() -> ArqRedis:
    """Get or create ARQ Redis connection pool."""
    global _arq_pool
    if _arq_pool is None:
        _arq_pool = await create_pool(parse_redis_url(settings.redis_url))
    return _arq_pool


async def close_arq_pool() -> None:
    """Close ARQ Redis connection pool."""
    global _arq_pool
    if _arq_pool is not None:
        await _arq_pool.close()
        _arq_pool = None
