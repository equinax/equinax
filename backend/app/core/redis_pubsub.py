"""Redis Pub/Sub utilities for SSE events."""

import json
from datetime import datetime
from typing import AsyncGenerator, Any

import redis.asyncio as redis

from app.config import settings

# Channel name for backtest events
BACKTEST_EVENTS_CHANNEL = "backtest:events"

# Channel name for data sync events
DATA_SYNC_EVENTS_CHANNEL = "data_sync:events"


async def get_redis_client() -> redis.Redis:
    """Get async Redis client for pub/sub."""
    return redis.from_url(settings.redis_url, decode_responses=True)


async def publish_event(event_type: str, job_id: str, data: dict[str, Any]) -> None:
    """
    Publish an event to Redis.

    Args:
        event_type: Type of event (progress, result, log, job_complete)
        job_id: Backtest job ID
        data: Event data payload
    """
    client = await get_redis_client()
    try:
        event = {
            "type": event_type,
            "job_id": job_id,
            "timestamp": datetime.utcnow().isoformat(),
            **data
        }
        await client.publish(BACKTEST_EVENTS_CHANNEL, json.dumps(event))
    finally:
        await client.aclose()


async def subscribe_events(job_id: str) -> AsyncGenerator[str, None]:
    """
    Subscribe to events for a specific job.

    Args:
        job_id: Backtest job ID to filter events for

    Yields:
        SSE-formatted event strings
    """
    client = await get_redis_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(BACKTEST_EVENTS_CHANNEL)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    # Only yield events for the requested job
                    if data.get("job_id") == job_id:
                        yield f"data: {json.dumps(data)}\n\n"
                except json.JSONDecodeError:
                    continue
    finally:
        await pubsub.unsubscribe(BACKTEST_EVENTS_CHANNEL)
        await client.aclose()


# =============================================================================
# Data Sync Events
# =============================================================================

async def publish_data_sync_event(event_type: str, job_id: str, data: dict[str, Any]) -> None:
    """
    Publish a data sync event to Redis.

    Args:
        event_type: Type of event (plan, progress, step_complete, job_complete, error)
        job_id: Sync job ID
        data: Event data payload
    """
    client = await get_redis_client()
    try:
        event = {
            "type": event_type,
            "job_id": job_id,
            "timestamp": datetime.utcnow().isoformat(),
            **data
        }
        await client.publish(DATA_SYNC_EVENTS_CHANNEL, json.dumps(event))
    finally:
        await client.aclose()


async def subscribe_data_sync_events(job_id: str) -> AsyncGenerator[str, None]:
    """
    Subscribe to data sync events for a specific job.

    Args:
        job_id: Sync job ID to filter events for

    Yields:
        SSE-formatted event strings
    """
    client = await get_redis_client()
    pubsub = client.pubsub()
    await pubsub.subscribe(DATA_SYNC_EVENTS_CHANNEL)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    # Only yield events for the requested job
                    if data.get("job_id") == job_id:
                        yield f"data: {json.dumps(data)}\n\n"
                except json.JSONDecodeError:
                    continue
    finally:
        await pubsub.unsubscribe(DATA_SYNC_EVENTS_CHANNEL)
        await client.aclose()
