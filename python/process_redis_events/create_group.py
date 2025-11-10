"""Create consumer group utility."""

from typing import Any

from redis.asyncio import Redis

from process_redis_events.constants import StartFrom


async def create_group(
    redis: "Redis[Any]", stream: str, consumer_group: str, start_from: StartFrom
) -> None:
    """Create a consumer group for a Redis stream.

    Args:
        redis: Redis client instance
        stream: Stream name
        consumer_group: Consumer group name
        start_from: Starting position (OLDEST or LATEST)

    Note:
        If the group already exists, this function silently continues.
    """
    try:
        await redis.xgroup_create(
            name=stream,
            groupname=consumer_group,
            id=start_from.value,
            mkstream=True,
        )
    except Exception as err:
        if "BUSYGROUP" in str(err):
            # Group already exists, continue
            pass
        else:
            raise
