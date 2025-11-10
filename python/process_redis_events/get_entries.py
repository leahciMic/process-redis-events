"""Get entries from Redis stream."""

import asyncio
from typing import Any, TypeVar

from pydantic import TypeAdapter
from redis.asyncio import Redis

from process_redis_events.chunk import chunk
from process_redis_events.constants import RedisStreamCursors
from process_redis_events.parse_json import parse_json
from process_redis_events.result_of import result_of

T = TypeVar("T")


class PendingInfo:
    """Information about a pending message."""

    def __init__(self, id: str, consumer_id: str, idle_time: int, attempts: int):
        self.id = id
        self.consumer_id = consumer_id
        self.idle_time = idle_time
        self.attempts = attempts


class EntryData:
    """Data for a stream entry."""

    def __init__(self, id: str, data: Any, attempts: int):
        self.id = id
        self.data = data
        self.attempts = attempts


async def get_entries(
    redis: "Redis[Any]",
    stream: str,
    consumer_group: str,
    consumer_id: str,
    block_for: int,
    batch_size: int,
    reclaim_after: int,
    signal: asyncio.Event,
) -> list[EntryData]:
    """Get entries from a Redis stream with autoclaim.

    Args:
        redis: Redis client
        stream: Stream name
        consumer_group: Consumer group name
        consumer_id: Consumer ID
        block_for: Time to block waiting for messages (ms)
        batch_size: Maximum number of messages to retrieve
        reclaim_after: Time before reclaiming messages (ms)
        signal: Event to signal shutdown

    Returns:
        List of entry data
    """
    # Autoclaim pending messages
    claimed_response = await redis.xautoclaim(
        name=stream,
        groupname=consumer_group,
        consumername=consumer_id,
        min_idle_time=reclaim_after,
        start_id=RedisStreamCursors.OLDEST.value,
        count=batch_size,
        justid=True,
    )

    if signal.is_set():
        return []

    # With justid=True, redis-py returns a flat list of IDs
    # Without justid, it returns [next_cursor, [(id, data)], deleted_ids]
    claimed_ids = claimed_response if isinstance(claimed_response, list) else []
    claimed_count = len(claimed_ids)

    # Get pending info for claimed messages
    retry_data: dict[str, int] = {}
    if claimed_ids:
        # Decode message IDs from bytes to str
        claimed_ids_str = [
            id.decode() if isinstance(id, bytes) else id for id in claimed_ids
        ]
        pending_infos = await asyncio.gather(
            *[
                _get_pending_info(redis, stream, consumer_group, id)
                for id in claimed_ids_str
            ]
        )
        retry_data = {info.id: info.attempts for info in pending_infos if info}

    if signal.is_set():
        return []

    # Read new messages
    count = claimed_count if claimed_count > 0 else batch_size
    cursor = (
        RedisStreamCursors.OLDEST.value
        if claimed_count > 0
        else RedisStreamCursors.LATEST.value
    )

    error, entries = await result_of(
        redis.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_id,
            streams={stream: cursor},
            count=count,
            block=block_for,
        )
    )

    if signal.is_set():
        return []

    if error:
        raise error

    if not entries or len(entries) == 0:
        return []

    # Parse entries
    queue_items: list[EntryData] = []
    for stream_name, messages in entries:
        for message_id, values in messages:
            # Decode message_id from bytes to str
            message_id_str = (
                message_id.decode() if isinstance(message_id, bytes) else message_id
            )

            # values is a dict[bytes, bytes]
            if not values or len(values) == 0:
                data = None
            elif b"data" in values:
                data = parse_json(values[b"data"])
            else:
                # Convert to regular dict
                data = {k.decode(): v.decode() for k, v in values.items()}

            attempts = retry_data.get(message_id_str, 0)
            queue_items.append(
                EntryData(id=message_id_str, data=data, attempts=attempts)
            )

    return queue_items


async def _get_pending_info(
    redis: "Redis[Any]", stream: str, consumer_group: str, message_id: str
) -> PendingInfo | None:
    """Get pending information for a specific message."""
    try:
        # Use XPENDING_RANGE to get details for a specific message ID
        # XPENDING_RANGE stream group min_id max_id count
        pending_response = await redis.xpending_range(
            name=stream,
            groupname=consumer_group,
            min=message_id,
            max=message_id,
            count=1,
        )
        if pending_response and len(pending_response) > 0:
            # Response format from redis-py is a list of dicts with keys:
            # 'message_id', 'consumer', 'time_since_delivered', 'times_delivered'
            info = pending_response[0]
            result = PendingInfo(
                id=(
                    info["message_id"].decode()
                    if isinstance(info["message_id"], bytes)
                    else info["message_id"]
                ),
                consumer_id=(
                    info["consumer"].decode()
                    if isinstance(info["consumer"], bytes)
                    else info["consumer"]
                ),
                idle_time=info["time_since_delivered"],
                attempts=info["times_delivered"],  # This is the delivery count we want
            )
            return result
    except Exception:
        pass
    return None
