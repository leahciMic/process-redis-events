"""Get entries from a Redis stream: expired leases first (XAUTOCLAIM), then new entries (XREADGROUP)."""

from __future__ import annotations

import asyncio
from typing import Any

from redis.asyncio import Redis

from process_redis_events.constants import RedisStreamCursors
from process_redis_events.dlq import decode_fields
from process_redis_events.entries import parse_fields


class EntryData:
    """A stream entry read by the consumer. ``attempts`` is the number of earlier deliveries (0 on the first)."""

    def __init__(
        self,
        id: str,
        data: Any,
        attempts: int,
        fields: dict[str, str] | None = None,
        parse_error: Exception | None = None,
    ):
        self.id = id
        self.data = data
        self.attempts = attempts
        self.fields = fields or {}
        self.parse_error = parse_error


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _entry(id: str, fields: Any, attempts: int) -> EntryData:
    data, error = parse_fields(fields)
    return EntryData(
        id=id,
        data=data,
        attempts=attempts,
        fields=decode_fields(fields),
        parse_error=error,
    )


async def get_entries(
    redis: "Redis[Any]",
    stream: str,
    consumer_group: str,
    consumer_id: str,
    block_for: int,
    batch_size: int,
    reclaim_after: int,
    signal: asyncio.Event,
    cursor: dict[str, str] | None = None,
) -> list[EntryData]:
    """Reads the next batch for a consumer: entries idle for longer than ``reclaim_after`` (from any consumer of the
    group) are claimed first; if there are none, new entries are read, blocking for up to ``block_for`` ms.

    ``cursor`` ({"value": "0-0"}) keeps the XAUTOCLAIM scan position between calls: XAUTOCLAIM inspects at most
    COUNT*10 PEL entries per call, so always starting at 0-0 could starve idle entries behind many in-flight ones.
    """
    cursor = (
        cursor if cursor is not None else {"value": RedisStreamCursors.OLDEST.value}
    )
    # Without JUSTID, XAUTOCLAIM returns the claimed entries themselves and increments their delivery count.
    claimed: Any = await redis.xautoclaim(
        name=stream,
        groupname=consumer_group,
        consumername=consumer_id,
        min_idle_time=reclaim_after,
        start_id=cursor["value"],
        count=batch_size,
    )
    cursor["value"] = (
        _s(claimed[0]) if claimed and claimed[0] else RedisStreamCursors.OLDEST.value
    )

    entries = [
        (_s(mid), fields) for mid, fields in (claimed[1] or []) if fields is not None
    ]
    # Redis >= 7 drops entries deleted from the stream from the PEL and lists their ids in element 3.
    deleted = [_s(x) for x in (claimed[2] if len(claimed) > 2 else [])]
    if deleted:
        await redis.xack(stream, consumer_group, *deleted)  # type: ignore[no-untyped-call]

    if entries:
        # One round trip for every claimed id's delivery count.
        pipe = redis.pipeline(transaction=False)
        for mid, _ in entries:
            pipe.xpending_range(
                name=stream, groupname=consumer_group, min=mid, max=mid, count=1
            )
        results = await pipe.execute()
        out: list[EntryData] = []
        for (mid, fields), rows in zip(entries, results, strict=True):
            # Acked or claimed by another consumer in the meantime.
            if not rows or _s(rows[0]["consumer"]) != consumer_id:
                continue
            # XREADGROUP set the count to 1 and every claim adds 1.
            out.append(_entry(mid, fields, max(int(rows[0]["times_delivered"]) - 1, 0)))
        return out

    if signal.is_set():
        return []

    read = asyncio.ensure_future(
        redis.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_id,
            streams={stream: RedisStreamCursors.LATEST.value},
            count=batch_size,
            block=block_for,
        )
    )
    stopped = asyncio.ensure_future(signal.wait())
    try:
        await asyncio.wait({read, stopped}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopped.cancel()
    if not read.done():
        # Shutting down: interrupt the blocking read. Anything it may have read stays pending and is reclaimed after
        # the lease (or at once by a consumer with the same name).
        read.cancel()
        try:
            await read
        except (asyncio.CancelledError, Exception):
            pass
        return []
    res: Any = read.result()
    return [_entry(_s(mid), fields, 0) for _, msgs in res or [] for mid, fields in msgs]
