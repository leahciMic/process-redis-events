"""Main Stream class for processing Redis streams."""

import asyncio
import json
from functools import lru_cache
from typing import Any, Awaitable, Callable, Generic, TypeVar, overload, Protocol

from redis.asyncio import Redis

from process_redis_events.chunk import chunk
from process_redis_events.constants import StartFrom
from process_redis_events.create_group import create_group
from process_redis_events.get_entries import EntryData, get_entries
from process_redis_events.heartbeat_manager import HeartbeatManager
from process_redis_events.queue_item import QueueItem
from process_redis_events.result_of import result_of
from process_redis_events.stream_event import StreamEvent
from process_redis_events.telemetry import RedisStreamTelemetry, TelemetryConfig
from process_redis_events.types import Json

T = TypeVar("T")
R = TypeVar("R")


class RetryFunction(Protocol):
    """Protocol for retry decision functions."""

    def __call__(self, attempt: int, entry: Any) -> bool:
        """Decide whether to retry based on attempt count and entry data."""
        ...


class MapFunction(Protocol[T, R]):
    """Protocol for map transformation functions."""

    def __call__(self, items: list[T]) -> Awaitable[list[R]]:
        """Transform a list of items to a list of results."""
        ...


class ProcessCallback(Protocol[T]):
    """Protocol for processing callback functions."""

    def __call__(self, item: QueueItem[T]) -> Awaitable[None]:
        """Process a single queue item."""
        ...


def _generate_id() -> str:
    """Generate a unique ID."""
    import uuid

    return str(uuid.uuid4()).replace("-", "")[:21]


class StreamInfo:
    """Information about a Redis stream."""

    def __init__(self, length: int, groups: int, entries_added: int | None = None):
        self.length = length
        self.groups = groups
        self.entries_added = entries_added


class ConsumerGroupInfo:
    """Information about a consumer group."""

    def __init__(
        self,
        name: str,
        consumers: int,
        pending: int,
        last_delivered_id: str,
        entries_read: int | None = None,
        lag: int | None = None,
    ):
        self.name = name
        self.consumers = consumers
        self.pending = pending
        self.last_delivered_id = last_delivered_id
        self.entries_read = entries_read
        self.lag = lag
        self.lag = lag


class ProcessOptions(Generic[T, R]):
    """Options for processing a stream with proper typing."""

    def __init__(
        self,
        *,
        signal: asyncio.Event,
        consumer_group: str,
        consumer_id: str | None = None,
        batch_size: int = 20,
        start_from: StartFrom = StartFrom.LATEST,
        map_fn: MapFunction[T, R] | None = None,
        should_retry: RetryFunction | None = None,
        concurrency: int = 10,
        lease_ms: int = 30000,
    ):
        """Initialize processing options.

        Args:
            signal: Event to signal shutdown
            consumer_group: Name of the Redis consumer group
            consumer_id: Optional consumer ID (generates one if not provided)
            batch_size: Number of messages to process in each batch
            start_from: Where to start reading from the stream
            map_fn: Optional function to transform data before processing
            should_retry: Optional function to determine retry logic
            concurrency: Maximum number of concurrent processors
            lease_ms: Message lease duration in milliseconds
        """
        self.signal = signal
        self.consumer_group = consumer_group
        self.consumer_id = consumer_id or _generate_id()
        self.batch_size = batch_size
        self.start_from = start_from
        self.map_fn = map_fn
        self.should_retry = should_retry or (lambda attempt, entry: True)
        self.concurrency = concurrency
        self.lease_ms = lease_ms


def create_process_options(
    signal: asyncio.Event,
    consumer_group: str,
    *,
    consumer_id: str | None = None,
    batch_size: int = 20,
    start_from: StartFrom = StartFrom.LATEST,
    map_fn: MapFunction[Any, Any] | None = None,
    should_retry: RetryFunction | None = None,
    concurrency: int = 10,
    lease_ms: int = 30000,
) -> ProcessOptions[Any, Any]:
    """Create ProcessOptions with sensible defaults.

    Args:
        signal: Event to signal shutdown
        consumer_group: Name of the Redis consumer group
        consumer_id: Optional consumer ID (generates one if not provided)
        batch_size: Number of messages to process in each batch
        start_from: Where to start reading from the stream
        map_fn: Optional function to transform data before processing
        should_retry: Optional function to determine retry logic
        concurrency: Maximum number of concurrent processors
        lease_ms: Message lease duration in milliseconds

    Returns:
        Configured ProcessOptions instance
    """
    return ProcessOptions(
        signal=signal,
        consumer_group=consumer_group,
        consumer_id=consumer_id,
        batch_size=batch_size,
        start_from=start_from,
        map_fn=map_fn,
        should_retry=should_retry,
        concurrency=concurrency,
        lease_ms=lease_ms,
    )


class Stream(Generic[T]):
    """Redis stream processor with consumer groups."""

    def __init__(
        self,
        name: str,
        create_redis: "Callable[[], Redis[Any]]",
        produce_events: bool = False,
        telemetry_config: TelemetryConfig | None = None,
    ):
        """Initialize a Stream.

        Args:
            name: Name of the stream
            create_redis: Factory function to create Redis clients
            produce_events: Whether to produce event messages
            telemetry_config: Optional telemetry configuration
        """
        self.name = name
        self.create_redis = create_redis
        self.produce_events = produce_events
        self.redis = create_redis()

        config = telemetry_config or TelemetryConfig(
            stream_name=name,
            consumer_group="all",
            consumer_id="all",
        )
        self.telemetry = RedisStreamTelemetry(config)

        self.stream_key = f"stream:{name}"
        self.dlq_stream_key = f"stream:dlq:{name}"
        self._event_stream: "Stream[StreamEvent] | None" = None

    async def get_stream_length(self) -> int:
        """Get the length of the stream."""
        return await self.redis.xlen(self.stream_key)

    async def get_stream_info(self) -> StreamInfo | None:
        """Get information about the stream."""
        error, info = await result_of(self.redis.xinfo_stream(self.stream_key))

        if error:
            if "ERR no such key" in str(error) or "NOKEY" in str(error):
                return None
            print(f"Error fetching info for stream {self.stream_key}: {error}")
            return None

        # redis-py returns a dict for xinfo_stream
        if not isinstance(info, dict):
            return None

        return StreamInfo(
            length=(
                int(info.get("length", 0))
                if "length" in info
                else int(info.get(b"length", 0))
            ),
            groups=(
                int(info.get("groups", 0))
                if "groups" in info
                else int(info.get(b"groups", 0))
            ),
            entries_added=(
                int(info["entries-added"])
                if "entries-added" in info
                else int(info[b"entries-added"]) if b"entries-added" in info else None
            ),
        )

    async def get_consumer_groups_info(self) -> list[ConsumerGroupInfo]:
        """Get information about all consumer groups."""
        error, groups_reply = await result_of(
            self.redis.xinfo_groups(self.stream_key)  # type: ignore[no-untyped-call]
        )

        if error:
            print(
                f"Error fetching consumer groups for stream {self.stream_key}: {error}"
            )
            return []

        if not isinstance(groups_reply, list):
            return []

        # redis-py returns a list of dicts for xinfo_groups
        parsed = []
        for entry in groups_reply:
            if not isinstance(entry, dict):
                continue

            # Handle both decoded (str) and non-decoded (bytes) keys
            name = entry.get("name") or entry.get(b"name")
            consumers = entry.get("consumers") or entry.get(b"consumers")
            pending = entry.get("pending") or entry.get(b"pending")
            last_delivered_id = entry.get("last-delivered-id") or entry.get(
                b"last-delivered-id"
            )
            entries_read = entry.get("entries-read") or entry.get(b"entries-read")
            lag = entry.get("lag") or entry.get(b"lag")

            if name is not None:
                # Decode if bytes
                if isinstance(name, bytes):
                    name = name.decode()
                if isinstance(last_delivered_id, bytes):
                    last_delivered_id = last_delivered_id.decode()

                parsed.append(
                    ConsumerGroupInfo(
                        name=name,
                        consumers=int(consumers) if consumers is not None else 0,
                        pending=int(pending) if pending is not None else 0,
                        last_delivered_id=last_delivered_id or "0-0",
                        entries_read=(
                            int(entries_read) if entries_read is not None else None
                        ),
                        lag=int(lag) if lag is not None else None,
                    )
                )

        return parsed

    def create_event_stream(self) -> "Stream[StreamEvent]":
        """Create or get the event stream for this stream."""
        if self._event_stream is None:
            self._event_stream = Stream[StreamEvent](
                name=f"{self.name}:events",
                create_redis=self.create_redis,
                produce_events=False,
            )
        return self._event_stream

    async def clear(self) -> None:
        """Clear the stream by deleting its key."""
        await self.redis.delete(self.stream_key)

    async def add(self, data: T) -> dict[str, str]:
        """Add data to the stream.

        Args:
            data: Data to add

        Returns:
            Dictionary with the message ID
        """
        id = await self.redis.xadd(
            name=self.stream_key, fields={"data": json.dumps(data)}
        )
        assert isinstance(id, (str, bytes)), "Failed to add message to stream"
        if isinstance(id, bytes):
            id = id.decode()
        return {"id": id}

    async def _check_retry(
        self,
        items: list[QueueItem[T]],
        should_retry: Callable[[int, T], bool],
        consumer_group: str,
        consumer_id: str,
    ) -> list[QueueItem[T]]:
        """Check if items should be retried or moved to DLQ."""
        filtered_items: list[QueueItem[T]] = []

        for item in items:
            if item.attempts > 0 and not should_retry(item.attempts, item.data):
                # Move to dead letter queue
                try:
                    await self.redis.xadd(
                        name=self.dlq_stream_key,
                        fields={
                            "originalId": item.id,
                            "attempts": str(item.attempts),
                            "consumerGroup": consumer_group,
                            "consumerId": consumer_id,
                            "timestamp": str(
                                int(asyncio.get_event_loop().time() * 1000)
                            ),
                            "data": json.dumps(item.data),
                        },
                    )
                except Exception as error:
                    print(
                        f"Failed to add message to dead letter queue {self.dlq_stream_key}: {error}"
                    )
                finally:
                    await self.redis.xack(self.stream_key, consumer_group, item.id)  # type: ignore[no-untyped-call]
            else:
                filtered_items.append(item)

        return filtered_items

    @overload
    async def process(
        self,
        options: ProcessOptions[T, T],
        callback: ProcessCallback[T],
    ) -> None: ...

    @overload
    async def process(
        self,
        options: ProcessOptions[T, R],
        callback: ProcessCallback[R],
    ) -> None: ...

    async def process(
        self,
        options: ProcessOptions[T, Any],
        callback: ProcessCallback[Any],
    ) -> None:
        """Process items from the stream.

        Args:
            options: Processing options with proper typing
            callback: Async function to process each item
        """
        # Extract options with full type safety
        signal = options.signal
        consumer_group = options.consumer_group
        consumer_id = options.consumer_id
        batch_size = options.batch_size
        start_from = options.start_from
        map_fn = options.map_fn
        should_retry = options.should_retry
        concurrency = options.concurrency
        lease_ms = options.lease_ms

        stream_redis = self.create_redis()
        redis = self.redis

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(concurrency)
        heartbeat = round(lease_ms / 2)
        xclaim_block_timeout = max(lease_ms - 1000, 100)
        reclaim_after = lease_ms + 100

        telemetry = self.telemetry.child(
            consumer_group=consumer_group,
            consumer_id=consumer_id,
        )

        heartbeat_manager = HeartbeatManager(
            redis, self.stream_key, consumer_group, consumer_id, heartbeat
        )
        await heartbeat_manager.start()

        event_stream = self.create_event_stream() if self.produce_events else None

        async def create_queue_item(entry: EntryData) -> QueueItem[T]:
            """Create a queue item from an entry."""
            heartbeat_manager.add(entry.id)

            async def report_progress(completion_ratio: float, status: str) -> None:
                if completion_ratio < 0 or completion_ratio > 1:
                    raise ValueError("completion_ratio must be between 0 and 1")
                if event_stream:
                    await event_stream.add(
                        {
                            "id": entry.id,
                            "type": "progress",
                            "completionRatio": completion_ratio,
                            "status": status,
                        }
                    )

            return QueueItem(
                id=entry.id,
                data=entry.data,
                attempts=entry.attempts,
                report_progress=report_progress,
            )

        await create_group(stream_redis, self.stream_key, consumer_group, start_from)

        # Track active tasks
        active_tasks: set[asyncio.Task[None]] = set()

        try:
            while not signal.is_set():
                # Wait for capacity
                while len(active_tasks) >= concurrency and not signal.is_set():
                    done, active_tasks = await asyncio.wait(
                        active_tasks, return_when=asyncio.FIRST_COMPLETED, timeout=0.1
                    )
                    # Handle any exceptions from completed tasks
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            print(f"Error in processing task: {e}")

                if signal.is_set():
                    break

                entries = await get_entries(
                    redis=stream_redis,
                    stream=self.stream_key,
                    consumer_group=consumer_group,
                    consumer_id=consumer_id,
                    block_for=xclaim_block_timeout,
                    batch_size=batch_size,
                    reclaim_after=reclaim_after,
                    signal=signal,
                )

                if not entries:
                    continue

                queue_items = [await create_queue_item(e) for e in entries]
                queue_items = await self._check_retry(
                    queue_items, should_retry, consumer_group, consumer_id
                )

                if signal.is_set():
                    break

                try:
                    if map_fn:
                        mapped = await map_fn([item.data for item in queue_items])
                        assert len(mapped) == len(
                            queue_items
                        ), "Map function returned incorrect number of items"
                        mapped_queue_items = [
                            QueueItem(
                                id=queue_items[i].id,
                                data=mapped[i],
                                attempts=queue_items[i].attempts,
                                report_progress=queue_items[i].report_progress,
                            )
                            for i in range(len(mapped))
                        ]
                    else:
                        mapped_queue_items = queue_items

                    telemetry.record_batch_size(len(mapped_queue_items))

                    # Create tasks for each item
                    for item in mapped_queue_items:
                        task = asyncio.create_task(
                            self._process_item(
                                item,
                                callback,
                                telemetry,
                                event_stream,
                                consumer_group,
                                heartbeat_manager,
                                should_retry,
                            )
                        )
                        active_tasks.add(task)

                except Exception as error:
                    print(f"Error processing entries: {error}")

        finally:
            # Wait for all active tasks to complete
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

            await heartbeat_manager.stop()
            await stream_redis.close()

    async def _process_item(
        self,
        item: QueueItem[Any],
        callback: Callable[[QueueItem[Any]], Awaitable[None]],
        telemetry: RedisStreamTelemetry,
        event_stream: "Stream[StreamEvent] | None",
        consumer_group: str,
        heartbeat_manager: HeartbeatManager,
        should_retry: RetryFunction,
    ) -> None:
        """Process a single item."""
        timer = telemetry.start_timer()
        try:
            if event_stream:
                await event_stream.add({"id": item.id, "type": "started"})

            await callback(item)

            telemetry.record_messages_processed(1, {"status": "success"})
            duration = timer()
            telemetry.record_processing_duration(duration)
            await self.redis.xack(self.stream_key, consumer_group, item.id)  # type: ignore[no-untyped-call]

            if event_stream:
                await event_stream.add({"id": item.id, "type": "completed"})

        except Exception as e:
            if event_stream:
                await event_stream.add(
                    {"id": item.id, "type": "failed", "error": str(e)}
                )
            telemetry.record_messages_processed(1, {"status": "error"})

            # When processing fails, simply don't acknowledge the message
            # Redis will track attempts through its internal pending system
            # The message will be reclaimed later and _check_retry will handle should_retry logic

        finally:
            heartbeat_manager.remove(item.id)
