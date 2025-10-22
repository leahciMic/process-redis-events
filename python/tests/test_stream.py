"""Tests for the Stream class."""

import asyncio
from typing import Any, TypedDict
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis

from process_redis_events import QueueItem, StartFrom, Stream, StreamEvent


class MyData(TypedDict):
    """Test data type."""

    foo: str


@pytest.fixture
async def redis_client() -> Redis:
    """Create a Redis client for testing."""
    client = Redis.from_url("redis://localhost")
    yield client
    await client.aclose()


@pytest.fixture
async def stream(redis_client: Redis) -> Stream[MyData]:
    """Create a test stream."""
    stream = Stream[MyData](
        name=f"test-stream-{asyncio.get_event_loop().time()}",
        create_redis=lambda: Redis.from_url("redis://localhost"),
        produce_events=True,
    )
    yield stream
    await stream.clear()
    event_stream = stream.create_event_stream()
    await event_stream.clear()
    await stream.redis.aclose()


@pytest.fixture
def shutdown_event() -> asyncio.Event:
    """Create a shutdown event."""
    return asyncio.Event()


@pytest.fixture
def mock_callback() -> AsyncMock:
    """Create a mock callback function."""
    return AsyncMock()


@pytest.fixture
def should_retry() -> MagicMock:
    """Create a mock should_retry function."""
    return MagicMock(return_value=True)


@pytest.fixture
def map_fn() -> AsyncMock:
    """Create a map function for transforming data."""

    async def _map(data: list[MyData]) -> list[dict[str, str]]:
        return [{"bar": x["foo"]} for x in data]

    return AsyncMock(side_effect=_map)


class TestStream:
    """Test suite for Stream class."""

    @pytest.mark.asyncio
    async def test_can_process_events(
        self,
        stream: Stream[MyData],
        mock_callback: AsyncMock,
        shutdown_event: asyncio.Event,
        should_retry: MagicMock,
        map_fn: AsyncMock,
    ) -> None:
        """Test that events can be processed."""
        # Start processing in background
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "map": map_fn,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                    "should_retry": should_retry,
                },
                callback=mock_callback,
            )
        )

        # Add data
        await stream.add({"foo": "bar"})

        # Wait for processing
        await asyncio.sleep(0.5)

        # Verify callback was called
        assert mock_callback.call_count >= 1
        call_args = mock_callback.call_args[0][0]
        assert isinstance(call_args, QueueItem)
        assert call_args.data["foo"] == "bar"
        assert call_args.attempts == 0
        assert callable(call_args.report_progress)

        # Cleanup
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_retries_failed_events(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that failed events are retried."""
        call_count = 0
        expected_data = {"foo": "baz"}

        async def callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Processing failed")

        # Start processing
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=callback,
            )
        )

        await stream.add(expected_data)

        # Wait for retries
        for _ in range(50):  # 5 seconds max
            if call_count >= 2:
                break
            await asyncio.sleep(0.1)

        assert call_count >= 2

        # Cleanup
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_retries_infinitely(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that events retry infinitely when should_retry always returns True."""
        call_count = 0

        async def callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            raise Exception("Processing failed")

        # Start processing
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=callback,
            )
        )

        await stream.add({"foo": "baz"})

        # Wait for multiple retries
        for _ in range(100):  # 10 seconds max
            if call_count >= 5:
                break
            await asyncio.sleep(0.1)

        assert call_count >= 5

        # Verify attempts increased
        # Note: Can't easily verify attempts without instrumenting callback

        # Cleanup
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_emits_progress_events(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that progress events are emitted."""
        events: list[Any] = []

        async def main_callback(event: QueueItem[MyData]) -> None:
            await event.report_progress(0.1, "Starting processing")
            await event.report_progress(0.5, "Halfway there")
            await event.report_progress(1.0, "Completed")

        async def event_callback(entry: QueueItem[StreamEvent]) -> None:
            events.append(entry.data)

        event_stream = stream.create_event_stream()

        # Start event processing
        event_task = asyncio.create_task(
            event_stream.process(
                options={
                    "consumer_group": "event-group",
                    "start_from": StartFrom.OLDEST,
                    "signal": shutdown_event,
                },
                callback=event_callback,
            )
        )

        # Start main processing
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=main_callback,
            )
        )

        result = await stream.add({"foo": "with-progress"})
        message_id = result["id"]

        # Wait for events
        for _ in range(100):  # 10 seconds max
            if len(events) >= 5:
                break
            await asyncio.sleep(0.1)

        assert len(events) >= 5

        # Verify event sequence
        assert events[0]["type"] == "started"
        assert events[0]["id"] == message_id

        # Find progress events
        progress_events = [e for e in events if e["type"] == "progress"]
        assert len(progress_events) >= 3

        # Verify completion
        completed_events = [e for e in events if e["type"] == "completed"]
        assert len(completed_events) >= 1

        # Cleanup
        shutdown_event.set()
        await asyncio.gather(process_task, event_task)

    @pytest.mark.asyncio
    @pytest.mark.timeout(15)
    async def test_emits_failed_events(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that failed events are emitted."""
        events: list[Any] = []

        async def main_callback(event: QueueItem[MyData]) -> None:
            raise Exception("Something went wrong")

        async def event_callback(entry: QueueItem[StreamEvent]) -> None:
            events.append(entry.data)

        event_stream = stream.create_event_stream()

        # Start event processing
        event_task = asyncio.create_task(
            event_stream.process(
                options={
                    "consumer_group": "event-group",
                    "start_from": StartFrom.OLDEST,
                    "signal": shutdown_event,
                },
                callback=event_callback,
            )
        )

        # Start main processing
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=main_callback,
            )
        )

        result = await stream.add({"foo": "with-error"})
        message_id = result["id"]

        # Wait for events
        for _ in range(50):  # 5 seconds max
            if len(events) >= 2:
                break
            await asyncio.sleep(0.1)

        assert len(events) >= 2
        assert events[0]["type"] == "started"
        assert events[0]["id"] == message_id

        failed_event = next(e for e in events if e["type"] == "failed")
        assert failed_event["id"] == message_id
        assert failed_event["error"] == "Something went wrong"

        # Cleanup
        shutdown_event.set()
        await asyncio.gather(process_task, event_task)

    @pytest.mark.asyncio
    @pytest.mark.timeout(5)
    async def test_auto_extended_leases(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that leases are automatically extended."""
        processing_started = asyncio.Event()
        processing_continue = asyncio.Event()
        call_count = 0

        async def callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            processing_started.set()
            await processing_continue.wait()

        # Start processing
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 100,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=callback,
            )
        )

        await stream.add({"foo": "one"})

        # Wait for processing to start
        await asyncio.wait_for(processing_started.wait(), timeout=2.0)

        # Wait to ensure heartbeats happen
        await asyncio.sleep(0.5)

        # Release processing
        processing_continue.set()

        # Wait a bit more
        await asyncio.sleep(0.2)

        # Should only be called once (not reprocessed)
        assert call_count == 1

        # Cleanup
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    async def test_get_stream_info(
        self,
        stream: Stream[MyData],
    ) -> None:
        """Test getting stream information."""
        await stream.add({"foo": "one"})
        await stream.add({"foo": "two"})

        info = await stream.get_stream_info()
        assert info is not None
        assert info.length == 2
        assert info.entries_added == 2

    @pytest.mark.asyncio
    async def test_get_consumer_groups_info(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test getting consumer group information."""
        processing_continue = asyncio.Event()

        async def callback(item: QueueItem[MyData]) -> None:
            await processing_continue.wait()

        # Start processing to create group
        process_task = asyncio.create_task(
            stream.process(
                options={
                    "consumer_group": "test-group",
                    "start_from": StartFrom.OLDEST,
                    "lease_ms": 10,
                    "signal": shutdown_event,
                    "batch_size": 1,
                    "concurrency": 1,
                },
                callback=callback,
            )
        )

        await stream.add({"foo": "one"})
        await stream.add({"foo": "two"})

        # Wait for messages to be claimed
        await asyncio.sleep(0.3)

        groups = await stream.get_consumer_groups_info()
        assert len(groups) >= 1
        test_group = next(g for g in groups if g.name == "test-group")
        assert test_group.consumers >= 1
        assert test_group.lag >= 0

        # Complete processing
        processing_continue.set()
        await asyncio.sleep(0.5)

        # Check again after processing
        groups = await stream.get_consumer_groups_info()
        test_group = next(g for g in groups if g.name == "test-group")
        assert test_group.pending == 0
        assert test_group.entries_read >= 2

        # Cleanup
        shutdown_event.set()
        await process_task
