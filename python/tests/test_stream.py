"""Tests for the Stream class."""

import asyncio
from typing import Any, TypedDict
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.asyncio import Redis

from process_redis_events import (
    QueueItem,
    StartFrom,
    Stream,
    StreamEvent,
    create_process_options,
)


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
    """Create a map function for transforming data (identity function)."""

    async def _map(data: list[MyData]) -> list[MyData]:
        return data

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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    map_fn=map_fn,
                    batch_size=1,
                    concurrency=1,
                    should_retry=should_retry,
                ),
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
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
    @pytest.mark.timeout(60)  # Event stream tests may need more time
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="event-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=1000,  # Shorter lease for faster event processing
                ),
                callback=event_callback,
            )
        )

        # Start main processing
        process_task = asyncio.create_task(
            stream.process(
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
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
    @pytest.mark.timeout(60)  # Progress event tests may need more time
    async def test_report_progress_emits_correct_progress_events(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that job.report_progress works and emits progress events with correct data."""
        events: list[Any] = []
        progress_calls = [
            (0.0, "Initializing"),
            (0.25, "Processing data"),
            (0.75, "Finalizing results"),
            (1.0, "Complete"),
        ]

        async def callback_with_progress(job: QueueItem[MyData]) -> None:
            for progress, message in progress_calls:
                await job.report_progress(progress, message)

        async def event_callback(entry: QueueItem[StreamEvent]) -> None:
            events.append(entry.data)

        event_stream = stream.create_event_stream()

        # Start event processing
        event_task = asyncio.create_task(
            event_stream.process(
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="event-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=1000,
                ),
                callback=event_callback,
            )
        )

        # Start main processing
        process_task = asyncio.create_task(
            stream.process(
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
                callback=callback_with_progress,
            )
        )

        result = await stream.add({"foo": "progress-test"})
        message_id = result["id"]

        # Wait for all events (started + 4 progress + completed)
        for _ in range(100):  # 10 seconds max
            if len(events) >= 6:
                break
            await asyncio.sleep(0.1)

        # Filter events for our message
        message_events = [e for e in events if e.get("id") == message_id]
        progress_events = [e for e in message_events if e["type"] == "progress"]

        # Verify we have the correct number of progress events
        assert (
            len(progress_events) == 4
        ), f"Expected 4 progress events, got {len(progress_events)}"

        # Verify progress event content
        expected_completion_ratios = [0.0, 0.25, 0.75, 1.0]
        expected_statuses = [
            "Initializing",
            "Processing data",
            "Finalizing results",
            "Complete",
        ]

        for i, event in enumerate(progress_events):
            assert (
                event["type"] == "progress"
            ), f"Progress event {i}: expected type 'progress', got '{event['type']}'"
            assert (
                event["id"] == message_id
            ), f"Progress event {i}: expected id '{message_id}', got '{event['id']}'"
            assert (
                event["completionRatio"] == expected_completion_ratios[i]
            ), f"Progress event {i}: expected completionRatio {expected_completion_ratios[i]}, got {event['completionRatio']}"
            assert (
                event["status"] == expected_statuses[i]
            ), f"Progress event {i}: expected status '{expected_statuses[i]}', got '{event['status']}'"

        # Verify completed event exists
        completed_events = [e for e in message_events if e["type"] == "completed"]
        assert len(completed_events) >= 1, "Should have at least one completed event"

        # Cleanup
        shutdown_event.set()
        await asyncio.gather(process_task, event_task)

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)  # Event stream tests may need more time
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="event-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=1000,  # Shorter lease for faster event processing
                ),
                callback=event_callback,
            )
        )

        # Start main processing
        process_task = asyncio.create_task(
            stream.process(
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=100,
                    batch_size=1,
                    concurrency=1,
                ),
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
                options=create_process_options(
                    signal=shutdown_event,
                    consumer_group="test-group",
                    start_from=StartFrom.OLDEST,
                    lease_ms=10,
                    batch_size=1,
                    concurrency=1,
                ),
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

    @pytest.mark.asyncio
    async def test_should_retry_false_prevents_reprocessing(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that should_retry=False prevents items from being reprocessed.

        This test should expose a bug where should_retry is not being called properly.
        """
        call_count = 0
        retry_call_count = 0

        def should_retry_false(attempt: int, entry: Any) -> bool:
            """Always return False - should never retry."""
            nonlocal retry_call_count
            retry_call_count += 1
            return False

        async def failing_callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            # Always fail on first attempt
            if call_count == 1:
                raise ValueError("Simulated failure")

        # Create options with should_retry=False and short lease
        options = create_process_options(
            signal=shutdown_event,
            consumer_group="test-group",
            batch_size=1,
            lease_ms=50,  # Very short lease for quick test
            concurrency=1,
            should_retry=should_retry_false,
        )

        # Start processing first to create the consumer group
        process_task = asyncio.create_task(stream.process(options, failing_callback))

        # Give time for consumer group creation
        await asyncio.sleep(0.1)

        # Add test data after processing has started
        await stream.add({"foo": "test"})

        # Wait for initial processing, failure, lease expiry, and reclaim
        await asyncio.sleep(0.5)

        # should_retry should have been called when deciding whether to retry
        assert retry_call_count > 0, "should_retry function should have been called"

        # The item should only be processed once (initial attempt)
        # because should_retry returns False
        assert (
            call_count == 1
        ), f"Expected 1 call but got {call_count} - should_retry not working"

        # Stop processing
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    async def test_should_retry_receives_correct_attempt_count(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that should_retry receives the correct attempt count from Redis.

        This test verifies that Redis properly tracks delivery attempts and
        passes the correct count to should_retry when reclaiming messages.
        """
        call_count = 0
        retry_attempts: list[int] = []

        def capture_retry_attempts(attempt: int, entry: Any) -> bool:
            """Capture the attempt counts and allow retries for first few attempts."""
            retry_attempts.append(attempt)
            return attempt <= 3  # Allow up to 3 retries

        async def failing_callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            # Always fail to trigger retries via Redis reclaim mechanism
            raise ValueError(f"Simulated failure #{call_count}")

        # Create options with retry tracking and very short lease for fast reclaim
        options = create_process_options(
            signal=shutdown_event,
            consumer_group="test-group",
            batch_size=1,
            lease_ms=50,  # Very short lease for quick reclaim
            concurrency=1,
            should_retry=capture_retry_attempts,
        )

        # Start processing
        process_task = asyncio.create_task(stream.process(options, failing_callback))

        # Give time for consumer group creation
        await asyncio.sleep(0.1)

        # Add test data
        await stream.add({"foo": "test-retry-attempts"})

        # Wait longer for Redis to reclaim and retry multiple times
        await asyncio.sleep(1.5)  # Allow time for multiple reclaim cycles

        # should_retry should have been called when Redis reclaims messages with attempts > 0
        assert (
            len(retry_attempts) >= 1
        ), f"Expected at least 1 retry call but got {len(retry_attempts)}"

        # Verify attempt counts start from 1 (Redis tracks delivery count)
        if len(retry_attempts) >= 1:
            assert (
                retry_attempts[0] >= 1
            ), f"First reclaimed attempt should be >= 1 but got {retry_attempts[0]}"

        if len(retry_attempts) >= 2:
            assert (
                retry_attempts[1] > retry_attempts[0]
            ), f"Attempts should increase: {retry_attempts}"

        # The callback should have been called multiple times due to reclaims
        assert (
            call_count >= 2
        ), f"Expected at least 2 processing attempts but got {call_count}"

        # Stop processing
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    async def test_should_retry_false_moves_to_dlq(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that should_retry=False moves items to dead letter queue."""
        call_count = 0
        retry_call_count = 0

        def should_retry_false(attempt: int, entry: Any) -> bool:
            """Always return False - should move to DLQ."""
            nonlocal retry_call_count
            retry_call_count += 1
            return False

        async def failing_callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fail to trigger retry logic")

        # Create options with should_retry=False and short lease
        options = create_process_options(
            signal=shutdown_event,
            consumer_group="test-group",
            batch_size=1,
            lease_ms=50,  # Short lease for quick reclaim
            concurrency=1,
            should_retry=should_retry_false,
        )

        # Start processing
        process_task = asyncio.create_task(stream.process(options, failing_callback))

        # Give time for consumer group creation
        await asyncio.sleep(0.1)

        # Add test data
        await stream.add({"foo": "should-go-to-dlq"})

        # Wait for processing, failure, reclaim, and DLQ move
        await asyncio.sleep(0.5)

        # Verify should_retry was called
        assert retry_call_count > 0, "should_retry should have been called on reclaim"

        # Check that message was moved to DLQ
        dlq_stream = stream.create_dead_letter_queue_stream()
        dlq_length = await stream.redis.xlen(dlq_stream.stream_key)
        assert dlq_length > 0, "Message should have been moved to DLQ"

        # Verify DLQ content
        dlq_entries = await stream.redis.xread({dlq_stream.stream_key: "0-0"})
        assert len(dlq_entries) > 0, "DLQ should contain entries"

        dlq_data = dlq_entries[0][1][0][1]  # [stream][entries][first_entry][fields]
        assert b"data" in dlq_data, "DLQ entry should have data field"

        # Parse the JSON data
        import json

        data_json = json.loads(dlq_data[b"data"].decode())
        assert "_dlqMetadata" in data_json, "DLQ entry should have _dlqMetadata"
        assert (
            "originalId" in data_json["_dlqMetadata"]
        ), "DLQ metadata should have originalId"
        assert (
            "attempts" in data_json["_dlqMetadata"]
        ), "DLQ metadata should have attempts"
        assert (
            "consumerGroup" in data_json["_dlqMetadata"]
        ), "DLQ metadata should have consumerGroup"
        assert (
            "consumerId" in data_json["_dlqMetadata"]
        ), "DLQ metadata should have consumerId"
        assert (
            "timestamp" in data_json["_dlqMetadata"]
        ), "DLQ metadata should have timestamp"
        assert (
            data_json["foo"] == "should-go-to-dlq"
        ), "Original data should be preserved"

        # Stop processing
        shutdown_event.set()
        await process_task

    @pytest.mark.asyncio
    async def test_should_retry_with_attempt_limits(
        self,
        stream: Stream[MyData],
        shutdown_event: asyncio.Event,
    ) -> None:
        """Test that should_retry receives correct attempt counts and respects limits."""
        call_count = 0
        retry_calls: list[tuple[int, Any]] = []

        def limited_retry(attempt: int, entry: Any) -> bool:
            """Allow up to 2 retry attempts."""
            retry_calls.append((attempt, entry))
            return attempt <= 2

        async def failing_callback(item: QueueItem[MyData]) -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError(f"Failure #{call_count}")

        # Create options with limited retries
        options = create_process_options(
            signal=shutdown_event,
            consumer_group="test-group",
            batch_size=1,
            lease_ms=30,  # Short lease for quick reclaim cycles
            concurrency=1,
            should_retry=limited_retry,
        )

        # Start processing
        process_task = asyncio.create_task(stream.process(options, failing_callback))

        # Give time for consumer group creation
        await asyncio.sleep(0.1)

        # Add test data
        test_data = {"foo": "limited-retries"}
        await stream.add(test_data)

        # Wait for multiple reclaim cycles
        await asyncio.sleep(1.0)  # Allow time for several attempts

        # Verify should_retry was called with increasing attempt counts
        assert (
            len(retry_calls) >= 1
        ), f"should_retry should have been called, got {len(retry_calls)} calls"

        # Check that attempt counts are reasonable (Redis tracks deliveries)
        for i, (attempt, entry) in enumerate(retry_calls):
            assert attempt >= 1, f"Attempt {i} should be >= 1, got {attempt}"
            assert (
                entry == test_data
            ), f"Entry data should match: {entry} vs {test_data}"

        # Eventually should move to DLQ when attempt limit exceeded
        await asyncio.sleep(0.5)  # Extra time for DLQ move
        dlq_length = await stream.redis.xlen(f"stream:dlq:{stream.name}")

        # Should have moved to DLQ after exceeding retry limit
        assert dlq_length > 0, "Message should eventually move to DLQ after retry limit"

        # Stop processing
        shutdown_event.set()
        await process_task
