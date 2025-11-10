"""Test if events are being added to the event stream."""

import asyncio
from redis.asyncio import Redis
from process_redis_events import Stream, StartFrom, QueueItem


async def main():
    redis = Redis.from_url("redis://localhost")

    # Create stream with events enabled
    stream = Stream(
        name="test-event-stream",
        create_redis=lambda: Redis.from_url("redis://localhost"),
        produce_events=True,
    )

    # Clear any existing data
    await stream.clear()
    event_stream = stream.create_event_stream()
    await event_stream.clear()

    print(f"Main stream: {stream.stream_key}")
    print(f"Event stream: {event_stream.stream_key}")

    # Add a message
    result = await stream.add({"test": "data"})
    print(f"\nAdded message: {result}")

    # Check what's in the main stream
    main_stream_info = await redis.xlen(stream.stream_key)
    print(f"Main stream length: {main_stream_info}")

    # Process one message
    shutdown_event = asyncio.Event()
    processed = []

    async def callback(item: QueueItem):
        print(f"\nProcessing: {item.data}")
        processed.append(item)
        await item.report_progress(0.5, "halfway")
        await item.report_progress(1.0, "done")
        shutdown_event.set()

    # Start processing
    process_task = asyncio.create_task(
        stream.process(
            options={
                "consumer_group": "test-group",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 1,
                "concurrency": 1,
            },
            callback=callback,
        )
    )

    # Wait for processing
    await asyncio.sleep(1)

    # Check event stream
    event_stream_info = await redis.xlen(event_stream.stream_key)
    print(f"\nEvent stream length: {event_stream_info}")

    # Read events directly
    try:
        events = await redis.xrange(event_stream.stream_key, count=10)
        print(f"Events in stream: {len(events)}")
        for event_id, event_data in events:
            print(f"  {event_id}: {event_data}")
    except Exception as e:
        print(f"Error reading events: {e}")

    # Cleanup
    shutdown_event.set()
    await asyncio.sleep(0.2)
    await stream.clear()
    await event_stream.clear()
    await redis.aclose()
    await stream.redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
