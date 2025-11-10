"""Test if event stream processing works."""

import asyncio
from redis.asyncio import Redis
from process_redis_events import Stream, StartFrom, QueueItem, StreamEvent


async def main():
    redis = Redis.from_url("redis://localhost")

    # Create stream with events enabled
    stream = Stream(
        name="test-event-proc",
        create_redis=lambda: Redis.from_url("redis://localhost"),
        produce_events=True,
    )

    # Clear any existing data
    await stream.clear()
    event_stream = stream.create_event_stream()
    await event_stream.clear()

    print(f"Main stream: {stream.stream_key}")
    print(f"Event stream: {event_stream.stream_key}")

    # Start event processing FIRST
    shutdown_event = asyncio.Event()
    received_events = []

    async def event_callback(item: QueueItem[StreamEvent]):
        print(f"Received event: {item.data}")
        received_events.append(item.data)
        if len(received_events) >= 4:
            shutdown_event.set()

    print("\nStarting event processor...")
    event_task = asyncio.create_task(
        event_stream.process(
            options={
                "consumer_group": "event-group",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 10,
            },
            callback=event_callback,
        )
    )

    # Give event processor time to start
    await asyncio.sleep(0.2)

    # Now process a main message
    processed = []

    async def callback(item: QueueItem):
        print(f"\nProcessing main message: {item.data}")
        processed.append(item)
        await item.report_progress(0.5, "halfway")
        await item.report_progress(1.0, "done")

    print("\nStarting main processor...")
    main_shutdown = asyncio.Event()
    process_task = asyncio.create_task(
        stream.process(
            options={
                "consumer_group": "main-group",
                "start_from": StartFrom.OLDEST,
                "signal": main_shutdown,
                "batch_size": 1,
                "concurrency": 1,
            },
            callback=callback,
        )
    )

    # Give processors time to start
    await asyncio.sleep(0.2)

    # Add a message
    print("\nAdding message...")
    result = await stream.add({"test": "data"})
    print(f"Added: {result}")

    # Wait for events to be processed
    print("\nWaiting for events...")
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=3.0)
        print(f"\nReceived {len(received_events)} events")
    except asyncio.TimeoutError:
        print(f"\nTimeout! Only received {len(received_events)} events")

        # Check what's actually in the event stream
        events_in_stream = await redis.xlen(event_stream.stream_key)
        print(f"Events in stream: {events_in_stream}")

        if events_in_stream > 0:
            raw_events = await redis.xrange(event_stream.stream_key, count=10)
            print("Raw events:")
            for event_id, event_data in raw_events:
                print(f"  {event_id}: {event_data}")

    # Cleanup
    main_shutdown.set()
    await asyncio.sleep(0.2)
    await stream.clear()
    await event_stream.clear()
    await redis.aclose()
    await stream.redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
