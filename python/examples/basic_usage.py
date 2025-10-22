"""Example usage of the process-redis-events library."""

import asyncio
from typing import TypedDict

from redis.asyncio import Redis

from process_redis_events import QueueItem, StartFrom, Stream


class OrderData(TypedDict):
    """Example order data structure."""

    order_id: str
    customer_id: str
    amount: float


class ProcessedOrder(TypedDict):
    """Processed order data structure."""

    order_id: str
    customer_id: str
    amount: float
    status: str
    processed_at: int


async def main() -> None:
    """Main example function."""
    # Create a stream for processing orders
    stream = Stream[OrderData](
        name="orders",
        create_redis=lambda: Redis.from_url("redis://localhost:6379"),
        produce_events=True,
    )

    # Add some orders to the stream
    print("Adding orders to stream...")
    await stream.add({"order_id": "001", "customer_id": "C123", "amount": 99.99})
    await stream.add({"order_id": "002", "customer_id": "C456", "amount": 149.99})
    await stream.add({"order_id": "003", "customer_id": "C789", "amount": 79.99})

    # Transformation function
    async def transform_orders(
        orders: list[OrderData],
    ) -> list[ProcessedOrder]:
        """Transform raw orders into processed orders."""
        import time

        return [
            {
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "amount": order["amount"],
                "status": "processed",
                "processed_at": int(time.time()),
            }
            for order in orders
        ]

    # Processing callback
    async def process_order(item: QueueItem[ProcessedOrder]) -> None:
        """Process a single order."""
        print(f"Processing order {item.data['order_id']}...")

        # Simulate processing steps
        await item.report_progress(0.25, "Validating order")
        await asyncio.sleep(0.1)

        await item.report_progress(0.5, "Processing payment")
        await asyncio.sleep(0.1)

        await item.report_progress(0.75, "Updating inventory")
        await asyncio.sleep(0.1)

        await item.report_progress(1.0, "Order completed")

        print(
            f"✓ Order {item.data['order_id']} completed "
            f"(attempt {item.attempts + 1})"
        )

    # Event tracking callback
    async def track_events(item: QueueItem) -> None:
        """Track processing events."""
        event = item.data
        if event["type"] == "progress":
            print(
                f"  Progress: {event['completion_ratio'] * 100:.0f}% - "
                f"{event['status']}"
            )
        elif event["type"] == "completed":
            print(f"  ✓ Event {event['id'][:8]} completed")
        elif event["type"] == "failed":
            print(f"  ✗ Event {event['id'][:8]} failed: {event['error']}")

    # Retry logic
    def should_retry(attempt: int, data: OrderData) -> bool:
        """Determine if an order should be retried."""
        # Retry up to 3 times
        return attempt < 3

    # Create shutdown event
    shutdown_event = asyncio.Event()

    # Start event stream processing
    event_stream = stream.create_event_stream()
    event_task = asyncio.create_task(
        event_stream.process(
            options={
                "consumer_group": "event-tracker",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 10,
            },
            callback=track_events,
        )
    )

    # Start main processing
    print("\nStarting order processing...")
    process_task = asyncio.create_task(
        stream.process(
            options={
                "consumer_group": "order-processor",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 5,
                "concurrency": 3,
                "lease_ms": 30000,
                "map": transform_orders,
                "should_retry": should_retry,
            },
            callback=process_order,
        )
    )

    # Let it run for a bit
    await asyncio.sleep(3)

    # Get stream info
    print("\nStream information:")
    info = await stream.get_stream_info()
    if info:
        print(f"  Length: {info.length}")
        print(f"  Groups: {info.groups}")
        print(f"  Entries added: {info.entries_added}")

    # Get consumer group info
    print("\nConsumer groups:")
    groups = await stream.get_consumer_groups_info()
    for group in groups:
        print(f"  {group.name}:")
        print(f"    Consumers: {group.consumers}")
        print(f"    Pending: {group.pending}")
        print(f"    Entries read: {group.entries_read}")
        print(f"    Lag: {group.lag}")

    # Shutdown
    print("\nShutting down...")
    shutdown_event.set()
    await asyncio.gather(process_task, event_task)

    # Cleanup
    await stream.clear()
    await event_stream.clear()
    await stream.redis.aclose()

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
