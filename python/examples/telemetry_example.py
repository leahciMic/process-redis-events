"""Example with custom telemetry configuration."""

import asyncio
from typing import TypedDict

from redis.asyncio import Redis

from process_redis_events import QueueItem, StartFrom, Stream
from process_redis_events.telemetry import TelemetryConfig


class MetricsData(TypedDict):
    """Data with metrics."""

    metric_name: str
    value: float


async def main() -> None:
    """Example with telemetry enabled."""
    # Configure telemetry
    telemetry_config = TelemetryConfig(
        enabled=True,
        stream_name="metrics-stream",
        consumer_group="metrics-processor",
        consumer_id="worker-1",
        meter_name="my-app-metrics",
        tracer_name="my-app-tracer",
        version="1.0.0",
    )

    stream = Stream[MetricsData](
        name="metrics-stream",
        create_redis=lambda: Redis.from_url("redis://localhost:6379"),
        produce_events=False,
        telemetry_config=telemetry_config,
    )

    # Add metrics
    print("Adding metrics...")
    for i in range(10):
        await stream.add({"metric_name": f"metric_{i}", "value": float(i * 10)})

    processed_count = 0

    async def process_metric(item: QueueItem[MetricsData]) -> None:
        """Process a metric."""
        nonlocal processed_count
        processed_count += 1

        print(f"Processing {item.data['metric_name']}: {item.data['value']}")

        # Simulate work
        await asyncio.sleep(0.1)

    shutdown_event = asyncio.Event()

    print("\nProcessing with telemetry enabled...\n")
    process_task = asyncio.create_task(
        stream.process(
            options={
                "consumer_group": "metrics-processor",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 5,
                "concurrency": 3,
                "lease_ms": 10000,
            },
            callback=process_metric,
        )
    )

    # Wait for processing
    while processed_count < 10:
        await asyncio.sleep(0.1)

    print(f"\n✓ Processed {processed_count} metrics")
    print("\nNote: OpenTelemetry metrics are being recorded.")
    print("Configure an exporter to send metrics to your backend.")

    # Shutdown
    shutdown_event.set()
    await process_task

    # Cleanup
    await stream.clear()
    await stream.redis.aclose()

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
