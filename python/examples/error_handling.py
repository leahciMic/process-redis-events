"""Example showing retry logic and dead letter queue handling."""

import asyncio
from typing import TypedDict

from redis.asyncio import Redis

from process_redis_events import QueueItem, StartFrom, Stream


class JobData(TypedDict):
    """Job data structure."""

    job_id: str
    action: str
    will_fail: bool


async def main() -> None:
    """Main example showing error handling."""
    stream = Stream[JobData](
        name="jobs-with-retries",
        create_redis=lambda: Redis.from_url("redis://localhost:6379"),
        produce_events=False,
    )

    # Add jobs - some will fail
    print("Adding jobs...")
    await stream.add({"job_id": "J1", "action": "process", "will_fail": False})
    await stream.add({"job_id": "J2", "action": "process", "will_fail": True})
    await stream.add({"job_id": "J3", "action": "process", "will_fail": False})

    attempt_counts: dict[str, int] = {}

    async def process_job(item: QueueItem[JobData]) -> None:
        """Process a job with potential failures."""
        job_id = item.data["job_id"]

        # Track attempts
        if job_id not in attempt_counts:
            attempt_counts[job_id] = 0
        attempt_counts[job_id] += 1

        print(
            f"Processing {job_id} (attempt {item.attempts + 1}/"
            f"total calls {attempt_counts[job_id]})"
        )

        if item.data["will_fail"] and item.attempts < 2:
            # Fail on first two attempts
            raise Exception(f"Job {job_id} failed on attempt {item.attempts + 1}")

        # Success
        print(f"  ✓ {job_id} succeeded!")

    def should_retry(attempt: int, data: JobData) -> bool:
        """Retry up to 3 times."""
        max_retries = 3
        will_retry = attempt < max_retries

        if not will_retry:
            print(f"  ✗ Job {data['job_id']} exceeded retry limit, moving to DLQ")

        return will_retry

    shutdown_event = asyncio.Event()

    print("\nStarting job processing...\n")
    process_task = asyncio.create_task(
        stream.process(
            options={
                "consumer_group": "job-processor",
                "start_from": StartFrom.OLDEST,
                "signal": shutdown_event,
                "batch_size": 5,
                "concurrency": 1,
                "lease_ms": 5000,
                "should_retry": should_retry,
            },
            callback=process_job,
        )
    )

    # Let it process
    await asyncio.sleep(2)

    # Check results
    print("\n" + "=" * 50)
    print("Processing Summary:")
    print("=" * 50)
    for job_id, count in sorted(attempt_counts.items()):
        print(f"{job_id}: {count} total attempts")

    # Check DLQ
    dlq_key = f"stream:dlq:{stream.name}"
    dlq_length = await stream.redis.xlen(dlq_key)
    print(f"\nDead Letter Queue length: {dlq_length}")

    if dlq_length > 0:
        print("\nMessages in DLQ:")
        messages = await stream.redis.xrange(dlq_key, count=10)
        for msg_id, fields in messages:
            data = {k.decode(): v.decode() for k, v in fields.items()}
            print(f"  - Original ID: {data.get('originalId', 'N/A')}")
            print(f"    Attempts: {data.get('attempts', 'N/A')}")
            print(f"    Data: {data.get('data', 'N/A')}")

    # Shutdown
    print("\nShutting down...")
    shutdown_event.set()
    await process_task

    # Cleanup
    await stream.clear()
    await stream.redis.delete(dlq_key)
    await stream.redis.aclose()

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
