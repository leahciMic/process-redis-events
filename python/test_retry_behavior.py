"""Test actual retry behavior."""

import asyncio
from redis.asyncio import Redis


async def main():
    redis = Redis(host="localhost", port=6379, decode_responses=False)

    stream_name = "test-retry-stream"
    await redis.delete(stream_name)

    # Add a message
    msg_id = await redis.xadd(stream_name, {"data": '{"foo": "bar"}'})
    print(f"Added message: {msg_id}")

    # Create consumer group
    await redis.xgroup_create(stream_name, "test-group", id="0", mkstream=True)

    # First read - simulate successful claim
    print("\n=== First read (consumer1) ===")
    messages1 = await redis.xreadgroup(
        "test-group", "consumer1", {stream_name: ">"}, count=1
    )
    print(f"Read: {messages1}")

    # Don't ack - simulating a failure
    print("\n=== Simulating failure (no xack) ===")
    print("Message NOT acknowledged")

    # Wait a bit for idle time
    await asyncio.sleep(0.2)

    # Try to reclaim with autoclaim (min_idle_time=100ms)
    print("\n=== Attempting autoclaim (consumer2, min_idle=100ms) ===")
    claimed = await redis.xautoclaim(
        stream_name, "test-group", "consumer2", 100, "0-0", count=10, justid=True
    )
    print(f"Autoclaim result: {claimed}")
    claimed_ids = claimed[1] if len(claimed) > 1 else claimed
    print(f"Claimed IDs: {claimed_ids}")

    if claimed_ids:
        # Now try to read with cursor 0-0
        print("\n=== Reading with cursor 0-0 ===")
        messages2 = await redis.xreadgroup(
            "test-group", "consumer2", {stream_name: "0-0"}, count=10
        )
        print(f"Read result: {messages2}")

        # Also try with >
        print("\n=== Reading with cursor > ===")
        messages3 = await redis.xreadgroup(
            "test-group", "consumer2", {stream_name: ">"}, count=10
        )
        print(f"Read result: {messages3}")

    # Cleanup
    await redis.delete(stream_name)
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
