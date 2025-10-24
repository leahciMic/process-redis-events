"""Test xinfo methods to see what they return."""

import asyncio
from redis.asyncio import Redis


async def main():
    redis = Redis(host="localhost", port=6379, decode_responses=False)

    # Create a test stream
    await redis.xadd("test-stream", {"data": "test"})

    # Get stream info
    info = await redis.xinfo_stream("test-stream")
    print(f"Stream info type: {type(info)}")
    print(f"Stream info: {info}")

    # Create a consumer group
    try:
        await redis.xgroup_create("test-stream", "test-group", id="0", mkstream=True)
    except Exception as e:
        print(f"Group already exists: {e}")

    # Get groups info
    groups = await redis.xinfo_groups("test-stream")
    print(f"\nGroups type: {type(groups)}")
    print(f"Groups: {groups}")

    # Cleanup
    await redis.delete("test-stream")
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
