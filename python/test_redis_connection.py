"""Quick test to verify Redis connectivity."""

import asyncio

from redis.asyncio import Redis


async def test_redis_connection():
    """Test if we can connect to Redis."""
    print("Testing Redis connection...")

    try:
        redis = Redis.from_url("redis://localhost:6379")

        # Try to ping
        response = await redis.ping()
        print(f"✓ Redis is accessible: PING returned {response}")

        # Try to set and get a value
        await redis.set("test_key", "test_value")
        value = await redis.get("test_key")
        print(f"✓ Read/write works: {value.decode() if value else None}")

        # Clean up
        await redis.delete("test_key")
        await redis.close()

        print("\n✓ Redis is ready for testing!")
        return True

    except Exception as e:
        print(f"\n✗ Redis connection failed: {e}")
        print("\nMake sure Redis is running:")
        print("  docker ps | grep redis")
        print("\nOr start it with:")
        print("  docker run -d --name redis -p 6379:6379 redis:latest")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_redis_connection())
    exit(0 if success else 1)
