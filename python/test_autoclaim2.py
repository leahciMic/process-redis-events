"""Test xautoclaim without justid with idle messages."""
import asyncio
from redis.asyncio import Redis

async def main():
    redis = Redis(host="localhost", port=6379, decode_responses=False)
    
    # Setup
    stream_name = "test-autoclaim-stream2"
    await redis.delete(stream_name)
    
    # Add a message
    msg_id = await redis.xadd(stream_name, {"data": "test message"})
    print(f"Added message: {msg_id}")
    
    # Create consumer group
    await redis.xgroup_create(stream_name, "test-group", id="0", mkstream=True)
    
    # Read message (claim it)
    messages = await redis.xreadgroup("test-group", "consumer1", {stream_name: ">"}, count=1)
    print(f"\nConsumer1 read: {messages}")
    
    # Wait a bit
    await asyncio.sleep(0.1)
    
    # Try autoclaim WITHOUT justid (should reclaim idle message)
    result_full = await redis.xautoclaim(stream_name, "test-group", "consumer2", 0, "0-0", count=10)
    print(f"\nAutoclaim result: {result_full}")
    print(f"Type: {type(result_full)}")
    if len(result_full) >= 2:
        print(f"Messages claimed: {result_full[1]}")
    
    # Cleanup
    await redis.delete(stream_name)
    await redis.aclose()

if __name__ == "__main__":
    asyncio.run(main())
