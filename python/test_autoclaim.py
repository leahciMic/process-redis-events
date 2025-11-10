"""Test xautoclaim behavior."""
import asyncio
from redis.asyncio import Redis

async def main():
    redis = Redis(host="localhost", port=6379, decode_responses=False)
    
    # Setup
    stream_name = "test-autoclaim-stream"
    await redis.delete(stream_name)
    
    # Add a message
    msg_id = await redis.xadd(stream_name, {"data": "test message"})
    print(f"Added message: {msg_id}")
    
    # Create consumer group
    await redis.xgroup_create(stream_name, "test-group", id="0", mkstream=True)
    
    # Read message (claim it)
    messages = await redis.xreadgroup("test-group", "consumer1", {stream_name: ">"}, count=1)
    print(f"\nRead messages: {messages}")
    
    # Now try autoclaim with JUSTID
    result_justid = await redis.xautoclaim(stream_name, "test-group", "consumer2", 0, "0-0", count=10, justid=True)
    print(f"\nAutoclaim with JUSTID: {result_justid}")
    print(f"Type: {type(result_justid)}, Length: {len(result_justid)}")
    
    # Reset
    await redis.xack(stream_name, "test-group", msg_id)
    await redis.xreadgroup("test-group", "consumer1", {stream_name: ">"}, count=1)
    
    # Try autoclaim WITHOUT justid
    result_full = await redis.xautoclaim(stream_name, "test-group", "consumer3", 0, "0-0", count=10)
    print(f"\nAutoclaim without JUSTID: {result_full}")
    print(f"Type: {type(result_full)}, Length: {len(result_full)}")
    
    # Cleanup
    await redis.delete(stream_name)
    await redis.aclose()

if __name__ == "__main__":
    asyncio.run(main())
