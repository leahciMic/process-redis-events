# Process Redis Events - Python

A Python library for processing Redis Stream events with consumer groups, providing robust event processing with automatic retries, dead letter queues, and OpenTelemetry integration.

## Features

- **Type-Safe**: Full type hints and mypy compatibility
- **Consumer Groups**: Built-in support for Redis consumer groups
- **Automatic Retries**: Configurable retry logic with dead letter queue
- **Heartbeat Management**: Automatic lease extension for long-running tasks
- **Telemetry**: OpenTelemetry integration for metrics and tracing
- **Event Streaming**: Optional event stream for progress tracking
- **Async/Await**: Built on asyncio for high performance

## Installation

```bash
pip install process-redis-events
```

For development:

```bash
pip install process-redis-events[dev]
```

## Quick Start

```python
import asyncio
from redis.asyncio import Redis
from process_redis_events import Stream, StartFrom

# Define your data type
from typing import TypedDict

class MyData(TypedDict):
    foo: str

# Create a stream
async def main():
    stream = Stream[MyData](
        name="my-stream",
        create_redis=lambda: Redis.from_url("redis://localhost"),
        produce_events=True
    )

    # Add data to the stream
    await stream.add({"foo": "bar"})

    # Process events
    async def process_item(item):
        print(f"Processing: {item.data}")
        # Your processing logic here

    await stream.process(
        options={
            "consumer_group": "my-group",
            "signal": asyncio.Event(),  # Use for graceful shutdown
        },
        callback=process_item
    )

asyncio.run(main())
```

## Advanced Usage

### With Data Transformation

```python
from process_redis_events import QueueItem

async def transform(data_list: list[MyData]) -> list[dict]:
    return [{"bar": item["foo"]} for item in data_list]

async def process_item(item: QueueItem[dict]):
    print(f"Transformed data: {item.data}")
    await item.report_progress(0.5, "Halfway done")
    await item.report_progress(1.0, "Complete")

await stream.process(
    options={
        "consumer_group": "my-group",
        "map": transform,
        "signal": shutdown_event,
        "batch_size": 20,
        "concurrency": 10,
        "lease_ms": 30000,
    },
    callback=process_item
)
```

### Error Handling and Retries

```python
def should_retry(attempt: int, data: MyData) -> bool:
    # Retry up to 3 times
    return attempt < 3

await stream.process(
    options={
        "consumer_group": "my-group",
        "should_retry": should_retry,
        "signal": shutdown_event,
    },
    callback=process_item
)
```

## API Reference

### Stream[T]

Main class for processing Redis streams.

**Constructor Parameters:**

- `name`: Stream name
- `create_redis`: Factory function returning Redis client
- `produce_events`: Enable event stream (default: False)
- `telemetry_config`: Optional telemetry configuration

**Methods:**

- `add(data: T) -> dict`: Add item to stream
- `process(options, callback)`: Process stream items
- `clear()`: Clear stream data
- `get_stream_info()`: Get stream statistics
- `get_consumer_groups_info()`: Get consumer group info
- `create_event_stream()`: Create event tracking stream

### QueueItem[T]

Represents a queue item being processed.

**Attributes:**

- `id`: Unique message ID
- `data`: Message data (type T)
- `attempts`: Number of processing attempts
- `report_progress`: Async function to report progress

### StartFrom

Enum for consumer group starting position:

- `StartFrom.OLDEST`: Start from beginning
- `StartFrom.LATEST`: Start from latest

## Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=process_redis_events

# Type checking
mypy process_redis_events
```

## License

ISC License - see LICENSE file for details

## Contributing

Contributions welcome! Please ensure:

- All tests pass
- Code is type-checked with mypy
- Code is formatted with black
- Follow existing patterns and conventions
