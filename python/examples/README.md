# Examples

This directory contains example scripts demonstrating various features of the `process-redis-events` library.

## Prerequisites

Make sure Redis is running:

```bash
docker run -d -p 6379:6379 redis:latest
```

Install the library:

```bash
pip install -e ..
```

## Examples

### 1. Basic Usage (`basic_usage.py`)

Demonstrates:

- Creating a stream
- Adding data
- Processing with transformation (map function)
- Progress tracking
- Event streaming
- Getting stream and consumer group information

Run:

```bash
python basic_usage.py
```

### 2. Error Handling (`error_handling.py`)

Demonstrates:

- Retry logic
- Dead letter queue (DLQ)
- Configurable retry limits
- Inspecting failed messages

Run:

```bash
python error_handling.py
```

### 3. Telemetry (`telemetry_example.py`)

Demonstrates:

- OpenTelemetry configuration
- Metrics collection
- Custom telemetry settings

Run:

```bash
python telemetry_example.py
```

## Common Patterns

### Graceful Shutdown

All examples use an `asyncio.Event` for graceful shutdown:

```python
shutdown_event = asyncio.Event()

# Start processing
process_task = asyncio.create_task(
    stream.process(
        options={"signal": shutdown_event, ...},
        callback=process_callback,
    )
)

# Later: signal shutdown
shutdown_event.set()
await process_task
```

### Type Safety

Define your data structures with TypedDict:

```python
from typing import TypedDict

class MyData(TypedDict):
    field1: str
    field2: int

stream = Stream[MyData](...)
```

### Concurrency Control

```python
await stream.process(
    options={
        "concurrency": 10,  # Process up to 10 items concurrently
        "batch_size": 20,   # Fetch up to 20 items per batch
        ...
    },
    callback=process_callback,
)
```

### Data Transformation

```python
async def transform(items: list[InputType]) -> list[OutputType]:
    return [transform_one(item) for item in items]

await stream.process(
    options={"map": transform, ...},
    callback=process_callback,  # Receives OutputType
)
```

## Troubleshooting

### Redis Connection Issues

If you see connection errors, ensure Redis is running:

```bash
redis-cli ping
```

Should return: `PONG`

### Import Errors

Make sure the package is installed:

```bash
pip install -e ..
```

### Test Cleanup

Examples automatically clean up test data. If you need to manually clean:

```python
await stream.clear()
await stream.redis.delete(f"stream:dlq:{stream.name}")
```
