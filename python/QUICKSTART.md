# Quick Start Guide

## Installation

### 1. Install from source

```bash
cd python
pip install -e .
```

### 2. Install with development dependencies

```bash
cd python
make install-dev
```

### 3. Using pip (once published)

```bash
pip install process-redis-events
```

## Basic Setup

### 1. Start Redis

Using Docker:

```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

Or use your existing Redis instance.

### 2. Create a simple processor

Create `my_processor.py`:

```python
import asyncio
from typing import TypedDict
from redis.asyncio import Redis
from process_redis_events import Stream, StartFrom, QueueItem

class Task(TypedDict):
    task_id: str
    description: str

async def main():
    # Create stream
    stream = Stream[Task](
        name="tasks",
        create_redis=lambda: Redis.from_url("redis://localhost:6379"),
    )

    # Add a task
    await stream.add({"task_id": "T1", "description": "Process invoice"})

    # Process tasks
    async def process_task(item: QueueItem[Task]):
        print(f"Processing: {item.data['description']}")
        # Your logic here

    shutdown_event = asyncio.Event()

    # Start processing (in background or separate process)
    await stream.process(
        options={
            "consumer_group": "workers",
            "start_from": StartFrom.OLDEST,
            "signal": shutdown_event,
        },
        callback=process_task,
    )

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Run it

```bash
python my_processor.py
```

## Next Steps

- Check out the [examples](examples/) directory for more advanced usage
- Read the [README.md](README.md) for full API documentation
- See [DEVELOPMENT.md](DEVELOPMENT.md) for contributing guidelines

## Verifying Installation

Run the tests:

```bash
cd python
make test
```

Type check:

```bash
make typecheck
```

## Common Issues

### ModuleNotFoundError

Make sure you're in the right directory and have installed the package:

```bash
cd python
pip install -e .
```

### Redis Connection Error

Verify Redis is running:

```bash
redis-cli ping
# Should return: PONG
```

### Type Errors

Make sure you're using Python 3.10 or higher:

```bash
python --version
# Should show: Python 3.10.x or higher
```

## Support

For issues and questions, please open an issue on GitHub.
