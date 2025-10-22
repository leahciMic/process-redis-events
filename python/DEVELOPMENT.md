# Development Guide

## Setup

1. **Create a virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   make install-dev
   ```

## Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test file
pytest tests/test_stream.py -v

# Run specific test
pytest tests/test_stream.py::TestStream::test_can_process_events -v
```

## Type Checking

```bash
make typecheck
```

## Linting and Formatting

```bash
# Check code style
make lint

# Auto-format code
make format
```

## Project Structure

```
process_redis_events/
├── __init__.py           # Package exports
├── stream.py             # Main Stream class
├── constants.py          # Enums and constants
├── queue_item.py         # QueueItem type
├── stream_event.py       # StreamEvent types
├── telemetry.py          # OpenTelemetry integration
├── types.py              # Type aliases
├── chunk.py              # Utility functions
├── create_group.py       # Consumer group creation
├── get_entries.py        # Stream entry retrieval
├── heartbeat_manager.py  # Heartbeat management
├── parse_json.py         # JSON parsing
└── result_of.py          # Error handling utility

tests/
├── __init__.py
└── test_stream.py        # Main test suite

examples/
└── basic_usage.py        # Usage examples
```

## Key Design Decisions

### Type Safety

The library uses Python 3.10+ type hints extensively:

- Generic types (`Stream[T]`, `QueueItem[T]`)
- TypedDict for structured data
- Proper async/await typing
- Full mypy strict mode compliance

### Async/Await

All I/O operations are async:

- Uses `redis.asyncio` for async Redis operations
- `asyncio.Event` for signaling
- `asyncio.Semaphore` for concurrency control
- Proper cleanup with context managers

### Error Handling

- `result_of` utility for safe error handling (inspired by Rust)
- Exceptions are caught and logged
- Failed messages can be retried or moved to DLQ

### Differences from TypeScript Version

1. **Event Signaling**: Uses `asyncio.Event` instead of `AbortSignal`
2. **Concurrency**: Uses `asyncio.Semaphore` and task tracking instead of `p-queue`
3. **ID Generation**: Uses `uuid4` instead of `nanoid`
4. **Memoization**: Uses `@lru_cache` instead of `memoize` package
5. **Type System**: Python's type system is different but equally expressive
6. **Redis Client**: Uses `redis-py` async client instead of `ioredis`

## Testing Strategy

Tests cover:

- ✓ Basic event processing
- ✓ Retry logic
- ✓ Infinite retries
- ✓ Progress event emission
- ✓ Failed event emission
- ✓ Automatic lease extension
- ✓ Stream information retrieval
- ✓ Consumer group information

## Performance Considerations

- Heartbeat interval: 15 seconds (configurable)
- Default batch size: 20 messages
- Default concurrency: 10 workers
- Default lease time: 30 seconds

## Contributing

1. Write tests for new features
2. Ensure all tests pass
3. Run type checking
4. Format code with black
5. Update documentation
