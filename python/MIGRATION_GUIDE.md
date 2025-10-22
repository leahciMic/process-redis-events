# Migration Guide: TypeScript → Python

This guide helps TypeScript users of `process-redis-events` understand the Python port.

## Quick Reference

### Imports

```typescript
// TypeScript
import { Stream, StartFrom, QueueItem, StreamEvent } from 'process-redis-events'
```

```python
# Python
from process_redis_events import Stream, StartFrom, QueueItem, StreamEvent
```

### Creating a Stream

```typescript
// TypeScript
const stream = new Stream<MyData>({
  name: 'my-stream',
  createRedis: () => new Redis(),
  produceEvents: true
})
```

```python
# Python
stream = Stream[MyData](
    name='my-stream',
    create_redis=lambda: Redis.from_url('redis://localhost'),
    produce_events=True
)
```

### Adding Data

```typescript
// TypeScript
await stream.add({ foo: 'bar' })
```

```python
# Python
await stream.add({'foo': 'bar'})
```

### Processing

```typescript
// TypeScript
const abortController = new AbortController()

await stream.process(
  {
    consumerGroup: 'my-group',
    startFrom: StartFrom.Oldest,
    signal: abortController.signal,
    batchSize: 20,
    concurrency: 10,
    leaseMs: 30000,
    map: async (items) => items.map((x) => ({ bar: x.foo })),
    shouldRetry: (attempt, data) => attempt < 3
  },
  async (item) => {
    console.log(item.data)
  }
)
```

```python
# Python
shutdown_event = asyncio.Event()

async def map_fn(items: list[MyData]) -> list[Transformed]:
    return [{'bar': x['foo']} for x in items]

async def callback(item: QueueItem[Transformed]) -> None:
    print(item.data)

await stream.process(
    options={
        'consumer_group': 'my-group',
        'start_from': StartFrom.OLDEST,
        'signal': shutdown_event,
        'batch_size': 20,
        'concurrency': 10,
        'lease_ms': 30000,
        'map': map_fn,
        'should_retry': lambda attempt, data: attempt < 3
    },
    callback=callback
)
```

## Key Differences

### 1. Naming Conventions

| TypeScript (camelCase) | Python (snake_case) |
| ---------------------- | ------------------- |
| `consumerGroup`        | `consumer_group`    |
| `consumerId`           | `consumer_id`       |
| `batchSize`            | `batch_size`        |
| `startFrom`            | `start_from`        |
| `shouldRetry`          | `should_retry`      |
| `leaseMs`              | `lease_ms`          |
| `createRedis`          | `create_redis`      |
| `produceEvents`        | `produce_events`    |
| `reportProgress`       | `report_progress`   |

### 2. Signaling

```typescript
// TypeScript
const controller = new AbortController()
signal: controller.signal
controller.abort()
```

```python
# Python
shutdown_event = asyncio.Event()
signal: shutdown_event
shutdown_event.set()
```

### 3. Type Annotations

```typescript
// TypeScript
interface MyData {
  foo: string
  count: number
}

const stream = new Stream<MyData>({ ... })
```

```python
# Python
from typing import TypedDict

class MyData(TypedDict):
    foo: str
    count: int

stream = Stream[MyData](...)
```

### 4. Options Object

TypeScript passes options directly:

```typescript
await stream.process({ option1, option2 }, callback)
```

Python uses a dictionary:

```python
await stream.process(
    options={'option1': value1, 'option2': value2},
    callback=callback
)
```

### 5. Async/Await

Both languages use async/await, but Python requires explicit event loop:

```typescript
// TypeScript - just call it
await main()
```

```python
# Python - need to run in event loop
import asyncio
asyncio.run(main())
```

## Feature Parity

### ✅ Fully Supported

- Generic types (`Stream<T>` → `Stream[T]`)
- Consumer groups
- Automatic retries
- Dead letter queue (DLQ)
- Heartbeat management
- OpenTelemetry integration
- Event streaming
- Progress reporting
- Data transformation (map)
- Configurable concurrency
- Stream information APIs
- Consumer group information

### 🔄 Implementation Differences

| Feature           | TypeScript    | Python              |
| ----------------- | ------------- | ------------------- |
| **Shutdown**      | `AbortSignal` | `asyncio.Event`     |
| **Concurrency**   | `p-queue`     | `asyncio.Semaphore` |
| **ID Generation** | `nanoid`      | `uuid4`             |
| **Redis Client**  | `ioredis`     | `redis-py`          |

## Type Safety Comparison

Both versions have excellent type safety:

### TypeScript

```typescript
const callback = async (item: QueueItem<OutputType>) => {
  // item.data is typed as OutputType
}
```

### Python

```python
async def callback(item: QueueItem[OutputType]) -> None:
    # item.data is typed as OutputType
    pass
```

Both will catch type errors at compile/check time!

## Common Patterns

### 1. Basic Processing

**TypeScript:**

```typescript
await stream.process({ consumerGroup, signal }, callback)
```

**Python:**

```python
await stream.process(
    options={'consumer_group': group, 'signal': event},
    callback=callback
)
```

### 2. With Transformation

**TypeScript:**

```typescript
await stream.process(
  {
    consumerGroup,
    signal,
    map: async (items) => transform(items)
  },
  callback
)
```

**Python:**

```python
await stream.process(
    options={
        'consumer_group': group,
        'signal': event,
        'map': transform
    },
    callback=callback
)
```

### 3. Error Handling

**TypeScript:**

```typescript
try {
  await stream.add(data)
} catch (error) {
  console.error(error)
}
```

**Python:**

```python
try:
    await stream.add(data)
except Exception as error:
    print(error)
```

## Testing

### TypeScript (Vitest)

```typescript
import { describe, it, expect } from 'vitest'

describe('stream', () => {
  it('can process events', async () => {
    // test code
  })
})
```

### Python (pytest)

```python
import pytest

class TestStream:
    @pytest.mark.asyncio
    async def test_can_process_events(self):
        # test code
        pass
```

## Running the Code

### TypeScript

```bash
npm run test
npm run build
```

### Python

```bash
make test
make typecheck
make lint
```

## IDE Support

Both versions have excellent IDE support:

- **TypeScript**: VSCode, WebStorm
- **Python**: VSCode (Pylance), PyCharm

Both provide:

- ✅ Autocomplete
- ✅ Type checking
- ✅ Go to definition
- ✅ Inline documentation
- ✅ Refactoring support

## Performance Considerations

Both implementations are highly performant:

- Similar throughput for typical workloads
- Python might be slightly slower for CPU-bound transformations
- Both are I/O bound by Redis, so network is the bottleneck
- Concurrency works well in both (Node.js event loop vs Python asyncio)

## Conclusion

The Python port maintains 100% feature parity with excellent type safety and idiomatic Python code. TypeScript users will find the API familiar and intuitive.

**Key Takeaway**: If you know the TypeScript version, you can use the Python version with minimal adjustments - just follow Python naming conventions and use `asyncio.Event` instead of `AbortSignal`.
