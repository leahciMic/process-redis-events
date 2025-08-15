# OpenTelemetry Integration

This library provides comprehensive OpenTelemetry instrumentation for Redis stream processing with automatic metrics, tracing, and structured logging.

## Quick Start

### 1. Install Dependencies

```bash
# Library dependencies (already included)
npm install @opentelemetry/api @opentelemetry/semantic-conventions

# Application dependencies (install in your app)
npm install @opentelemetry/sdk-node @opentelemetry/exporter-prometheus
```

### 2. Basic Usage

```typescript
import {
  InstrumentedStream,
  processRedisStreamWithTelemetry
} from 'process-redis-events'

// Create an instrumented stream
const stream = new InstrumentedStream<{ userId: string }>('user-events')

// Process with automatic telemetry
const stop = processRedisStreamWithTelemetry(
  {
    stream: 'user-events',
    consumerGroup: 'processors',
    batchSize: 10
  },
  async (entries) => {
    for (const entry of entries) {
      await processMessage(entry.value)
      await entry.ack()
    }
  },
  2 // concurrent threads
)
```

## Metrics

The library automatically collects the following metrics:

### Counters

| Metric Name                             | Description              | Labels                                                       |
| --------------------------------------- | ------------------------ | ------------------------------------------------------------ |
| `redis_stream_messages_processed_total` | Total messages processed | `stream_name`, `consumer_group`, `consumer_id`, `status`     |
| `redis_stream_errors_total`             | Total processing errors  | `stream_name`, `consumer_group`, `consumer_id`, `error_type` |
| `redis_stream_message_retries_total`    | Message retry attempts   | `stream_name`, `consumer_group`, `attempt_count`             |
| `redis_operations_total`                | Redis command executions | `operation`, `stream_name`, `status`                         |

### Histograms

| Metric Name                                | Description                 | Labels                                         |
| ------------------------------------------ | --------------------------- | ---------------------------------------------- |
| `redis_stream_processing_duration_seconds` | Message processing time     | `stream_name`, `consumer_group`, `consumer_id` |
| `redis_stream_batch_size`                  | Distribution of batch sizes | `stream_name`, `consumer_group`                |
| `redis_operation_duration_seconds`         | Redis command duration      | `operation`, `stream_name`, `status`           |

### Gauges

| Metric Name                     | Description                | Labels                          |
| ------------------------------- | -------------------------- | ------------------------------- |
| `redis_stream_pending_messages` | Pending messages in stream | `stream_name`, `consumer_group` |
| `redis_stream_active_consumers` | Active consumer threads    | `stream_name`, `consumer_group` |

## Distributed Tracing

### Automatic Spans

The library creates spans for:

- `redis.stream.process` - Overall processing operation
- `redis.stream.get_entries` - Fetching entries from stream
- `redis.stream.process_batch` - Processing a batch of messages
- `redis.{operation}` - Individual Redis commands (xread, xack, etc.)

### Custom Spans

```typescript
const telemetry = stream.getTelemetry()

const span = telemetry.createSpan('custom.operation', {
  'custom.attribute': 'value'
})

await telemetry.withSpanAsync(span, async () => {
  // Your instrumented code here
})
```

## Configuration

### Global Configuration

```typescript
import { configureTelemetry } from 'process-redis-events'

configureTelemetry({
  enabled: true,
  metricsEnabled: true,
  tracingEnabled: true,
  meterName: 'my-app',
  tracerName: 'my-app',
  version: '1.0.0'
})
```

### Per-Instance Configuration

```typescript
const stream = new InstrumentedStream('my-stream', createRedis, {
  enabled: true,
  metricsEnabled: true,
  tracingEnabled: false // Disable tracing for this instance
})
```

### Disable Telemetry

```typescript
const stream = new InstrumentedStream('my-stream', createRedis, {
  enabled: false // All telemetry disabled
})
```

## OpenTelemetry SDK Setup

In your application (not in the library), set up the OpenTelemetry SDK:

```typescript
import { NodeSDK } from '@opentelemetry/sdk-node'
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus'
import { Resource } from '@opentelemetry/resources'
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions'

const prometheusExporter = new PrometheusExporter({
  port: 9464 // Metrics available at http://localhost:9464/metrics
})

const sdk = new NodeSDK({
  resource: new Resource({
    [SEMRESATTRS_SERVICE_NAME]: 'redis-stream-processor'
  }),
  metricReader: prometheusExporter
  // Add trace exporters for Jaeger, OTLP, etc.
})

sdk.start()
```

## Monitoring Examples

### Stream Health Monitoring

```typescript
const stream = new InstrumentedStream('orders')

// Check stream metrics periodically
setInterval(async () => {
  const info = await stream.getStreamInfo()
  const groupInfo = await stream.getConsumerGroupInfo('processors')

  console.log(`Stream length: ${info.length}`)
  console.log(`Pending messages: ${groupInfo.pending}`)
  console.log(`Active consumers: ${groupInfo.consumers}`)

  // Alert on high backlog
  if (groupInfo.pending > 1000) {
    sendAlert('High message backlog detected!')
  }
}, 30000)
```

### Custom Metrics

```typescript
const telemetry = stream.getTelemetry()

// Record custom business metrics
telemetry.recordMessagesProcessed(batchSize, {
  streamName: 'orders',
  consumerGroup: 'payment-processor',
  consumerId: 'worker-1',
  status: 'success'
})
```

## Grafana Dashboard

Create dashboards using these PromQL queries:

```promql
# Processing rate
rate(redis_stream_messages_processed_total[5m])

# Error rate
rate(redis_stream_errors_total[5m]) / rate(redis_stream_messages_processed_total[5m])

# Processing latency percentiles
histogram_quantile(0.95, redis_stream_processing_duration_seconds_bucket)

# Stream backlog
redis_stream_pending_messages

# Active consumers
redis_stream_active_consumers
```

## Best Practices

1. **Use appropriate batch sizes**: Monitor `redis_stream_batch_size` histogram to optimize throughput
2. **Monitor error rates**: Set alerts on high error rates or specific error types
3. **Track processing latency**: Use P95/P99 latency metrics for SLA monitoring
4. **Monitor stream backlog**: Alert when pending messages grow too large
5. **Use custom spans**: Add business-specific tracing for complex processing logic

## Performance Impact

The telemetry instrumentation has minimal performance overhead:

- Metrics: ~1-2μs per operation
- Tracing: ~5-10μs per span (when enabled)
- No-op mode: ~0.1μs overhead when disabled

Use the `enabled: false` configuration in performance-critical scenarios where telemetry isn't needed.

## Example Metrics Output

```
# HELP redis_stream_messages_processed_total Total number of messages processed from Redis streams
# TYPE redis_stream_messages_processed_total counter
redis_stream_messages_processed_total{stream_name="orders",consumer_group="processors",consumer_id="worker-1",status="success"} 1543

# HELP redis_stream_processing_duration_seconds Time taken to process a batch of messages
# TYPE redis_stream_processing_duration_seconds histogram
redis_stream_processing_duration_seconds_bucket{stream_name="orders",consumer_group="processors",consumer_id="worker-1",le="0.1"} 892
redis_stream_processing_duration_seconds_bucket{stream_name="orders",consumer_group="processors",consumer_id="worker-1",le="0.5"} 1456
redis_stream_processing_duration_seconds_bucket{stream_name="orders",consumer_group="processors",consumer_id="worker-1",le="+Inf"} 1543
```
