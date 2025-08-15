import {
  metrics,
  trace,
  Span,
  SpanKind,
  SpanStatusCode,
  context,
  Counter,
  Gauge,
  Histogram,
  Meter,
  Tracer
} from '@opentelemetry/api'

// Telemetry configuration
export interface TelemetryConfig {
  enabled: boolean
  metricsEnabled: boolean
  tracingEnabled: boolean
  meterName?: string
  tracerName?: string
  version?: string
}

const DEFAULT_CONFIG: TelemetryConfig = {
  enabled: true,
  metricsEnabled: true,
  tracingEnabled: true,
  meterName: 'process-redis-events',
  tracerName: 'process-redis-events',
  version: '2.0.6'
}

export class RedisStreamTelemetry {
  private config: TelemetryConfig
  private meter: Meter
  private tracer: Tracer

  // Metrics
  private messagesProcessedCounter!: Counter
  private processingDurationHistogram!: Histogram
  private errorCounter!: Counter
  private retryCounter!: Counter
  private activeConsumersGauge!: Gauge
  private batchSizeHistogram!: Histogram
  private pendingMessagesGauge!: Gauge
  private redisOperationsCounter!: Counter
  private redisOperationDurationHistogram!: Histogram

  constructor(config: Partial<TelemetryConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config }

    if (!this.config.enabled) {
      // Create no-op implementations when disabled
      this.meter = metrics.getMeter('noop')
      this.tracer = trace.getTracer('noop')
      this.initializeNoOpMetrics()
      return
    }

    this.meter = metrics.getMeter(this.config.meterName!, this.config.version)

    this.tracer = trace.getTracer(this.config.tracerName!, this.config.version)

    this.initializeMetrics()
  }

  private initializeMetrics() {
    if (!this.config.metricsEnabled) {
      this.initializeNoOpMetrics()
      return
    }

    // Counter for processed messages
    this.messagesProcessedCounter = this.meter.createCounter(
      'redis_stream_messages_processed_total',
      {
        description: 'Total number of messages processed from Redis streams',
        unit: '1'
      }
    )

    // Histogram for processing duration
    this.processingDurationHistogram = this.meter.createHistogram(
      'redis_stream_processing_duration_seconds',
      {
        description: 'Time taken to process a batch of messages',
        unit: 's'
      }
    )

    // Counter for errors
    this.errorCounter = this.meter.createCounter('redis_stream_errors_total', {
      description: 'Total number of processing errors',
      unit: '1'
    })

    // Counter for retries
    this.retryCounter = this.meter.createCounter(
      'redis_stream_message_retries_total',
      {
        description: 'Number of message retry attempts',
        unit: '1'
      }
    )

    // Gauge for active consumers
    this.activeConsumersGauge = this.meter.createGauge(
      'redis_stream_active_consumers',
      {
        description: 'Number of currently active consumer threads',
        unit: '1'
      }
    )

    // Histogram for batch sizes
    this.batchSizeHistogram = this.meter.createHistogram(
      'redis_stream_batch_size',
      {
        description: 'Distribution of batch sizes processed',
        unit: '1'
      }
    )

    // Gauge for pending messages
    this.pendingMessagesGauge = this.meter.createGauge(
      'redis_stream_pending_messages',
      {
        description: 'Number of pending messages in the stream',
        unit: '1'
      }
    )

    // Counter for Redis operations
    this.redisOperationsCounter = this.meter.createCounter(
      'redis_operations_total',
      {
        description: 'Total number of Redis operations',
        unit: '1'
      }
    )

    // Histogram for Redis operation duration
    this.redisOperationDurationHistogram = this.meter.createHistogram(
      'redis_operation_duration_seconds',
      {
        description: 'Duration of Redis operations',
        unit: 's'
      }
    )
  }

  private initializeNoOpMetrics() {
    const noOpCounter = { add: () => {} } as Counter
    const noOpHistogram = { record: () => {} } as Histogram
    const noOpGauge = { record: () => {} } as Gauge

    this.messagesProcessedCounter = noOpCounter
    this.processingDurationHistogram = noOpHistogram
    this.errorCounter = noOpCounter
    this.retryCounter = noOpCounter
    this.activeConsumersGauge = noOpGauge
    this.batchSizeHistogram = noOpHistogram
    this.pendingMessagesGauge = noOpGauge
    this.redisOperationsCounter = noOpCounter
    this.redisOperationDurationHistogram = noOpHistogram
  }

  // Tracing methods
  createSpan(
    name: string,
    attributes: Record<string, string | number> = {}
  ): Span {
    if (!this.config.tracingEnabled) {
      return trace.getActiveSpan() || trace.getTracer('noop').startSpan('noop')
    }

    return this.tracer.startSpan(name, {
      kind: SpanKind.INTERNAL,
      attributes
    })
  }

  // Metrics recording methods
  recordMessagesProcessed(
    count: number,
    labels: {
      streamName: string
      consumerGroup: string
      consumerId: string
      status: 'success' | 'error'
    }
  ) {
    this.messagesProcessedCounter.add(count, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup,
      consumer_id: labels.consumerId,
      status: labels.status
    })
  }

  recordProcessingDuration(
    duration: number,
    labels: {
      streamName: string
      consumerGroup: string
      consumerId: string
    }
  ) {
    this.processingDurationHistogram.record(duration, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup,
      consumer_id: labels.consumerId
    })
  }

  recordError(labels: {
    streamName: string
    consumerGroup: string
    consumerId: string
    errorType: string
  }) {
    this.errorCounter.add(1, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup,
      consumer_id: labels.consumerId,
      error_type: labels.errorType
    })
  }

  recordRetry(labels: {
    streamName: string
    consumerGroup: string
    attemptCount: number
  }) {
    this.retryCounter.add(1, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup,
      attempt_count: labels.attemptCount.toString()
    })
  }

  recordActiveConsumers(
    count: number,
    labels: {
      streamName: string
      consumerGroup: string
    }
  ) {
    this.activeConsumersGauge.record(count, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup
    })
  }

  recordBatchSize(
    size: number,
    labels: {
      streamName: string
      consumerGroup: string
    }
  ) {
    this.batchSizeHistogram.record(size, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup
    })
  }

  recordPendingMessages(
    count: number,
    labels: {
      streamName: string
      consumerGroup: string
    }
  ) {
    this.pendingMessagesGauge.record(count, {
      stream_name: labels.streamName,
      consumer_group: labels.consumerGroup
    })
  }

  recordRedisOperation(
    operation: string,
    duration: number,
    labels: {
      streamName?: string
      success: boolean
    }
  ) {
    const attributes = {
      operation,
      ...(labels.streamName && { stream_name: labels.streamName }),
      status: labels.success ? 'success' : 'error'
    }

    this.redisOperationsCounter.add(1, attributes)
    this.redisOperationDurationHistogram.record(duration, attributes)
  }

  // Utility methods for measuring time
  startTimer(): () => number {
    const start = Date.now()
    return () => (Date.now() - start) / 1000
  }

  // Context management for distributed tracing
  withSpan<T>(span: Span, fn: () => T): T {
    return context.with(trace.setSpan(context.active(), span), () => {
      try {
        const result = fn()
        span.setStatus({ code: SpanStatusCode.OK })
        return result
      } catch (error) {
        span.recordException(error as Error)
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: (error as Error).message
        })
        throw error
      } finally {
        span.end()
      }
    })
  }

  async withSpanAsync<T>(span: Span, fn: () => Promise<T>): Promise<T> {
    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const result = await fn()
        span.setStatus({ code: SpanStatusCode.OK })
        return result
      } catch (error) {
        span.recordException(error as Error)
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: (error as Error).message
        })
        throw error
      } finally {
        span.end()
      }
    })
  }
}

// Global instance (optional - users can create their own)
let globalTelemetry: RedisStreamTelemetry | null = null

export function getTelemetry(
  config?: Partial<TelemetryConfig>
): RedisStreamTelemetry {
  if (!globalTelemetry || config) {
    globalTelemetry = new RedisStreamTelemetry(config)
  }
  return globalTelemetry
}

export function configureTelemetry(config: Partial<TelemetryConfig>): void {
  globalTelemetry = new RedisStreamTelemetry(config)
}
