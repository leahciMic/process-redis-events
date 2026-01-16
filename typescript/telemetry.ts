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

export interface TelemetryConfig {
  enabled?: boolean

  version?: string
  meterName?: string
  tracerName?: string

  streamName: string
  consumerGroup: string
  consumerId: string
}

const defaults = {
  enabled: true,
  meterName: 'process-redis-events',
  tracerName: 'process-redis-events',
  version: '5.0.6'
}

export class RedisStreamTelemetry {
  private config: TelemetryConfig
  private meter: Meter
  private tracer: Tracer

  private messagesProcessedCounter!: Counter
  private processingDurationHistogram!: Histogram
  private batchSizeHistogram!: Histogram
  private pendingMessagesGauge!: Gauge
  private pendingGauge!: Gauge
  private entriesReadCounter!: Counter
  private lagGauge!: Gauge
  private streamLengthGauge!: Gauge
  private entriesAddedCounter!: Counter
  private consumerGroupsGauge!: Gauge

  constructor(config: TelemetryConfig) {
    this.config = { ...defaults, ...config }

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

  child(config: Partial<TelemetryConfig>): RedisStreamTelemetry {
    return new RedisStreamTelemetry({ ...this.config, ...config })
  }

  private initializeMetrics() {
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

    // Gauge for pending messages per consumer group
    this.pendingGauge = this.meter.createGauge('redis_stream_pending', {
      description: 'Number of pending messages for a consumer group',
      unit: '1'
    })

    // Counter for entries read
    this.entriesReadCounter = this.meter.createCounter(
      'redis_stream_entries_read_total',
      {
        description: 'Total number of entries read from the stream',
        unit: '1'
      }
    )

    // Gauge for consumer group lag
    this.lagGauge = this.meter.createGauge('redis_stream_lag', {
      description: 'Lag of consumer group behind the stream',
      unit: '1'
    })

    // Gauge for stream length (XLEN)
    this.streamLengthGauge = this.meter.createGauge('redis_stream_length', {
      description: 'Current length of the stream (XLEN)',
      unit: '1'
    })

    // Counter for entries added to stream
    this.entriesAddedCounter = this.meter.createCounter(
      'redis_stream_entries_added_total',
      {
        description: 'Total number of entries added to the stream',
        unit: '1'
      }
    )

    // Gauge for number of consumer groups
    this.consumerGroupsGauge = this.meter.createGauge(
      'redis_stream_consumer_groups_total',
      {
        description: 'Total number of consumer groups for the stream',
        unit: '1'
      }
    )
  }

  private initializeNoOpMetrics() {
    const noOpCounter = { add: () => {} } as Counter
    const noOpHistogram = { record: () => {} } as Histogram
    const noOpGauge = { record: () => {} } as Gauge

    this.messagesProcessedCounter = noOpCounter
    this.processingDurationHistogram = noOpHistogram
    this.batchSizeHistogram = noOpHistogram
    this.pendingMessagesGauge = noOpGauge
    this.pendingGauge = noOpGauge
    this.entriesReadCounter = noOpCounter
    this.lagGauge = noOpGauge
    this.streamLengthGauge = noOpGauge
    this.entriesAddedCounter = noOpCounter
    this.consumerGroupsGauge = noOpGauge
  }

  // Tracing methods
  createSpan(
    name: string,
    attributes: Record<string, string | number> = {}
  ): Span {
    return this.tracer.startSpan(name, {
      kind: SpanKind.INTERNAL,
      attributes
    })
  }

  // Metrics recording methods
  recordMessagesProcessed(
    count: number,
    labels: { status: 'success' | 'error' }
  ) {
    this.messagesProcessedCounter.add(count, {
      stream_name: this.config.streamName,
      consumer_group: this.config.consumerGroup,
      consumer_id: this.config.consumerId,
      status: labels.status
    })
  }

  recordProcessingDuration(duration: number) {
    this.processingDurationHistogram.record(duration, {
      stream_name: this.config.streamName,
      consumer_group: this.config.consumerGroup,
      consumer_id: this.config.consumerId
    })
  }

  recordBatchSize(size: number) {
    this.batchSizeHistogram.record(size, {
      stream_name: this.config.streamName,
      consumer_group: this.config.consumerGroup
    })
  }

  recordPendingMessages(count: number, consumerGroup: string) {
    this.pendingMessagesGauge.record(count, {
      stream_name: this.config.streamName,
      consumer_group: consumerGroup
    })
  }

  recordPending(count: number, consumerGroup: string) {
    this.pendingGauge.record(count, {
      stream_name: this.config.streamName,
      consumer_group: consumerGroup
    })
  }

  recordEntriesRead(count: number, consumerGroup: string) {
    this.entriesReadCounter.add(count, {
      stream_name: this.config.streamName,
      consumer_group: consumerGroup
    })
  }

  recordLag(lag: number, consumerGroup: string) {
    this.lagGauge.record(lag, {
      stream_name: this.config.streamName,
      consumer_group: consumerGroup
    })
  }

  recordStreamLength(length: number) {
    this.streamLengthGauge.record(length, {
      stream_name: this.config.streamName
    })
  }

  recordEntriesAdded(count: number) {
    this.entriesAddedCounter.add(count, {
      stream_name: this.config.streamName
    })
  }

  recordConsumerGroups(count: number) {
    this.consumerGroupsGauge.record(count, {
      stream_name: this.config.streamName
    })
  }

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
