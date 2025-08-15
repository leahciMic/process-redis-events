/**
 * Example showing how to use the Redis Stream library with OpenTelemetry instrumentation
 *
 * Note: This example shows the library usage. In a real application, you would
 * need to install and configure the OpenTelemetry SDK packages:
 *
 * npm install @opentelemetry/sdk-node @opentelemetry/exporter-prometheus @opentelemetry/resources
 */

import { Redis } from 'ioredis'
import {
  Stream,
  processRedisStream,
  configureTelemetry,
  type TelemetryConfig
} from '../index.js'

// Example 1: Setting up OpenTelemetry SDK (in your application, not in the library)
function setupOpenTelemetry() {
  console.log('OpenTelemetry SDK setup would go here')
  console.log(
    'Install: npm install @opentelemetry/sdk-node @opentelemetry/exporter-prometheus'
  )

  // Example setup code (uncomment when you have the packages):
  /*
  const { NodeSDK } = require('@opentelemetry/sdk-node')
  const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus')
  const { Resource } = require('@opentelemetry/resources')
  const { SEMRESATTRS_SERVICE_NAME, SEMRESATTRS_SERVICE_VERSION } = require('@opentelemetry/semantic-conventions')

  const prometheusExporter = new PrometheusExporter({
    port: 9090, // Metrics will be available at http://localhost:9090/metrics
  })

  const sdk = new NodeSDK({
    resource: new Resource({
      [SEMRESATTRS_SERVICE_NAME]: 'redis-stream-processor',
      [SEMRESATTRS_SERVICE_VERSION]: '1.0.0',
    }),
    metricReader: prometheusExporter,
  })

  sdk.start()
  */
}

// Example 2: Basic usage with default telemetry
async function basicExample() {
  // Configure telemetry (optional - uses defaults if not called)
  configureTelemetry({
    enabled: true,
    metricsEnabled: true,
    tracingEnabled: true
  })

  const stream = new Stream<{ userId: string; action: string }>('user-events')

  // Add some test data
  await stream.add({ userId: '123', action: 'login' })
  await stream.add({ userId: '456', action: 'logout' })

  // Process the stream with automatic telemetry
  const stopProcessing = processRedisStream(
    {
      stream: 'user-events',
      consumerGroup: 'processors',
      batchSize: 5,
      timeout: 30000
    },
    async (entries) => {
      console.log(`Processing ${entries.length} entries`)

      for (const entry of entries) {
        console.log(`Processing: ${JSON.stringify(entry.value)}`)

        // Simulate processing time
        await new Promise((resolve) => setTimeout(resolve, 100))

        // Acknowledge the message
        await entry.ack()
      }
    },
    2 // Use 2 concurrent threads
  )

  // Stop after 30 seconds
  setTimeout(() => {
    console.log('Stopping processing...')
    stopProcessing()
  }, 30000)
}

// Example 3: Custom telemetry configuration
async function customTelemetryExample() {
  const customConfig: Partial<TelemetryConfig> = {
    enabled: true,
    metricsEnabled: true,
    tracingEnabled: true,
    meterName: 'my-custom-meter',
    tracerName: 'my-custom-tracer',
    version: '1.0.0'
  }

  const stream = new Stream<{ data: string }>(
    'custom-stream',
    () => new Redis({ host: 'localhost', port: 6379 }), // Custom Redis config
    customConfig
  )

  // Get telemetry instance for custom metrics
  const telemetry = stream.getTelemetry()

  // Process with error handling and custom metrics
  stream.process(
    {
      consumerGroup: 'custom-group',
      telemetryConfig: customConfig
    },
    async (entries) => {
      const span = telemetry.createSpan('custom.processing', {
        'batch.size': entries.length
      })

      await telemetry.withSpanAsync(span, async () => {
        for (const entry of entries) {
          try {
            // Your processing logic here
            await processMessage(entry.value)
            await entry.ack()
          } catch (error) {
            console.error('Processing failed:', error)
            // Message will be retried automatically
          }
        }
      })
    }
  )
}

// Example 4: Monitoring stream health
async function monitoringExample() {
  const stream = new Stream<any>('monitoring-stream')

  // Check stream info periodically
  setInterval(async () => {
    try {
      const streamInfo = await stream.getStreamInfo()
      console.log('Stream info:', streamInfo)

      const groupInfo = await stream.getConsumerGroupInfo('monitoring-group')
      console.log('Consumer group info:', groupInfo)

      // Custom alerts based on metrics
      if (groupInfo.pending > 1000) {
        console.warn('High backlog detected!')
        // Send alert
      }
    } catch (error) {
      console.error('Failed to get stream info:', error)
    }
  }, 10000) // Check every 10 seconds
}

// Example 5: Batch operations with telemetry
async function batchExample() {
  const stream = new Stream<{ id: number; data: string }>('batch-stream')

  // Add batch of messages
  const messages = Array.from({ length: 100 }, (_, i) => ({
    id: i,
    data: `message-${i}`
  }))

  console.log('Adding batch of messages...')
  for (const message of messages) {
    await stream.add(message)
  }
  console.log(`Added ${messages.length} messages`)

  // Process with high throughput settings
  const stop = processRedisStream(
    {
      stream: 'batch-stream',
      consumerGroup: 'batch-processors',
      batchSize: 20, // Larger batches
      waitForBatch: true,
      blockFor: 1000 // Wait longer for full batches
    },
    async (entries) => {
      console.log(`Processing batch of ${entries.length} messages`)

      // Parallel processing within batch
      await Promise.all(
        entries.map(async (entry) => {
          await processMessage(entry.value)
          await entry.ack()
        })
      )
    },
    5 // Use 5 concurrent threads for high throughput
  )

  // Stop after some time for demo
  setTimeout(() => stop(), 30000)
}

// Helper function for processing messages
async function processMessage(data: any): Promise<void> {
  // Simulate processing time
  await new Promise((resolve) => setTimeout(resolve, Math.random() * 100))

  // Simulate occasional errors
  if (Math.random() < 0.05) {
    // 5% error rate
    throw new Error('Random processing error')
  }

  console.log('Processed:', data)
}

// Run examples
async function main() {
  console.log('Setting up OpenTelemetry...')
  setupOpenTelemetry()

  console.log('Running basic example...')
  await basicExample()

  // Uncomment to run other examples
  // await customTelemetryExample()
  // await monitoringExample()
  // await batchExample()
}

// Export functions for external use
export {
  setupOpenTelemetry,
  basicExample,
  customTelemetryExample,
  monitoringExample,
  batchExample
}

// Only run if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}
