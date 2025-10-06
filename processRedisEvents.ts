import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'
import { HeartbeatManager } from './lib/HeartbeatManager.js'

import { StartFrom } from './lib/constants.js'

import { createGroup } from './lib/createGroup.js'
import type { QueueItem } from './lib/QueueItem.js'
import { getEntries } from './lib/getEntries.js'

type Args<T> = {
  createRedis: () => Redis
  stream: string
  consumerGroup: string
  consumerId?: string
  batchSize?: number
  timeout?: number
  startFrom?: StartFrom
  blockFor?: number
  signal: AbortSignal
  shouldRetry?: (attempt: number, entry: T) => boolean
  onError?: (id: string, error: Error) => Promise<void>
  onStart?: (id: string) => Promise<void>
  onComplete?: (id: string) => Promise<void>
  onProgress?: (
    id: string,
    completionRatio: number,
    status: string
  ) => Promise<void>
  telemetry?: RedisStreamTelemetry
}

export type processRedisStreamOptions<T> = Omit<Args<T>, 'createRedis'>

export function processRedisStream<T>(
  options: Args<T> & {
    telemetryConfig?: Partial<TelemetryConfig>
  },
  callback: (entries: QueueItem<T>[]) => Promise<void>,
  threads = 1
) {
  const telemetry = options.telemetry || getTelemetry(options.telemetryConfig)
  const abortController = new AbortController()

  // Record active consumers
  telemetry.recordActiveConsumers(threads, {
    streamName: options.stream,
    consumerGroup: options.consumerGroup
  })

  for (let i = 0; i < threads; i++) {
    runThread({ ...options, telemetry }, abortController.signal, callback)
  }

  options.signal.addEventListener('abort', () => {
    telemetry.recordActiveConsumers(0, {
      streamName: options.stream,
      consumerGroup: options.consumerGroup
    })
  })
}

async function runThread<T>(
  {
    createRedis = () => new Redis(),
    stream,
    consumerGroup,
    consumerId = nanoid(),
    batchSize = 10,
    timeout = 10_000,
    blockFor = 100,
    startFrom = StartFrom.Latest,
    shouldRetry,
    telemetry = getTelemetry(),
    onComplete,
    onError,
    onStart,
    onProgress
  }: Args<T> & { telemetry: RedisStreamTelemetry },
  signal: AbortSignal,
  callback: (entries: QueueItem<T>[]) => Promise<void>
) {
  const redis = createRedis()
  const globalRedis = createRedis()

  const heartbeatManager = new HeartbeatManager(
    globalRedis,
    stream,
    consumerGroup,
    consumerId
  )

  signal.addEventListener('abort', async () => {
    await redis.quit()
    await globalRedis.quit()
  })

  await createGroup(redis, stream, consumerGroup, startFrom)

  async function run() {
    while (!signal.aborted) {
      const entries = await getEntries({
        redis,
        stream,
        consumerGroup,
        consumerId,
        blockFor,
        batchSize,
        timeout: timeout * 2,
        telemetry,
        signal
      })

      const allItems =
        entries?.map(({ id, value, attempts }): QueueItem<T> => {
          heartbeatManager.add(id)

          return {
            id,
            value,
            attempts,
            error: (error: Error) => {
              heartbeatManager.remove(id)
              onError?.(id, error)
            },
            reportProgress: async (completionRatio: number, status: string) => {
              if (completionRatio < 0 || completionRatio > 1) {
                throw new Error('Percent must be between 0 and 1')
              }
              onProgress?.(id, completionRatio, status)
            },
            ack: async () => {
              heartbeatManager.remove(id)
              await globalRedis.xack(stream, consumerGroup, id)
              await onComplete?.(id)
            },
            del: async () => {
              heartbeatManager.remove(id)
              await globalRedis.xdel(stream, id)
            }
          }
        }) ?? []

      const queueItems = await Promise.all(
        allItems.map(async (item) => {
          if (
            shouldRetry &&
            item.attempts > 0 &&
            !shouldRetry(item.attempts, item.value)
          ) {
            // Message exceeded retry limit - move to dead letter queue
            const dlqStreamName = `${stream}-dlq`

            try {
              // Add to dead letter queue with original data plus metadata
              await redis.xadd(
                dlqStreamName,
                '*',
                'originalId',
                item.id,
                'attempts',
                item.attempts.toString(),
                'consumerGroup',
                consumerGroup,
                'consumerId',
                consumerId,
                'timestamp',
                Date.now().toString(),
                'data',
                JSON.stringify(item.value)
              )

              // Acknowledge original message to remove from stream
              await item.ack()

              // Record as dead letter
              telemetry.recordDeadLetter({
                streamName: stream,
                consumerGroup,
                consumerId,
                reason: 'retry_limit_exceeded'
              })
            } catch (error) {
              // If we can't add to DLQ, log error but still acknowledge to prevent infinite loop
              console.error(
                `Failed to add message to dead letter queue ${dlqStreamName}:`,
                error
              )
              await item.ack()
            }

            return null // Filter out this item
          }
          return item
        })
      ).then((items) =>
        items.filter((item): item is QueueItem<T> => item !== null)
      )

      if (signal.aborted) {
        return
      }
      const timer = telemetry.startTimer()

      queueItems.forEach((entry) => {
        onStart?.(entry.id)
      })

      try {
        await callback(queueItems)

        const duration = timer()

        // Record successful processing
        telemetry.recordMessagesProcessed(queueItems.length, {
          streamName: stream,
          consumerGroup,
          consumerId,
          status: 'success'
        })

        telemetry.recordProcessingDuration(duration, {
          streamName: stream,
          consumerGroup,
          consumerId
        })
      } catch (error) {
        const duration = timer()

        // Record failed processing
        telemetry.recordMessagesProcessed(queueItems.length, {
          streamName: stream,
          consumerGroup,
          consumerId,
          status: 'error'
        })

        telemetry.recordProcessingDuration(duration, {
          streamName: stream,
          consumerGroup,
          consumerId
        })

        telemetry.recordError({
          streamName: stream,
          consumerGroup,
          consumerId,
          errorType: (error as Error).constructor.name
        })

        console.error('Error processing entries:', error)
      } finally {
        for (const entry of queuedEntries) {
          heartbeatManager.remove(entry.id)
        }
      }
    }

    setTimeout(() => run())
  }

  run()
}
