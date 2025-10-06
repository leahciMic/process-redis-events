import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'
import { HeartbeatManager } from './lib/HeartbeatManager.js'

import {
  StartFrom,
  AbortErrorAndCloseRedisConnection
} from './lib/constants.js'

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
  waitForBatch?: boolean
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
  return (closeRedisConnection = true) => {
    // Reset active consumers to 0
    telemetry.recordActiveConsumers(0, {
      streamName: options.stream,
      consumerGroup: options.consumerGroup
    })

    if (closeRedisConnection) {
      abortController.abort(new AbortErrorAndCloseRedisConnection())
    } else {
      abortController.abort()
    }
  }
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
    waitForBatch = true,
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
    if (signal.reason instanceof AbortErrorAndCloseRedisConnection) {
      await redis.quit()
    }
  })

  await createGroup(redis, stream, consumerGroup, startFrom)

  async function run() {
    const queuedEntries: QueueItem<T>[] = []
    let timeLimitExceeded = false
    const timeoutId = setTimeout(() => {
      timeLimitExceeded = true
    }, blockFor)

    while (
      !signal.aborted &&
      queuedEntries.length < batchSize &&
      !timeLimitExceeded
    ) {
      const entries = await getEntries<T>({
        redis,
        globalRedis,
        stream,
        consumerGroup,
        consumerId,
        blockFor,
        batchSize,
        timeout: timeout * 2,
        telemetry,
        shouldRetry,
        onError,
        onComplete,
        onProgress,
        signal,
        heartbeatManager
      })

      if (entries) {
        queuedEntries.push(...entries)
      }

      if (signal.aborted) {
        return
      }

      if (!waitForBatch) {
        break
      }
    }

    clearTimeout(timeoutId)

    if (queuedEntries.length > 0) {
      const timer = telemetry.startTimer()

      queuedEntries.forEach((entry) => {
        onStart?.(entry.id)
      })

      try {
        await callback(queuedEntries)

        const duration = timer()

        // Record successful processing
        telemetry.recordMessagesProcessed(queuedEntries.length, {
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
        telemetry.recordMessagesProcessed(queuedEntries.length, {
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
      }
    }

    setTimeout(() => run())
  }

  run()
}
