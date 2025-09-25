import { Redis, ReplyError } from 'ioredis'
import { nanoid } from 'nanoid'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'
import z from 'zod'
import { resultOf } from './lib/resultOf.js'

function chunk<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = []
  let index = 0

  while (index < array.length) {
    chunks.push(array.slice(index, index + size))
    index += size
  }

  return chunks
}

class AbortErrorAndCloseRedisConnection extends Error {}

export enum StartFrom {
  Oldest = '0',
  Latest = '$'
}

enum RedisStreamCursors {
  Oldest = '0-0',
  Latest = '>'
}

async function createGroup(
  redis: Redis,
  stream: string,
  consumerGroup: string,
  startFrom: StartFrom
) {
  try {
    await redis.xgroup('CREATE', stream, consumerGroup, startFrom, 'MKSTREAM')
  } catch (err) {
    if (err instanceof Error && /BUSYGROUP/.test(err.message)) {
      // continue
    } else {
      throw err
    }
  }
}

function parseJson<T>(value: unknown): T {
  if (typeof value !== 'string') {
    throw new Error('Value is not a string')
  }
  return JSON.parse(value)
}

async function getEntries<T>({
  redis,
  globalRedis,
  stream,
  consumerGroup,
  consumerId,
  blockFor,
  batchSize,
  timeout,
  telemetry,
  shouldRetry,
  onError,
  onComplete,
  onProgress,
  signal
}: {
  redis: Redis
  globalRedis: Redis
  consumerId: string
  timeout: number
  batchSize: number
  blockFor: number
  stream: string
  consumerGroup: string
  telemetry: RedisStreamTelemetry
  onError?: (id: string, error: Error) => Promise<void>
  onComplete?: (id: string) => Promise<void>
  onProgress?: (
    id: string,
    completionRatio: number,
    status: string
  ) => Promise<void>
  shouldRetry?: (attempt: number, entry: T) => boolean
  signal: AbortSignal
}) {
  const claimed = <[string, string[], unknown[]]>(
    await redis.xautoclaim(
      stream,
      consumerGroup,
      consumerId,
      timeout,
      RedisStreamCursors.Oldest,
      'COUNT',
      batchSize,
      'JUSTID'
    )
  )

  if (signal.aborted) {
    return []
  }

  const claimedCount = claimed[1].length

  if (claimedCount > 0) {
    telemetry.recordRetry({
      streamName: stream,
      consumerGroup,
      attemptCount: claimedCount
    })
  }

  const claimedIds = claimed[1]

  const xPendingInfo =
    claimedIds.length > 0
      ? await Promise.all(
          claimedIds.map(async (id) => {
            const pendingResponse = await redis.xpending(
              stream,
              consumerGroup,
              id,
              id,
              1
            )
            const pending = z
              .tuple([
                z.tuple([z.string(), z.string(), z.number(), z.number()])
              ])
              .parse(pendingResponse)

            return {
              id: pending[0][0],
              consumerId: pending[0][1],
              idleTime: pending[0][2],
              attempts: Number(pending[0][3])
            }
          })
        )
      : []

  const retryData = Object.fromEntries(
    xPendingInfo.map(({ id, attempts }) => [id, attempts])
  )

  if (signal.aborted) {
    return []
  }

  type XReadGroup = [[string, [string, string[]][]]]
  const [getEntriesError, entries] = await resultOf(
    <Promise<null | XReadGroup>>(
      redis.xreadgroup(
        'GROUP',
        consumerGroup,
        consumerId,
        'COUNT',
        claimedCount || batchSize,
        'BLOCK',
        blockFor,
        'STREAMS',
        stream,
        claimedCount > 0 ? RedisStreamCursors.Oldest : RedisStreamCursors.Latest
      )
    )
  )

  if (signal.aborted) {
    return []
  }

  if (getEntriesError) {
    throw getEntriesError
  }

  const allItems = entries?.[0][1].map(([id, values]) => {
    const value =
      values.length === 0
        ? null
        : values.length === 2
        ? parseJson(values[1])
        : Object.fromEntries(chunk(values, 2))
    const attempts = retryData?.[id] ?? 0
    const entry: QueueItem<T> = {
      id,
      value,
      attempts,
      error: (error: Error) => {
        onError?.(id, error)
      },
      reportProgress: async (completionRatio: number, status: string) => {
        if (completionRatio < 0 || completionRatio > 1) {
          throw new Error('Percent must be between 0 and 1')
        }
        onProgress?.(id, completionRatio, status)
      },
      ack: async () => {
        await globalRedis.xack(stream, consumerGroup, id)
        await onComplete?.(id)
      },
      del: async () => {
        await globalRedis.xdel(stream, id)
      }
    }
    return entry
  })

  // Filter out items that shouldn't be retried
  const queueItems = allItems
    ? await Promise.all(
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
    : []

  // Record batch size if we got entries
  if (queueItems && queueItems.length > 0) {
    telemetry.recordBatchSize(queueItems.length, {
      streamName: stream,
      consumerGroup
    })
  }

  return queueItems
}

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

export type QueueItem<T> = {
  id: string
  value: T
  attempts: number
  error: (error: Error) => void
  ack: () => Promise<void>
  del: () => Promise<void>
  reportProgress: (completionRatio: number, status: string) => Promise<void>
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
        signal
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
