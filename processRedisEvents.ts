import { Redis, ReplyError } from 'ioredis'
import { nanoid } from 'nanoid'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'

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
  } catch (err: typeof ReplyError | any) {
    if (err instanceof ReplyError && /BUSYGROUP/.test(err.message)) {
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
  stream,
  consumerGroup,
  consumerId,
  blockFor,
  batchSize,
  timeout,
  telemetry
}: {
  redis: Redis
  consumerId: string
  timeout: number
  batchSize: number
  blockFor: number
  stream: string
  consumerGroup: string
  telemetry: RedisStreamTelemetry
}) {
  const claimed = <[string, Array<[string, string[]]>]>(
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

  const claimedCount = claimed[1].length

  if (claimedCount > 0) {
    telemetry.recordRetry({
      streamName: stream,
      consumerGroup,
      attemptCount: claimedCount
    })
  }

  type XPendingData = [string, string, string, string][]

  const retryData =
    claimedCount > 0
      ? Object.fromEntries(
          (<XPendingData>(
            await redis.xpending(
              stream,
              consumerGroup,
              'IDLE',
              timeout,
              '-',
              '+',
              claimedCount,
              consumerId
            )
          )).map(([id, _consumerId, _idleTime, attempts]) => [
            id,
            Number(attempts ?? 0)
          ])
        )
      : null

  type XReadGroup = [[string, [string, string[]][]]]
  const entries = <null | XReadGroup>(
    await redis.xreadgroup(
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

  const queueItems = entries?.[0][1].map(([id, values]) => {
    const value =
      values.length === 0
        ? null
        : values.length === 2
        ? parseJson(values[1])
        : Object.fromEntries(chunk(values, 2))
    const entry: QueueItem<T> = {
      id,
      value,
      attempts: retryData?.[id] ?? 0,
      ack: async () => {
        await redis.xack(stream, consumerGroup, id)
      },
      del: async () => {
        await redis.xdel(stream, id)
      }
    }
    return entry
  })

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
  redis?: Redis
  stream: string
  consumerGroup: string
  consumerId?: string
  batchSize?: number
  timeout?: number
  startFrom?: StartFrom
  blockFor?: number
  waitForBatch?: boolean
  shouldRetry?: (attempt: number, entry: T) => boolean
  telemetry?: RedisStreamTelemetry
}
export type processRedisStreamOptions<T> = Omit<Args<T>, 'redis'>

export function processRedisStream<T>(
  options: Omit<Args<T>, 'redis'> & {
    telemetryConfig?: Partial<TelemetryConfig>
  },
  callback: (entries: QueueItem<T>[]) => Promise<void>,
  threads = 1,
  createRedis = () => new Redis()
) {
  const telemetry = options.telemetry || getTelemetry(options.telemetryConfig)
  const abortController = new AbortController()

  // Record active consumers
  telemetry.recordActiveConsumers(threads, {
    streamName: options.stream,
    consumerGroup: options.consumerGroup
  })

  for (let i = 0; i < threads; i++) {
    runThread(
      { ...options, redis: createRedis(), telemetry },
      abortController.signal,
      callback
    )
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
  ack: () => Promise<void>
  del: () => Promise<void>
}

async function runThread<T>(
  {
    redis = new Redis(),
    stream,
    consumerGroup,
    consumerId = nanoid(),
    batchSize = 10,
    timeout = 10_000,
    blockFor = 100,
    startFrom = StartFrom.Latest,
    waitForBatch = true,
    telemetry = getTelemetry()
  }: Args<T> & { telemetry: RedisStreamTelemetry },
  signal: AbortSignal,
  callback: (entries: QueueItem<T>[]) => Promise<void>
) {
  signal.addEventListener('abort', async () => {
    if (signal.reason instanceof AbortErrorAndCloseRedisConnection) {
      // console.log("quit redis");
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

    while (queuedEntries.length < batchSize && !timeLimitExceeded) {
      const entries = await getEntries<T>({
        redis,
        stream,
        consumerGroup,
        consumerId,
        blockFor,
        batchSize,
        timeout: timeout * 2,
        telemetry
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

        // Re-throw the error to maintain existing behavior
        throw error
      }
    }

    setTimeout(() => run())
  }

  run()
}
