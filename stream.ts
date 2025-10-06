import { Redis } from 'ioredis'
import { RedisStreamTelemetry, type TelemetryConfig } from './telemetry.js'
import assert from 'node:assert'
import memoize from 'memoize'
import type { QueueItem } from './lib/QueueItem.js'
import { StartFrom, type Json } from './lib/constants.js'
import type { StreamEvent } from './lib/StreamEvent.js'
import { HeartbeatManager } from './lib/HeartbeatManager.js'
import { nanoid } from 'nanoid'
import { createGroup } from './lib/createGroup.js'
import { getEntries } from './lib/getEntries.js'
import pQueue from 'p-queue'

export class Stream<T extends Json, X extends unknown> {
  /** A Redis client instance */
  private readonly redis: Redis
  private readonly telemetry: RedisStreamTelemetry

  /** The name of the stream */
  private readonly name: string
  /** Function to create Redis clients */
  private readonly createRedis = () => new Redis()
  /** Whether to produce event messages for stream processing */
  private readonly produceEvents: boolean = false
  /** Bulk map for efficiently transforming stream data */
  private readonly map: (data: T[]) => Promise<X[]> | X[]

  constructor(options: {
    name: string
    createRedis: () => Redis
    map?: (data: T[]) => X[]
    produceEvents?: boolean
    telemetryConfig?: TelemetryConfig
  }) {
    this.name = options.name
    this.createRedis = options.createRedis
    this.produceEvents = options.produceEvents ?? false
    this.redis = this.createRedis()
    this.telemetry = new RedisStreamTelemetry({
      streamName: this.name,
      consumerGroup: 'all',
      consumerId: 'all',
      ...(options.telemetryConfig ?? {})
    })
    this.map = options.map ?? ((x) => x as unknown as X[])
  }

  createEventStream = memoize(this.createEventStreamImpl.bind(this))

  private createEventStreamImpl() {
    return new Stream<StreamEvent, StreamEvent>({
      name: `${this.name}:events`,
      createRedis: this.createRedis,
      produceEvents: false,
      map: (x) => x
    })
  }

  async clear() {
    await this.redis.del(this.name)
  }

  async add(data: T) {
    const id = await this.redis.xadd(
      this.name,
      '*',
      'data',
      JSON.stringify(data)
    )
    assert(typeof id === 'string', 'Failed to add message to stream')
    return { id }
  }

  private async checkRetry(
    items: QueueItem<T>[],
    shouldRetry: (attempts: number, data: T) => boolean | undefined,
    streamName: string,
    consumerGroup: string,
    consumerId: string
  ): Promise<QueueItem<T>[]> {
    return await Promise.all(
      items.map(async (item) => {
        if (
          shouldRetry &&
          item.attempts > 0 &&
          !shouldRetry(item.attempts, item.data)
        ) {
          // Message exceeded retry limit - move to dead letter queue
          const dlqStreamName = `${streamName}-dlq`

          try {
            // Add to dead letter queue with original data plus metadata
            await this.redis.xadd(
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
              JSON.stringify(item.data)
            )
          } catch (error) {
            // If we can't add to DLQ, log error but still acknowledge to prevent infinite loop
            console.error(
              `Failed to add message to dead letter queue ${dlqStreamName}:`,
              error
            )
          } finally {
            await this.redis.xack(streamName, consumerGroup, item.id)
          }

          return null // Filter out this item
        }
        return item
      })
    ).then((items) =>
      items.filter((item): item is QueueItem<T> => item !== null)
    )
  }

  async process(
    {
      consumerGroup,
      consumerId = nanoid(),
      batchSize = 20,
      startFrom = StartFrom.Latest,
      shouldRetry = () => true,
      signal,
      concurrency = 10
    }: {
      createRedis: () => Redis
      signal: AbortSignal
      stream: string
      consumerGroup: string
      consumerId?: string
      batchSize?: number
      startFrom?: StartFrom
      waitForBatch?: boolean
      shouldRetry?: (attempt: number, entry: T) => boolean
      concurrency?: number
    },
    callback: (queueItem: QueueItem<X>) => Promise<void>
  ) {
    const streamRedis = this.createRedis()
    const redis = this.redis
    const streamName = this.name
    const queue = new pQueue({ concurrency })

    const telemetry = this.telemetry.child({
      consumerGroup,
      consumerId
    })

    const heartbeatManager = new HeartbeatManager(
      redis,
      streamName,
      consumerGroup,
      consumerId
    )

    const eventStream = this.produceEvents ? this.createEventStream() : null

    signal.addEventListener('abort', async () => {
      await streamRedis.quit()
      await redis.quit()
    })

    function createQueueItem({
      id,
      data,
      attempts
    }: {
      id: string
      data: T
      attempts: number
    }): QueueItem<T> {
      heartbeatManager.add(id)

      return {
        id,
        data,
        attempts,
        reportProgress: async (completionRatio: number, status: string) => {
          if (completionRatio < 0 || completionRatio > 1) {
            throw new Error('Percent must be between 0 and 1')
          }
          await eventStream?.add({
            id,
            completionRatio,
            status,
            type: 'progress'
          })
        }
      }
    }

    await createGroup(streamRedis, streamName, consumerGroup, startFrom)

    while (!signal.aborted) {
      await queue.onSizeLessThan(concurrency)

      const entries = await getEntries({
        redis: streamRedis,
        stream: streamName,
        consumerGroup,
        consumerId,
        blockFor: 5_000,
        batchSize,
        timeout: 30_000,
        signal
      })

      const queueItems = await this.checkRetry(
        entries?.map(createQueueItem) ?? [],
        shouldRetry,
        streamName,
        consumerGroup,
        consumerId
      )

      if (signal.aborted) {
        return
      }

      try {
        const map = await this.map(queueItems.map((x) => x.data))

        assert(
          map.length === queueItems.length,
          'Map function returned incorrect number of items'
        )

        const mappedQueueItems = map.map((data, index) => ({
          ...queueItems[index]!,
          data
        }))

        telemetry.recordBatchSize(mappedQueueItems.length)

        queue.addAll(
          mappedQueueItems.map((item) => async () => {
            const timer = telemetry.startTimer()
            try {
              await eventStream?.add({ id: item.id, type: 'started' })
              await callback(item)
              telemetry.recordMessagesProcessed(1, { status: 'success' })
              const duration = timer()
              telemetry.recordProcessingDuration(duration)
              await redis.xack(streamName, consumerGroup, item.id)
              await eventStream?.add({ id: item.id, type: 'completed' })
            } catch (e) {
              await eventStream?.add({
                id: item.id,
                type: 'failed',
                error: (e as Error).message
              })
              telemetry.recordMessagesProcessed(1, { status: 'error' })
            } finally {
              heartbeatManager.remove(item.id)
            }
          }),
          { signal }
        )
      } catch (error) {
        console.error('Error processing entries:', error)
      }
    }
  }
}
