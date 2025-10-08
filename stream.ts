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

type Mapped<M> = M extends (a: unknown[]) => readonly (infer R)[] ? R : never

export class Stream<
  T extends Json,
  MapFn extends (data: T[]) => Promise<readonly unknown[]> | unknown[] = (
    data: T[]
  ) => T[]
> {
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
  private readonly map: MapFn

  private readonly streamKey: string
  private readonly dlqStreamKey: string

  constructor(options: {
    name: string
    createRedis: () => Redis
    map?: MapFn
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
    this.map = (options.map ?? ((xs: T[]) => xs)) as MapFn
    this.streamKey = `stream:${this.name}`
    this.dlqStreamKey = `stream:dlq:${this.name}`
  }

  createEventStream = memoize(this.createEventStreamImpl.bind(this))

  private createEventStreamImpl() {
    return new Stream<StreamEvent>({
      name: `${this.name}:events`,
      createRedis: this.createRedis,
      produceEvents: false,
      map: (x) => x
    })
  }

  async clear() {
    await this.redis.del(this.streamKey)
  }

  async add(data: T) {
    const id = await this.redis.xadd(
      this.streamKey,
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

          try {
            // Add to dead letter queue with original data plus metadata
            await this.redis.xadd(
              this.dlqStreamKey,
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
              `Failed to add message to dead letter queue ${this.dlqStreamKey}:`,
              error
            )
          } finally {
            await this.redis.xack(this.streamKey, consumerGroup, item.id)
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
      concurrency = 10,
      /** Lease duration for processing messages, leases are auto-extended */
      leaseMs = 30_000
    }: {
      signal: AbortSignal
      consumerGroup: string
      consumerId?: string
      batchSize?: number
      startFrom?: StartFrom
      shouldRetry?: (attempt: number, entry: T) => boolean
      concurrency?: number
      leaseMs?: number
    },
    callback: (queueItem: QueueItem<Mapped<MapFn>>) => Promise<void>
  ) {
    const streamRedis = this.createRedis()
    const redis = this.redis
    const queue = new pQueue({ concurrency })
    const heartbeat = Math.max(leaseMs / 2, 5_000)
    const xclaimBlockTimeout = Math.max(leaseMs - 1_000, 100)
    const reclaimAfter = leaseMs + 100

    const telemetry = this.telemetry.child({
      consumerGroup,
      consumerId
    })

    const heartbeatManager = new HeartbeatManager(
      redis,
      this.streamKey,
      consumerGroup,
      consumerId,
      heartbeat
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

    await createGroup(streamRedis, this.streamKey, consumerGroup, startFrom)

    while (!signal.aborted) {
      await queue.onSizeLessThan(concurrency)

      const entries = await getEntries({
        redis: streamRedis,
        stream: this.streamKey,
        consumerGroup,
        consumerId,
        blockFor: xclaimBlockTimeout,
        batchSize,
        reclaimAfter,
        signal
      })

      console.log(`Fetched ${entries?.length ?? 0} entries from stream`)

      const queueItems = await this.checkRetry(
        entries?.map(createQueueItem) ?? [],
        shouldRetry,
        consumerGroup,
        consumerId
      )

      console.log(`Checked ${queueItems.length} queue items for retries`)

      if (signal.aborted) {
        return
      }

      try {
        const mapped = await this.map(queueItems.map((x) => x.data))

        assert(
          mapped.length === queueItems.length,
          'Map function returned incorrect number of items'
        )

        const mappedQueueItems: QueueItem<Mapped<MapFn>>[] = mapped.map(
          (data, index) => ({
            ...queueItems[index]!,
            data: data as Mapped<MapFn>
          })
        )

        telemetry.recordBatchSize(mappedQueueItems.length)

        queue.addAll(
          mappedQueueItems.map((item) => async () => {
            const timer = telemetry.startTimer()
            try {
              await eventStream?.add({ id: item.id, type: 'started' })
              console.log('calling callback for item', item.id, item.data)
              await callback(item)
              telemetry.recordMessagesProcessed(1, { status: 'success' })
              const duration = timer()
              telemetry.recordProcessingDuration(duration)
              await redis.xack(this.streamKey, consumerGroup, item.id)
              await eventStream?.add({ id: item.id, type: 'completed' })
            } catch (e) {
              await eventStream?.add({
                id: item.id,
                type: 'failed',
                error: (e as Error).message
              })
              telemetry.recordMessagesProcessed(1, { status: 'error' })
              console.log(`Error processing item ${item.id}:`, e)
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
