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
import z from 'zod'
import { chunk } from './lib/chunk.js'
import { resultOf } from './lib/resultOf.js'

const RedisGroupSchema = z.object({
  name: z.string(),
  consumers: z.number(),
  pending: z.number(),
  entriesRead: z.number(),
  lag: z.number()
})

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

  public async getStreamLength() {
    return await this.redis.xlen(this.streamKey)
  }

  public async getStreamInfo() {
    const [infoErr, info] = await resultOf(
      this.redis.xinfo('STREAM', this.streamKey) as Promise<
        Array<string | number>
      >
    )
    if (infoErr) {
      if (
        (infoErr as Error).message.includes('ERR no such key') ||
        (infoErr as Error).message.includes('NOKEY')
      ) {
        return null // Stream does not exist
      }
      console.error(
        `Error fetching info for stream ${this.streamKey}:`,
        infoErr
      )
      return null
    }

    const obj: Record<string, unknown> = Object.fromEntries(chunk(info, 2))

    const RedisStreamInfoSchema = z.object({
      length: z.number(),
      groups: z.number(),
      entriesAdded: z.number().optional()
    })

    return RedisStreamInfoSchema.parse({
      length: Number(obj['length']),
      groups: Number(obj['groups']),
      entriesAdded: obj['entries-added'] as number | undefined
    })
  }

  public async getConsumerGroupsInfo() {
    const [groupsReplyErr, groupsReply] = await resultOf(
      this.redis.xinfo('GROUPS', this.streamKey) as Promise<
        Array<Array<string | number>>
      >
    )
    if (groupsReplyErr) {
      console.error(
        `Error fetching consumer groups for stream ${this.streamKey}:`,
        groupsReplyErr
      )
      return []
    }

    const parsed = groupsReply.map((entry) => {
      const obj: Record<string, unknown> = Object.fromEntries(chunk(entry, 2))

      return RedisGroupSchema.parse({
        name: obj['name'],
        consumers: Number(obj['consumers']),
        pending: Number(obj['pending']),
        entriesRead: Number(obj['entries-read']),
        lag: Number(obj['lag'])
      })
    })

    return parsed
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
    const heartbeat = Math.round(leaseMs / 2)
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

      const queueItems = await this.checkRetry(
        entries?.map(createQueueItem) ?? [],
        shouldRetry,
        consumerGroup,
        consumerId
      )

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
            } finally {
              heartbeatManager.remove(item.id)
            }
          })
          // { signal }
        )
      } catch (error) {
        console.error('Error processing entries:', error)
      }
    }
  }
}
