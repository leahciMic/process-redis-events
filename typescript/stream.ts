import { Redis } from 'ioredis'
import { RedisStreamTelemetry, type TelemetryConfig } from './telemetry.js'
import assert from 'node:assert'
import memoize from 'memoize'
import type { QueueItem } from './lib/QueueItem.js'
import { StartFrom } from './lib/constants.js'
import type { StreamEvent } from './lib/StreamEvent.js'
import { HeartbeatManager } from './lib/HeartbeatManager.js'
import { nanoid } from 'nanoid'
import { createGroup } from './lib/createGroup.js'
import { getEntries } from './lib/getEntries.js'
import pQueue from 'p-queue'
import z from 'zod'
import { chunk } from './lib/chunk.js'
import { resultOf } from './lib/resultOf.js'

type MaybePromise<T> = T | Promise<T>

const RedisGroupSchema = z.object({
  name: z.string(),
  consumers: z.number(),
  pending: z.number(),
  entriesRead: z.number(),
  lag: z.number()
})

export class Stream<T extends Record<string, unknown>> {
  /** A Redis client instance */
  private readonly redis: Redis
  private readonly telemetry: RedisStreamTelemetry

  /** The name of the stream */
  public readonly name: string
  /** Function to create Redis clients */
  private readonly createRedis = () => new Redis()
  /** Whether to produce event messages for stream processing */
  private readonly produceEvents: boolean = false
  /** Whether to use dead letter queue */
  private readonly useDLQ: boolean = true

  private readonly streamKey: string
  private readonly dlqStreamKey: string
  private readonly eventsStreamKey: string

  constructor(options: {
    name: string
    createRedis: () => Redis
    produceEvents?: boolean
    useDLQ?: boolean
    telemetryConfig?: TelemetryConfig
  }) {
    this.name = options.name
    this.createRedis = options.createRedis
    this.produceEvents = options.produceEvents ?? false
    this.useDLQ = options.useDLQ ?? true
    this.redis = this.createRedis()
    this.telemetry = new RedisStreamTelemetry({
      streamName: this.name,
      consumerGroup: 'all',
      consumerId: 'all',
      ...(options.telemetryConfig ?? {})
    })
    this.streamKey = `stream:${this.name}`
    this.dlqStreamKey = `dlq:${this.name}`
    this.eventsStreamKey = `events:${this.name}`
  }

  async sendStreamTelemetry() {
    /**
     * Collect the stream length, consumer groups info, DLQ length, and send telemetry
     */
    try {
      const streamLength = await this.getStreamLength()
      this.telemetry.recordStreamLength(streamLength ?? 0)
    } catch (error) {
      console.error(error, 'Error getting stream length for telemetry')
    }

    try {
      const streamInfo = await this.getStreamInfo()
      this.telemetry.recordEntriesAdded(streamInfo?.entriesAdded ?? 0)
    } catch (error) {
      console.error(error, 'Error getting stream info for telemetry')
    }

    try {
      const consumerGroupsInfo = await this.getConsumerGroupsInfo()

      for (const groupInfo of consumerGroupsInfo) {
        this.telemetry.recordPendingMessages(groupInfo.pending, groupInfo.name)
        this.telemetry.recordEntriesRead(groupInfo.entriesRead, groupInfo.name)
        this.telemetry.recordLag(groupInfo.lag, groupInfo.name)
      }
    } catch (error) {
      console.error(error, 'Error getting consumer groups info for telemetry')
    }

    if (!this.name.startsWith('dlq:') && !this.name.startsWith('events:')) {
      // Don't send telemetry for DLQ or events streams, the parent stream will handle it
      await this.createDeadLetterQueueStream().sendStreamTelemetry()
      await this.createEventStream().sendStreamTelemetry()
    }
  }

  public async getStreamLength() {
    try {
      return await this.redis.xlen(this.streamKey)
    } catch (err) {
      if (err instanceof Error && err.message.includes('no such key')) {
        return 0
      }
      throw err
    }
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
      throw infoErr
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
      if (groupsReplyErr.message.includes('no such key')) {
        return []
      }

      throw groupsReplyErr
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
      name: this.eventsStreamKey,
      createRedis: this.createRedis,
      produceEvents: false,
      useDLQ: false
    })
  }

  createDeadLetterQueueStream = memoize(
    this.createDeadLetterQueueStreamImpl.bind(this)
  )

  private createDeadLetterQueueStreamImpl() {
    return new Stream<T>({
      name: this.dlqStreamKey,
      createRedis: this.createRedis,
      useDLQ: false,
      produceEvents: false
    })
  }

  async clear() {
    await this.redis.del(this.streamKey).catch(() => {})
    // await this.redis.del(this.dlqStreamKey).catch(() => {})

    // const eventStream = this.createEventStream()
    // await eventStream.clear().catch(() => {})
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
            if (this.produceEvents) {
              const eventStream = this.createEventStream()
              await eventStream.add({
                id: item.id,
                type: 'failed',
                error: 'Exceeded retry limit'
              })
            }

            if (this.useDLQ) {
              this.createDeadLetterQueueStream().add({
                ...item.data,
                _dlqMetadata: {
                  originalId: item.id,
                  attempts: item.attempts,
                  consumerGroup,
                  consumerId,
                  timestamp: Date.now()
                }
              })
            }
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
    options: {
      signal: AbortSignal
      consumerGroup: string
      consumerId?: string
      batchSize?: number
      startFrom?: StartFrom
      map?: undefined
      shouldRetry?: (attempt: number, entry: T) => boolean
      concurrency?: number
      leaseMs?: number
    },
    callback: (queueItem: QueueItem<T>) => Promise<void>
  ): Promise<void>

  async process<R>(
    options: {
      signal: AbortSignal
      consumerGroup: string
      consumerId?: string
      batchSize?: number
      startFrom?: StartFrom
      map: (data: readonly T[]) => MaybePromise<readonly R[]>
      shouldRetry?: (attempt: number, entry: T) => boolean
      concurrency?: number
      leaseMs?: number
    },
    callback: (queueItem: QueueItem<R>) => Promise<void>
  ): Promise<void>

  async process<R>(
    {
      consumerGroup,
      consumerId = nanoid(),
      batchSize = 20,
      startFrom = StartFrom.Latest,
      shouldRetry = () => true,
      signal,
      concurrency = 10,
      map,
      /** Lease duration for processing messages, leases are auto-extended */
      leaseMs = 30_000
    }: {
      signal: AbortSignal
      consumerGroup: string
      consumerId?: string
      batchSize?: number
      startFrom?: StartFrom
      map?: (data: readonly any[]) => MaybePromise<readonly any[]>
      shouldRetry?: (attempt: number, entry: T) => boolean
      concurrency?: number
      leaseMs?: number
    },
    callback: (queueItem: QueueItem<R>) => Promise<void>
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
        const mapped = map
          ? await map(queueItems.map((x) => x.data))
          : queueItems.map((x) => x.data)

        assert(
          mapped.length === queueItems.length,
          'Map function returned incorrect number of items'
        )

        const mappedQueueItems: QueueItem<R>[] = mapped.map((data, index) => ({
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
