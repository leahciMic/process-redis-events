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
import { getEntries, type RawEntry } from './lib/getEntries.js'
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

  /**
   * Moves the items that `shouldRetry` rejects to the dead letter queue (when
   * enabled) and acks them; returns the rest. An item is only acked once its
   * DLQ write succeeded: if that fails it stays pending and is retried after
   * the lease.
   */
  private async checkRetry(
    items: QueueItem<T>[],
    shouldRetry: (attempts: number, data: T) => boolean | undefined,
    consumerGroup: string,
    consumerId: string,
    release: (id: string) => void
  ): Promise<QueueItem<T>[]> {
    const kept = await Promise.all(
      items.map(async (item) => {
        let retry: boolean | undefined = true
        try {
          retry = item.attempts === 0 || shouldRetry(item.attempts, item.data)
        } catch (error) {
          console.error(
            `shouldRetry threw for message ${item.id} in ${this.streamKey}; retrying it:`,
            error
          )
        }
        if (retry) return item

        // Message exceeded retry limit - move to dead letter queue
        try {
          await this.moveToDeadLetterQueue(item.id, consumerGroup, () =>
            this.useDLQ
              ? this.createDeadLetterQueueStream().add({
                  ...item.data,
                  _dlqMetadata: {
                    originalId: item.id,
                    attempts: item.attempts,
                    consumerGroup,
                    consumerId,
                    timestamp: Date.now()
                  }
                })
              : undefined
          )
          await this.addEventSafely({
            id: item.id,
            type: 'failed',
            error: 'Exceeded retry limit'
          })
        } catch (error) {
          // Leave it pending: it's reclaimed and checked again after the lease.
          console.error(
            `Failed to move message ${item.id} to dead letter queue ${this.dlqStreamKey}; it stays pending and will be retried:`,
            error
          )
        } finally {
          release(item.id)
        }
        return null // Filter out this item
      })
    )
    return kept.filter((item): item is QueueItem<T> => item !== null)
  }

  /** Writes to the DLQ (if `write` does anything), then acks the message. */
  private async moveToDeadLetterQueue(
    id: string,
    consumerGroup: string,
    write: () => Promise<unknown> | undefined
  ) {
    await write()
    await this.redis.xack(this.streamKey, consumerGroup, id)
  }

  /** Event messages are best-effort: a failure is logged, never thrown. */
  private async addEventSafely(event: StreamEvent) {
    if (!this.produceEvents) return
    try {
      await this.createEventStream().add(event)
    } catch (error) {
      console.error(`Failed to add event to ${this.eventsStreamKey}:`, error)
    }
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
      onError?: (error: unknown, queueItem: QueueItem<T>) => void
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
      onError?: (error: unknown, queueItem: QueueItem<R>) => void
    },
    callback: (queueItem: QueueItem<R>) => Promise<void>
  ): Promise<void>

  /**
   * Processes the stream until `signal` aborts. On abort it stops reading,
   * waits for the callbacks in flight to finish (and ack), then closes its
   * Redis connections and resolves.
   *
   * `attempts` on a queue item (and the first argument of `shouldRetry`) is
   * the number of earlier deliveries: 0 on the first delivery, 1 on the first
   * retry. `shouldRetry` isn't called on the first delivery.
   *
   * A callback that throws is reported to `onError` (default: console.error)
   * and the message is retried after `leaseMs`.
   */
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
      leaseMs = 30_000,
      onError = (error, queueItem) => {
        console.error(
          `Error processing message ${queueItem.id} from ${this.streamKey}:`,
          error
        )
      }
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
      onError?: (error: unknown, queueItem: QueueItem<any>) => void
    },
    callback: (queueItem: QueueItem<R>) => Promise<void>
  ) {
    const streamRedis = this.createRedis()
    const redis = this.redis
    const queue = new pQueue({ concurrency })
    const heartbeat = Math.round(leaseMs / 2)
    const xclaimBlockTimeout = Math.max(leaseMs - 1_000, 100)
    const reclaimAfter = leaseMs + 100
    const cursor = { value: '0-0' }

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
    const release = (id: string) => heartbeatManager.remove(id)

    const eventStream = this.produceEvents ? this.createEventStream() : null

    // Interrupt a blocking read right away; entries it may have just read
    // stay pending and are reclaimed after the lease.
    const onAbort = () => streamRedis.disconnect()
    signal.addEventListener('abort', onAbort, { once: true })

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

    const safeOnError = (error: unknown, item: QueueItem<R>) => {
      try {
        onError(error, item)
      } catch (onErrorError) {
        console.error('onError threw:', onErrorError)
      }
    }

    const processItem = async (item: QueueItem<R>) => {
      const timer = telemetry.startTimer()
      try {
        await eventStream?.add({ id: item.id, type: 'started' })
        await callback(item)
      } catch (e) {
        // Not acked: the message is retried after the lease.
        heartbeatManager.remove(item.id)
        telemetry.recordMessagesProcessed(1, { status: 'error' })
        safeOnError(e, item)
        await this.addEventSafely({
          id: item.id,
          type: 'failed',
          error: (e as Error)?.message ?? String(e)
        })
        return
      }
      telemetry.recordMessagesProcessed(1, { status: 'success' })
      telemetry.recordProcessingDuration(timer())
      try {
        await redis.xack(this.streamKey, consumerGroup, item.id)
      } catch (e) {
        // The message is processed again after the lease.
        console.error(
          `Failed to ack message ${item.id} in ${this.streamKey}:`,
          e
        )
        return
      } finally {
        heartbeatManager.remove(item.id)
      }
      await this.addEventSafely({ id: item.id, type: 'completed' })
    }

    const deadLetterUnparseable = async (
      entry: Extract<RawEntry, { parseError: Error }>
    ) => {
      try {
        await this.moveToDeadLetterQueue(entry.id, consumerGroup, () => {
          if (!this.useDLQ) {
            console.error(
              `Dropping message ${entry.id} from ${this.streamKey}: it can't be parsed:`,
              entry.parseError
            )
            return undefined
          }
          console.error(
            `Moving message ${entry.id} from ${this.streamKey} to ${this.dlqStreamKey}: it can't be parsed:`,
            entry.parseError
          )
          return this.createDeadLetterQueueStream().add({
            _raw: entry.fields,
            _dlqMetadata: {
              originalId: entry.id,
              attempts: entry.attempts,
              consumerGroup,
              consumerId,
              timestamp: Date.now(),
              error: entry.parseError.message
            }
          } as unknown as T)
        })
      } catch (error) {
        console.error(
          `Failed to move unparseable message ${entry.id} to ${this.dlqStreamKey}; it stays pending and will be retried:`,
          error
        )
      }
    }

    let backoffMs = 0
    const sleep = (ms: number) =>
      new Promise<void>((resolve) => {
        const timeout = setTimeout(done, ms)
        function done() {
          clearTimeout(timeout)
          signal.removeEventListener('abort', done)
          resolve()
        }
        signal.addEventListener('abort', done, { once: true })
      })

    try {
      while (!signal.aborted) {
        try {
          await createGroup(streamRedis, this.streamKey, consumerGroup, startFrom)
          break
        } catch (error) {
          if (signal.aborted) break
          backoffMs = Math.min(Math.max(backoffMs * 2, 100), 5_000)
          console.error(
            `Error creating consumer group ${consumerGroup} on ${this.streamKey}, retrying in ${backoffMs}ms:`,
            error
          )
          await sleep(backoffMs)
        }
      }
      backoffMs = 0

      while (!signal.aborted) {
        let batch: QueueItem<T>[] = []
        try {
          await queue.onSizeLessThan(concurrency)
          if (signal.aborted) break

          const entries = await getEntries({
            redis: streamRedis,
            stream: this.streamKey,
            consumerGroup,
            consumerId,
            blockFor: xclaimBlockTimeout,
            batchSize,
            reclaimAfter,
            signal,
            cursor
          })
          backoffMs = 0

          if (signal.aborted) {
            // Not started: they stay pending and are reclaimed after the lease.
            break
          }

          const parsed: QueueItem<T>[] = []
          for (const entry of entries) {
            if (entry.parseError) {
              await deadLetterUnparseable(entry)
            } else {
              parsed.push(
                createQueueItem({
                  id: entry.id,
                  data: entry.data as T,
                  attempts: entry.attempts
                })
              )
            }
          }
          batch = parsed

          batch = await this.checkRetry(
            batch,
            shouldRetry,
            consumerGroup,
            consumerId,
            release
          )

          if (batch.length === 0) continue

          const mapped = map
            ? await map(batch.map((x) => x.data))
            : batch.map((x) => x.data)

          assert(
            mapped.length === batch.length,
            'Map function returned incorrect number of items'
          )

          const mappedQueueItems: QueueItem<R>[] = mapped.map(
            (data, index) => ({
              ...batch[index]!,
              data
            })
          )
          batch = []

          telemetry.recordBatchSize(mappedQueueItems.length)

          for (const item of mappedQueueItems) {
            void queue.add(() => processItem(item))
          }
        } catch (error) {
          // A batch that wasn't handed to the queue (e.g. map threw) is
          // retried after the lease.
          for (const item of batch) heartbeatManager.remove(item.id)
          if (batch.length > 0) {
            telemetry.recordMessagesProcessed(batch.length, { status: 'error' })
          }
          if (signal.aborted) break

          if (error instanceof Error && /NOGROUP/.test(error.message)) {
            // The stream or group was deleted: recreate it, as a restart would.
            await createGroup(
              streamRedis,
              this.streamKey,
              consumerGroup,
              startFrom
            ).catch(() => {})
          }
          backoffMs = Math.min(Math.max(backoffMs * 2, 100), 5_000)
          console.error(
            `Error processing entries from ${this.streamKey}, retrying in ${backoffMs}ms:`,
            error
          )
          await sleep(backoffMs)
        }
      }
    } finally {
      signal.removeEventListener('abort', onAbort)
      await queue.onIdle()
      heartbeatManager.close()
      streamRedis.disconnect()
      await redis.quit().catch(() => {})
    }
  }
}
