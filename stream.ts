import { Redis } from 'ioredis'
import {
  processRedisStream,
  processRedisStreamOptions
} from './processRedisEvents.js'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'
import assert from 'node:assert'
import memoize from 'memoize'
import type { QueueItem } from './lib/QueueItem.js'
import { StartFrom, type Json } from './lib/constants.js'
import type { StreamEvent } from './lib/StreamEvent.js'
import { HeartbeatManager } from './lib/HeartbeatManager.js'
import { nanoid } from 'nanoid'
import { createGroup } from './lib/createGroup.js'
import { getEntries } from './lib/getEntries.js'

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
  private readonly map: (data: T) => X

  constructor(options: {
    streamName: string
    createRedis: () => Redis
    map: (data: T) => X
    produceEvents?: boolean
    telemetryConfig?: Partial<TelemetryConfig>
  }) {
    this.name = options.streamName
    this.createRedis = options.createRedis
    this.produceEvents = options.produceEvents ?? false
    this.redis = this.createRedis()
    this.telemetry = getTelemetry(options.telemetryConfig)
    this.map = options.map
  }

  createEventStream = memoize(this.createEventStreamImpl.bind(this))

  private createEventStreamImpl() {
    return new Stream<StreamEvent, StreamEvent>({
      streamName: `${this.name}:events`,
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

  async process(
    {
      consumerGroup,
      consumerId = nanoid(),
      batchSize = 10,
      timeout = 10_000,
      blockFor = 100,
      startFrom = StartFrom.Latest,
      shouldRetry,
      onComplete,
      onError,
      onStart,
      onProgress,
      telemetry,
      signal
    }: Omit<processRedisStreamOptions<T>, 'stream'> & {
      telemetryConfig?: Partial<TelemetryConfig>
    },
    callback: (entries: QueueItem<T>[]) => Promise<void>
  ) {
    const redis = this.createRedis()

    const heartbeatManager = new HeartbeatManager(
      this.redis,
      this.name,
      consumerGroup,
      consumerId
    )

    signal.addEventListener('abort', async () => {
      await redis.quit()
      await this.redis.quit()
    })

    await createGroup(redis, this.name, consumerGroup, startFrom)

    while (!signal.aborted) {
      const entries = await getEntries({
        redis,
        stream: this.name,
        consumerGroup,
        consumerId,
        blockFor,
        batchSize,
        timeout: timeout * 2,
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
              await this.redis.xack(this.name, consumerGroup, id)
              await onComplete?.(id)
            },
            del: async () => {
              heartbeatManager.remove(id)
              await this.redis.xdel(this.name, id)
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
            const dlqStreamName = `${this.name}-dlq`

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
                streamName: this.name,
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
          streamName: this.name,
          consumerGroup,
          consumerId,
          status: 'success'
        })

        telemetry.recordProcessingDuration(duration, {
          streamName: this.name,
          consumerGroup,
          consumerId
        })
      } catch (error) {
        const duration = timer()

        // Record failed processing
        telemetry.recordMessagesProcessed(queueItems.length, {
          streamName: this.name,
          consumerGroup,
          consumerId,
          status: 'error'
        })

        telemetry.recordProcessingDuration(duration, {
          streamName: this.name,
          consumerGroup,
          consumerId
        })

        telemetry.recordError({
          streamName: this.name,
          consumerGroup,
          consumerId,
          errorType: (error as Error).constructor.name
        })

        console.error('Error processing entries:', error)
      } finally {
        for (const entry of queueItems) {
          heartbeatManager.remove(entry.id)
        }
      }
    }
  }

  // Utility methods for stream monitoring
  async getStreamInfo(): Promise<{
    length: number
    lastGeneratedId: string
    groups: number
  }> {
    const info = (await this.redis.xinfo('STREAM', this.name)) as any[]

    const result = {
      length: 0,
      lastGeneratedId: '0-0',
      groups: 0
    }

    // Redis returns info as an array of key-value pairs
    for (let i = 0; i < info.length; i += 2) {
      const key = info[i]
      const value = info[i + 1]

      switch (key) {
        case 'length':
          result.length = Number(value)
          break
        case 'last-generated-id':
          result.lastGeneratedId = value as string
          break
        case 'groups':
          result.groups = Number(value)
          break
      }
    }

    // Record stream length as pending messages metric
    this.telemetry.recordPendingMessages(result.length, {
      streamName: this.name,
      consumerGroup: 'all' // Special label for overall stream length
    })

    return result
  }

  async getConsumerGroupInfo(consumerGroup: string): Promise<{
    pending: number
    consumers: number
    lastDeliveredId: string
  }> {
    const groups = (await this.redis.xinfo('GROUPS', this.name)) as any[][]

    // Find the specific consumer group
    const groupInfo = groups.find((group: any[]) => {
      for (let i = 0; i < group.length; i += 2) {
        if (group[i] === 'name' && group[i + 1] === consumerGroup) {
          return true
        }
      }
      return false
    })

    if (!groupInfo) {
      throw new Error(`Consumer group '${consumerGroup}' not found`)
    }

    const result = {
      pending: 0,
      consumers: 0,
      lastDeliveredId: '0-0'
    }

    // Parse group info
    for (let i = 0; i < groupInfo.length; i += 2) {
      const key = groupInfo[i]
      const value = groupInfo[i + 1]

      switch (key) {
        case 'pending':
          result.pending = Number(value)
          break
        case 'consumers':
          result.consumers = Number(value)
          break
        case 'last-delivered-id':
          result.lastDeliveredId = value as string
          break
      }
    }

    // Record pending messages for this consumer group
    this.telemetry.recordPendingMessages(result.pending, {
      streamName: this.name,
      consumerGroup
    })

    return result
  }

  // Get dead letter queue info
  async getDeadLetterQueueInfo(): Promise<{
    length: number
    lastGeneratedId: string
  }> {
    const dlqStreamName = `${this.name}-dlq`

    try {
      const info = (await this.redis.xinfo('STREAM', dlqStreamName)) as any[]

      const result = {
        length: 0,
        lastGeneratedId: '0-0'
      }

      // Parse stream info
      for (let i = 0; i < info.length; i += 2) {
        const key = info[i]
        const value = info[i + 1]

        switch (key) {
          case 'length':
            result.length = Number(value)
            break
          case 'last-generated-id':
            result.lastGeneratedId = value as string
            break
        }
      }

      return result
    } catch (error) {
      // Dead letter queue doesn't exist yet
      return {
        length: 0,
        lastGeneratedId: '0-0'
      }
    }
  }

  // Read messages from dead letter queue
  async getDeadLetterMessages(count = 10): Promise<
    Array<{
      id: string
      originalId: string
      attempts: number
      consumerGroup: string
      consumerId: string
      timestamp: string
      data: any
    }>
  > {
    const dlqStreamName = `${this.name}-dlq`

    try {
      const messages = await this.redis.xrevrange(
        dlqStreamName,
        '+',
        '-',
        'COUNT',
        count
      )

      return messages.map(([id, fields]: [string, string[]]) => {
        const fieldMap: Record<string, string> = {}
        for (let i = 0; i < fields.length - 1; i += 2) {
          const key = fields[i]
          const value = fields[i + 1]
          if (key && value) {
            fieldMap[key] = value
          }
        }

        return {
          id,
          originalId: fieldMap['originalId'] || '',
          attempts: Number(fieldMap['attempts'] || 0),
          consumerGroup: fieldMap['consumerGroup'] || '',
          consumerId: fieldMap['consumerId'] || '',
          timestamp: fieldMap['timestamp'] || '',
          data: fieldMap['data'] ? JSON.parse(fieldMap['data']) : null
        }
      })
    } catch (error) {
      // Dead letter queue doesn't exist yet
      return []
    }
  }
}
