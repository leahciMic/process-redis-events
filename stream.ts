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

type Json = string | number | boolean | null | Json[] | { [key: string]: Json }

type StreamProgressEvent = {
  type: 'progress'
  completionRatio: number
  status: string
}

type StreamCompletedEvent = {
  type: 'completed'
}

type StreamFailedEvent = {
  type: 'failed'
  error: string
}

type StreamStartedEvent = {
  type: 'started'
}

export type StreamEvent = { id: string } & (
  | StreamProgressEvent
  | StreamCompletedEvent
  | StreamFailedEvent
  | StreamStartedEvent
)

export class Stream<T extends Json> {
  private readonly redis: Redis
  private readonly telemetry: RedisStreamTelemetry

  private readonly streamName: string
  private readonly createRedis = () => new Redis()
  private readonly produceEvents: boolean = false

  constructor(options: {
    streamName: string
    createRedis: () => Redis
    produceEvents?: boolean
    telemetryConfig?: Partial<TelemetryConfig>
  }) {
    this.streamName = options.streamName
    this.createRedis = options.createRedis
    this.produceEvents = options.produceEvents ?? false
    this.redis = this.createRedis()
    this.telemetry = getTelemetry(options.telemetryConfig)
  }

  createEventStream = memoize(this.createEventStreamImpl.bind(this))

  private createEventStreamImpl() {
    return new Stream<StreamEvent>({
      streamName: `${this.streamName}:events`,
      createRedis: this.createRedis,
      produceEvents: false
    })
  }

  async clear() {
    await this.redis.del(this.streamName)
  }

  async add(data: T) {
    const id = await this.redis.xadd(
      this.streamName,
      '*',
      'data',
      JSON.stringify(data)
    )
    assert(typeof id === 'string', 'Failed to add message to stream')
    return { id }
  }

  process(
    options: Omit<processRedisStreamOptions<T>, 'stream'> & {
      telemetryConfig?: Partial<TelemetryConfig>
    },
    callback: (entries: QueueItem<T>[]) => Promise<void>,
    concurrency = 1
  ) {
    return processRedisStream(
      {
        stream: this.streamName,
        telemetry: this.telemetry,
        createRedis: this.createRedis,
        onStart: async (id) => {
          if (this.produceEvents) {
            await this.createEventStream().add({ id, type: 'started' })
          }
        },
        onComplete: async (id) => {
          if (this.produceEvents) {
            await this.createEventStream().add({ id, type: 'completed' })
            // Clean up job metadata after completion
            await this.redis.del(`stream:${this.streamName}:j:${id}`)
          }
        },
        onError: async (id, error) => {
          if (this.produceEvents) {
            await this.createEventStream().add({
              id,
              type: 'failed',
              error: error.message
            })
          }
        },
        onProgress: async (id, completionRatio, status) => {
          if (this.produceEvents) {
            await this.createEventStream().add({
              id,
              type: 'progress',
              completionRatio,
              status
            })
          }
        },
        ...options
      },
      callback,
      concurrency
    )
  }

  // Get telemetry instance for custom metrics
  getTelemetry(): RedisStreamTelemetry {
    return this.telemetry
  }

  // Utility methods for stream monitoring
  async getStreamInfo(): Promise<{
    length: number
    lastGeneratedId: string
    groups: number
  }> {
    const info = (await this.redis.xinfo('STREAM', this.streamName)) as any[]

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
      streamName: this.streamName,
      consumerGroup: 'all' // Special label for overall stream length
    })

    return result
  }

  async getConsumerGroupInfo(consumerGroup: string): Promise<{
    pending: number
    consumers: number
    lastDeliveredId: string
  }> {
    const groups = (await this.redis.xinfo(
      'GROUPS',
      this.streamName
    )) as any[][]

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
      streamName: this.streamName,
      consumerGroup
    })

    return result
  }

  // Get dead letter queue info
  async getDeadLetterQueueInfo(): Promise<{
    length: number
    lastGeneratedId: string
  }> {
    const dlqStreamName = `${this.streamName}-dlq`

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
    const dlqStreamName = `${this.streamName}-dlq`

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
