import { Redis } from 'ioredis'
import {
  QueueItem,
  processRedisStream,
  processRedisStreamOptions
} from './processRedisEvents.js'
import {
  RedisStreamTelemetry,
  getTelemetry,
  type TelemetryConfig
} from './telemetry.js'
import assert from 'node:assert'
import pDefer from 'p-defer'
type Json = string | number | boolean | null | Json[] | { [key: string]: Json }
export class Stream<T extends Json> {
  private redis: Redis
  private telemetry: RedisStreamTelemetry

  constructor(
    private readonly streamName: string,
    public readonly createRedis = () => new Redis(),
    telemetryConfig?: Partial<TelemetryConfig>
  ) {
    this.redis = this.createRedis()
    this.telemetry = getTelemetry(telemetryConfig)
  }

  async clear() {
    await this.redis.del(this.streamName)
  }

  async percent(jobId: string) {
    const percent = await this.redis.hget(
      `stream:${this.streamName}:j:${jobId}`,
      'percent'
    )
    return percent ? Number(percent) || 0 : 0
  }

  async *percentStream(jobId: string) {
    const sub = this.createRedis()
    const channel = `stream:${this.streamName}:j:${jobId}:percent`

    let waiting = pDefer()
    const queue: number[] = []

    sub.on('message', (_chan, message) => {
      if (_chan === channel) {
        const percent = Number(message)
        queue.push(percent)
        waiting.resolve()
      }
    })

    await sub.subscribe(channel)
    try {
      while (true) {
        if (queue.length === 0) {
          waiting = pDefer()
          await waiting.promise
        }
        while (queue.length > 0) {
          yield queue.shift()!
        }
      }
    } finally {
      await sub.unsubscribe(channel)
      sub.removeAllListeners('message')
      sub.quit()
    }
  }

  async reportPercent(jobId: string, percent: number) {
    await this.redis.hset(
      `stream:${this.streamName}:j:${jobId}`,
      'percent',
      percent.toString()
    )
    this.redis.publish(
      `stream:${this.streamName}:j:${jobId}:percent`,
      percent.toString()
    )
  }

  async add(data: T) {
    const id = await this.redis.xadd(
      this.streamName,
      '*',
      'data',
      JSON.stringify(data)
    )
    assert(typeof id === 'string', 'Failed to add message to stream')
    return {
      id,
      percent: this.percent.bind(this, id)
    }
  }

  process(
    options: Omit<processRedisStreamOptions<T>, 'stream'> & {
      telemetryConfig?: Partial<TelemetryConfig>
    },
    callback: (entries: QueueItem<T>[]) => Promise<void>
  ) {
    return processRedisStream(
      {
        stream: this.streamName,
        telemetry: this.telemetry,
        ...options
      },
      callback,
      1,
      this.createRedis
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
