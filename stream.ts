import { Redis } from 'ioredis'
import {
  QueueItem,
  processRedisStream,
  processRedisStreamOptions
} from './processRedisEvents.js'

export class Stream<T extends Record<string, string>> {
  private redis: Redis
  constructor(
    private readonly streamName: string,
    public readonly createRedis = () => new Redis()
  ) {
    this.redis = this.createRedis()
  }

  async add(data: T) {
    const dataArr = Object.entries(data).flat()
    await this.redis.xadd(this.streamName, '*', ...dataArr)
  }

  process(
    options: Omit<processRedisStreamOptions<T>, 'stream'>,
    callback: (entries: QueueItem<T>[]) => Promise<void>
  ) {
    return processRedisStream(
      {
        stream: this.streamName,
        ...options
      },
      callback,
      1,
      this.createRedis
    )
  }
}
