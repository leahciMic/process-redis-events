import { Redis } from 'ioredis'
import {
  QueueItem,
  processRedisStream,
  processRedisStreamOptions
} from './processRedisEvents.js'
type Json = string | number | boolean | null | Json[] | { [key: string]: Json }
export class Stream<T extends Json> {
  private redis: Redis
  constructor(
    private readonly streamName: string,
    public readonly createRedis = () => new Redis()
  ) {
    this.redis = this.createRedis()
  }

  async add(data: T) {
    await this.redis.xadd(this.streamName, '*', 'data', JSON.stringify(data))
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
