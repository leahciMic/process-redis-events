import type { Redis } from 'ioredis'
import type { StartFrom } from './constants.js'

export async function createGroup(
  redis: Redis,
  stream: string,
  consumerGroup: string,
  startFrom: StartFrom
) {
  try {
    await redis.xgroup('CREATE', stream, consumerGroup, startFrom, 'MKSTREAM')
  } catch (err) {
    if (err instanceof Error && /BUSYGROUP/.test(err.message)) {
      // continue
    } else {
      throw err
    }
  }
}
