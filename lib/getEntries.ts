import type { Redis } from 'ioredis'
import { RedisStreamCursors } from './constants.js'
import { resultOf } from './resultOf.js'
import { chunk } from './chunk.js'
import { parseJson } from './parseJson.js'
import z from 'zod'

export async function getEntries({
  redis,
  stream,
  consumerGroup,
  consumerId,
  blockFor,
  batchSize,
  reclaimAfter,
  signal
}: {
  redis: Redis
  consumerId: string
  reclaimAfter: number
  batchSize: number
  blockFor: number
  stream: string
  consumerGroup: string
  signal: AbortSignal
}) {
  const claimed = <[string, string[], unknown[]]>(
    await redis.xautoclaim(
      stream,
      consumerGroup,
      consumerId,
      reclaimAfter,
      RedisStreamCursors.Oldest,
      'COUNT',
      batchSize,
      'JUSTID'
    )
  )

  if (signal.aborted) {
    return []
  }

  const claimedIds = claimed[1]
  const claimedCount = claimedIds.length

  const xPendingInfo =
    claimedIds.length > 0
      ? await Promise.all(
          claimedIds.map(async (id) => {
            const pendingResponse = await redis.xpending(
              stream,
              consumerGroup,
              id,
              id,
              1
            )
            const pending = z
              .tuple([
                z.tuple([z.string(), z.string(), z.number(), z.number()])
              ])
              .parse(pendingResponse)

            return {
              id: pending[0][0],
              consumerId: pending[0][1],
              idleTime: pending[0][2],
              attempts: Number(pending[0][3])
            }
          })
        )
      : []

  const retryData = Object.fromEntries(
    xPendingInfo.map(({ id, attempts }) => [id, attempts])
  )

  if (signal.aborted) {
    return []
  }

  type XReadGroup = [[string, [string, string[]][]]]
  const [getEntriesError, entries] = await resultOf(
    <Promise<null | XReadGroup>>(
      redis.xreadgroup(
        'GROUP',
        consumerGroup,
        consumerId,
        'COUNT',
        claimedCount || batchSize,
        'BLOCK',
        blockFor,
        'STREAMS',
        stream,
        claimedCount > 0 ? RedisStreamCursors.Oldest : RedisStreamCursors.Latest
      )
    )
  )

  if (signal.aborted) {
    return []
  }

  if (getEntriesError) {
    throw getEntriesError
  }

  const queueItems = entries?.[0][1].map(([id, values]) => {
    const data =
      values.length === 0
        ? null
        : values.length === 2
        ? parseJson(values[1])
        : Object.fromEntries(chunk(values, 2))
    const attempts = retryData?.[id] ?? 0

    return {
      id,
      data,
      attempts
    }
  })

  return queueItems
}
