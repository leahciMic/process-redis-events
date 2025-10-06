import type { Redis } from 'ioredis'
import type { RedisStreamTelemetry } from '../telemetry.js'
import type { HeartbeatManager } from './HeartbeatManager.js'
import { RedisStreamCursors } from './constants.js'
import { resultOf } from './resultOf.js'
import { chunk } from './chunk.js'
import type { QueueItem } from './QueueItem.js'
import { parseJson } from './parseJson.js'
import z from 'zod'

export async function getEntries<T>({
  redis,
  globalRedis,
  stream,
  consumerGroup,
  consumerId,
  blockFor,
  batchSize,
  timeout,
  telemetry,
  shouldRetry,
  onError,
  onComplete,
  onProgress,
  signal,
  heartbeatManager
}: {
  redis: Redis
  globalRedis: Redis
  consumerId: string
  timeout: number
  batchSize: number
  blockFor: number
  stream: string
  consumerGroup: string
  telemetry: RedisStreamTelemetry
  onError?: (id: string, error: Error) => Promise<void>
  onComplete?: (id: string) => Promise<void>
  onProgress?: (
    id: string,
    completionRatio: number,
    status: string
  ) => Promise<void>
  shouldRetry?: (attempt: number, entry: T) => boolean
  signal: AbortSignal
  heartbeatManager: HeartbeatManager
}) {
  const claimed = <[string, string[], unknown[]]>(
    await redis.xautoclaim(
      stream,
      consumerGroup,
      consumerId,
      timeout,
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

  const allItems = entries?.[0][1].map(([id, values]) => {
    const value =
      values.length === 0
        ? null
        : values.length === 2
        ? parseJson(values[1])
        : Object.fromEntries(chunk(values, 2))
    const attempts = retryData?.[id] ?? 0

    heartbeatManager.add(id)

    const entry: QueueItem<T> = {
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
        await globalRedis.xack(stream, consumerGroup, id)
        await onComplete?.(id)
      },
      del: async () => {
        heartbeatManager.remove(id)
        await globalRedis.xdel(stream, id)
      }
    }
    return entry
  })

  // Filter out items that shouldn't be retried
  const queueItems = allItems
    ? await Promise.all(
        allItems.map(async (item) => {
          if (
            shouldRetry &&
            item.attempts > 0 &&
            !shouldRetry(item.attempts, item.value)
          ) {
            // Message exceeded retry limit - move to dead letter queue
            const dlqStreamName = `${stream}-dlq`

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
                streamName: stream,
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
    : []

  // Record batch size if we got entries
  if (queueItems && queueItems.length > 0) {
    telemetry.recordBatchSize(queueItems.length, {
      streamName: stream,
      consumerGroup
    })
  }

  return queueItems
}
