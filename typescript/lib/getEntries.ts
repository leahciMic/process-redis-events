import type { Redis } from 'ioredis'
import { RedisStreamCursors } from './constants.js'
import { chunk } from './chunk.js'

/** A stream entry read by the consumer. */
export type RawEntry =
  | { id: string; data: unknown; attempts: number; parseError?: undefined }
  | {
      id: string
      /** The entry's raw fields, as an object */
      fields: Record<string, string>
      attempts: number
      /** Set when the entry's `data` field isn't valid JSON */
      parseError: Error
    }

type StreamEntryReply = [string, string[] | null] | null

/**
 * Turn a stream entry's field list into the message data. `add()` writes a
 * single `data` field holding JSON; anything else (e.g. a legacy `id <x>`
 * message) becomes an object of its fields.
 */
export function parseEntry(
  id: string,
  values: string[] | null,
  attempts: number
): RawEntry {
  if (!values || values.length === 0) {
    return { id, data: null, attempts }
  }
  if (values.length === 2 && values[0] === 'data') {
    try {
      return { id, data: JSON.parse(values[1]!), attempts }
    } catch (error) {
      return {
        id,
        fields: { data: values[1]! },
        attempts,
        parseError: error as Error
      }
    }
  }
  return {
    id,
    data: Object.fromEntries(chunk(values, 2)),
    attempts
  }
}

/**
 * Reads the next batch for a consumer: entries idle for longer than
 * `reclaimAfter` (from any consumer of the group) are claimed first; if there
 * are none, new entries are read.
 *
 * `attempts` is the number of earlier deliveries: 0 for the first delivery,
 * 1 for the first retry, ...
 */
export async function getEntries({
  redis,
  stream,
  consumerGroup,
  consumerId,
  blockFor,
  batchSize,
  reclaimAfter,
  signal,
  cursor = { value: RedisStreamCursors.Oldest as string }
}: {
  redis: Redis
  consumerId: string
  reclaimAfter: number
  batchSize: number
  blockFor: number
  stream: string
  consumerGroup: string
  signal: AbortSignal
  /** XAUTOCLAIM scan position, kept between calls */
  cursor?: { value: string }
}): Promise<RawEntry[]> {
  // Without JUSTID, XAUTOCLAIM returns the claimed entries themselves and
  // increments their delivery count.
  const claimed = (await redis.xautoclaim(
    stream,
    consumerGroup,
    consumerId,
    reclaimAfter,
    cursor.value,
    'COUNT',
    batchSize
  )) as [string, StreamEntryReply[], string[]?]

  // Continue the PEL scan where it stopped instead of rescanning from the
  // start (XAUTOCLAIM inspects at most COUNT*10 PEL entries per call, so
  // always starting at 0-0 could starve idle entries behind many in-flight
  // ones). Redis returns 0-0 once the scan wraps around.
  cursor.value = claimed[0]

  // Redis 6.2 returns deleted entries as nil and keeps them in the PEL;
  // Redis >= 7 drops them from the PEL and lists their ids in element 3.
  const deletedIds = [...(claimed[2] ?? [])]
  const claimedEntries: [string, string[]][] = []
  for (const entry of claimed[1]) {
    if (!entry) continue
    if (entry[1] === null) {
      deletedIds.push(entry[0])
    } else {
      claimedEntries.push(entry as [string, string[]])
    }
  }

  if (deletedIds.length > 0) {
    await redis.xack(stream, consumerGroup, ...deletedIds)
  }

  if (claimedEntries.length > 0) {
    // One round trip for every claimed id's delivery count.
    const pipeline = redis.pipeline()
    for (const [id] of claimedEntries) {
      pipeline.xpending(stream, consumerGroup, id, id, 1)
    }
    const results = (await pipeline.exec()) ?? []

    const entries: RawEntry[] = []
    claimedEntries.forEach(([id, values], index) => {
      const [error, reply] = results[index] ?? [null, null]
      if (error) throw error
      const pending = (reply as [string, string, number, number][])[0]
      // Acked or claimed by another consumer in the meantime.
      if (!pending || pending[1] !== consumerId) return
      // XREADGROUP set the count to 1 and every claim adds 1.
      entries.push(parseEntry(id, values, Math.max(Number(pending[3]) - 1, 0)))
    })
    return entries
  }

  if (signal.aborted) {
    return []
  }

  type XReadGroup = [[string, [string, string[]][]]]
  const entries = (await redis.xreadgroup(
    'GROUP',
    consumerGroup,
    consumerId,
    'COUNT',
    batchSize,
    'BLOCK',
    blockFor,
    'STREAMS',
    stream,
    RedisStreamCursors.Latest
  )) as null | XReadGroup

  return (entries?.[0][1] ?? []).map(([id, values]) =>
    parseEntry(id, values, 0)
  )
}
