import type { Redis } from 'ioredis'

/**
 * Resets the idle time of the ids still owned by this consumer and returns
 * the ids it no longer owns (acked, or claimed by another consumer after a
 * lease expired). A plain `XCLAIM ... IDLE 0` would take a message back from
 * the consumer that reclaimed it, and both would then process it. Same script
 * as the Python library's entries.HEARTBEAT_LUA.
 */
export const HEARTBEAT_LUA = `
local key, group, consumer = KEYS[1], ARGV[1], ARGV[2]
local lost = {}
for i = 3, #ARGV do
  local p = redis.call('XPENDING', key, group, ARGV[i], ARGV[i], 1)
  if p[1] and p[1][2] == consumer then
    redis.call('XCLAIM', key, group, consumer, 0, ARGV[i], 'IDLE', 0, 'JUSTID')
  else
    lost[#lost + 1] = ARGV[i]
  end
end
return lost
`

export class HeartbeatManager {
  private intervalId: NodeJS.Timeout | null = null
  private jobIds: Set<string> = new Set()

  constructor(
    private readonly redis: Redis,
    private readonly stream: string,
    private readonly consumerGroup: string,
    private readonly consumerId: string,
    heartbeatIntervalMs = 15_000
  ) {
    this.intervalId = setInterval(() => {
      void this.sendHeartbeats()
    }, heartbeatIntervalMs)
  }

  close() {
    if (this.intervalId) {
      clearInterval(this.intervalId)
      this.intervalId = null
    }
    this.jobIds.clear()
  }

  /** Resolves with the ids in progress that this consumer no longer owns. */
  async sendHeartbeats(): Promise<string[]> {
    const ids = [...this.jobIds]
    const lost: string[] = []
    for (let start = 0; start < ids.length; start += 500) {
      try {
        const reply = (await this.redis.eval(
          HEARTBEAT_LUA,
          1,
          this.stream,
          this.consumerGroup,
          this.consumerId,
          ...ids.slice(start, start + 500)
        )) as string[] | null
        lost.push(...(reply ?? []))
      } catch (err) {
        console.error('Error sending heartbeats:', err)
      }
    }
    if (lost.length > 0) {
      console.error(
        `${lost.length} message(s) in progress on ${this.stream} were acknowledged or reclaimed by another consumer: ${lost.slice(0, 5).join(', ')}`
      )
    }
    return lost
  }
  add(jobId: string) {
    this.jobIds.add(jobId)
  }
  remove(jobId: string) {
    this.jobIds.delete(jobId)
  }
}
