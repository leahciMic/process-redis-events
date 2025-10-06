import type { Redis } from 'ioredis'

export class HeartbeatManager {
  private intervalId: NodeJS.Timeout | null = null
  private jobIds: Set<string> = new Set()

  constructor(
    private readonly redis: Redis,
    private readonly stream: string,
    private readonly consumerGroup: string,
    private readonly consumerId: string,
    intervalMs = 30_000
  ) {
    this.intervalId = setInterval(() => {
      this.sendHeartbeats()
    }, intervalMs)
  }

  close() {
    if (this.intervalId) {
      clearInterval(this.intervalId)
      this.intervalId = null
    }
    this.jobIds.clear()
  }

  sendHeartbeats() {
    if (this.jobIds.size === 0) {
      return
    }
    const pipeline = this.redis.pipeline()
    this.jobIds.forEach((jobId) => {
      pipeline.xclaim(
        this.stream,
        this.consumerGroup,
        this.consumerId,
        0,
        jobId,
        'IDLE',
        0,
        'JUSTID'
      )
    })
    pipeline.exec().catch((err) => {
      console.error('Error sending heartbeats:', err)
    })
  }
  add(jobId: string) {
    this.jobIds.add(jobId)
  }
  remove(jobId: string) {
    this.jobIds.delete(jobId)
  }
}
