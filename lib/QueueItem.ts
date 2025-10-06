export type QueueItem<T> = {
  id: string
  value: T
  attempts: number
  error: (error: Error) => void
  ack: () => Promise<void>
  del: () => Promise<void>
  reportProgress: (completionRatio: number, status: string) => Promise<void>
}
