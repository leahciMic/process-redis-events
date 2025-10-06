export type QueueItem<T> = {
  id: string
  value: T
  attempts: number
  ack: () => Promise<void>
  reportProgress: (completionRatio: number, status: string) => Promise<void>
}
