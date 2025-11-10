export type QueueItem<T> = {
  id: string
  data: T
  attempts: number
  reportProgress: (completionRatio: number, status: string) => Promise<void>
}
