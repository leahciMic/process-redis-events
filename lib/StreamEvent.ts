type StreamProgressEvent = {
  type: 'progress'
  completionRatio: number
  status: string
}

type StreamCompletedEvent = {
  type: 'completed'
}

type StreamFailedEvent = {
  type: 'failed'
  error: string
}

type StreamStartedEvent = {
  type: 'started'
}

export type StreamEvent = { id: string } & (
  | StreamProgressEvent
  | StreamCompletedEvent
  | StreamFailedEvent
  | StreamStartedEvent
)
