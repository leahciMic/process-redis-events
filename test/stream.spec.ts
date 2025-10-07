import {
  describe,
  beforeEach,
  expect,
  it,
  vi,
  afterEach,
  type Mock
} from 'vitest'
import { Stream, StartFrom, type StreamEvent } from '../index.js'
import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

let stream: Stream<{ foo: string }>
let mockFn: Mock<Parameters<(typeof stream)['process']>[1]>
let shouldRetry: ReturnType<typeof vi.fn>

describe('stream', () => {
  let abortController: AbortController
  beforeEach(async () => {
    vi.resetAllMocks()
    abortController = new AbortController()
    stream = new Stream({
      name: nanoid(),
      createRedis: () => new Redis(),
      produceEvents: true
    })
    mockFn = vi.fn()
    shouldRetry = vi.fn().mockReturnValue(true)
    stream.process(
      {
        consumerGroup: 'test-group',
        startFrom: StartFrom.Oldest,
        leaseMs: 10,
        signal: abortController.signal,
        shouldRetry
      },
      mockFn
    )
  })

  afterEach(async () => {
    abortController.abort()
    await stream.clear()
    await stream.createEventStream().clear()
  })

  it('can process events', async () => {
    await stream.add({ foo: 'bar' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())

    expect(mockFn).toHaveBeenCalledWith({
      id: expect.any(String),
      attempts: 0,
      data: { foo: 'bar' },
      reportProgress: expect.any(Function)
    })
  })

  it('retries failed events', async () => {
    mockFn.mockImplementationOnce(() => {
      throw new Error('Processing failed')
    })

    await stream.add({ foo: 'baz' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalledTimes(2), {
      timeout: 10_000
    })

    expect(mockFn).toHaveBeenCalledWith({
      reportProgress: expect.any(Function),
      attempts: 1,
      data: { foo: 'baz' },
      id: expect.any(String)
    })
  })

  it('retries infinitely', async () => {
    mockFn.mockImplementation(() => {
      throw new Error('Processing failed')
    })

    await stream.add({ foo: 'baz' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalledTimes(5), {
      timeout: 10_000
    })

    expect(mockFn).toHaveBeenCalledWith({
      reportProgress: expect.any(Function),
      id: expect.any(String),
      attempts: 4,
      data: { foo: 'baz' }
    })
  })

  it('emits progress events', async () => {
    const eventStream = stream.createEventStream()

    mockFn.mockImplementationOnce(async (event) => {
      await event.reportProgress(0.1, 'Starting processing')
      await event.reportProgress(0.5, 'Halfway there')
      await event.reportProgress(1, 'Completed')
    })

    const events: StreamEvent[] = []
    eventStream.process(
      {
        consumerGroup: 'event-group',
        startFrom: StartFrom.Oldest,
        signal: abortController.signal
      },
      async (entry) => {
        events.push(entry.data)
      }
    )

    const { id } = await stream.add({ foo: 'with-progress' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())
    await vi.waitFor(() => expect(events.length).toBe(5), {
      timeout: 10_000
    })

    expect(events).toEqual([
      { id, type: 'started' },
      {
        id,
        type: 'progress',
        completionRatio: 0.1,
        status: 'Starting processing'
      },
      { id, type: 'progress', completionRatio: 0.5, status: 'Halfway there' },
      { id, type: 'progress', completionRatio: 1, status: 'Completed' },
      { id, type: 'completed' }
    ])
  })

  it('emits failed events', async () => {
    const eventStream = stream.createEventStream()

    mockFn.mockImplementationOnce(async () => {
      throw new Error('Something went wrong')
    })

    const events: StreamEvent[] = []

    eventStream.process(
      {
        consumerGroup: 'event-group',
        startFrom: StartFrom.Oldest,
        signal: abortController.signal
      },
      async (entry) => {
        events.push(entry.data)
      }
    )

    const { id } = await stream.add({ foo: 'with-error' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())
    await vi.waitFor(() => expect(events.length).toBe(2), {
      timeout: 10_000
    })

    expect(events).toEqual([
      { id, type: 'started' },
      { id, type: 'failed', error: 'Something went wrong' }
    ])
  })
})
