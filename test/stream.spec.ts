import { describe, beforeEach, expect, it, vi, afterEach } from 'vitest'
import { Stream, type StreamEvent } from '../stream.js'
import { StartFrom } from '../processRedisEvents.js'
import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'

let stream: Stream<{ foo: string }>
let mockFn: ReturnType<typeof vi.fn>
let stop: (closeRedisConnection: boolean) => void
let shouldRetry: ReturnType<typeof vi.fn>

describe('stream', () => {
  beforeEach(async () => {
    vi.resetAllMocks()
    stream = new Stream({
      name: nanoid(),
      createRedis: () => new Redis(),
      produceEvents: true
    })
    mockFn = vi.fn()
    shouldRetry = vi.fn().mockReturnValue(true)
    stop = stream.process(
      {
        consumerGroup: 'test-group',
        startFrom: StartFrom.Oldest,
        timeout: 200,
        shouldRetry,
        blockFor: 10
      },
      mockFn
    )
  })

  afterEach(async () => {
    stop(true)
    await stream.clear()
    await stream.createEventStream().clear()
  })

  it('can process events', async () => {
    await stream.add({ foo: 'bar' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())

    expect(mockFn).toHaveBeenCalledWith([
      {
        ack: expect.any(Function),
        del: expect.any(Function),
        id: expect.any(String),
        error: expect.any(Function),
        attempts: 0,
        value: { foo: 'bar' },
        reportProgress: expect.any(Function)
      }
    ])
  })

  it('retries failed events', async () => {
    mockFn.mockImplementationOnce(() => {
      throw new Error('Processing failed')
    })

    await stream.add({ foo: 'baz' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalledTimes(2), {
      timeout: 10_000
    })

    expect(mockFn).toHaveBeenCalledWith([
      {
        ack: expect.any(Function),
        del: expect.any(Function),
        id: expect.any(String),
        reportProgress: expect.any(Function),
        error: expect.any(Function),
        attempts: 1,
        value: { foo: 'baz' }
      }
    ])
  })

  it('retries infinitely', async () => {
    mockFn.mockImplementationOnce(() => {
      throw new Error('Processing failed')
    })

    await stream.add({ foo: 'baz' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalledTimes(5), {
      timeout: 10_000
    })

    expect(mockFn).toHaveBeenCalledWith([
      {
        ack: expect.any(Function),
        del: expect.any(Function),
        reportProgress: expect.any(Function),
        error: expect.any(Function),
        id: expect.any(String),
        attempts: 4,
        value: { foo: 'baz' }
      }
    ])
  })

  it('emits progress events', async () => {
    const eventStream = stream.createEventStream()

    mockFn.mockImplementationOnce(async ([event]) => {
      await event.reportProgress(0.1, 'Starting processing')
      await event.reportProgress(0.5, 'Halfway there')
      await event.reportProgress(1, 'Completed')
      await event.ack()
    })

    const events: StreamEvent[] = []
    const stop = eventStream.process(
      {
        consumerGroup: 'event-group',
        startFrom: StartFrom.Oldest,
        timeout: 200,
        blockFor: 10
      },
      async (entries) => {
        events.push(...entries.map((e) => e.value))
        await Promise.all(entries.map((e) => e.ack()))
      }
    )

    const { id } = await stream.add({ foo: 'with-progress' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())
    await vi.waitFor(() => expect(events.length).toBe(5), {
      timeout: 10_000
    })
    stop(false)

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

    mockFn.mockImplementationOnce(async ([event]) => {
      await event.error(new Error('Something went wrong'))
    })

    const events: StreamEvent[] = []

    const stop = eventStream.process(
      {
        consumerGroup: 'event-group',
        startFrom: StartFrom.Oldest,
        timeout: 200,
        blockFor: 10
      },
      async (entries) => {
        events.push(...entries.map((e) => e.value))
        await Promise.all(entries.map((e) => e.ack()))
      }
    )

    const { id } = await stream.add({ foo: 'with-error' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())
    await vi.waitFor(() => expect(events.length).toBe(2), {
      timeout: 10_000
    })
    stop(false)

    expect(events).toEqual([
      { id, type: 'started' },
      { id, type: 'failed', error: 'Something went wrong' }
    ])
  })
})
