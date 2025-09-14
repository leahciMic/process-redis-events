import { describe, beforeEach, expect, it, vi, afterEach } from 'vitest'
import { Stream } from '../stream.js'
import { StartFrom } from '../processRedisEvents.js'

let stream: Stream<{ foo: string }>
let mockFn: ReturnType<typeof vi.fn>
let stop: (closeRedisConnection: boolean) => void
let shouldRetry: ReturnType<typeof vi.fn>

describe('stream', () => {
  beforeEach(async () => {
    vi.resetAllMocks()
    stream = new Stream('test-stream')
    await stream.clear()
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

  afterEach(() => {
    stop(true)
  })

  it('can process events', async () => {
    await stream.add({ foo: 'bar' })
    await vi.waitFor(() => expect(mockFn).toHaveBeenCalled())

    expect(mockFn).toHaveBeenCalledWith([
      {
        ack: expect.any(Function),
        del: expect.any(Function),
        id: expect.any(String),
        attempts: 0,
        value: { foo: 'bar' }
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
        id: expect.any(String),
        attempts: 4,
        value: { foo: 'baz' }
      }
    ])
  })
})
