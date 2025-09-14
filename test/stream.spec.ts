import { describe, beforeEach, expect, it, vi, afterEach } from 'vitest'
import { Stream } from '../stream.js'
import { StartFrom } from '../processRedisEvents.js'

let stream: Stream<{ foo: string }>
let mockFn: ReturnType<typeof vi.fn>
let stop: (closeRedisConnection: boolean) => void

describe('stream', () => {
  beforeEach(async () => {
    stream = new Stream('test-stream')
    console.log('Clearing stream')
    await stream.clear()
    mockFn = vi.fn()
    stop = stream.process(
      {
        consumerGroup: 'test-group',
        startFrom: StartFrom.Oldest,
        timeout: 100,
        blockFor: 10
      },
      mockFn
    )
  })

  afterEach(() => {
    console.log('Stopping processing')
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
      timeout: 5000
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
})
