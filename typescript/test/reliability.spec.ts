import { describe, expect, it, vi, afterEach } from 'vitest'
import { Stream, StartFrom, type QueueItem } from '../index.js'
import { HeartbeatManager } from '../lib/HeartbeatManager.js'
import { Redis } from 'ioredis'
import { nanoid } from 'nanoid'
import pDefer from 'p-defer'

// Tests run against REDIS_URL (default redis://localhost:6379). Use a
// throwaway Redis: the tests create and delete streams.
const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379'
const createRedis = () => new Redis(REDIS_URL)

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Data = { foo: string }

const cleanups: Array<() => Promise<unknown>> = []

afterEach(async () => {
  for (const fn of cleanups.splice(0).reverse()) {
    await fn().catch(() => {})
  }
  vi.restoreAllMocks()
})

function setup(opts: { createRedis?: () => Redis } = {}) {
  const name = nanoid()
  const admin = createRedis()
  const stream = new Stream<Data>({
    name,
    createRedis: opts.createRedis ?? createRedis
  })
  const controller = new AbortController()
  const key = `stream:${name}`
  // The DLQ is itself a Stream named `dlq:<name>`
  const dlqKey = `stream:dlq:${name}`
  cleanups.push(async () => {
    controller.abort()
    await admin.del(key, dlqKey, `stream:events:${name}`)
    await admin.quit()
  })
  const pending = async (group = 'g') =>
    ((await admin.xpending(key, group)) as [number, ...unknown[]])[0]
  return { name, admin, stream, controller, key, dlqKey, pending }
}

describe('reliability', () => {
  it('processes the entries XAUTOCLAIM claimed, not older in-flight ones', async () => {
    const { admin, stream, controller, key, pending } = setup()
    await admin.xgroup('CREATE', key, 'g', '0', 'MKSTREAM')
    const a = (await admin.xadd(key, '*', 'data', '{"foo":"a"}'))!
    const b = (await admin.xadd(key, '*', 'data', '{"foo":"b"}'))!
    // A consumer that reads both entries and dies.
    await admin.xreadgroup('GROUP', 'g', 'dead', 'COUNT', 2, 'STREAMS', key, '>')

    const release = pDefer()
    const calls: QueueItem<Data>[] = []
    const done = stream.process(
      {
        signal: controller.signal,
        consumerGroup: 'g',
        consumerId: 'c1',
        batchSize: 1,
        concurrency: 2,
        leaseMs: 300
      },
      async (item) => {
        calls.push(item)
        // a stays in flight (kept alive by heartbeats) while b is reclaimed
        if (item.data.foo === 'a') await release.promise
      }
    )

    await vi.waitFor(() => expect(calls.length).toBeGreaterThanOrEqual(2), {
      timeout: 5_000
    })
    await sleep(1_500)
    expect(calls.map((c) => c.id)).toEqual([a, b])
    // Both were delivered once to "dead", so this is their first retry.
    expect(calls.map((c) => c.attempts)).toEqual([1, 1])
    expect(calls[1]!.data).toEqual({ foo: 'b' })

    release.resolve()
    await vi.waitFor(async () => expect(await pending()).toBe(0))
    controller.abort()
    await done
  })

  it('acks pending entries that were deleted from the stream', async () => {
    const { admin, stream, controller, key, pending } = setup()
    await admin.xgroup('CREATE', key, 'g', '0', 'MKSTREAM')
    const id = (await admin.xadd(key, '*', 'data', '{"foo":"gone"}'))!
    await admin.xreadgroup('GROUP', 'g', 'dead', 'STREAMS', key, '>')
    await admin.xdel(key, id)
    expect(await pending()).toBe(1)

    const callback = vi.fn()
    stream.process(
      { signal: controller.signal, consumerGroup: 'g', leaseMs: 200 },
      callback
    )
    await vi.waitFor(async () => expect(await pending()).toBe(0), {
      timeout: 5_000
    })
    expect(callback).not.toHaveBeenCalled()
  })

  it('first delivery has attempts 0 and the first retry attempts 1', async () => {
    const { stream, controller } = setup()
    const shouldRetry = vi.fn().mockReturnValue(true)
    const attempts: number[] = []
    stream.process(
      {
        signal: controller.signal,
        consumerGroup: 'g',
        startFrom: StartFrom.Oldest,
        leaseMs: 200,
        shouldRetry,
        onError: () => {}
      },
      async (item) => {
        attempts.push(item.attempts)
        if (attempts.length < 3) throw new Error('fail')
      }
    )
    await stream.add({ foo: 'x' })
    await vi.waitFor(() => expect(attempts).toEqual([0, 1, 2]), {
      timeout: 5_000
    })
    expect(shouldRetry.mock.calls.map((c) => c[0])).toEqual([1, 2])
  })

  it('retries a batch whose map function threw', async () => {
    const { stream, controller } = setup()
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const map = vi
      .fn()
      .mockImplementationOnce(() => {
        throw new Error('map failed')
      })
      .mockImplementation((data: Data[]) => data)
    const callback = vi.fn()
    stream.process(
      {
        signal: controller.signal,
        consumerGroup: 'g',
        startFrom: StartFrom.Oldest,
        leaseMs: 200,
        map
      },
      callback
    )
    await stream.add({ foo: 'x' })
    await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1), {
      timeout: 5_000
    })
    expect(callback.mock.calls[0]![0]).toMatchObject({
      attempts: 1,
      data: { foo: 'x' }
    })
    expect(errorSpy).toHaveBeenCalled()
  })

  it('does not ack an entry whose DLQ write failed, and retries it', async () => {
    const { stream, controller, admin, dlqKey, pending } = setup()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const dlq = stream.createDeadLetterQueueStream()
    const dlqAdd = vi
      .spyOn(dlq, 'add')
      .mockRejectedValueOnce(new Error('DLQ unavailable'))
    stream.process(
      {
        signal: controller.signal,
        consumerGroup: 'g',
        startFrom: StartFrom.Oldest,
        leaseMs: 200,
        shouldRetry: () => false,
        onError: () => {}
      },
      async () => {
        throw new Error('fail')
      }
    )
    const { id } = await stream.add({ foo: 'x' })
    await vi.waitFor(() => expect(dlqAdd).toHaveBeenCalledTimes(2), {
      timeout: 5_000
    })
    await vi.waitFor(async () => expect(await pending()).toBe(0))
    const entries = await admin.xrange(dlqKey, '-', '+')
    expect(entries).toHaveLength(1)
    const dlqData = JSON.parse(entries[0]![1][1]!)
    expect(dlqData).toMatchObject({
      foo: 'x',
      _dlqMetadata: { originalId: id, attempts: 2, consumerGroup: 'g' }
    })
  })

  it('reports callback errors to onError', async () => {
    const { stream, controller } = setup()
    const onError = vi.fn()
    const error = new Error('boom')
    stream.process(
      {
        signal: controller.signal,
        consumerGroup: 'g',
        startFrom: StartFrom.Oldest,
        onError
      },
      async () => {
        throw error
      }
    )
    const { id } = await stream.add({ foo: 'x' })
    await vi.waitFor(() => expect(onError).toHaveBeenCalled())
    expect(onError).toHaveBeenCalledWith(
      error,
      expect.objectContaining({ id, data: { foo: 'x' }, attempts: 0 })
    )
  })

  it('logs callback errors with console.error by default', async () => {
    const { stream, controller } = setup()
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const error = new Error('boom')
    stream.process(
      { signal: controller.signal, consumerGroup: 'g', startFrom: StartFrom.Oldest },
      async () => {
        throw error
      }
    )
    await stream.add({ foo: 'x' })
    await vi.waitFor(() =>
      expect(errorSpy).toHaveBeenCalledWith(expect.any(String), error)
    )
  })

  it('moves unparseable messages to the DLQ and keeps processing', async () => {
    const { stream, controller, admin, key, dlqKey, pending } = setup()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    await admin.xgroup('CREATE', key, 'g', '0', 'MKSTREAM')
    const bad = (await admin.xadd(key, '*', 'data', '{not json'))!
    const legacy = (await admin.xadd(key, '*', 'id', 'abc123'))!
    const good = (await stream.add({ foo: 'ok' })).id

    const callback = vi.fn()
    const done = stream.process(
      { signal: controller.signal, consumerGroup: 'g', leaseMs: 200 },
      callback
    )
    await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(2), {
      timeout: 5_000
    })
    expect(callback.mock.calls.map((c) => [c[0].id, c[0].data])).toEqual([
      [legacy, { id: 'abc123' }],
      [good, { foo: 'ok' }]
    ])
    await vi.waitFor(async () => expect(await pending()).toBe(0))
    const entries = await admin.xrange(dlqKey, '-', '+')
    expect(entries).toHaveLength(1)
    expect(JSON.parse(entries[0]![1][1]!)).toMatchObject({
      _raw: { data: '{not json' },
      _dlqMetadata: { originalId: bad, consumerGroup: 'g' }
    })
    controller.abort()
    await expect(done).resolves.toBeUndefined()
  })

  it('on abort, waits for in-flight callbacks, acks them and resolves', async () => {
    const { stream, controller, pending } = setup()
    const close = vi.spyOn(HeartbeatManager.prototype, 'close')
    const release = pDefer()
    const started = pDefer()
    let resolved = false
    const done = stream
      .process(
        // default leaseMs (30s): the read blocks for ~29s
        { signal: controller.signal, consumerGroup: 'g', startFrom: StartFrom.Oldest },
        async () => {
          started.resolve()
          await release.promise
        }
      )
      .then(() => {
        resolved = true
      })
    await stream.add({ foo: 'x' })
    await started.promise
    await sleep(100)
    controller.abort()
    await sleep(300)
    expect(resolved).toBe(false)
    release.resolve()
    const t = Date.now()
    await done
    expect(Date.now() - t).toBeLessThan(2_000)
    expect(close).toHaveBeenCalled()
    // the ack went through before the connection was closed
    expect(await pending()).toBe(0)
  })

  it('aborting an idle consumer resolves promptly', async () => {
    const { stream, controller } = setup()
    const done = stream.process(
      { signal: controller.signal, consumerGroup: 'g' },
      async () => {}
    )
    await sleep(200)
    const t = Date.now()
    controller.abort()
    await done
    expect(Date.now() - t).toBeLessThan(2_000)
  })

  it('survives transient Redis errors in the read loop', async () => {
    let failures = 2
    const flaky = () => {
      const r = createRedis()
      const orig = r.xautoclaim.bind(r) as (...args: unknown[]) => unknown
      ;(r as any).xautoclaim = (...args: unknown[]) => {
        if (failures-- > 0) {
          return Promise.reject(new Error('LOADING Redis is loading'))
        }
        return orig(...args)
      }
      return r
    }
    const { stream, controller } = setup({ createRedis: flaky })
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const callback = vi.fn()
    let rejected: unknown = null
    const done = stream
      .process(
        { signal: controller.signal, consumerGroup: 'g', startFrom: StartFrom.Oldest },
        callback
      )
      .catch((e) => {
        rejected = e
      })
    await stream.add({ foo: 'x' })
    await vi.waitFor(() => expect(callback).toHaveBeenCalled(), {
      timeout: 5_000
    })
    expect(rejected).toBeNull()
    expect(errorSpy).toHaveBeenCalled()
    controller.abort()
    await done
  })
})
