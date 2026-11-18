"""The asyncio Stream.process: fixes that bring it in line with the TypeScript library (against a real Redis,
REDIS_URL, default redis://localhost:6379; scratch streams deleted afterwards)."""

import asyncio
import json
import os
import time
import uuid
from typing import Any
from urllib.parse import urlparse

import pytest
from redis.asyncio import Redis

import process_redis_events.stream as stream_module
from process_redis_events import QueueItem, StartFrom, Stream, create_process_options

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
_url = urlparse(REDIS_URL)


def client(**kw: Any) -> Redis:
    # Redis(...) rather than from_url: that's what gets redis-py 8's 5 s default socket timeout
    return Redis(host=_url.hostname or "localhost", port=_url.port or 6379, **kw)


@pytest.fixture
async def stream():
    r = client()
    try:
        await r.ping()
    except Exception:
        pytest.skip("no Redis")
    s = Stream[dict](name=f"test-pre-{uuid.uuid4().hex[:8]}", create_redis=client)
    yield s
    await r.delete(s.stream_key, f"stream:{s.dlq_stream_key}")
    await r.aclose()
    await s.redis.aclose()


async def dlq(s: Stream[Any]) -> list[dict]:
    r = client()
    try:
        return [
            json.loads(f[b"data"])
            for _, f in await r.xrange(f"stream:{s.dlq_stream_key}")
        ]
    finally:
        await r.aclose()


async def pending(s: Stream[Any], group: str = "g") -> int:
    r = client()
    try:
        return int((await r.xpending(s.stream_key, group))["pending"])
    finally:
        await r.aclose()


async def run_until(task: asyncio.Task, cond, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not await cond():
        if task.done():
            task.result()  # raises if process() crashed
        await asyncio.sleep(0.05)


async def test_a_quiet_stream_with_the_default_lease_does_not_time_out_the_read(stream):
    """lease 30 s means BLOCK 29 s; redis-py 8's default 5 s socket timeout used to fail the read and end process()."""
    stop = asyncio.Event()
    seen: list[Any] = []

    async def cb(item: QueueItem[Any]) -> None:
        seen.append(item.data)

    task = asyncio.create_task(
        stream.process(create_process_options(stop, "g", start_from=StartFrom.OLDEST), cb)  # type: ignore[arg-type]
    )
    await asyncio.sleep(6.5)
    assert not task.done(), task.exception() if task.done() else None
    await stream.add({"n": "1"})

    async def got() -> bool:
        return seen == [{"n": "1"}]

    await run_until(task, got)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert seen == [{"n": "1"}]


async def test_stopping_interrupts_the_blocking_read(stream):
    stop = asyncio.Event()

    async def cb(item: QueueItem[Any]) -> None:
        pass

    task = asyncio.create_task(
        stream.process(create_process_options(stop, "g", lease_ms=60_000), cb)
    )
    await asyncio.sleep(0.5)
    t = time.monotonic()
    stop.set()
    await asyncio.wait_for(task, 5)
    assert time.monotonic() - t < 2


async def test_unparseable_messages_go_to_the_dlq_and_processing_goes_on(stream):
    r = client()
    await r.xadd(stream.stream_key, {"data": "{not json"})
    await r.aclose()
    await stream.add({"n": "ok"})
    stop = asyncio.Event()
    seen: list[Any] = []

    async def cb(item: QueueItem[Any]) -> None:
        seen.append(item.data)

    task = asyncio.create_task(
        stream.process(
            create_process_options(
                stop, "g", start_from=StartFrom.OLDEST, lease_ms=500
            ),
            cb,
        )
    )

    async def done() -> bool:
        return seen == [{"n": "ok"}] and len(await dlq(stream)) == 1

    await run_until(task, done)
    stop.set()
    await task
    (dead,) = await dlq(stream)
    assert dead["_raw"] == {"data": "{not json"} and dead["_dlqMetadata"]["originalId"]
    assert await pending(stream) == 0


async def test_dead_letters_have_an_epoch_timestamp_and_the_ts_attempts(stream):
    await stream.add({"n": "bad"})
    stop = asyncio.Event()

    async def cb(item: QueueItem[Any]) -> None:
        raise ValueError("boom")

    opts = create_process_options(
        stop,
        "g",
        start_from=StartFrom.OLDEST,
        lease_ms=200,
        should_retry=lambda attempts, data: attempts < 2,
    )
    task = asyncio.create_task(stream.process(opts, cb))

    async def dead() -> bool:
        return len(await dlq(stream)) == 1

    await run_until(task, dead)
    stop.set()
    await task
    (entry,) = await dlq(stream)
    meta = entry["_dlqMetadata"]
    assert (
        entry["n"] == "bad" and meta["attempts"] == 2 and meta["consumerGroup"] == "g"
    )
    assert abs(meta["timestamp"] - time.time() * 1000) < 60_000
    assert await pending(stream) == 0


async def test_a_message_stays_pending_when_its_dead_letter_cannot_be_written(
    stream, monkeypatch
):
    await stream.add({"n": "bad"})
    stop = asyncio.Event()

    async def cb(item: QueueItem[Any]) -> None:
        raise ValueError("boom")

    async def broken(self, id, group, data):  # noqa: ANN001
        raise ConnectionError("Redis down")

    monkeypatch.setattr(Stream, "_move_to_dlq", broken)
    opts = create_process_options(
        stop,
        "g",
        start_from=StartFrom.OLDEST,
        lease_ms=200,
        should_retry=lambda attempts, data: False,
    )
    task = asyncio.create_task(stream.process(opts, cb))
    await asyncio.sleep(1.5)
    stop.set()
    await task
    assert await pending(stream) == 1  # the old code acked it in `finally`: lost


async def test_a_reclaim_never_rereads_a_message_still_in_progress(stream):
    """The old get_entries read the consumer's own PEL from 0-0 after every XAUTOCLAIM, which returned its oldest
    pending message: one still being processed, handed to a second callback."""
    await stream.add({"n": "slow"})
    await stream.add({"n": "flaky"})
    stop = asyncio.Event()
    calls: dict[str, int] = {}

    async def cb(item: QueueItem[Any]) -> None:
        n = item.data["n"]
        calls[n] = calls.get(n, 0) + 1
        if n == "slow":
            await asyncio.sleep(2)
        elif calls[n] == 1:
            raise ValueError("first try fails")

    opts = create_process_options(
        stop,
        "g",
        start_from=StartFrom.OLDEST,
        lease_ms=300,
        batch_size=1,
        concurrency=2,
    )
    task = asyncio.create_task(stream.process(opts, cb))

    async def done() -> bool:
        return calls.get("flaky", 0) >= 2 and await pending(stream) == 0

    await run_until(task, done)
    stop.set()
    await task
    assert calls == {"slow": 1, "flaky": 2}


async def test_a_failing_read_is_retried_not_fatal(stream, monkeypatch):
    real = stream_module.get_entries
    fails = [ConnectionError("Redis down")] * 2

    async def flaky(**kw: Any):
        if fails:
            raise fails.pop()
        return await real(**kw)

    monkeypatch.setattr(stream_module, "get_entries", flaky)
    monkeypatch.setattr(stream_module, "backoff_delay", lambda attempt, cap=None: 0.01)
    await stream.add({"n": "1"})
    stop = asyncio.Event()
    seen: list[Any] = []

    async def cb(item: QueueItem[Any]) -> None:
        seen.append(item.data)

    task = asyncio.create_task(
        stream.process(
            create_process_options(stop, "g", start_from=StartFrom.OLDEST), cb
        )
    )

    async def got() -> bool:
        return bool(seen)

    await run_until(task, got)
    stop.set()
    await task
    assert seen == [{"n": "1"}] and not fails


async def test_on_error_gets_the_callback_error(stream):
    await stream.add({"n": "x"})
    stop = asyncio.Event()
    errors: list[str] = []

    async def cb(item: QueueItem[Any]) -> None:
        raise ValueError("boom")

    opts = create_process_options(
        stop,
        "g",
        start_from=StartFrom.OLDEST,
        on_error=lambda e, item: errors.append(f"{item.id}:{e}"),
    )
    task = asyncio.create_task(stream.process(opts, cb))

    async def got() -> bool:
        return bool(errors)

    await run_until(task, got)
    stop.set()
    await task
    assert errors[0].endswith(":boom")
