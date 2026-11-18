"""The synchronous consumer's semantics against a real Redis (REDIS_URL, default redis://localhost:6379), on scratch
streams that are deleted afterwards. Skipped without Redis. Many of these come from clok's product-enrich and
category reclassifier consumers, which this consumer replaces."""

import json
import os
import threading
import time
import uuid

import pytest
import redis

import process_redis_events.consumer as consumer_module
from process_redis_events import BatchConsumer, Paused, StreamConsumer, TransientError

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


@pytest.fixture()
def r():
    client = redis.Redis.from_url(REDIS_URL, socket_timeout=30)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("no Redis")
    yield client
    client.close()


@pytest.fixture()
def stream(r):
    name = f"test-pre-{uuid.uuid4().hex[:8]}"
    yield name
    r.delete(f"stream:{name}", f"stream:dlq:{name}")


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr(
        consumer_module, "backoff_delay", lambda attempt, rnd=None, cap=None: 0.01
    )


class MilvusDown(ConnectionRefusedError):
    """Stands in for a dependency's connection failure (the library classifies ConnectionError as "network")."""


def add(r, stream, *ids):
    for i in ids:
        r.xadd(f"stream:{stream}", {"data": json.dumps({"productId": i})})


def make(r, stream, handler, **kw):
    kw.setdefault("batch_size", 10)
    kw.setdefault("max_wait_s", 0.2)
    kw.setdefault("lease_ms", 200)
    kw.setdefault("consumer_id", "c1")
    c = BatchConsumer(r, stream, "g", "test", handler, block_ms=100, **kw)
    c.ensure_group()
    return c


def deliveries(r, stream):
    return {
        k.decode() if isinstance(k, bytes) else k: v
        for k, v in _deliveries(r, stream).items()
    }


def _deliveries(r, stream):
    rows = r.xpending_range(f"stream:{stream}", "g", min="-", max="+", count=1000)
    return {row["message_id"]: int(row["times_delivered"]) for row in rows}


def pending(r, stream):
    return r.xpending(f"stream:{stream}", "g")["pending"]


def dlq(r, stream):
    return [json.loads(f[b"data"]) for _, f in r.xrange(f"stream:dlq:{stream}")]


def test_batches_messages_added_before_the_group(r, stream):
    add(r, stream, *[f"p{i}" for i in range(25)])
    batches = []
    c = make(r, stream, lambda ps: batches.append([p["productId"] for p in ps]))
    while c.run_once():
        pass
    assert [len(b) for b in batches] == [10, 10, 5]
    assert sum(batches, []) == [f"p{i}" for i in range(25)]
    assert pending(r, stream) == 0


def test_group_lag_counts_from_the_ids_even_when_redis_lag_is_wrong(r, stream):
    add(r, stream, *[f"p{i}" for i in range(5)])
    c = make(r, stream, lambda ps: None, batch_size=3)
    assert c.group_lag() == (5, 0)
    c.run_once()
    assert c.group_lag() == (2, 0)
    # a Redis restart leaves entries-read from the last AOF rewrite: lag 4 for good, though nothing is unread
    c.run_once()
    key = f"stream:{stream}"
    r.xgroup_setid(key, "g", r.xrevrange(key, count=1)[0][0], entries_read=1)
    assert r.xinfo_groups(key)[0]["lag"] == 4
    assert c.group_lag() == (0, 0)


def test_waits_for_the_batch_to_fill_up_to_max_wait(r, stream):
    add(r, stream, "a")
    c = make(r, stream, lambda ps: None, max_wait_s=0.5)
    t = time.monotonic()
    batch = c.collect()
    assert len(batch) == 1 and 0.4 < time.monotonic() - t < 2


def test_transient_failures_are_retried_in_place(r, stream):
    add(r, stream, "a", "b")
    calls = []

    def handler(ps):
        calls.append(len(ps))
        if len(calls) < 3:
            raise TransientError("milvus down")

    c = make(r, stream, handler)
    c.run_once()
    assert calls == [2, 2, 2] and pending(r, stream) == 0


def test_a_bad_message_is_isolated_by_bisection_then_dead_lettered(r, stream):
    add(r, stream, "a", "b", "bad", "c")
    done = []

    def handler(ps):
        ids = [p["productId"] for p in ps]
        if "bad" in ids:
            raise ValueError("poison")
        done.extend(ids)

    c = make(r, stream, handler, max_attempts=2)
    c.run_once()
    assert sorted(done) == ["a", "b", "c"]
    assert pending(r, stream) == 1  # "bad", redelivered after the lease
    for _ in range(3):
        time.sleep(0.25)
        c.run_once()
    assert pending(r, stream) == 0
    (dead,) = dlq(r, stream)
    assert dead["productId"] == "bad" and dead["_dlqMetadata"]["consumerGroup"] == "g"


def test_invalid_messages_go_to_the_dead_letter_queue_at_once(r, stream):
    r.xadd(f"stream:{stream}", {"data": "not json"})
    r.xadd(f"stream:{stream}", {"data": json.dumps({"other": 1})})
    add(r, stream, "ok")
    seen = []
    c = make(
        r,
        stream,
        lambda ps: seen.extend(p["productId"] for p in ps),
        validate=lambda p: None if p.get("productId") else "no productId",
    )
    c.run_once()
    assert seen == ["ok"] and pending(r, stream) == 0
    assert [d["_dlqMetadata"]["reason"] for d in dlq(r, stream)] == [
        "not JSON",
        "no productId",
    ]


def test_heartbeat_keeps_a_slow_batch_from_being_reclaimed(r, stream):
    add(r, stream, "slow")
    other = make(r, stream, lambda ps: None)
    other.consumer = "c2"

    def handler(ps):
        time.sleep(0.6)  # three leases
        assert other._reclaim(10) == []

    c = make(r, stream, handler)
    c.start_heartbeat()
    c.run_once()
    c.stop.set()
    assert pending(r, stream) == 0


def test_an_expired_lease_is_reclaimed_by_another_consumer(r, stream):
    add(r, stream, "a")
    dead = make(r, stream, lambda ps: None)
    assert len(dead._read_new(10, 100)) == 1  # read, then "dies"
    time.sleep(0.25)
    seen = []
    c = make(r, stream, lambda ps: seen.extend(p["productId"] for p in ps))
    c.consumer = "c2"
    c.run_once()
    assert seen == ["a"] and pending(r, stream) == 0


class _Unavailable:
    """A fake dependency that is down until `until` (time.monotonic()), failing like production's Milvus did on
    17 Nov: MilvusException(CONNECT_FAILED) raised from gRPC's UNAVAILABLE."""

    def __init__(self, seconds: float):
        self.until = time.monotonic() + seconds

    def call(self):
        if time.monotonic() < self.until:
            raise MilvusDown("Connection refused")


def test_a_transient_outage_uses_up_no_delivery_and_keeps_the_lease(r, stream):
    add(r, stream, "a", "b", "c")
    dep = _Unavailable(1.0)  # five leases long
    calls, kinds, states, seen = [], [], [], []
    thief = make(r, stream, lambda ps: None)
    thief.consumer = "c2"

    def handler(ps):
        calls.append(len(ps))
        assert thief._reclaim(10) == []  # the heartbeat keeps the batch ours throughout
        dep.call()
        seen.append(deliveries(r, stream))

    c = make(
        r,
        stream,
        handler,
        on_transient=kinds.append,
        on_outage=states.append,
        max_attempts=1,
        max_wait_s=0.01,  # (collecting for 0.2 s would use up the 0.2 s lease before the heartbeat covers it)
    )
    c.start_heartbeat()
    c.run_once()
    c.stop.set()
    assert set(calls) == {3} and len(calls) > 3  # retried whole, never bisected
    assert seen and set(seen[0].values()) == {
        1
    }  # still the first delivery when it succeeded
    assert pending(r, stream) == 0 and dlq(r, stream) == []
    assert set(kinds) == {"network"} and states == [True, False]
    assert not c.outage.active


def test_a_long_outage_fails_the_batch_whole_after_the_retry_window(r, stream):
    add(r, stream, "a", "b", "c", "d")
    dep = _Unavailable(60)
    calls = []

    def handler(ps):
        calls.append(len(ps))
        dep.call()

    c = make(r, stream, handler, transient_max_s=0.3)
    assert c.process(c.collect()) == "failed"
    assert set(calls) == {4}  # no bisection of a dependency outage
    assert pending(r, stream) == 4 and dlq(r, stream) == []
    assert not c.outage.active
    # the normal attempt path: redelivered after the lease, a delivery used up
    time.sleep(0.25)
    batch = c.collect()
    assert len(batch) == 4 and {m.deliveries for m in batch} == {2}


def test_a_permanent_failure_bisects_and_dead_letters_only_the_bad_message(r, stream):
    add(r, stream, *[f"p{i}" for i in range(7)], "bad", "p7")
    done, kinds = [], []

    def handler(ps):
        ids = [p["productId"] for p in ps]
        if "bad" in ids:
            raise KeyError("bad")
        done.extend(ids)

    c = make(r, stream, handler, max_attempts=3, on_transient=kinds.append)
    c.run_once()
    for _ in range(4):
        time.sleep(0.25)
        c.run_once()
    assert sorted(done) == sorted(f"p{i}" for i in range(8))
    (dead,) = dlq(r, stream)
    assert (
        dead["productId"] == "bad"
        and dead["_dlqMetadata"]["reason"] == "failed 3 deliveries"
    )
    assert pending(r, stream) == 0 and kinds == []


def test_claimed_messages_survive_a_failing_read(r, stream, monkeypatch):
    """17 Nov: XAUTOCLAIM took the 466 pending messages (a delivery each), then the blocking XREADGROUP timed out
    and they were dropped until the next lease, four times, and dead-lettered."""
    add(r, stream, "a", "b")
    dead = make(r, stream, lambda ps: None)
    assert len(dead._read_new(10, 100)) == 2  # read, then "dies"
    time.sleep(0.25)
    seen = []
    c = make(r, stream, lambda ps: seen.extend(p["productId"] for p in ps))
    c.consumer = "c2"

    def timeout(count, block_ms):
        raise redis.TimeoutError("Timeout reading from socket")

    monkeypatch.setattr(c, "_read_new", timeout)
    c.run_once()
    assert sorted(seen) == ["a", "b"] and pending(r, stream) == 0


def test_the_blocking_read_stays_under_the_socket_timeout(r, stream):
    c = BatchConsumer(
        redis.Redis.from_url(REDIS_URL, socket_timeout=5),
        stream,
        "g",
        "test",
        lambda ps: None,
        block_ms=5000,
    )
    assert c.block_ms == 4000


def test_a_transient_failure_outside_a_batch_does_not_crash_the_loop(r, stream):
    """17 Nov: loading the models connects to Milvus; refused, it crashed the pod (4 restarts)."""
    add(r, stream, "a")
    dep = _Unavailable(0.3)
    states, seen = [], []

    def ready():
        dep.call()
        return True

    c = make(
        r,
        stream,
        lambda ps: seen.extend(p["productId"] for p in ps),
        should_read=ready,
        on_outage=states.append,
    )
    c.run(drain=True)
    assert seen == ["a"] and states == [True, False] and pending(r, stream) == 0


def test_a_failing_ack_is_retried_not_the_batch(r, stream, monkeypatch):
    add(r, stream, "a")
    calls = []
    c = make(r, stream, lambda ps: calls.append(len(ps)))
    real = c.r.xack
    fails = [redis.TimeoutError("Timeout reading from socket")] * 2

    def flaky(*args):
        if fails:
            raise fails.pop()
        return real(*args)

    monkeypatch.setattr(c.r, "xack", flaky)
    assert c.run_once() == 1
    assert calls == [1] and pending(r, stream) == 0


# ---------------------------------------------------------------------- one message at a time (StreamConsumer)


def make_single(r, stream, handler, **kw):
    c = StreamConsumer(
        r,
        stream,
        "g",
        "test",
        handler,
        lease_ms=200,
        block_ms=100,
        consumer_id="c1",
        **kw,
    )
    c.ensure_group()
    return c


def test_single_permanent_failure_is_redelivered_then_dead_lettered(r, stream):
    add(r, stream, "bad")
    calls = []

    def handler(msg):
        calls.append(msg)
        raise ValueError("boom")

    c = make_single(r, stream, handler, max_attempts=2)
    deadline = time.time() + 5
    while time.time() < deadline and not r.exists(f"stream:dlq:{stream}"):
        c.run_once()
        time.sleep(0.1)
    assert len(calls) == 2
    (dead,) = dlq(r, stream)
    assert dead["productId"] == "bad" and dead["_dlqMetadata"]["consumerGroup"] == "g"
    # attempts = earlier deliveries, as the TypeScript library writes it
    assert dead["_dlqMetadata"]["attempts"] == 2
    assert (
        isinstance(dead["_dlqMetadata"]["timestamp"], int)
        and dead["_dlqMetadata"]["timestamp"] > 1.7e12
    )
    assert pending(r, stream) == 0


def test_single_transient_and_paused_retry_in_place_without_using_deliveries(r, stream):
    add(r, stream, "t")
    errors = [
        TransientError("down"),
        Paused("cap", wait_s=0.01),
        TransientError("down"),
    ]
    seen = []

    def handler(msg):
        seen.append(deliveries(r, stream))
        if errors:
            raise errors.pop(0)

    c = make_single(r, stream, handler)
    c.run_once()
    assert [set(d.values()) for d in seen] == [{1}] * 4 and pending(r, stream) == 0


def test_nothing_is_read_while_should_read_is_false(r, stream):
    add(r, stream, "x")
    seen = []
    stop = threading.Event()
    c = make_single(
        r, stream, seen.append, should_read=lambda: False, stop=stop, idle_check_s=0.05
    )
    t = threading.Thread(target=c.run)
    t.start()
    time.sleep(0.3)
    stop.set()
    t.join(5)
    assert not t.is_alive()
    assert seen == [] and c.group_lag() == (1, 0)


def test_drain_stops_when_should_read_is_false(r, stream):
    add(r, stream, "x")
    c = make_single(r, stream, lambda m: None, should_read=lambda: False)
    c.run(drain=True)  # returns instead of waiting for should_read()
    assert c.group_lag() == (1, 0)


# ---------------------------------------------------------------------- parity fixes


def test_a_restart_under_the_same_name_takes_over_its_own_pending_at_once(r, stream):
    """The lease is long (10 min in product-enrich); a pod that comes back under its name mustn't wait for it."""
    add(r, stream, "a", "b", "c")
    dead = make(r, stream, lambda ps: None, lease_ms=600_000)
    assert len(dead._read_new(10, 100)) == 3  # read, then "killed"
    seen = []
    c = make(
        r, stream, lambda ps: seen.extend(p["productId"] for p in ps), lease_ms=600_000
    )
    batch = c.collect()
    assert [m.payload["productId"] for m in batch] == ["a", "b", "c"]
    assert {m.deliveries for m in batch} == {
        2
    }  # a redelivery: a crash loop on a poison batch ends in the DLQ
    c.process(batch)
    assert seen == ["a", "b", "c"] and pending(r, stream) == 0
    # another name doesn't: it waits for the lease
    add(r, stream, "d")
    assert len(dead._read_new(10, 100)) == 1
    other = make(r, stream, lambda ps: None, lease_ms=600_000, consumer_id="c2")
    assert other.collect() == []


def test_consumer_name_comes_from_the_environment(r, stream, monkeypatch):
    monkeypatch.setenv("STREAM_CONSUMER_NAME", "product-enrich")
    assert (
        BatchConsumer(r, stream, "g", "svc", lambda ps: None).consumer
        == "product-enrich"
    )
    monkeypatch.delenv("STREAM_CONSUMER_NAME")
    monkeypatch.setenv("HOSTNAME", "pod-abc")
    assert (
        BatchConsumer(r, stream, "g", "svc", lambda ps: None).consumer == "pod-abc-svc"
    )
    assert (
        BatchConsumer(r, stream, "g", "svc", lambda ps: None, consumer_id="x").consumer
        == "x"
    )


def test_the_heartbeat_never_takes_a_message_back_from_its_new_owner(r, stream):
    add(r, stream, "a")
    slow = make(r, stream, lambda ps: None)
    (m,) = slow._read_new(10, 100)
    slow._current = [m.id]
    time.sleep(0.25)  # the lease expires (slow's heartbeat was stuck)
    thief = make(r, stream, lambda ps: None, consumer_id="c2")
    assert [x.id for x in thief._reclaim(10)] == [m.id]
    assert slow.heartbeat() == [m.id]  # lost, and not claimed back
    (row,) = r.xpending_range(f"stream:{stream}", "g", "-", "+", 10)
    assert row["consumer"] == b"c2"


def test_per_message_errors_fail_only_those_messages(r, stream):
    add(r, stream, "a", "bad", "c")
    calls = []

    def handler(ps):
        calls.append(len(ps))
        return [ValueError("bad") if p["productId"] == "bad" else None for p in ps]

    c = make(r, stream, handler, max_attempts=1)
    assert c.process(c.collect()) == "failed"
    assert calls == [3] and pending(r, stream) == 1  # no bisection needed
    time.sleep(0.25)
    c.run_once()
    (dead,) = dlq(r, stream)
    assert dead["productId"] == "bad" and dead["_dlqMetadata"]["attempts"] == 1


def test_unparseable_and_non_object_messages_keep_their_raw_fields(r, stream):
    r.xadd(f"stream:{stream}", {"data": "{not json"})
    r.xadd(f"stream:{stream}", {"data": json.dumps([1, 2])})
    c = make(r, stream, lambda ps: None)
    c.run_once()
    first, second = dlq(r, stream)
    assert (
        first["_raw"] == {"data": "{not json"}
        and first["_dlqMetadata"]["attempts"] == 0
    )
    assert "error" in first["_dlqMetadata"]
    assert (
        second["_raw"] == {"data": "[1, 2]"}
        and second["_dlqMetadata"]["reason"] == "not a JSON object"
    )
    assert pending(r, stream) == 0


def test_legacy_messages_become_an_object_of_their_fields(r, stream):
    r.xadd(f"stream:{stream}", {"id": "p1", "nameChanged": "false"})
    seen = []
    c = make(r, stream, seen.extend)
    c.run_once()
    assert seen == [{"id": "p1", "nameChanged": "false"}]


def test_a_message_is_acknowledged_only_once_its_dead_letter_is_written(
    r, stream, monkeypatch
):
    add(r, stream, "bad")
    c = make(r, stream, lambda ps: None, max_attempts=0, transient_max_s=0.2)
    real = c.r.xadd

    def failing(key, *args, **kw):
        if key.startswith("stream:dlq:"):
            raise redis.ConnectionError("down")
        return real(key, *args, **kw)

    monkeypatch.setattr(c.r, "xadd", failing)
    with pytest.raises(redis.ConnectionError):
        c.run_once()
    assert (
        pending(r, stream) == 1 and dlq(r, stream) == []
    )  # still pending: comes back after the lease
    monkeypatch.setattr(c.r, "xadd", real)
    time.sleep(0.25)
    c.run_once()
    assert pending(r, stream) == 0 and len(dlq(r, stream)) == 1


def test_reclaiming_continues_past_many_in_flight_messages(r, stream):
    """XAUTOCLAIM scans at most COUNT*10 PEL entries per call: always starting at 0-0 starved idle entries behind
    in-flight ones."""
    key = f"stream:{stream}"
    add(r, stream, *[f"p{i}" for i in range(60)])
    busy = make(r, stream, lambda ps: None, lease_ms=600_000)
    msgs = busy._read_new(60, 100)
    time.sleep(0.05)
    # the first 50 are "in flight" (recently heartbeated), the last 10 idle
    r.xclaim(key, "g", "c1", 0, [m.id for m in msgs[:50]], idle=0, justid=True)
    r.xclaim(key, "g", "c1", 0, [m.id for m in msgs[50:]], idle=10_000_000, justid=True)
    c = make(
        r, stream, lambda ps: None, lease_ms=1_000_000, consumer_id="c2", batch_size=1
    )
    got = []
    for _ in range(12):
        got += c._reclaim(1)
    assert {m.payload["productId"] for m in got} >= {"p50"}


def test_a_deleted_group_is_recreated(r, stream):
    add(r, stream, "a")
    seen = []
    stop = threading.Event()
    c = make(
        r,
        stream,
        lambda ps: seen.extend(p["productId"] for p in ps),
        stop=stop,
        max_wait_s=0.01,
    )
    t = threading.Thread(target=c.run)
    t.start()
    deadline = time.time() + 5
    while time.time() < deadline and seen != ["a"]:
        time.sleep(0.05)
    r.xgroup_destroy(f"stream:{stream}", "g")
    add(r, stream, "b")
    while time.time() < deadline and "b" not in seen:
        time.sleep(0.05)
    stop.set()
    t.join(5)
    assert seen[:1] == ["a"] and "b" in seen


def test_the_default_block_fits_any_socket_timeout(r, stream):
    c = BatchConsumer(redis.Redis.from_url(REDIS_URL, socket_timeout=3), stream, "g", "test", lambda ps: None)
    assert c.block_ms == 2000
    assert BatchConsumer(r, stream, "g", "test", lambda ps: None).block_ms == 5000  # socket_timeout 30
