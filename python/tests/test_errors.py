"""Transient vs permanent classification, the BLOCK guard and the backoff."""

import redis

from process_redis_events import (
    TransientError,
    backoff_delay,
    is_transient,
    safe_block_ms,
    transient_kind,
)
from process_redis_events.naming import consumer_name


def _chained(outer: BaseException, cause: BaseException) -> BaseException:
    try:
        raise outer from cause
    except BaseException as e:
        return e


def test_transient_kind():
    cases = {
        redis.TimeoutError("Timeout reading from socket"): "redis",
        redis.ConnectionError("Connection refused"): "redis",
        redis.BusyLoadingError("Redis is loading the dataset in memory"): "redis",
        redis.ResponseError("BUSY Redis is busy running a script"): "redis",
        redis.exceptions.ReadOnlyError(
            "You can't write against a read only replica."
        ): "redis",
        ConnectionResetError(): "network",
        TimeoutError(): "network",
        TransientError("x"): "other",
    }
    for err, kind in cases.items():
        assert transient_kind(err) == kind, err
    for err in [
        ValueError("x"),
        KeyError("k"),
        redis.ResponseError("WRONGTYPE Operation against a key"),
    ]:
        assert transient_kind(err) is None, err
    assert is_transient(_chained(RuntimeError("wrapped"), ConnectionResetError()))
    assert not is_transient(_chained(RuntimeError("wrapped"), ValueError()))


def test_http_errors():
    import urllib.error

    assert transient_kind(urllib.error.HTTPError("u", 503, "x", None, None)) == "http"  # type: ignore[arg-type]
    assert transient_kind(urllib.error.HTTPError("u", 404, "x", None, None)) is None  # type: ignore[arg-type]


def test_safe_block_ms_stays_under_the_socket_timeout():
    assert (
        safe_block_ms(redis.Redis(), 5000) == 4000
    )  # redis-py 8 defaults socket_timeout to 5 s
    assert safe_block_ms(redis.Redis(), 29_000) == 4000
    assert safe_block_ms(redis.Redis(socket_timeout=30), 5000) == 5000
    assert safe_block_ms(redis.Redis(socket_timeout=None), 5000) == 5000
    assert safe_block_ms(redis.Redis(socket_timeout=1), 5000) == 500
    from redis.asyncio import Redis as AsyncRedis

    assert safe_block_ms(AsyncRedis(), 29_000) == 4000


def test_backoff_delay_is_capped_with_jitter():
    assert backoff_delay(1, rnd=lambda: 0.0) == 0.5
    assert backoff_delay(1, rnd=lambda: 1.0) == 1.0
    assert backoff_delay(20, rnd=lambda: 1.0, cap=60) == 60
    assert backoff_delay(20, rnd=lambda: 0.0, cap=60) == 30


def test_consumer_name(monkeypatch):
    monkeypatch.delenv("STREAM_CONSUMER_NAME", raising=False)
    monkeypatch.setenv("HOSTNAME", "host")
    assert consumer_name("svc") == "host-svc"
    assert consumer_name("svc", index=2) == "host-svc-2"
    monkeypatch.setenv("STREAM_CONSUMER_NAME", "fixed")
    assert consumer_name("svc") == "fixed"
    assert consumer_name("svc", "explicit") == "explicit"
    assert len(consumer_name()) == 21 or consumer_name() == "fixed"
