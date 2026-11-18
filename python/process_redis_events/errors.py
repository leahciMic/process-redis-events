"""Transient vs permanent failures, and the backoff used while retrying in place.

A transient failure is one where the same work may succeed later without any change: a dependency (Redis,
Postgres, Milvus/gRPC, an HTTP service, the network) is down, overloaded or timing out. Consumers retry those in
place, keeping the message leased and using up no delivery. Anything else is permanent: the delivery fails, the
message is redelivered after its lease and, after `max_attempts` deliveries, goes to the dead letter queue.

The classification is by exception type (plus SQLSTATE, gRPC status or HTTP status), following ``__cause__`` and
``__context__``, never by message text except for Milvus server statuses that have no type of their own. Optional
dependencies (psycopg, pymilvus, grpc, requests, httpx) are imported lazily and only used when installed.
"""

from __future__ import annotations

import random
from collections.abc import Callable

import redis

BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 30.0

#: The kinds transient_kind() returns.
TRANSIENT_KINDS = ("postgres", "redis", "milvus", "http", "network", "other")


class TransientError(Exception):
    """A failure that is known to be temporary: the same call may succeed later."""


class Paused(Exception):
    """Processing can't go on for now (a spend cap, a rejected API key): keep the message pending, heartbeated,
    and retry it after ``wait_s`` seconds. No delivery is used up and no new message is read meanwhile.
    """

    def __init__(self, message: str, wait_s: float = 60.0):
        super().__init__(message)
        self.wait_s = wait_s


# Postgres SQLSTATEs worth retrying: connection exception (08), insufficient resources (53: too many connections,
# out of memory), operator intervention (57P01-05: admin/crash shutdown, cannot connect now, ...), serialization
# failure, deadlock, lock_not_available (55P03, lock_timeout) and query_canceled (57014, statement_timeout).
_TRANSIENT_SQLSTATES = frozenset(
    {"57P01", "57P02", "57P03", "57P04", "57P05", "40001", "40P01", "55P03", "57014"}
)

# gRPC status names (Milvus's transport) that mean "try again later"
_TRANSIENT_GRPC = frozenset(
    {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED", "ABORTED"}
)


def _transient_sqlstate(code: str | None) -> bool:
    return bool(code) and (code[:2] in ("08", "53") or code in _TRANSIENT_SQLSTATES)  # type: ignore[index]


def _grpc_code_name(code: object) -> str | None:
    name = getattr(code, "name", None)
    return name if isinstance(name, str) else None


def _http_status_transient(status: object) -> bool:
    return isinstance(status, int) and (status == 429 or 500 <= status <= 599)


def _redis_transient(err: BaseException) -> bool:
    if not isinstance(err, redis.RedisError):
        return False
    # ConnectionError covers BusyLoadingError (Redis loading its AOF); TimeoutError a slow or blocked server;
    # TryAgain/ClusterDown/MasterDown and ReadOnly (a replica during failover) clear by themselves.
    if isinstance(
        err,
        redis.ConnectionError
        | redis.TimeoutError
        | redis.exceptions.TryAgainError
        | redis.exceptions.ClusterDownError
        | redis.exceptions.MasterDownError
        | redis.exceptions.ReadOnlyError,
    ):
        return True
    # redis-py >= 5.1 (not in the type stubs)
    max_connections = getattr(redis.exceptions, "MaxConnectionsError", None)
    if max_connections is not None and isinstance(err, max_connections):
        return True
    # "BUSY Redis is busy running a script" has no exception class of its own
    return isinstance(err, redis.ResponseError) and str(err).startswith("BUSY ")


def _postgres_transient(err: BaseException) -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    if not isinstance(err, psycopg.Error):
        return False
    sqlstate = getattr(err, "sqlstate", None)
    if _transient_sqlstate(sqlstate):
        return True
    # libpq-level failures carry no SQLSTATE: "server closed the connection unexpectedly", "connection refused",
    # "the connection is closed", a pool timeout. An OperationalError with another SQLSTATE (program_limit_exceeded,
    # ...) is permanent.
    return isinstance(err, psycopg.OperationalError) and sqlstate is None


def _milvus_transient(err: BaseException) -> bool:
    try:
        from pymilvus.exceptions import (
            ConnectError,
            ErrorCode,
            MilvusException,
            MilvusUnavailableException,
        )
    except ImportError:
        MilvusException = None  # noqa: N806
    if MilvusException is not None and isinstance(err, MilvusException):
        if isinstance(err, ConnectError | MilvusUnavailableException) or getattr(
            err, "retriable", False
        ):
            return True
        code = getattr(err, "code", None)
        # The retry decorator re-raises a gRPC failure as MilvusException(<grpc.StatusCode>, ...).
        if _grpc_code_name(code) in _TRANSIENT_GRPC:
            return True
        # 2: the client's Status.CONNECT_FAILED and the server's ServiceUnavailable; 8: rate limited. (Not 1, 4 or
        # 14: the generic default, COLLECTION_NOT_EXISTS and FILE_NOT_FOUND.)
        if code in (2, ErrorCode.RATE_LIMIT):
            return True
        # Server-side statuses that name no type of their own (a proxy or query node still starting) and the retry
        # decorator's timeout wrapper: the one place where the message is all there is.
        msg = str(getattr(err, "message", "") or err).lower()
        return any(
            t in msg
            for t in (
                "service not ready",
                "service unavailable",
                "not ready yet",
                "retry timeout",
            )
        )
    try:
        import grpc
    except ImportError:
        return False
    if isinstance(err, grpc.RpcError):
        code_fn = getattr(err, "code", None)
        return callable(code_fn) and _grpc_code_name(code_fn()) in _TRANSIENT_GRPC
    return False


def _http_transient(err: BaseException) -> bool:
    try:
        import requests
    except ImportError:
        requests = None
    if requests is not None:
        if isinstance(err, requests.ConnectionError | requests.Timeout):
            return True
        if isinstance(err, requests.HTTPError) and _http_status_transient(
            getattr(err.response, "status_code", None)
        ):
            return True
    try:
        import httpx
    except ImportError:
        httpx = None
    if httpx is not None:
        if isinstance(err, httpx.TransportError):
            return True
        if isinstance(err, httpx.HTTPStatusError) and _http_status_transient(
            err.response.status_code
        ):
            return True
    import urllib.error

    return isinstance(err, urllib.error.HTTPError) and _http_status_transient(err.code)


def transient_kind(err: BaseException | None, depth: int = 0) -> str | None:
    """Which dependency a temporary failure came from (postgres, redis, milvus, http, network, other), or None when
    the error is permanent (retrying the same work won't help). Follows __cause__/__context__, so a wrapped
    connection error still counts."""
    if err is None or depth > 5:
        return None
    if isinstance(err, TransientError):
        return "other"
    if _redis_transient(err):
        return "redis"
    if _postgres_transient(err):
        return "postgres"
    if _milvus_transient(err):
        return "milvus"
    if _http_transient(err):
        return "http"
    if isinstance(err, ConnectionError | TimeoutError):
        return "network"
    return transient_kind(err.__cause__ or err.__context__, depth + 1)


def is_transient(err: BaseException | None) -> bool:
    return transient_kind(err) is not None


def backoff_delay(
    attempt: int, rnd: Callable[[], float] = random.random, cap: float = BACKOFF_MAX_S
) -> float:
    """Delay before retry number ``attempt`` (1-based): exponential from 1 s, capped at ``cap``, with "equal jitter"
    (between half and all of it) so consumers that failed together don't retry together. As the TypeScript
    consumers' backoffDelay()."""
    exp = min(cap, BACKOFF_BASE_S * 2 ** max(0, attempt - 1))
    return float(exp / 2 + rnd() * exp / 2)
