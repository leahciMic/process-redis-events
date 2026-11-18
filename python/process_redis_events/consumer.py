"""A synchronous (threaded) batch consumer of a process-redis-events stream, for services whose work is blocking
(Postgres, Milvus, model inference, paid LLM calls) rather than asyncio. Same stream format and the same semantics
as the TypeScript consumers:

- Format: stream ``<name>`` lives in key ``stream:<name>``, one field ``data`` holding a JSON object; failed
  messages go to ``stream:dlq:<name>`` (payload plus ``_dlqMetadata``, see dlq.py).
- Batches: up to ``batch_size`` messages, waiting at most ``max_wait_s`` after the first one for the batch to fill
  up, then ``handler(payloads)`` once. ``batch_size=1`` (or StreamConsumer) handles one message at a time.
  Messages whose lease expired are reclaimed first (XAUTOCLAIM, with a cursor kept between calls so idle entries
  behind many in-flight ones aren't starved), then new ones read (XREADGROUP ``>``).
- Stable consumer name (naming.consumer_name: ``STREAM_CONSUMER_NAME``, else ``<hostname>-<service>``). On start
  the consumer first takes over its own pending messages (XREADGROUP from id 0, a delivery each), so a restart
  under the same name doesn't wait for the lease.
- Lease and heartbeat: while a batch is processed, a heartbeat thread resets the idle time of its messages every
  lease/3, but only of those this consumer still owns (entries.HEARTBEAT_LUA), so a heartbeat never takes a
  message back from a consumer that reclaimed it.
- Transient failures (errors.transient_kind) are retried in place, the whole batch, with capped exponential backoff
  and jitter, for up to ``transient_max_s`` per batch (bisected halves share the window): nothing is acknowledged,
  no delivery is used up and the heartbeat keeps the lease. Past that, the batch fails as a whole (no bisection:
  the dependency is down, not a message bad) and is redelivered after the lease, which uses up a delivery.
  ``Paused`` keeps the batch, heartbeated, and retries it after ``wait_s`` without using up the window.
- Redis failing around the handler (the reads after a reclaim, XPENDING, XACK, the DLQ write) is retried in place
  too: messages already claimed are processed, an XACK that times out is retried without running the handler
  again, and a message is acknowledged only once its DLQ entry is written.
- Permanent failures: a batch of several messages is bisected, down to single messages, so one bad message doesn't
  hold the others back. ``handler`` may also return a list with an error (anything but None) at the index of each
  message that failed. A failed message isn't acknowledged: it's redelivered after the lease, and once it has been
  delivered more than ``max_attempts`` times it goes to the dead letter queue. A message that isn't a JSON object
  (or that ``validate`` rejects) goes there at once.
- The blocking read is kept shorter than the client's socket timeout (timeouts.safe_block_ms).
- ``should_read()`` False: nothing is read (messages stay pending/unread); asked again every ``idle_check_s``.
- Outside a batch, a transient failure (reading, ``should_read()`` connecting to a dependency) is retried for as
  long as it lasts rather than ending ``run()``.
- Shutdown: set ``stop`` (e.g. from SIGTERM, install_signal_handlers): waits end at once and the loop returns after
  the current handler call; an unfinished batch stays pending and is reclaimed after the lease, or at once by the
  next process with the same name.
- Consumers of the group that have been idle for an hour with nothing pending are removed (dead pods).

Handlers must be idempotent: a message can be delivered more than once (a crash before the ack, a lease that
expired while the heartbeat couldn't reach Redis).
"""

from __future__ import annotations

import logging
import signal as signal_module
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import redis

from process_redis_events.dlq import decode_fields, dlq_data, dlq_key
from process_redis_events.entries import HEARTBEAT_LUA, group_backlog, parse_fields
from process_redis_events.errors import Paused, backoff_delay, transient_kind
from process_redis_events.naming import consumer_name
from process_redis_events.timeouts import safe_block_ms

log = logging.getLogger("process_redis_events")

T = TypeVar("T")

TRANSIENT_GIVE_UP_S = 15 * 60
IDLE_CONSUMER_MS = 60 * 60 * 1000


def _s(v: Any) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


@dataclass
class Message:
    id: str
    payload: Any
    #: Deliveries so far, this one included (1 on the first delivery). ``deliveries - 1`` is the TypeScript
    #: library's ``attempts``.
    deliveries: int
    fields: dict[str, str] = field(default_factory=dict)
    parse_error: Exception | None = None

    @property
    def attempts(self) -> int:
        """Earlier deliveries (0 on the first), as QueueItem.attempts."""
        return self.deliveries - 1

    @property
    def millis(self) -> int:
        """When it was added (the stream id's timestamp), in ms since the epoch."""
        return int(self.id.split("-", 1)[0])


def _safe(fn: Callable[..., Any], *args: Any) -> None:
    """Metrics callbacks must never fail a batch."""
    try:
        fn(*args)
    except Exception as e:
        log.debug("callback %r failed: %r", fn, e)


class Outage:
    """One transient outage at a time: a warning when it starts, debug lines while it lasts, an info line when it's
    over (not a line per retry)."""

    def __init__(
        self,
        service: str,
        on_transient: Callable[[str], None],
        on_outage: Callable[[bool], None],
    ):
        self.service = service
        self.on_transient = on_transient
        self.on_outage = on_outage
        self.since: float | None = None
        self.retries = 0
        self.kinds: set[str] = set()

    @property
    def active(self) -> bool:
        return self.since is not None

    def retry(
        self, kind: str, what: str, err: BaseException, delay: float, give_up_s: float
    ) -> None:
        self.retries += 1
        self.kinds.add(kind)
        _safe(self.on_transient, kind)
        if self.since is None:
            self.since = time.monotonic()
            _safe(self.on_outage, True)
            log.warning(
                "%s: %s unavailable (%s: %r); retrying %s in place %s, using up no delivery",
                self.service,
                kind,
                type(err).__name__,
                err,
                what,
                (
                    f"for up to {give_up_s:.0f}s"
                    if give_up_s != float("inf")
                    else "until it's back"
                ),
            )
        else:
            log.debug(
                "%s: %s still failing (%s), retry %d in %.1fs: %r",
                self.service,
                what,
                kind,
                self.retries,
                delay,
                err,
            )

    def over(self, how: str = "recovered") -> None:
        if self.since is None:
            return
        log.log(
            logging.INFO if how == "recovered" else logging.WARNING,
            "%s: %s after %.0fs of transient failures (%s; %d retries)",
            self.service,
            how,
            time.monotonic() - self.since,
            ", ".join(sorted(self.kinds)),
            self.retries,
        )
        self.since = None
        self.retries = 0
        self.kinds = set()
        _safe(self.on_outage, False)


class GiveUp(Exception):
    """A transient failure lasted longer than transient_max_s (or the consumer is stopping)."""


def install_signal_handlers(stop: threading.Event, signals: Sequence[int] = ()) -> None:
    """Sets ``stop`` on SIGTERM and SIGINT (or ``signals``). Call it from the main thread."""
    for sig in signals or (signal_module.SIGTERM, signal_module.SIGINT):
        signal_module.signal(sig, lambda *_: stop.set())


class BatchConsumer:
    """See the module docstring. ``handler(payloads)`` returns None (all succeeded) or a sequence with an error (or
    None) per payload; raising fails the whole batch (transient errors are retried in place).
    """

    def __init__(
        self,
        r: "redis.Redis[Any]",
        stream: str,
        group: str,
        service: str,
        handler: Callable[[list[Any]], Sequence[object | None] | object | None],
        *,
        batch_size: int = 500,
        max_wait_s: float = 30.0,
        lease_ms: int = 10 * 60 * 1000,
        max_attempts: int = 3,
        block_ms: int = 5000,
        should_read: Callable[[], bool] = lambda: True,
        idle_check_s: float = 30.0,
        stop: threading.Event | None = None,
        consumer_id: str | None = None,
        on_batch: Callable[[str, list[Message], float], None] | None = None,
        validate: Callable[[dict[str, Any]], str | None] = lambda payload: None,
        transient_max_s: float = TRANSIENT_GIVE_UP_S,
        backoff_max_s: float = 60.0,
        on_transient: Callable[[str], None] = lambda kind: None,
        on_outage: Callable[[bool], None] = lambda active: None,
        classify: Callable[[BaseException], str | None] = transient_kind,
        reclaim_own: bool = True,
        remove_idle: bool = True,
    ):
        self.r = r
        self.stream = stream
        self.key = f"stream:{stream}"
        self.dlq_key = dlq_key(self.key)
        self.group = group
        self.service = service
        self.handler = handler
        self.batch_size = batch_size
        self.max_wait_s = max_wait_s
        self.lease_ms = lease_ms
        self.max_attempts = max_attempts
        self._block_ms = 0
        self.block_ms = block_ms
        self.should_read = should_read
        self.idle_check_s = idle_check_s
        self.stop = stop or threading.Event()
        self.consumer = consumer_name(service, consumer_id)
        # on_batch(outcome, messages, seconds): outcome ok / failed / dead_letter / stopped
        self.on_batch = on_batch or (lambda outcome, msgs, s: None)
        # validate(payload) -> why it can never be processed (it goes to the dead letter queue at once), or None
        self.validate = validate
        self.transient_max_s = transient_max_s
        self.backoff_max_s = backoff_max_s
        self.classify = classify
        self.outage = Outage(service, on_transient, on_outage)
        self.reclaim_own = reclaim_own
        self.remove_idle = remove_idle
        self._own_cursor: str | None = "0-0" if reclaim_own else None
        self._claim_cursor = "0-0"
        self._give_up = 0.0
        self._current: list[str] = []
        self._lock = threading.Lock()
        self._heartbeat: threading.Thread | None = None
        self._heartbeat_script = r.register_script(HEARTBEAT_LUA)
        self._next_cleanup = 0.0

    @property
    def block_ms(self) -> int:
        return self._block_ms

    @block_ms.setter
    def block_ms(self, value: int) -> None:
        safe = safe_block_ms(self.r, value)
        if safe != value:
            log.warning(
                "%s: block %d ms is too long for the Redis client's socket timeout; reading with %d ms",
                self.service,
                value,
                safe,
            )
        self._block_ms = safe

    def _xack(self, *ids: str) -> int:
        return int(self.r.xack(self.key, self.group, *ids))  # type: ignore[no-untyped-call]

    # ------------------------------------------------------------------ setup
    def ensure_group(self) -> None:
        """Creates the group at the oldest entry (messages added before it existed are processed)."""
        try:
            self.r.xgroup_create(self.key, self.group, id="0", mkstream=True)
            log.info("created group %s on %s", self.group, self.key)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _heartbeat_loop(self) -> None:
        interval = max(self.lease_ms / 3000, 0.05)
        while not self.stop.wait(interval):
            self.heartbeat()

    def heartbeat(self) -> list[str]:
        """Resets the idle time of the messages in progress that this consumer still owns; returns the ids it lost."""
        with self._lock:
            ids = list(self._current)
        lost: list[str] = []
        for start in range(0, len(ids), 500):
            try:
                reply = self._heartbeat_script(
                    keys=[self.key],
                    args=[self.group, self.consumer, *ids[start : start + 500]],
                )
            except redis.RedisError as e:
                log.warning("%s: heartbeat failed: %s", self.service, e)
                continue
            lost += [_s(x) for x in reply or []]
        if lost:
            with self._lock:
                gone = [i for i in lost if i in self._current]
            if gone:
                log.warning(
                    "%s: %d message(s) in progress were acknowledged or reclaimed by another consumer (lease %d ms "
                    "expired?): %s",
                    self.service,
                    len(gone),
                    self.lease_ms,
                    ", ".join(gone[:5]),
                )
        return lost

    def start_heartbeat(self) -> None:
        if self._heartbeat is None or not self._heartbeat.is_alive():
            self._heartbeat = threading.Thread(
                target=self._heartbeat_loop, name="heartbeat", daemon=True
            )
            self._heartbeat.start()

    # ------------------------------------------------------------------ reading
    def _message(self, mid: str, fields: Any, deliveries: int) -> Message:
        payload, error = parse_fields(fields)
        return Message(mid, payload, deliveries, decode_fields(fields), error)

    def _deliveries(self, ids: list[str]) -> dict[str, int]:
        """Delivery counts of ids this consumer owns (ids claimed or acked by someone else meanwhile are left out)."""
        if not ids:
            return {}
        pipe = self.r.pipeline(transaction=False)
        for mid in ids:
            pipe.xpending_range(self.key, self.group, min=mid, max=mid, count=1)
        out: dict[str, int] = {}
        for mid, rows in zip(ids, pipe.execute(), strict=True):
            if rows and _s(rows[0]["consumer"]) == self.consumer:
                out[mid] = int(rows[0]["times_delivered"])
        return out

    def _with_counts(self, entries: list[tuple[str, Any]]) -> list[Message]:
        ids = [mid for mid, _ in entries]
        counts = (
            self._redis_retry(lambda: self._deliveries(ids), "reading delivery counts")
            if ids
            else {}
        )
        return [
            self._message(mid, fields, counts[mid])
            for mid, fields in entries
            if mid in counts
        ]

    def _read_own(self, count: int) -> list[Message]:
        """This consumer's own pending messages (left by an earlier process with the same name), each a delivery."""
        if self._own_cursor is None:
            return []
        res: Any = self.r.xreadgroup(
            self.group, self.consumer, {self.key: self._own_cursor}, count=count
        )
        entries = [(_s(mid), fields) for _, msgs in res or [] for mid, fields in msgs]
        if not entries:
            self._own_cursor = None
            return []
        self._own_cursor = entries[-1][0]
        deleted = [mid for mid, fields in entries if not fields]
        if deleted:
            # deleted from the stream (trimmed) while pending: nothing to process
            self._redis_retry(
                lambda: self._xack(*deleted),
                "acknowledging deleted entries",
            )
        live = [(mid, fields) for mid, fields in entries if fields]
        msgs = self._with_counts(live)
        if msgs:
            log.info(
                "%s: took over %d pending message(s) of %s",
                self.service,
                len(msgs),
                self.consumer,
            )
        return msgs

    def _reclaim(self, count: int) -> list[Message]:
        """Messages whose lease expired (any consumer of the group, this one included), a delivery each."""
        claimed: Any = self.r.xautoclaim(
            self.key,
            self.group,
            self.consumer,
            self.lease_ms,
            start_id=self._claim_cursor,
            count=count,
        )
        # Continue the PEL scan where it stopped (XAUTOCLAIM inspects at most COUNT*10 entries per call); Redis
        # returns 0-0 once the scan wraps around.
        self._claim_cursor = _s(claimed[0]) if claimed and claimed[0] else "0-0"
        entries = [
            (_s(mid), fields)
            for mid, fields in (claimed[1] or [])
            if fields is not None
        ]
        # Entries deleted from the stream while pending come back as ids only (Redis 7 drops them from the PEL).
        deleted = [_s(mid) for mid in (claimed[2] if len(claimed) > 2 else [])]
        if deleted:
            self._redis_retry(
                lambda: self._xack(*deleted),
                "acknowledging deleted entries",
            )
        return self._with_counts(entries)

    def _read_new(self, count: int, block_ms: int | None) -> list[Message]:
        res: Any = self.r.xreadgroup(
            self.group, self.consumer, {self.key: ">"}, count=count, block=block_ms
        )
        return [
            self._message(_s(mid), fields, 1)
            for _, msgs in res or []
            for mid, fields in msgs
        ]

    def collect(self) -> list[Message]:
        """Up to batch_size messages: this consumer's own pending ones first (after a restart), then expired leases,
        then new ones, waiting at most max_wait_s after the first message for the batch to fill up. Empty when
        nothing arrived within block_ms.

        Once some messages are claimed (each claim counts a delivery), a failing read only ends the batch early:
        the messages already claimed are processed rather than dropped until their lease expires.
        """
        batch = self._read_own(self.batch_size)
        if batch:
            return batch
        batch = self._reclaim(self.batch_size)
        try:
            if len(batch) < self.batch_size:
                batch += self._read_new(self.batch_size - len(batch), self.block_ms)
            if not batch:
                return batch
            deadline = time.monotonic() + self.max_wait_s
            while len(batch) < self.batch_size and not self.stop.is_set():
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                batch += self._read_new(
                    self.batch_size - len(batch), max(int(min(left, 1.0) * 1000), 1)
                )
        except redis.RedisError as e:
            if not batch:
                raise
            log.debug(
                "%s: reading more failed (%r); processing the %d messages claimed",
                self.service,
                e,
                len(batch),
            )
        return batch

    # ------------------------------------------------------------------ processing
    def _wait(self, seconds: float) -> bool:
        """Sleeps; False if stopped meanwhile."""
        return not self.stop.wait(seconds)

    def _retry_wait(
        self, kind: str, what: str, err: BaseException, attempt: int
    ) -> None:
        """Waits before retrying a transient failure in place; GiveUp once transient_max_s is used up or stopping."""
        left = self._give_up - time.monotonic()
        if left <= 0:
            raise GiveUp(what) from err
        delay = min(backoff_delay(attempt, cap=self.backoff_max_s), left)
        self.outage.retry(kind, what, err, delay, self.transient_max_s)
        if not self._wait(delay):
            raise GiveUp("stopping") from err

    def _redis_retry(self, fn: Callable[[], T], what: str) -> T:
        """fn(), retrying transient Redis failures in place (within the current batch's transient_max_s, or a new
        window outside a batch)."""
        if self._give_up <= time.monotonic():
            self._give_up = time.monotonic() + self.transient_max_s
        attempt = 0
        while True:
            try:
                return fn()
            except redis.RedisError as e:
                kind = self.classify(e)
                if kind is None:
                    raise
                attempt += 1
                try:
                    self._retry_wait(kind, what, e, attempt)
                except GiveUp:
                    raise e from None

    def _ack(self, msgs: list[Message]) -> None:
        # the work is done: acknowledging it gets a full window of its own, whatever the batch used up
        self._give_up = max(self._give_up, time.monotonic() + self.transient_max_s)
        for start in range(0, len(msgs), 1000):
            ids = [m.id for m in msgs[start : start + 1000]]

            def ack(ids: list[str] = ids) -> int:
                return self._xack(*ids)

            self._redis_retry(ack, "acknowledging the batch")

    def _report(self, outcome: str, msgs: list[Message], seconds: float) -> None:
        _safe(self.on_batch, outcome, msgs, seconds)

    def _dead_letter(self, msg: Message, reason: str, error: str | None = None) -> None:
        data = dlq_data(
            msg.payload,
            original_id=msg.id,
            attempts=msg.attempts,
            consumer_group=self.group,
            consumer_id=self.consumer,
            raw_fields=None if isinstance(msg.payload, dict) else msg.fields,
            error=error,
            reason=reason,
        )
        self._give_up = max(self._give_up, time.monotonic() + self.transient_max_s)
        # Acknowledged only once the DLQ entry is written: if writing fails for good, the message stays pending and
        # comes back after its lease.
        self._redis_retry(
            lambda: self.r.xadd(self.dlq_key, {"data": data}), "writing a dead letter"
        )
        self._redis_retry(
            lambda: self._xack(msg.id),
            "acknowledging a dead letter",
        )
        log.error(
            "%s: message %s moved to %s: %s", self.service, msg.id, self.dlq_key, reason
        )
        self._report("dead_letter", [msg], 0.0)

    def process(self, batch: list[Message]) -> str:
        """Handles a batch; returns ok, failed (some not acknowledged) or stopped."""
        live: list[Message] = []
        for m in batch:
            if m.parse_error is not None:
                self._dead_letter(m, "not JSON", error=str(m.parse_error))
            elif not isinstance(m.payload, dict):
                self._dead_letter(m, "not a JSON object")
            elif m.deliveries > self.max_attempts:
                self._dead_letter(m, f"failed {m.deliveries - 1} deliveries")
            elif (why := self.validate(m.payload)) is not None:
                self._dead_letter(m, why)
            else:
                live.append(m)
        if not live:
            return "ok"
        with self._lock:
            self._current = [m.id for m in live]
        # one window of transient retries for the batch and all its bisected parts
        self._give_up = time.monotonic() + self.transient_max_s
        try:
            return self._process(live)
        finally:
            with self._lock:
                self._current = []
            self._give_up = 0.0

    def _fail_one(self, m: Message, e: object) -> None:
        last = m.deliveries >= self.max_attempts
        log.error(
            "%s: message %s failed (delivery %d/%d, %s): %r",
            self.service,
            m.id,
            m.deliveries,
            self.max_attempts,
            (
                "next goes to the dead letter queue"
                if last
                else f"retried in {self.lease_ms / 1000:.0f}s"
            ),
            e,
            exc_info=(
                e if isinstance(e, BaseException) and self.classify(e) is None else None
            ),
        )

    def _process(self, batch: list[Message]) -> str:
        started = time.monotonic()
        attempt = 0
        while True:
            try:
                result = self.handler([m.payload for m in batch])
            except Paused as e:
                log.warning(
                    "%s: paused on a batch of %d: %s (retrying in %.0fs)",
                    self.service,
                    len(batch),
                    e,
                    e.wait_s,
                )
                if not self._wait(e.wait_s):
                    self._report("stopped", batch, time.monotonic() - started)
                    return "stopped"
                # a pause isn't an outage: the retry window starts again
                self._give_up = time.monotonic() + self.transient_max_s
                attempt = 0
                continue
            except Exception as e:
                kind = self.classify(e)
                if kind is not None:
                    attempt += 1
                    try:
                        self._retry_wait(kind, f"a batch of {len(batch)}", e, attempt)
                        continue
                    except GiveUp as g:
                        if str(g) == "stopping":
                            self._report("stopped", batch, time.monotonic() - started)
                            return "stopped"
                        self._report("failed", batch, time.monotonic() - started)
                        self.outage.over("gave up")
                        log.error(
                            "%s: batch of %d still failing after %.0fs of transient errors; not acknowledged, "
                            "redelivered after the lease (%.0fs): %r",
                            self.service,
                            len(batch),
                            self.transient_max_s,
                            self.lease_ms / 1000,
                            e,
                        )
                        return "failed"
                self.outage.over()
                self._report("failed", batch, time.monotonic() - started)
                if len(batch) > 1:
                    log.warning(
                        "%s: batch of %d failed (%r); processing it in halves",
                        self.service,
                        len(batch),
                        e,
                    )
                    half = len(batch) // 2
                    outcomes = [self._process(batch[:half])]
                    if outcomes[0] != "stopped" and not self.stop.is_set():
                        outcomes.append(self._process(batch[half:]))
                    return (
                        "stopped"
                        if "stopped" in outcomes or self.stop.is_set()
                        else "failed"
                    )
                self._fail_one(batch[0], e)
                return "failed"
            self.outage.over()
            errors = list(result) if isinstance(result, list | tuple) else []
            if errors and len(errors) != len(batch):
                raise ValueError(
                    f"handler returned {len(errors)} results for {len(batch)} messages"
                )
            failed = [
                m for m, err in zip(batch, errors, strict=False) if err is not None
            ]
            failed_ids = {m.id for m in failed}
            done = [m for m in batch if m.id not in failed_ids]
            self._ack(done)
            for m, err in zip(batch, errors, strict=False):
                if err is not None:
                    self._fail_one(m, err)
            if failed:
                self._report("failed", failed, time.monotonic() - started)
            if done:
                self._report("ok", done, time.monotonic() - started)
            return "failed" if failed else "ok"

    # ------------------------------------------------------------------ housekeeping
    def remove_idle_consumers(self) -> None:
        """Removes consumers of the group (not this one) idle for an hour with nothing pending: dead processes."""
        try:
            consumers: Any = self.r.xinfo_consumers(self.key, self.group)  # type: ignore[no-untyped-call]
            for c in consumers:
                name = _s(c["name"])
                if (
                    name != self.consumer
                    and int(c["pending"]) == 0
                    and int(c["idle"]) > IDLE_CONSUMER_MS
                ):
                    self.r.xgroup_delconsumer(self.key, self.group, name)  # type: ignore[no-untyped-call]
                    log.info(
                        "%s: removed idle consumer %s from %s %s",
                        self.service,
                        name,
                        self.key,
                        self.group,
                    )
        except redis.RedisError as e:
            if "NOGROUP" not in str(e) and "no such key" not in str(e):
                log.warning("%s: removing idle consumers failed: %s", self.service, e)

    def group_lag(self) -> tuple[int | None, int | None]:
        """(unread, pending) of the group, or None when the stream or group doesn't exist (yet). Unread is counted
        from the entry ids (entries.group_backlog), not Redis's lag, which can stay wrong after a Redis restart.
        """
        try:
            backlog = group_backlog(self.r, self.key, self.group)
        except redis.RedisError:
            return None, None
        if backlog is None:
            return None, None
        return backlog[0], backlog[1]

    # ------------------------------------------------------------------ loop
    def run_once(self) -> int:
        """Processes one batch; returns its size (0 when nothing was waiting)."""
        batch = self.collect()
        if batch:
            self.process(batch)
        return len(batch)

    def run(self, drain: bool = False) -> None:
        """Consumes until stopped (or, with drain, until nothing is left to read or should_read() is False)."""
        log.info(
            "%s: consuming %s as %s/%s (batches of up to %d, %.0fs wait, lease %d ms, %d attempts)",
            self.service,
            self.key,
            self.group,
            self.consumer,
            self.batch_size,
            self.max_wait_s,
            self.lease_ms,
            self.max_attempts,
        )
        self.start_heartbeat()
        failures = 0
        group_ready = False
        while not self.stop.is_set():
            try:
                if not self.should_read():
                    if drain:
                        break
                    self._wait(self.idle_check_s)
                    continue
                if not group_ready:
                    self.ensure_group()
                    group_ready = True
                if self.remove_idle and time.monotonic() >= self._next_cleanup:
                    self._next_cleanup = time.monotonic() + IDLE_CONSUMER_MS / 1000
                    self.remove_idle_consumers()
                n = self.run_once()
                failures = 0
                if self.outage.active:
                    self.outage.over()
                if drain and n == 0:
                    break
            except Exception as e:
                # Outside a batch (reading, should_read() loading models and connecting to a dependency): retry any
                # transient failure (and any Redis error) for as long as it lasts, rather than end the loop.
                kind = self.classify(e) or (
                    "redis" if isinstance(e, redis.RedisError) else None
                )
                if kind is None:
                    raise
                if isinstance(e, redis.ResponseError) and "NOGROUP" in str(e):
                    group_ready = False  # the stream or group was deleted: recreate it, as a restart would
                failures += 1
                delay = backoff_delay(failures, cap=self.backoff_max_s)
                self.outage.retry(kind, "reading", e, delay, float("inf"))
                self._wait(delay)
        log.info("%s: stopped", self.service)


class StreamConsumer(BatchConsumer):
    """One message at a time: ``handler(payload)`` per message (slow, paid calls; order per key is kept). Defaults
    to a 5 minute lease."""

    def __init__(
        self,
        r: "redis.Redis[Any]",
        stream: str,
        group: str,
        service: str,
        handler: Callable[[dict[str, Any]], object],
        *,
        lease_ms: int = 5 * 60 * 1000,
        **kwargs: Any,
    ):
        kwargs.setdefault("batch_size", 1)
        kwargs.setdefault("max_wait_s", 0.0)
        self.message_handler = handler
        super().__init__(
            r, stream, group, service, self._handle_batch, lease_ms=lease_ms, **kwargs
        )

    def _handle_batch(self, payloads: list[Any]) -> None:
        for payload in payloads:
            self.message_handler(payload)
