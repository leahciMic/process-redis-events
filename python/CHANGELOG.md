# Changelog (Python)

## 5.2.0

Parity with the TypeScript library (5.1/5.2) and clok's production consumers.

- New: `BatchConsumer` / `StreamConsumer`, a synchronous (threaded) consumer: batches with a fill-up wait,
  owner-checked heartbeats, transient retry in place (no delivery used up, 15 min window shared by bisected halves),
  `Paused`, bisection of permanent failures, per-message results, dead letters acked only once written, a stable
  consumer name (`STREAM_CONSUMER_NAME`) that takes over its own pending messages on start, removal of idle
  consumers, `group_backlog()` counted from the ids (Redis 7.2's `lag` is wrong after an AOF reload).
- New: `transient_kind`/`is_transient`, `TransientError`, `Paused`, `backoff_delay`, `safe_block_ms`,
  `consumer_name`, `dlq_data`, `parse_fields`.
- Fixed in `Stream.process`:
  - the blocking read (lease - 1 s, 29 s by default) outlived redis-py 8's default 5 s socket timeout, so a quiet
    stream raised "Timeout reading from socket" and ended `process()`; BLOCK now stays under the socket timeout;
  - any Redis error in the read loop, or a failing `create_group`, ended `process()`; now retried with backoff,
    and a deleted group is recreated;
  - after an XAUTOCLAIM the consumer read its own PEL from `0-0`, which returned its oldest pending message (one
    still in progress) instead of the claimed ones, so a message could be processed twice at once; XAUTOCLAIM now
    returns the entries itself, with a cursor kept between calls (no starvation behind in-flight entries), and
    claims taken by another consumer meanwhile are skipped;
  - a message was acked even when writing its dead letter failed (lost); now acked only after the write;
  - `_dlqMetadata.timestamp` was the event loop's monotonic clock, not ms since the epoch;
  - a `data` field that isn't JSON raised out of `process()`; it now goes to the DLQ with its raw fields (`_raw`);
  - legacy entries (one field per value) are decoded to strings;
  - the heartbeat took messages back from a consumer that had reclaimed them; it now touches only its own;
  - callback errors were swallowed silently; they're logged, or passed to `on_error`;
  - setting the stop event now interrupts the blocking read;
  - `redis_stream_pending_messages` was recorded with one label for every group (and as a histogram); now one
    series per group, a gauge where opentelemetry-api has one;
  - `Stream.add` writes compact JSON with non-ASCII as is, the same bytes as `JSON.stringify`.
- Dropped the unused `pydantic` and `typing-extensions` dependencies.
- Tests: against a real Redis (`REDIS_URL`), including the shared format fixture.
