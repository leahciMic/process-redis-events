"""The dead letter queue entry, as process-redis-events (TypeScript and Python) writes it.

A message that can't be processed goes to ``stream:dlq:<name>`` (the stream ``dlq:<name>``) as one ``data`` field
holding JSON: the original payload with ``_dlqMetadata`` added. A message whose payload isn't a JSON object (or
isn't JSON at all) keeps its raw fields under ``_raw`` instead. To replay one, XADD its ``data`` without
``_dlqMetadata`` back to ``stream:<name>``.
"""

from __future__ import annotations

import json
import time
from typing import Any


def dumps(value: Any) -> str:
    """JSON as JSON.stringify writes it (compact, non-ASCII as is), so both libraries write the same bytes."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def dlq_key(stream_key: str) -> str:
    """``stream:<name>`` -> ``stream:dlq:<name>``."""
    prefix = "stream:"
    name = stream_key[len(prefix) :] if stream_key.startswith(prefix) else stream_key
    return f"{prefix}dlq:{name}"


def dlq_data(
    payload: Any,
    *,
    original_id: str,
    attempts: int,
    consumer_group: str,
    consumer_id: str,
    raw_fields: dict[str, str] | None = None,
    error: str | None = None,
    reason: str | None = None,
) -> str:
    """The JSON for the DLQ entry's ``data`` field. ``attempts`` is the number of earlier deliveries (what
    ``QueueItem.attempts`` and TypeScript's ``_dlqMetadata.attempts`` hold): 0 for a message dead-lettered on its
    first delivery."""
    meta: dict[str, Any] = {
        "originalId": original_id,
        "attempts": attempts,
        "consumerGroup": consumer_group,
        "consumerId": consumer_id,
        "timestamp": int(time.time() * 1000),
    }
    if error is not None:
        meta["error"] = error
    if reason is not None:
        meta["reason"] = reason
    if isinstance(payload, dict):
        body: dict[str, Any] = {**payload}
    else:
        body = {
            "_raw": (raw_fields if raw_fields is not None else {"data": dumps(payload)})
        }
    body["_dlqMetadata"] = meta
    return dumps(body)


def decode_fields(fields: dict[Any, Any] | None) -> dict[str, str]:
    return {_s(k): _s(v) for k, v in (fields or {}).items()}


def _s(v: Any) -> str:
    return v.decode(errors="replace") if isinstance(v, bytes) else str(v)
