"""Keeping a blocking read shorter than the client's socket timeout.

redis-py (8.x) defaults ``socket_timeout`` to 5 s for ``Redis(...)`` (not for ``Redis.from_url``). A blocking
XREADGROUP whose BLOCK is as long as that fails with "Timeout reading from socket" on a quiet stream. That's a
spurious error, and in a consumer that has just claimed messages (XAUTOCLAIM counts a delivery) it used to drop
them until their lease expired (clok #1470: 466 messages dead-lettered without ever failing).
"""

from __future__ import annotations

from typing import Any

#: How much shorter than the socket timeout a blocking read must be.
MARGIN_MS = 1000


def socket_timeout_s(client: Any) -> float | None:
    """The client's socket timeout in seconds (sync or asyncio redis-py client), or None for none."""
    pool = getattr(client, "connection_pool", None)
    kwargs = getattr(pool, "connection_kwargs", None) or {}
    timeout = kwargs.get("socket_timeout")
    return float(timeout) if isinstance(timeout, int | float) and timeout > 0 else None


def safe_block_ms(client: Any, block_ms: int) -> int:
    """``block_ms``, shortened when the client's socket timeout would expire first: at most the timeout minus 1 s
    (and at least half of it)."""
    timeout = socket_timeout_s(client)
    if timeout is not None and block_ms >= timeout * 1000 - MARGIN_MS:
        return max(int(timeout * 1000) - MARGIN_MS, int(timeout * 500))
    return block_ms
