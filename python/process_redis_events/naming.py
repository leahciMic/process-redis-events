"""Consumer names that survive a restart.

A consumer's pending messages (read but not acknowledged) belong to its name. A process that comes back under the
same name takes them over at once (see ``reclaim_own``); one that comes back under a new name has to wait for their
lease to expire before XAUTOCLAIM hands them to anyone. In Kubernetes the hostname is the pod name, which a
Deployment changes on every restart, so set ``STREAM_CONSUMER_NAME`` (e.g. to the service name) where only one
replica runs at a time (strategy Recreate, or a StatefulSet's stable pod name).
"""

from __future__ import annotations

import os
import socket
import uuid

#: Environment variable naming the consumer (wins over the hostname).
CONSUMER_NAME_ENV = "STREAM_CONSUMER_NAME"


def consumer_name(
    service: str | None = None, explicit: str | None = None, *, index: int | None = None
) -> str:
    """The consumer name to use: ``explicit``, else ``$STREAM_CONSUMER_NAME``, else ``<hostname>-<service>``
    (``HOSTNAME`` or socket.gethostname()); without a service, a random id (process-redis-events' old default,
    which never takes over its own pending messages). ``index`` adds ``-<n>`` for parallel consumers of one
    process."""
    name = explicit or os.environ.get(CONSUMER_NAME_ENV, "").strip()
    if not name:
        if service:
            host = os.environ.get("HOSTNAME", "").strip() or socket.gethostname()
            name = f"{host}-{service}"
        else:
            name = uuid.uuid4().hex[:21]
    return name if index is None else f"{name}-{index}"
