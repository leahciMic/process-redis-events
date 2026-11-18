"""Heartbeat manager for automatic lease extension."""

import asyncio
import logging
from typing import Any

from redis.asyncio import Redis

from process_redis_events.entries import HEARTBEAT_LUA

log = logging.getLogger("process_redis_events")


class HeartbeatManager:
    """Resets the idle time of the messages being processed every ``interval_ms``, so they aren't reclaimed while
    they're in progress. Only messages this consumer still owns are touched (entries.HEARTBEAT_LUA): a plain
    XCLAIM would take a message back from a consumer that reclaimed it after the lease expired.
    """

    def __init__(
        self,
        redis: "Redis[Any]",
        stream: str,
        consumer_group: str,
        consumer_id: str,
        interval_ms: int,
    ):
        self.redis = redis
        self.stream = stream
        self.consumer_group = consumer_group
        self.consumer_id = consumer_id
        self.heartbeat_interval = interval_ms / 1000.0
        self.job_ids: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._script = redis.register_script(HEARTBEAT_LUA)

    async def start(self) -> None:
        """Start the heartbeat loop."""
        if self._task is None:
            self._running = True
            self._task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.job_ids.clear()

    async def _heartbeat_loop(self) -> None:
        """Background loop that sends heartbeats."""
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            await self._send_heartbeats()

    async def _send_heartbeats(self) -> list[str]:
        """Send heartbeats for all active jobs; returns the ids this consumer no longer owns."""
        ids = list(self.job_ids)
        if not ids:
            return []
        lost: list[str] = []
        for start in range(0, len(ids), 500):
            try:
                reply = await self._script(
                    keys=[self.stream],
                    args=[
                        self.consumer_group,
                        self.consumer_id,
                        *ids[start : start + 500],
                    ],
                )
            except Exception as err:
                log.warning("Error sending heartbeats for %s: %s", self.stream, err)
                continue
            lost += [
                x.decode() if isinstance(x, bytes) else str(x) for x in reply or []
            ]
        if lost:
            log.warning(
                "%d message(s) in progress on %s were acknowledged or reclaimed by another consumer: %s",
                len(lost),
                self.stream,
                ", ".join(lost[:5]),
            )
        return lost

    def add(self, job_id: str) -> None:
        """Add a job ID to be heartbeat."""
        self.job_ids.add(job_id)

    def remove(self, job_id: str) -> None:
        """Remove a job ID from heartbeat tracking."""
        self.job_ids.discard(job_id)
