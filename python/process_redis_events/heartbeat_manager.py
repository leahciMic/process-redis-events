"""Heartbeat manager for automatic lease extension."""

import asyncio
from typing import Any

from redis.asyncio import Redis


class HeartbeatManager:
    """Manages automatic heartbeat/lease extension for messages being processed."""

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

    async def _send_heartbeats(self) -> None:
        """Send heartbeats for all active jobs."""
        if not self.job_ids:
            return

        try:
            pipeline = self.redis.pipeline()
            for job_id in self.job_ids:
                pipeline.xclaim(
                    name=self.stream,
                    groupname=self.consumer_group,
                    consumername=self.consumer_id,
                    min_idle_time=0,
                    message_ids=[job_id],
                    idle=0,
                    justid=True,
                )
            await pipeline.execute()
        except Exception as err:
            print(f"Error sending heartbeats: {err}")

    def add(self, job_id: str) -> None:
        """Add a job ID to be heartbeat."""
        self.job_ids.add(job_id)

    def remove(self, job_id: str) -> None:
        """Remove a job ID from heartbeat tracking."""
        self.job_ids.discard(job_id)
