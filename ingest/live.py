"""Live tail broadcaster for the web UI.

The UI at `/` is a **live tail**, not a delivery guarantee. Kafka is the durable
path; this is a window onto it. That single decision drives everything here:
when a viewer cannot keep up, the right answer is to drop events for that
viewer, not to slow down ingestion or buffer without limit.

The naive version — `background_tasks.add_task(broadcast_event, event)` per
event, writing straight to each socket — was measured doing both:

    ingest throughput, no viewers          18,516 events/s
    ingest throughput, 3 slow viewers      11,408 events/s   (-38%)
    viewers actually drained                   132 of 30,000 broadcast
    gateway RSS, 40k events, 1 stalled viewer  68 MB -> 96 MB, never reclaimed

Three things fix that:

1. **The ingest path never touches a socket.** `publish()` is synchronous and
   non-blocking: it serialises once and drops the message into a bounded queue
   per connection. A dedicated task per connection drains that queue.
2. **Queues are bounded and drop the oldest.** On a live tail the newest line
   is the interesting one; a viewer catching up on a backlog is showing stale
   data. Overflow is counted, never silent.
3. **The broadcast is rate-capped.** A browser cannot render 18,000 lines a
   second and a human cannot read them. Above the cap, events are sampled out
   and counted, so the UI stays responsive at any ingest rate.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Protocol

# A browser tab renders maybe a few hundred lines/sec before it stops being
# useful. Anything above this is sampled out rather than queued.
DEFAULT_MAX_EVENTS_PER_SEC = 200
# Per-connection buffer. Roughly a second of headroom at the cap.
DEFAULT_QUEUE_SIZE = 256
# Guards against unbounded fan-out cost and file-descriptor exhaustion.
DEFAULT_MAX_CLIENTS = 32


class Sender(Protocol):
    """The bit of a WebSocket this module needs. Keeps tests free of sockets."""

    async def send_text(self, data: str) -> None: ...


class Subscriber:
    """One connected viewer: a bounded queue plus the task draining it."""

    def __init__(self, sender: Sender, queue_size: int) -> None:
        self.sender = sender
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_size)
        self.dropped = 0
        self.sent = 0
        self._task: asyncio.Task | None = None
        self._closed = False

    def offer(self, message: str) -> bool:
        """Enqueue without blocking. Returns False if the message was dropped.

        Drops the *oldest* queued message to make room, because the newest line
        is what a live tail is for.
        """
        if self._closed:
            return False
        try:
            self.queue.put_nowait(message)
            return True
        except asyncio.QueueFull:
            try:
                self.queue.get_nowait()          # evict oldest
                self.queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self.dropped += 1
            try:
                self.queue.put_nowait(message)
            except asyncio.QueueFull:
                return False
            return False

    async def _drain(self) -> None:
        try:
            while not self._closed:
                message = await self.queue.get()
                try:
                    await self.sender.send_text(message)
                    self.sent += 1
                except Exception:
                    # The viewer went away mid-send. Not an ingest problem.
                    break
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            pass

    def start(self) -> None:
        self._task = asyncio.create_task(self._drain())

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        # Release anything still queued so a stalled viewer's backlog is not
        # held after it disconnects.
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break


class LiveBroadcaster:
    """Fan-out to connected viewers that can never slow down ingestion."""

    def __init__(
        self,
        max_clients: int = DEFAULT_MAX_CLIENTS,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        max_events_per_sec: int = DEFAULT_MAX_EVENTS_PER_SEC,
        clock=time.monotonic,
    ) -> None:
        self.max_clients = max_clients
        self.queue_size = queue_size
        self.max_events_per_sec = max_events_per_sec
        self._clock = clock
        self._subscribers: list[Subscriber] = []
        # Token bucket for the rate cap.
        self._allowance = float(max_events_per_sec)
        self._last_check = clock()
        # Counters, surfaced as Prometheus metrics by the caller.
        self.published = 0
        self.sampled_out = 0
        self.dropped = 0
        self.rejected_connections = 0

    # -- connection management -------------------------------------------
    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def add(self, sender: Sender) -> Subscriber | None:
        """Register a viewer, or None when at capacity."""
        if len(self._subscribers) >= self.max_clients:
            self.rejected_connections += 1
            return None
        sub = Subscriber(sender, self.queue_size)
        self._subscribers.append(sub)
        sub.start()
        return sub

    async def remove(self, sub: Subscriber) -> None:
        if sub in self._subscribers:
            self._subscribers.remove(sub)
        self.dropped += sub.dropped
        await sub.stop()

    async def close(self) -> None:
        for sub in list(self._subscribers):
            await self.remove(sub)

    # -- publishing -------------------------------------------------------
    def _within_rate(self) -> bool:
        if self.max_events_per_sec <= 0:
            return False
        now = self._clock()
        elapsed = now - self._last_check
        self._last_check = now
        self._allowance = min(
            float(self.max_events_per_sec),
            self._allowance + elapsed * self.max_events_per_sec,
        )
        if self._allowance < 1.0:
            return False
        self._allowance -= 1.0
        return True

    def publish(self, event: dict[str, Any]) -> bool:
        """Offer an event to every viewer. Never blocks, never raises.

        Called from the request path, so it must stay cheap: with no viewers
        it is a single length check.
        """
        if not self._subscribers:
            return False
        if not self._within_rate():
            self.sampled_out += 1
            return False

        message = json.dumps(event, separators=(",", ":"), default=str)
        for sub in self._subscribers:
            if not sub.offer(message):
                self.dropped += 1
        self.published += 1
        return True

    def stats(self) -> dict[str, int]:
        return {
            "subscribers": len(self._subscribers),
            "published": self.published,
            "sampled_out": self.sampled_out,
            "dropped": self.dropped + sum(s.dropped for s in self._subscribers),
            "rejected_connections": self.rejected_connections,
        }
