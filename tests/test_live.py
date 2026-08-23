"""Live-tail broadcaster tests.

The UI is a window onto the stream, not a delivery guarantee. These tests pin
the three properties that make that safe: ingestion is never blocked, buffers
are bounded, and everything dropped is counted.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from ingest.live import LiveBroadcaster, Subscriber


class FakeSender:
    """A viewer. `block` simulates one that has stopped reading."""

    def __init__(self, block: bool = False) -> None:
        self.received: list[str] = []
        self.block = block
        self.fail = False

    async def send_text(self, data: str) -> None:
        if self.fail:
            raise ConnectionResetError("viewer went away")
        if self.block:
            await asyncio.sleep(3600)
        self.received.append(data)


async def settle(times: int = 6) -> None:
    """Let the per-subscriber drain tasks run."""
    for _ in range(times):
        await asyncio.sleep(0)


# --- Basic delivery ----------------------------------------------------------


@pytest.mark.asyncio
async def test_event_reaches_a_connected_viewer():
    b = LiveBroadcaster()
    sender = FakeSender()
    b.add(sender)
    b.publish({"service": "a", "message": "hello"})
    await settle()
    assert len(sender.received) == 1
    assert json.loads(sender.received[0])["message"] == "hello"
    await b.close()


@pytest.mark.asyncio
async def test_every_viewer_gets_the_event():
    b = LiveBroadcaster()
    senders = [FakeSender() for _ in range(3)]
    for s in senders:
        b.add(s)
    b.publish({"service": "a"})
    await settle()
    assert all(len(s.received) == 1 for s in senders)
    await b.close()


@pytest.mark.asyncio
async def test_publishing_with_no_viewers_is_a_noop():
    b = LiveBroadcaster()
    assert b.publish({"service": "a"}) is False
    assert b.stats()["published"] == 0
    await b.close()


# --- Ingestion must never be blocked ----------------------------------------


@pytest.mark.asyncio
async def test_publish_never_blocks_on_a_stalled_viewer():
    """The property the whole design exists for: a viewer that has stopped
    reading must not slow the ingest path down."""
    b = LiveBroadcaster(queue_size=4, max_events_per_sec=100000)
    b.add(FakeSender(block=True))
    await settle()

    loop = asyncio.get_running_loop()
    start = loop.time()
    for i in range(5000):
        b.publish({"seq": i})
    elapsed = loop.time() - start

    assert elapsed < 1.0, f"publish path took {elapsed:.2f}s - it is blocking"
    await b.close()


@pytest.mark.asyncio
async def test_a_stalled_viewer_does_not_grow_memory_without_bound():
    b = LiveBroadcaster(queue_size=8, max_events_per_sec=100000)
    sub = b.add(FakeSender(block=True))
    await settle()
    for i in range(10000):
        b.publish({"seq": i})
    # One in flight in the drain task, the rest bounded by the queue.
    assert sub.queue.qsize() <= 8
    assert b.stats()["dropped"] > 9000
    await b.close()


# --- Bounded queues drop the oldest -----------------------------------------


@pytest.mark.asyncio
async def test_overflow_drops_oldest_so_the_tail_stays_current():
    """A live tail showing a stale backlog is worse than one with gaps."""
    b = LiveBroadcaster(queue_size=3, max_events_per_sec=100000)
    sub = b.add(FakeSender(block=True))
    await settle()
    for i in range(10):
        b.publish({"seq": i})
    queued = [json.loads(sub.queue.get_nowait())["seq"] for _ in range(sub.queue.qsize())]
    assert queued == sorted(queued)
    assert max(queued) >= 7, f"kept stale events instead of recent ones: {queued}"
    await b.close()


@pytest.mark.asyncio
async def test_drops_are_counted_not_silent():
    b = LiveBroadcaster(queue_size=2, max_events_per_sec=100000)
    b.add(FakeSender(block=True))
    await settle()
    for i in range(50):
        b.publish({"seq": i})
    assert b.stats()["dropped"] > 0
    await b.close()


# --- Rate cap ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_rate_cap_samples_rather_than_flooding_the_browser():
    """18k events/sec is unreadable and unrenderable; above the cap we sample."""
    ticks = [0.0]
    b = LiveBroadcaster(queue_size=10000, max_events_per_sec=100, clock=lambda: ticks[0])
    sender = FakeSender()
    b.add(sender)
    for i in range(5000):
        b.publish({"seq": i})
    stats = b.stats()
    assert stats["published"] <= 101, f"rate cap did not hold: {stats}"
    assert stats["sampled_out"] > 4800
    await b.close()


@pytest.mark.asyncio
async def test_rate_budget_refills_over_time():
    ticks = [0.0]
    b = LiveBroadcaster(queue_size=10000, max_events_per_sec=100, clock=lambda: ticks[0])
    b.add(FakeSender())
    for i in range(200):
        b.publish({"seq": i})
    first = b.stats()["published"]
    ticks[0] += 1.0                      # one second passes
    for i in range(200):
        b.publish({"seq": i})
    assert b.stats()["published"] > first
    await b.close()


# --- Connection management ---------------------------------------------------


@pytest.mark.asyncio
async def test_viewer_cap_is_enforced():
    b = LiveBroadcaster(max_clients=2)
    assert b.add(FakeSender()) is not None
    assert b.add(FakeSender()) is not None
    assert b.add(FakeSender()) is None, "accepted a viewer beyond the cap"
    assert b.stats()["rejected_connections"] == 1
    await b.close()


@pytest.mark.asyncio
async def test_removing_a_viewer_releases_its_backlog():
    """RSS stayed elevated after a stalled viewer disconnected in the original
    implementation; the queue must be released on removal."""
    b = LiveBroadcaster(queue_size=64, max_events_per_sec=100000)
    sub = b.add(FakeSender(block=True))
    await settle()
    for i in range(500):
        b.publish({"seq": i})
    assert sub.queue.qsize() > 0
    await b.remove(sub)
    assert sub.queue.qsize() == 0
    assert b.subscriber_count == 0


@pytest.mark.asyncio
async def test_a_failing_viewer_does_not_break_the_others():
    b = LiveBroadcaster()
    broken, healthy = FakeSender(), FakeSender()
    broken.fail = True
    b.add(broken)
    b.add(healthy)
    for i in range(3):
        b.publish({"seq": i})
    await settle(10)
    assert len(healthy.received) == 3
    await b.close()


@pytest.mark.asyncio
async def test_close_detaches_everyone():
    b = LiveBroadcaster()
    for _ in range(3):
        b.add(FakeSender())
    await b.close()
    assert b.subscriber_count == 0


# --- Serialisation -----------------------------------------------------------


@pytest.mark.asyncio
async def test_non_json_types_do_not_raise():
    """publish() runs on the ingest path and must never raise."""
    import datetime as dt
    b = LiveBroadcaster()
    sender = FakeSender()
    b.add(sender)
    b.publish({"when": dt.datetime(2026, 8, 23), "service": "a"})
    await settle()
    assert len(sender.received) == 1
    await b.close()
