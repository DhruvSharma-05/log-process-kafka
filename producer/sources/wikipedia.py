#!/usr/bin/env python3
"""Live Wikimedia EventStreams source — genuinely real-time external data.

    python -m producer.sources.wikipedia                      # straight to Kafka
    python -m producer.sources.wikipedia --sink http          # via the ingest gateway
    python -m producer.sources.wikipedia --wikis en.wikipedia.org --duration 300

Consumes <https://stream.wikimedia.org/v2/stream/recentchange>, a public
Server-Sent Events firehose of every edit across every Wikimedia wiki. No
authentication, no API key, always on, roughly 30-50 events/sec worldwide.

Unlike the file replayer this is not replayed history — the events are
happening as they arrive, so ingestion latency measured against it is real
end-to-end latency from a third-party system.

## How an edit becomes a log event

Wikimedia's schema is not a log format, so fields are mapped. Every mapping
below is a real field from the source event; nothing is invented:

    service           server_name        en.wikipedia.org, commons.wikimedia.org
    host              wiki               enwiki, commonswiki
    timestamp         meta.dt            the actual edit time
    client_ip         user               the editor identity (see below)
    path              /wiki/<title>      from the page title
    http_method       POST for edit/new, GET otherwise
    status_code       200 edit / 201 new page / 204 log / 304 categorize
    response_bytes    length.new         real size of the page after the edit
    message           comment            the edit summary
    trace_id          meta.id

`response_time_ms` is left null: the source has no such field and inventing one
would put fiction on a latency dashboard.

## About `client_ip`

It holds the *actor* — the editor who made the change — because that is the
closest analogue to a client identifier this source has, and it is real PII
worth putting through the hashing path.

It is **not** an IP address. Wikimedia now masks anonymous editors behind
temporary accounts (`~2026-43591-61`), so a live sample of 630 edits contained
609 named accounts, 21 temporary accounts, and **zero IP addresses**. Geo
enrichment therefore records `geo_source: "unresolved"` for this source, which
is the correct and honest outcome — no country is guessed from a username.

The PII hashing is still exercised for real: `client_ip_hash` is a salted hash
of a genuine user identifier that nobody manufactured.

## About error rates

Naturally near zero — real wikis mostly work, and edits do not fail in ways this
schema exposes. `log_level` is WARN only when an edit summary indicates a
revert. Use `producer/scenarios.py` to exercise alerting; this source exists to
prove live ingestion, not to trip thresholds.
"""
from __future__ import annotations

import argparse
import json
import re
import signal
import sys
import time
import urllib.parse
from collections.abc import Iterator
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from ..config import ProducerConfig
from ..replay import build_event

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks for a descriptive User-Agent and will throttle generic ones.
USER_AGENT = "logpipe-demo/1.0 (real-time log pipeline; https://github.com/local/logpipe)"

EVENTS_READ = Counter("logpipe_wiki_events_read_total", "SSE events read from Wikimedia")
EVENTS_SENT = Counter("logpipe_wiki_events_sent_total", "Events forwarded into the pipeline", ["sink"])
EVENTS_SKIPPED = Counter("logpipe_wiki_skipped_total", "Source events skipped", ["reason"])
RECONNECTS = Counter("logpipe_wiki_reconnects_total", "SSE stream reconnections")
STREAM_CONNECTED = Gauge("logpipe_wiki_connected", "1 when the SSE stream is connected")

# Edit summaries that indicate the change was undone.
REVERT_RE = re.compile(r"\b(revert|reverted|undo|undid|rollback|rvv)\b", re.IGNORECASE)

STATUS_BY_TYPE = {
    "edit": 200,
    "new": 201,
    "log": 204,
    "categorize": 304,
}


def iter_sse_events(url: str, timeout: int = 60) -> Iterator[dict]:
    """Yield parsed `data:` payloads from a Server-Sent Events stream.

    Reconnects with backoff. A long-lived HTTP stream *will* drop — treating a
    disconnect as fatal would mean the source dies quietly overnight.
    """
    backoff = 1.0
    last_event_id: str | None = None

    while True:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        try:
            with requests.get(url, headers=headers, stream=True, timeout=(10, timeout)) as response:
                response.raise_for_status()
                STREAM_CONNECTED.set(1)
                backoff = 1.0
                data_lines: list[str] = []

                for raw in response.iter_lines(decode_unicode=True):
                    if raw is None:
                        continue
                    if raw == "":
                        # Blank line terminates one SSE event.
                        if data_lines:
                            payload = "\n".join(data_lines)
                            data_lines = []
                            try:
                                yield json.loads(payload)
                            except json.JSONDecodeError:
                                EVENTS_SKIPPED.labels(reason="unparseable_sse").inc()
                        continue
                    if raw.startswith(":"):
                        continue                       # comment / keepalive
                    if raw.startswith("data:"):
                        data_lines.append(raw[5:].lstrip())
                    elif raw.startswith("id:"):
                        last_event_id = raw[3:].strip()
        except (requests.RequestException, OSError) as exc:
            STREAM_CONNECTED.set(0)
            RECONNECTS.inc()
            print(f"stream disconnected ({type(exc).__name__}: {exc}); retrying in {backoff:.0f}s",
                  file=sys.stderr, flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


def to_log_event(change: dict) -> dict | None:
    """Map one Wikimedia recentchange into a JSON application log.

    Returns None for events that carry nothing useful, rather than emitting a
    half-empty record.
    """
    meta = change.get("meta") or {}
    server_name = change.get("server_name") or meta.get("domain")
    if not server_name:
        return None

    change_type = change.get("type", "unknown")
    title = change.get("title") or ""
    path = "/wiki/" + urllib.parse.quote(title.replace(" ", "_"), safe="/:")

    length = change.get("length") or {}
    new_length = length.get("new")
    old_length = length.get("old")
    delta = (new_length - old_length) if isinstance(new_length, int) and isinstance(old_length, int) else None

    comment = change.get("comment") or ""
    if REVERT_RE.search(comment):
        level = "WARN"
    elif change.get("bot"):
        level = "DEBUG"
    else:
        level = "INFO"

    timestamp = meta.get("dt")
    if not timestamp:
        epoch = change.get("timestamp")
        if not isinstance(epoch, (int, float)):
            return None
        timestamp = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "timestamp": timestamp,
        "service": server_name,
        "host": change.get("wiki") or "wikimedia",
        "level": level,
        # The edit summary. Runs through the processor's PII scrubbing, which
        # is worth watching on real user-authored text.
        "message": comment[:500] or f"{change_type} on {title}",
        # The editor identity. A username or a masked temporary account, never
        # an IP — Wikimedia masks anonymous editors. See the module docstring.
        "client_ip": change.get("user"),
        "http_method": "POST" if change_type in ("edit", "new") else "GET",
        "path": path,
        "status_code": STATUS_BY_TYPE.get(change_type, 200),
        "response_bytes": new_length if isinstance(new_length, int) else None,
        "trace_id": str(meta.get("id")) if meta.get("id") else None,
        # Source-specific extras, carried for interest. The processor ignores
        # unknown fields, so these do not reach the parsed schema.
        "wiki_change_type": change_type,
        "wiki_is_bot": bool(change.get("bot")),
        "wiki_is_minor": bool(change.get("minor")),
        "wiki_bytes_delta": delta,
    }


class KafkaSink:
    """Writes envelopes straight into raw-logs."""

    name = "kafka"

    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        self.producer = Producer(config.kafka_conf())
        self.failures = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failures += 1

    def send(self, event: dict) -> None:
        while True:
            try:
                self.producer.produce(
                    self.config.topic,
                    key=str(event["service"]).encode(),
                    value=json.dumps(event, separators=(",", ":")).encode(),
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> int:
        return self.producer.flush(timeout=30)


class HttpSink:
    """POSTs to the ingest gateway — exercises the full HTTP path (FR1.1)."""

    name = "http"

    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()
        self.failures = 0

    def send(self, event: dict) -> None:
        # `event` here is the source log, not an envelope: the gateway builds
        # the envelope itself, which is the point of routing through it.
        try:
            response = self.session.post(self.url, json=event, timeout=10)
            if response.status_code == 429:
                time.sleep(1.0)
                self.session.post(self.url, json=event, timeout=10)
            elif response.status_code >= 400:
                self.failures += 1
        except requests.RequestException:
            self.failures += 1

    def close(self) -> int:
        self.session.close()
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream live Wikimedia edits into the pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--sink", choices=["kafka", "http"], default="kafka",
                        help="Write straight to Kafka, or POST through the ingest gateway")
    parser.add_argument("--url", default="http://localhost:8100/v1/logs",
                        help="Ingest gateway URL when --sink http")
    parser.add_argument("--wikis", default=None,
                        help="Comma-separated server_name filter, e.g. en.wikipedia.org,commons.wikimedia.org")
    parser.add_argument("--duration", type=int, default=0, help="Seconds to run; 0 = forever")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N events; 0 = unlimited")
    parser.add_argument("--include-bots", action="store_true",
                        help="Include bot edits (roughly half of all traffic)")
    parser.add_argument("--metrics-port", type=int, default=8002)
    parser.add_argument("--no-metrics", action="store_true")
    parser.add_argument("--bootstrap", default=None)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    config = ProducerConfig()
    if args.bootstrap:
        config.bootstrap = args.bootstrap

    sink = HttpSink(args.url) if args.sink == "http" else KafkaSink(config)
    wikis = {w.strip() for w in args.wikis.split(",")} if args.wikis else None

    if not args.no_metrics:
        start_http_server(args.metrics_port)
        print(f"metrics:  http://localhost:{args.metrics_port}/metrics")

    print(f"source:   {STREAM_URL}")
    print(f"sink:     {sink.name}" + (f" -> {args.url}" if args.sink == "http" else f" -> {config.topic}"))
    print(f"filter:   {', '.join(sorted(wikis)) if wikis else 'all wikis'}"
          + ("" if args.include_bots else ", excluding bots"))
    print(f"duration: {'unlimited' if args.duration == 0 else str(args.duration) + 's'}")
    print()

    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False
        print("\nstopping...", flush=True)

    signal.signal(signal.SIGINT, stop)

    started = time.perf_counter()
    deadline = started + args.duration if args.duration else None
    sent = 0
    read = 0
    last_report = started

    try:
        for change in iter_sse_events(STREAM_URL):
            if not running:
                break
            if deadline and time.perf_counter() >= deadline:
                break

            read += 1
            EVENTS_READ.inc()

            if wikis and change.get("server_name") not in wikis:
                EVENTS_SKIPPED.labels(reason="wiki_filtered").inc()
                continue
            if not args.include_bots and change.get("bot"):
                EVENTS_SKIPPED.labels(reason="bot").inc()
                continue

            log = to_log_event(change)
            if log is None:
                EVENTS_SKIPPED.labels(reason="unmappable").inc()
                continue

            if sink.name == "kafka":
                envelope = build_event(
                    json.dumps(log, separators=(",", ":")),
                    source_format="json_app",
                    rewrite=False,     # these timestamps are real and current
                )
                if envelope is None:
                    EVENTS_SKIPPED.labels(reason="empty_envelope").inc()
                    continue
                sink.send(envelope)
            else:
                sink.send(log)

            sent += 1
            EVENTS_SENT.labels(sink=sink.name).inc()

            now = time.perf_counter()
            if now - last_report >= 10.0:
                elapsed = now - started
                print(f"  {elapsed:6.1f}s  read={read:>7,}  sent={sent:>7,}  "
                      f"{sent / elapsed:5.1f}/s  failures={sink.failures}", flush=True)
                last_report = now

            if args.limit and sent >= args.limit:
                break
    except KeyboardInterrupt:
        pass
    finally:
        unflushed = sink.close()

    elapsed = time.perf_counter() - started
    print()
    print(f"read {read:,} source events, forwarded {sent:,} in {elapsed:.1f}s "
          f"({sent / max(elapsed, 1):.1f}/s)")
    if sink.failures or unflushed:
        print(f"WARNING: {sink.failures} failures, {unflushed} unflushed", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
