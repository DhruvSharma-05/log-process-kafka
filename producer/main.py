#!/usr/bin/env python3
"""Log producer — streams log lines into the `raw-logs` Kafka topic.

    # Replay real access logs at 2000 events/sec
    python -m producer.main --file data/NASA_access_log_Aug95 --rate 2000

    # Loop the file forever with synthetic latency, for dashboard demos
    python -m producer.main --file data/NASA_access_log_Aug95 --rate 500 --loop --synth-latency

    # Max-throughput load test (M6)
    python -m producer.main --file data/NASA_access_log_Aug95 --rate 0 --limit 1000000

    # Pipe a synthetic generator in
    flog -f apache_combined -n 100000 | python -m producer.main --stdin --rate 5000

Metrics are exposed on http://localhost:8001/metrics for Prometheus (M5).
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from collections.abc import Iterator
from pathlib import Path

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from .config import ProducerConfig, ReplayOptions
from .replay import build_event, iter_file, iter_stream

EVENTS_PRODUCED = Counter("logpipe_producer_events_total", "Events acked by Kafka")
EVENTS_FAILED = Counter("logpipe_producer_failed_total", "Events Kafka refused", ["reason"])
BYTES_PRODUCED = Counter("logpipe_producer_bytes_total", "Payload bytes acked by Kafka")
LINES_SKIPPED = Counter("logpipe_producer_skipped_total", "Blank or unusable source lines")
CURRENT_RATE = Gauge("logpipe_producer_rate", "Observed events/sec over the last interval")
QUEUE_DEPTH = Gauge("logpipe_producer_queue_depth", "Messages buffered in librdkafka")


class RateLimiter:
    """Paces emission to a target events/sec.

    Sleeps once per chunk rather than once per message: Windows timer
    granularity is ~15 ms, so per-message sleeps cap out around 60 events/sec.
    A chunk of rate/50 gives ~20 ms sleeps and holds the target closely.
    """

    def __init__(self, rate: int) -> None:
        self.rate = rate
        self.chunk = max(1, rate // 50) if rate > 0 else 0
        self._start = time.perf_counter()
        self._count = 0

    def tick(self) -> None:
        if self.rate <= 0:
            return
        self._count += 1
        if self._count % self.chunk:
            return
        target = self._start + self._count / self.rate
        drift = target - time.perf_counter()
        if drift > 0:
            time.sleep(drift)


class Stats:
    """Periodic console progress, independent of the Prometheus counters."""

    def __init__(self, interval: float = 2.0) -> None:
        self.interval = interval
        self.started = time.perf_counter()
        self._last_report = self.started
        self._last_count = 0
        self.produced = 0
        self.failed = 0

    def maybe_report(self, queue_depth: int, force: bool = False) -> None:
        now = time.perf_counter()
        if not force and now - self._last_report < self.interval:
            return
        window = now - self._last_report
        rate = (self.produced - self._last_count) / window if window > 0 else 0.0
        CURRENT_RATE.set(rate)
        elapsed = now - self.started
        print(
            f"  {elapsed:7.1f}s  sent={self.produced:>9,}  "
            f"rate={rate:>8,.0f}/s  failed={self.failed}  queued={queue_depth}",
            flush=True,
        )
        self._last_report = now
        self._last_count = self.produced


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream log lines into Kafka's raw-logs topic.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", type=Path, help="Log file to replay")
    source.add_argument("--stdin", action="store_true", help="Read lines from stdin (e.g. piped from flog)")

    parser.add_argument("--rate", type=int, default=1000, help="Events/sec; 0 = unlimited (default: 1000)")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N events")
    parser.add_argument("--loop", action="store_true", help="Restart the file when it ends")
    parser.add_argument("--topic", default=None, help="Override the destination topic")
    parser.add_argument("--bootstrap", default=None, help="Override the Kafka bootstrap servers")
    parser.add_argument(
        "--source-format",
        default="nginx_combined",
        choices=["nginx_combined", "json_app"],
        help="Format hint written into the event for the processor (default: nginx_combined)",
    )
    parser.add_argument(
        "--no-rewrite-timestamps",
        action="store_true",
        help="Keep original log timestamps instead of rewriting them to now",
    )
    parser.add_argument(
        "--synth-latency",
        action="store_true",
        help="Append a synthetic request time; the source logs have none (see replay.synth_latency)",
    )
    parser.add_argument("--metrics-port", type=int, default=None, help="Prometheus port (default: 8001)")
    parser.add_argument("--no-metrics", action="store_true", help="Do not start the metrics server")
    return parser.parse_args(argv)


def make_delivery_callback(stats: Stats):
    def on_delivery(err, msg) -> None:
        if err is not None:
            stats.failed += 1
            EVENTS_FAILED.labels(reason=err.name() if hasattr(err, "name") else "unknown").inc()
            return
        stats.produced += 1
        EVENTS_PRODUCED.inc()
        BYTES_PRODUCED.inc(len(msg))

    return on_delivery


def line_source(args: argparse.Namespace) -> Iterator[str]:
    if args.stdin:
        return iter_stream(sys.stdin)
    if not args.file.exists():
        raise SystemExit(
            f"error: {args.file} not found.\n"
            f"Run: powershell -ExecutionPolicy Bypass -File scripts\\download_data.ps1"
        )
    return iter_file(args.file, loop=args.loop)


def run(args: argparse.Namespace) -> int:
    config = ProducerConfig()
    if args.bootstrap:
        config.bootstrap = args.bootstrap
    if args.topic:
        config.topic = args.topic
    if args.metrics_port:
        config.metrics_port = args.metrics_port

    options = ReplayOptions(
        rate=args.rate,
        limit=args.limit,
        loop=args.loop,
        rewrite_timestamps=not args.no_rewrite_timestamps,
        synth_latency=args.synth_latency,
    )

    if not args.no_metrics:
        start_http_server(config.metrics_port)
        print(f"metrics:   http://localhost:{config.metrics_port}/metrics")

    print(f"bootstrap: {config.bootstrap}")
    print(f"topic:     {config.topic}")
    print(f"rate:      {'unlimited' if options.rate == 0 else f'{options.rate:,}/s'}")
    print(f"source:    {'stdin' if args.stdin else args.file}")
    print()

    producer = Producer(config.kafka_conf())
    stats = Stats()
    on_delivery = make_delivery_callback(stats)
    limiter = RateLimiter(options.rate)
    rng = random.Random(1337)  # fixed seed: reproducible synthetic latency

    stopping = False

    def handle_signal(_signum, _frame) -> None:
        nonlocal stopping
        if stopping:  # second Ctrl-C: give up on draining
            raise KeyboardInterrupt
        stopping = True
        print("\nstopping — draining producer queue...", flush=True)

    signal.signal(signal.SIGINT, handle_signal)

    sent = 0
    try:
        for line in line_source(args):
            if stopping:
                break

            event = build_event(
                line,
                source_format=args.source_format,
                rewrite=options.rewrite_timestamps,
                latency=options.synth_latency,
                rng=rng,
            )
            if event is None:
                LINES_SKIPPED.inc()
                continue

            payload = json.dumps(event, separators=(",", ":")).encode()
            while True:
                try:
                    producer.produce(
                        config.topic,
                        # `service` as partition key — per-service ordering (FR1.3)
                        key=str(event["service"]).encode(),
                        value=payload,
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    # Local queue full: the broker is the bottleneck. Serve
                    # delivery callbacks and retry rather than dropping.
                    producer.poll(0.5)

            sent += 1
            producer.poll(0)
            limiter.tick()
            stats.maybe_report(len(producer))

            if options.limit and sent >= options.limit:
                break
    except KeyboardInterrupt:
        print("\nforced stop — buffered messages may be lost", flush=True)
    finally:
        remaining = producer.flush(timeout=30)
        QUEUE_DEPTH.set(remaining)
        stats.maybe_report(remaining, force=True)

    elapsed = time.perf_counter() - stats.started
    print()
    print(f"read {sent:,} lines in {elapsed:.1f}s")
    print(f"acked {stats.produced:,}  failed {stats.failed:,}  avg {stats.produced / elapsed:,.0f} events/sec")
    if remaining:
        print(f"WARNING: {remaining:,} message(s) never flushed", file=sys.stderr)
        return 1
    return 1 if stats.failed else 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
