#!/usr/bin/env python3
"""Stream processor — raw-logs -> parsed-logs / error-logs / dead-letter-queue.

    python -m processor.main
    python -m processor.main --from-beginning --batch-size 1000

Pipeline per record: decode -> parse -> enrich -> redact -> validate -> produce.
A failure at any stage produces a DLQ event carrying the stage and reason, and
the loop moves on. No single record can stop the stream (FR2.3, PRD risk #2).

Delivery semantics are at-least-once (FR2.5): offsets are committed only after
the derived events have been flushed to the broker. A crash mid-batch replays
that batch; `event_id` is content-derived, so storage dedupes the repeats.

Endpoints: http://localhost:8000/{healthz,readyz,metrics}
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

from .config import GEOIP_DB_PATH, ProcessorConfig
from .dlq import build_dlq_event, serialise
from .enrich import GeoResolver, enrich
from .health import HealthState, start_http_server
from .metrics import (
    ASSIGNED_PARTITIONS,
    BATCH_SECONDS,
    BATCHES_COMMITTED,
    DLQ_RATIO,
    EVENTS_CONSUMED,
    EVENTS_DLQ,
    EVENTS_ERRORS_ROUTED,
    EVENTS_PARSED,
    INGEST_LAG_SECONDS,
    LAST_POLL_UNIXTIME,
    PROCESSING_SECONDS,
    PRODUCE_FAILURES,
)
from .parsers import ParseError, parse
from .redact import redact
from .validation import ValidationError, validate

REQUIRED_RAW_FIELDS = ("event_id", "host", "service", "source_format", "raw_message")


class ProcessingFailure(Exception):
    """A record that must go to the DLQ, tagged with the stage that rejected it."""

    def __init__(self, stage: str, reason: str) -> None:
        super().__init__(reason)
        self.stage = stage
        self.reason = reason


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def decode_record(payload: bytes) -> dict:
    try:
        event = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProcessingFailure("decode", f"invalid JSON envelope: {exc.msg}") from exc
    if not isinstance(event, dict):
        raise ProcessingFailure("decode", f"envelope is {type(event).__name__}, expected object")
    missing = [field for field in REQUIRED_RAW_FIELDS if not event.get(field)]
    if missing:
        raise ProcessingFailure("decode", f"envelope missing required field(s): {', '.join(missing)}")
    return event


def process_record(raw: dict, resolver: GeoResolver) -> dict:
    """Turn a raw envelope into a validated parsed event, or raise ProcessingFailure."""
    try:
        parsed = parse(str(raw["raw_message"]), str(raw["source_format"]))
    except ParseError as exc:
        raise ProcessingFailure("parse", str(exc)) from exc
    except Exception as exc:  # noqa: BLE001 - an unexpected parser bug is still just a bad record
        raise ProcessingFailure("parse", f"{type(exc).__name__}: {exc}") from exc

    try:
        parsed = enrich(parsed, resolver)
        parsed = redact(parsed)
    except Exception as exc:  # noqa: BLE001
        raise ProcessingFailure("enrich", f"{type(exc).__name__}: {exc}") from exc

    timestamp = parsed.pop("timestamp")
    event = {
        "event_id": raw["event_id"],
        "timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ingested_at": raw.get("ingested_at"),
        "processed_at": _now_iso(),
        "host": raw["host"],
        "service": raw["service"],
        "source_format": raw["source_format"],
        **parsed,
    }

    try:
        validate(event, "parsed_log_event")
    except ValidationError as exc:
        raise ProcessingFailure("validate", str(exc)) from exc
    return event


def _record_ingest_lag(event: dict) -> None:
    ingested_at = event.get("ingested_at")
    if not isinstance(ingested_at, str):
        return
    try:
        started = datetime.fromisoformat(ingested_at.replace("Z", "+00:00"))
    except ValueError:
        return
    lag = (datetime.now(timezone.utc) - started).total_seconds()
    if lag >= 0:
        INGEST_LAG_SECONDS.observe(lag)


class Processor:
    def __init__(self, config: ProcessorConfig, from_beginning: bool = False) -> None:
        self.config = config
        self.state = HealthState()
        self.resolver = GeoResolver(GEOIP_DB_PATH)
        self.batch_failures = 0
        self.running = True

        consumer_conf = dict(config.consumer_conf())
        if from_beginning:
            consumer_conf["auto.offset.reset"] = "earliest"
        self.consumer = Consumer(consumer_conf)
        self.producer = Producer(config.producer_conf())

        self.stats = {"consumed": 0, "parsed": 0, "dlq": 0, "errors": 0}
        self._last_report = time.perf_counter()
        self._last_consumed = 0

    # -- Kafka plumbing ----------------------------------------------------
    def _on_assign(self, consumer, partitions) -> None:
        self.state.set_assigned(len(partitions))
        ASSIGNED_PARTITIONS.set(len(partitions))
        listing = ", ".join(str(p.partition) for p in partitions) or "none"
        print(f"assigned partitions: [{listing}]", flush=True)

    def _on_revoke(self, consumer, partitions) -> None:
        self.state.set_assigned(0)
        ASSIGNED_PARTITIONS.set(0)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.batch_failures += 1
            PRODUCE_FAILURES.labels(topic=msg.topic() if msg else "unknown").inc()

    def _produce(self, topic: str, key: str | None, payload: bytes) -> None:
        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key.encode() if key else None,
                    value=payload,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                # Downstream is the bottleneck; drain callbacks and retry
                # rather than dropping the event.
                self.producer.poll(0.5)

    def _emit_dlq(self, original, *, reason: str, stage: str, service: str | None = None) -> None:
        event = build_dlq_event(original, reason=reason, stage=stage, service=service)
        self._produce(self.config.dlq_topic, service, serialise(event))
        EVENTS_DLQ.labels(stage=stage).inc()
        self.stats["dlq"] += 1

    # -- Main loop ---------------------------------------------------------
    def handle_batch(self, messages: list) -> None:
        started = time.perf_counter()
        dlq_before = self.stats["dlq"]
        self.batch_failures = 0

        for message in messages:
            EVENTS_CONSUMED.inc()
            self.stats["consumed"] += 1
            payload = message.value()

            raw: dict | None = None
            try:
                event_start = time.perf_counter()
                raw = decode_record(payload)
                event = process_record(raw, self.resolver)
                PROCESSING_SECONDS.observe(time.perf_counter() - event_start)
            except ProcessingFailure as failure:
                self._emit_dlq(
                    raw if raw is not None else payload,
                    reason=failure.reason,
                    stage=failure.stage,
                    service=(raw or {}).get("service"),
                )
                continue

            body = json.dumps(event, separators=(",", ":")).encode()
            service = str(event["service"])
            self._produce(self.config.parsed_topic, service, body)
            EVENTS_PARSED.labels(source_format=str(event.get("source_format"))).inc()
            self.stats["parsed"] += 1
            _record_ingest_lag(event)

            # error-logs is a filtered subset, not a replacement destination:
            # error events appear in both topics (PRD §3.4).
            if event.get("is_error"):
                self._produce(self.config.error_topic, service, body)
                EVENTS_ERRORS_ROUTED.inc()
                self.stats["errors"] += 1

            self.producer.poll(0)

        # Durability gate: everything derived from this batch must be acked
        # before the input offsets move. This is what makes replay-on-crash
        # safe rather than lossy.
        unflushed = self.producer.flush(timeout=30)
        if unflushed or self.batch_failures:
            print(
                f"WARNING: {unflushed} unflushed, {self.batch_failures} failed delivery — "
                f"not committing offsets; this batch will be reprocessed",
                file=sys.stderr,
                flush=True,
            )
        else:
            self.consumer.commit(asynchronous=False)
            BATCHES_COMMITTED.inc()

        if messages:
            DLQ_RATIO.set((self.stats["dlq"] - dlq_before) / len(messages))
        BATCH_SECONDS.observe(time.perf_counter() - started)

    def report(self, force: bool = False) -> None:
        now = time.perf_counter()
        window = now - self._last_report
        if not force and window < 5.0:
            return
        rate = (self.stats["consumed"] - self._last_consumed) / window if window > 0 else 0.0
        print(
            f"  consumed={self.stats['consumed']:>9,}  parsed={self.stats['parsed']:>9,}  "
            f"errors={self.stats['errors']:>7,}  dlq={self.stats['dlq']:>7,}  {rate:>7,.0f}/s",
            flush=True,
        )
        self._last_report = now
        self._last_consumed = self.stats["consumed"]

    def run(self) -> int:
        config = self.config
        server = start_http_server(config.http_port, self.state)
        self.consumer.subscribe(
            [config.in_topic], on_assign=self._on_assign, on_revoke=self._on_revoke
        )

        print(f"bootstrap: {config.bootstrap}")
        print(f"group:     {config.group_id}")
        print(f"in:        {config.in_topic}")
        print(f"out:       {config.parsed_topic} | {config.error_topic} | {config.dlq_topic}")
        print(f"geo:       {self.resolver.backend}")
        print(f"http:      http://localhost:{config.http_port}/healthz | /readyz | /metrics")
        print()

        def stop(_signum, _frame) -> None:
            if not self.running:
                raise KeyboardInterrupt
            self.running = False
            self.state.begin_shutdown()
            print("\nstopping — finishing current batch...", flush=True)

        signal.signal(signal.SIGINT, stop)
        try:
            signal.signal(signal.SIGTERM, stop)
        except (AttributeError, ValueError):
            pass  # SIGTERM is not settable everywhere on Windows

        try:
            while self.running:
                messages = self.consumer.consume(
                    num_messages=config.batch_size, timeout=config.poll_timeout
                )
                self.state.mark_poll()
                LAST_POLL_UNIXTIME.set(time.time())

                if not messages:
                    self.report()
                    continue

                usable = []
                for message in messages:
                    error = message.error()
                    if error is None:
                        usable.append(message)
                    elif error.code() != KafkaError._PARTITION_EOF:
                        print(f"consumer error: {error}", file=sys.stderr, flush=True)

                if usable:
                    self.handle_batch(usable)
                self.report()
        except KeyboardInterrupt:
            print("\nforced stop", flush=True)
        finally:
            self.report(force=True)
            self.producer.flush(timeout=15)
            self.consumer.close()
            self.resolver.close()
            server.shutdown()

        print(
            f"\nconsumed {self.stats['consumed']:,}  parsed {self.stats['parsed']:,}  "
            f"errors {self.stats['errors']:,}  dlq {self.stats['dlq']:,}"
        )
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse, enrich and route raw log events.")
    parser.add_argument("--group", default=None, help="Consumer group id")
    parser.add_argument("--batch-size", type=int, default=None, help="Records per poll")
    parser.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers")
    parser.add_argument("--http-port", type=int, default=None, help="Health/metrics port")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start at the earliest offset when the group has no committed position",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    config = ProcessorConfig()
    if args.bootstrap:
        config.bootstrap = args.bootstrap
    if args.group:
        config.group_id = args.group
    if args.batch_size:
        config.batch_size = args.batch_size
    if args.http_port:
        config.http_port = args.http_port
    return Processor(config, from_beginning=args.from_beginning).run()


if __name__ == "__main__":
    raise SystemExit(main())
