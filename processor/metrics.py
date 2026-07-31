"""Prometheus metrics for the stream processor (Observability NFR, FR5.1)."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

EVENTS_CONSUMED = Counter(
    "logpipe_processor_consumed_total", "Records read from the input topic"
)
EVENTS_PARSED = Counter(
    "logpipe_processor_parsed_total", "Events successfully parsed and produced", ["source_format"]
)
EVENTS_ERRORS_ROUTED = Counter(
    "logpipe_processor_error_events_total", "Events also routed to error-logs"
)
EVENTS_DLQ = Counter(
    "logpipe_processor_dlq_total", "Events routed to the dead-letter queue", ["stage"]
)
PRODUCE_FAILURES = Counter(
    "logpipe_processor_produce_failures_total", "Downstream produce failures", ["topic"]
)
BATCHES_COMMITTED = Counter(
    "logpipe_processor_batches_committed_total", "Offset commits after a durable produce"
)

PROCESSING_SECONDS = Histogram(
    "logpipe_processor_event_seconds",
    "Per-event parse + enrich + redact + validate time",
    buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.1),
)
BATCH_SECONDS = Histogram(
    "logpipe_processor_batch_seconds",
    "Whole-batch time including produce, flush and commit",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
INGEST_LAG_SECONDS = Histogram(
    "logpipe_processor_ingest_lag_seconds",
    "Wall-clock delay between producer ingest and processor completion",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

LAST_POLL_UNIXTIME = Gauge(
    "logpipe_processor_last_poll_unixtime", "Unix time of the most recent successful poll"
)
ASSIGNED_PARTITIONS = Gauge(
    "logpipe_processor_assigned_partitions", "Partitions currently assigned to this instance"
)
DLQ_RATIO = Gauge(
    "logpipe_processor_dlq_ratio", "DLQ events as a fraction of the last batch"
)
