-- Hot storage target table (FR3.1, FR3.3).
--
-- ReplacingMergeTree is what keeps FR2.5's promise: the pipeline is
-- at-least-once, so a crash mid-batch replays events. Because `event_id` is a
-- content hash, a replayed event is byte-identical, sorts to the same position,
-- and collapses on merge instead of double-counting.
--
-- All DDL here is idempotent — safe to re-apply with `make ch-init`.

CREATE DATABASE IF NOT EXISTS logpipe;

CREATE TABLE IF NOT EXISTS logpipe.logs_hot
(
    event_id          String,

    -- Event time, from the log line itself. Everything is filtered by this.
    timestamp         DateTime64(3, 'UTC'),
    -- Pipeline entry time, stamped by the producer.
    ingested_at       DateTime64(3, 'UTC'),
    -- Processor completion time.
    processed_at      DateTime64(3, 'UTC'),
    -- Storage arrival time. stored_at - ingested_at is the FR3.1 latency SLA.
    stored_at         DateTime64(3, 'UTC') DEFAULT now64(3),

    host              LowCardinality(String),
    service           LowCardinality(String),
    source_format     LowCardinality(String),

    client_ip         Nullable(String),
    client_ip_hash    Nullable(String),
    geo_country       LowCardinality(Nullable(String)),
    geo_source        LowCardinality(String),

    http_method       LowCardinality(Nullable(String)),
    path              Nullable(String),
    path_group        LowCardinality(Nullable(String)),
    status_code       Nullable(UInt16),
    response_bytes    Nullable(UInt64),
    response_time_ms  Nullable(UInt32),

    log_level         LowCardinality(String),
    is_error          Bool,
    message           Nullable(String),
    user_agent        Nullable(String),
    referer           Nullable(String),
    trace_id          Nullable(String),

    -- Skip indexes for the FR3.3 query patterns that are not the sort key.
    INDEX idx_status     status_code TYPE set(64)  GRANULARITY 4,
    INDEX idx_level      log_level   TYPE set(8)   GRANULARITY 4,
    INDEX idx_path_group path_group  TYPE set(128) GRANULARITY 4,
    INDEX idx_error      is_error    TYPE set(2)   GRANULARITY 4
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMMDD(timestamp)
-- service first: every dashboard and incident query filters on it (FR3.3).
-- event_id last: completes the dedupe key without hurting range scans.
ORDER BY (service, timestamp, event_id)
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192;
