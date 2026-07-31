-- Kafka ingestion (FR3.1).
--
-- ClickHouse consumes `parsed-logs` itself through the Kafka table engine, so
-- there is no Kafka Connect deployment to run, configure or monitor. The Kafka
-- table is a *stream*, not storage: reading from it consumes offsets. Only the
-- materialized view should ever select from it.

CREATE TABLE IF NOT EXISTS logpipe.kafka_parsed_logs
(
    event_id          String,
    timestamp         String,
    ingested_at       String,
    processed_at      String,
    host              String,
    service           String,
    source_format     String,
    client_ip         Nullable(String),
    client_ip_hash    Nullable(String),
    geo_country       Nullable(String),
    geo_source        String,
    http_method       Nullable(String),
    path              Nullable(String),
    path_group        Nullable(String),
    status_code       Nullable(UInt16),
    response_bytes    Nullable(UInt64),
    response_time_ms  Nullable(UInt32),
    log_level         String,
    is_error          Bool,
    message           Nullable(String),
    user_agent        Nullable(String),
    referer           Nullable(String),
    trace_id          Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'parsed-logs',
    kafka_group_name = 'clickhouse-parsed',
    kafka_format = 'JSONEachRow',
    -- One consumer per two partitions; parsed-logs has 6.
    kafka_num_consumers = 3,
    kafka_max_block_size = 65536,
    -- Batch writes rather than one INSERT per message (PRD storage-bottleneck risk).
    kafka_poll_max_batch_size = 10000,
    kafka_flush_interval_ms = 1000,
    -- A malformed message must not wedge the consumer. These are logged and
    -- skipped; the processor's DLQ is the real guard upstream of this.
    kafka_skip_broken_messages = 100,
    input_format_skip_unknown_fields = 1,
    date_time_input_format = 'best_effort';

-- Timestamps arrive as ISO-8601 strings with a Z suffix and microsecond
-- precision. parseDateTime64BestEffort handles both, and doing the conversion
-- here (rather than in the Kafka table) keeps a bad timestamp from breaking
-- the whole block.
CREATE MATERIALIZED VIEW IF NOT EXISTS logpipe.mv_parsed_logs
TO logpipe.logs_hot
AS
SELECT
    event_id,
    parseDateTime64BestEffortOrZero(timestamp, 3, 'UTC')   AS timestamp,
    parseDateTime64BestEffortOrZero(ingested_at, 3, 'UTC') AS ingested_at,
    parseDateTime64BestEffortOrZero(processed_at, 3, 'UTC') AS processed_at,
    now64(3)                                               AS stored_at,
    host,
    service,
    source_format,
    client_ip,
    client_ip_hash,
    geo_country,
    geo_source,
    http_method,
    path,
    path_group,
    status_code,
    response_bytes,
    response_time_ms,
    log_level,
    is_error,
    message,
    user_agent,
    referer,
    trace_id
FROM logpipe.kafka_parsed_logs;
