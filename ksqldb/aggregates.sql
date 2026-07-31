-- Windowed aggregates (FR2.4).
--
-- Request count and error count per service per 1-minute tumbling window,
-- published to `service-metrics-1m` as a stream of aggregate records.
--
-- Apply with: make ksql-init
--
-- Note on time semantics: this windows on Kafka record time, not the parsed
-- event time. Our `timestamp` field has variable fractional-second precision
-- ("...:32Z" and "...:32.104000Z" both occur), which ksqlDB's TIMESTAMP_FORMAT
-- cannot express as a single pattern. Since the producer stamps records within
-- milliseconds of ingestion, record time and event time agree closely enough
-- for a 1-minute window. True event-time windowing with out-of-order handling
-- is the Flink argument in the PRD's Section 9 — deferred, not forgotten.

SET 'auto.offset.reset' = 'latest';

CREATE STREAM IF NOT EXISTS parsed_logs_stream (
    event_id         VARCHAR,
    service          VARCHAR,
    host             VARCHAR,
    status_code      INT,
    log_level        VARCHAR,
    is_error         BOOLEAN,
    response_time_ms INT,
    path_group       VARCHAR
) WITH (
    KAFKA_TOPIC = 'parsed-logs',
    VALUE_FORMAT = 'JSON'
);

CREATE TABLE IF NOT EXISTS service_metrics_1m
WITH (
    KAFKA_TOPIC = 'service-metrics-1m',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1,
    REPLICAS = 1,
    -- Must match the topic created by scripts/create_topics.sh (3d). Without
    -- it ksqlDB assumes Kafka's 7-day default and refuses to attach to an
    -- existing topic whose retention differs.
    RETENTION_MS = 259200000
) AS
SELECT
    service,
    AS_VALUE(service)                                        AS service_name,
    WINDOWSTART                                              AS window_start,
    WINDOWEND                                                AS window_end,
    COUNT(*)                                                 AS request_count,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END)                AS error_count,
    -- Cast to DOUBLE before dividing. A `100.0` literal makes this DECIMAL
    -- arithmetic, whose scale grows with each operation until ROUND overflows
    -- and silently returns NULL — which looks exactly like "no errors".
    ROUND(100 * CAST(SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS DOUBLE)
              / CAST(COUNT(*) AS DOUBLE), 3)                 AS error_rate_pct,
    AVG(CAST(response_time_ms AS DOUBLE))                    AS avg_response_ms,
    MAX(response_time_ms)                                    AS max_response_ms
FROM parsed_logs_stream
-- RETENTION must be declared: ksqlDB otherwise derives a 7-day sink-topic
-- retention and refuses to write to `service-metrics-1m`, which the topic
-- design pins at 3 days. GRACE PERIOD admits records that arrive slightly
-- late without holding every window open.
WINDOW TUMBLING (SIZE 1 MINUTE, RETENTION 3 DAYS, GRACE PERIOD 30 SECONDS)
GROUP BY service
EMIT CHANGES;
