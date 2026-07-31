-- Reference queries. Not applied by `make ch-init`; run them by hand or copy
-- into Grafana panels in M4.

-- FR3.1 — end-to-end ingestion latency, producer stamp to storage arrival.
-- The PRD target is < 10s at p95.
SELECT
    count()                                                              AS events,
    round(quantile(0.50)(dateDiff('millisecond', ingested_at, stored_at)) / 1000, 2) AS p50_s,
    round(quantile(0.95)(dateDiff('millisecond', ingested_at, stored_at)) / 1000, 2) AS p95_s,
    round(quantile(0.99)(dateDiff('millisecond', ingested_at, stored_at)) / 1000, 2) AS p99_s,
    round(max(dateDiff('millisecond', ingested_at, stored_at)) / 1000, 2)            AS max_s
FROM logpipe.logs_hot
WHERE stored_at > now() - INTERVAL 1 HOUR;

-- FR3.3 — filter by time range, service, status code and log level.
SELECT timestamp, service, status_code, log_level, path, response_time_ms
FROM logpipe.logs_hot
WHERE timestamp > now() - INTERVAL 15 MINUTE
  AND service = 'checkout-api'
  AND status_code >= 500
  AND log_level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 50;

-- FR4.1 — request volume and error rate per service per minute.
SELECT
    toStartOfMinute(timestamp)                        AS minute,
    service,
    count()                                           AS requests,
    countIf(is_error)                                 AS errors,
    round(100 * countIf(is_error) / count(), 3)       AS error_rate_pct,
    round(quantile(0.95)(response_time_ms))           AS p95_ms
FROM logpipe.logs_hot
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY minute, service
ORDER BY minute DESC, requests DESC;

-- FR2.5 — dedupe check. Any event_id appearing more than once after a merge
-- means ReplacingMergeTree is not collapsing replays as intended.
SELECT event_id, count() AS copies
FROM logpipe.logs_hot FINAL
GROUP BY event_id
HAVING copies > 1
ORDER BY copies DESC
LIMIT 10;

-- Where the data physically lives — proves the hot/cold TTL move happened.
SELECT
    partition,
    disk_name,
    rows,
    formatReadableSize(bytes_on_disk) AS size,
    min_time,
    max_time
FROM system.parts
WHERE database = 'logpipe' AND table = 'logs_hot' AND active
ORDER BY partition, disk_name;

-- Kafka engine health: is ClickHouse keeping up with parsed-logs?
-- `exceptions` is an array of recent failures; empty means a clean consumer.
SELECT
    table,
    consumer_id,
    num_messages_read,
    num_commits,
    last_poll_time,
    arrayStringConcat(exceptions.text, ' | ') AS recent_errors
FROM system.kafka_consumers
WHERE database = 'logpipe';
