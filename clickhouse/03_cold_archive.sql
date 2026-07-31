-- Cold-tier archival (FR3.2).
--
-- Parts age out of local disk onto S3/MinIO automatically once their data
-- passes the hot window. The rows stay queryable — only their storage changes,
-- which is the point: compliance retention without paying for hot storage.
--
-- HOT_DAYS is applied as a literal below; keep it in sync with the
-- parsed-logs Kafka retention (7d) so the tiers line up.

ALTER TABLE logpipe.logs_hot
    MODIFY TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold';

-- Long-term storage keeps the salted hash, not the raw client address
-- (Data privacy NFR). Applied as a column-level TTL so it happens on the same
-- schedule as the move, with no separate job to run.
ALTER TABLE logpipe.logs_hot
    MODIFY COLUMN client_ip Nullable(String) TTL toDateTime(timestamp) + INTERVAL 7 DAY;
