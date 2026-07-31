-- Tear down the ksqlDB objects so aggregates.sql can be re-applied after an
-- edit. Run with: make ksql-reset
--
-- The sink topic is deliberately NOT deleted: `service-metrics-1m` is created
-- and configured by scripts/create_topics.sh, and ksqlDB must attach to it
-- rather than own it.

TERMINATE ALL;

DROP TABLE IF EXISTS service_metrics_1m;

DROP STREAM IF EXISTS parsed_logs_stream;
