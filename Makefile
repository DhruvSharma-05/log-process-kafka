.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up down ps logs topics smoke reset clean deps data produce load peek \
        process test dlq counts

help:  ## Show available targets
	@echo "Setup:"
	@echo "  make deps     Install Python dependencies"
	@echo "  make up       Start the stack (waits for health)"
	@echo "  make topics   Create pipeline topics (idempotent)"
	@echo "  make data     Download NASA + loghub datasets"
	@echo ""
	@echo "Run:"
	@echo "  make produce  Replay NASA logs into raw-logs at 1000 events/sec"
	@echo "  make process  Run the stream processor (parse/enrich/DLQ)"
	@echo "  make load     Max-rate load test (200k events)"
	@echo ""
	@echo "Demo:"
	@echo "  make baseline     Healthy multi-service traffic (10 min)"
	@echo "  make spike        Inject a 35% error spike on checkout-api (5 min)"
	@echo "  make corrupt      Inject 5% unparseable lines (2 min)"
	@echo ""
	@echo "Aggregates & dashboards:"
	@echo "  make ksql-init    Create the ksqlDB windowed aggregate (FR2.4)"
	@echo "  make ksql-reset   Tear down ksqlDB objects so they can be re-applied"
	@echo "  make metrics-1m   Tail the service-metrics-1m aggregate topic"
	@echo "  make alerts       Show current Grafana alert-rule state"
	@echo ""
	@echo "Storage:"
	@echo "  make ch-init  Apply the ClickHouse schema (idempotent)"
	@echo "  make ch       Open a ClickHouse SQL shell"
	@echo "  make latency  End-to-end ingestion latency percentiles"
	@echo "  make parts    Which partitions live on hot vs cold storage"
	@echo ""
	@echo "Inspect:"
	@echo "  make test     Unit tests"
	@echo "  make smoke    M0 Kafka round-trip test"
	@echo "  make counts   Message count per topic"
	@echo "  make peek     Print 5 messages from raw-logs"
	@echo "  make dlq      Print 5 dead-lettered events"
	@echo "  make ps       Service status"
	@echo "  make logs     Tail all logs"
	@echo ""
	@echo "Teardown:"
	@echo "  make down     Stop the stack (keeps data)"
	@echo "  make reset    Stop and delete all Kafka data"

deps:  ## Install Python dependencies
	python -m pip install -r requirements-dev.txt

up:  ## Start the stack and wait for services to be healthy
	$(COMPOSE) up -d --wait
	@echo ""
	@echo "  Kafka (host)      localhost:29092"
	@echo "  Schema Registry   http://localhost:8081"
	@echo "  Kafka UI          http://localhost:8080"

down:  ## Stop the stack, keep volumes
	$(COMPOSE) down

ps:  ## Show service status
	$(COMPOSE) ps

logs:  ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

topics:  ## Create pipeline topics
	$(COMPOSE) exec kafka bash /scripts/create_topics.sh

smoke:  ## Produce and consume 10 messages
	python scripts/smoke_test.py

data:  ## Download the NASA and loghub datasets into data/
	powershell -ExecutionPolicy Bypass -File scripts/download_data.ps1

produce:  ## Replay NASA access logs into raw-logs at 1000 events/sec
	python -m producer.main --file data/NASA_access_log_Aug95 --rate 1000 --synth-latency

process:  ## Run the stream processor: parse, enrich, route to parsed/error/DLQ
	python -m processor.main

load:  ## Max-rate load test: 200k events as fast as the broker accepts them
	python -m producer.main --file data/NASA_access_log_Aug95 --rate 0 --limit 200000

test:  ## Run the unit tests
	python -m pytest tests -q

baseline:  ## Healthy multi-service traffic for 10 minutes
	python -m producer.scenarios --scenario baseline --rate 200 --duration 600

spike:  ## Inject a 35% error spike on checkout-api for 5 minutes (trips FR4.2)
	python -m producer.scenarios --scenario error-spike --service checkout-api \
		--error-rate 0.35 --rate 150 --duration 300

corrupt:  ## Inject 5% unparseable lines for 2 minutes (fills the DLQ)
	python -m producer.scenarios --scenario malformed --corrupt-rate 0.05 \
		--rate 200 --duration 120

KSQL_RUN = docker run --rm --network logpipe_default -v "$(CURDIR)/ksqldb:/sql:ro" \
	confluentinc/ksqldb-cli:0.29.0 ksql http://ksqldb:8088

ksql-init:  ## Create the ksqlDB windowed aggregate (FR2.4)
	$(KSQL_RUN) --file /sql/aggregates.sql

ksql-reset:  ## Drop the ksqlDB stream/table so aggregates.sql can be re-applied
	$(KSQL_RUN) --file /sql/reset.sql

metrics-1m:  ## Tail the ksqlDB aggregate output
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic service-metrics-1m --max-messages 10 --timeout-ms 90000

alerts:  ## Show current Grafana alert-rule state
	@curl -s -u admin:admin http://localhost:3000/api/prometheus/grafana/api/v1/rules \
		| python -c "import json,sys; d=json.load(sys.stdin); [print(f\"{r['name']}: {r['state']}\") or [print(f\"   {a['labels'].get('service','-')}: {a['state']}\") for a in r.get('alerts',[])] for g in d['data']['groups'] for r in g['rules']]"

ch-init:  ## Apply the ClickHouse schema (idempotent)
	$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < clickhouse/01_tables.sql
	$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < clickhouse/02_kafka_engine.sql
	$(COMPOSE) exec -T clickhouse clickhouse-client --multiquery < clickhouse/03_cold_archive.sql
	@echo "schema applied"

ch:  ## Open a ClickHouse SQL shell
	$(COMPOSE) exec clickhouse clickhouse-client --database logpipe

latency:  ## End-to-end ingestion latency (producer stamp -> queryable), FR3.1
	@$(COMPOSE) exec -T clickhouse clickhouse-client --query "\
		SELECT count() AS events, \
		round(quantile(0.50)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p50_s, \
		round(quantile(0.95)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p95_s, \
		round(quantile(0.99)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p99_s \
		FROM logpipe.logs_hot WHERE stored_at > now() - INTERVAL 5 MINUTE FORMAT Vertical"

parts:  ## Show which partitions live on hot vs cold storage
	@$(COMPOSE) exec -T clickhouse clickhouse-client --query "\
		SELECT partition, disk_name, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size \
		FROM system.parts WHERE database='logpipe' AND table='logs_hot' AND active \
		GROUP BY partition, disk_name ORDER BY partition FORMAT PrettyCompactMonoBlock"

peek:  ## Print 5 messages from raw-logs
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic raw-logs --from-beginning --max-messages 5 --timeout-ms 15000

dlq:  ## Print 5 dead-lettered events with their failure reasons
	$(COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
		--topic dead-letter-queue --from-beginning --max-messages 5 --timeout-ms 15000

counts:  ## Message count per topic
	@for t in raw-logs parsed-logs error-logs dead-letter-queue; do \
		printf "%-20s" $$t; \
		$(COMPOSE) exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
			--bootstrap-server localhost:9092 --topic $$t 2>/dev/null \
			| awk -F: '{s+=$$3} END {printf "%10d\n", s}'; \
	done

reset:  ## Stop and wipe all Kafka data
	$(COMPOSE) down -v

clean: reset  ## Alias for reset
