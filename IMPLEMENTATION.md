# Implementation Plan — Real-Time Log Processing System

Companion to `Kafka_Log_Processing_System_PRD.pdf` (v1.0). The PRD says *what* and *why*;
this document says *what we build, in what order, with which tools*.

**Status:** ✅ **All milestones M0 – M7 complete** (verified 2026-08-07). Every PRD success metric
met and measured. Remaining open items are listed in §9.
**Environment:** Windows 11 + Docker Desktop (WSL2 backend), Python 3.11+.

---

## 1. Stack Decisions

These resolve the PRD's Section 9 open questions and lock the "candidate tech" columns.

| Layer | Choice | Why |
| --- | --- | --- |
| Broker | **Apache Kafka 3.7+ (KRaft, no ZooKeeper)** | Single-process, fast local startup, current-gen setup. |
| Schema registry | **Confluent Schema Registry** (JSON Schema, not Avro) | Satisfies the schema-drift risk without Avro codegen overhead in Python. |
| Ingestion | **Custom Python producer** (`confluent-kafka`) + **flog** for load | Log replay needs timestamp rewriting, which Filebeat can't do. flog covers raw throughput testing. |
| Stream processor | **Python consumer** (`confluent-kafka`) | Parse / enrich / DLQ / branch. Explicit, readable failure handling. |
| Windowed aggregates | **ksqlDB** | FR2.4 (1-min tumbling error rate per service) in ~6 lines of SQL vs ~150 of Python. |
| Hot storage | **ClickHouse** | Native Kafka table engine (no Connect needed), `ReplacingMergeTree` for FR2.5 dedupe, `TTL ... TO DISK` for FR3.2 archival, ~1–2 GB RAM. |
| Cold storage | **MinIO** (S3-compatible) | Local, no cloud dependency (Portability NFR). |
| Dashboards | **Grafana** + ClickHouse datasource plugin | One tool for logs, metrics, and alerts — no Kibana needed. |
| Alerting | **Grafana Alerting** → Slack incoming webhook | FR4.2 / FR4.3. |
| Pipeline observability | **Prometheus** + `kafka-exporter` + processor `/metrics` | FR5.1, Observability NFR. |

**Dropped from the PRD:** Kafka Connect (ClickHouse consumes Kafka directly), Elasticsearch/Kibana,
and the `pipeline-metrics` Kafka topic — see §3.5.

---

## 2. Architecture (as built)

```text
  NASA access logs ─┐
  flog (synthetic)  ├─► producer/ ──► [raw-logs] ──► processor/ ──┬──► [parsed-logs] ──► ClickHouse (Kafka engine)
  scenario gen ─────┘    (Python)                    (Python)     │                          │
                                                                  ├──► [error-logs]          ├─► logs_hot (MergeTree, 7d TTL)
                                                                  └──► [dead-letter-queue]   └─► MinIO (cold, S3 TTL)
                                                                          │
                                              ksqlDB ◄── [parsed-logs] ───┘
                                                 │
                                                 └──► [service-metrics-1m] ──► ClickHouse ──► Grafana ──► Slack alert

  Prometheus ◄── kafka-exporter (lag) + processor:8000/metrics + clickhouse:9363
```

---

## 3. Corrections to the PRD

Found while reviewing; applied throughout this plan.

### 3.1 Add `event_id` to the event schema

FR2.5 requires "dedupe on event ID" but no such field exists in §3.3. The producer stamps:
`event_id = sha256(host + raw_message + timestamp)[:32]`. ClickHouse dedupes on it via `ReplacingMergeTree`.

### 3.2 Add `response_time_ms`

FR4.1 promises a latency dashboard panel but neither schema carries a duration. Our log format
includes `$request_time`; the field is nullable so NASA data (which lacks it) still parses.

### 3.3 Geo-IP on private IPs

The PRD example maps `10.0.4.12` → `"IN"`, which is impossible — RFC1918 ranges resolve to nothing.
Rule: private/loopback/link-local → `geo_country: null`, and that is **not** a DLQ condition.

### 3.4 Replication factor

Durability NFR says RF ≥ 2 in dev, but M0 describes a single broker — topic creation would fail.
**Decision:** dev runs 1 broker at RF=1; `docker-compose.prod.yml` runs 3 brokers at RF=3, and the
README documents the tradeoff. Milestone M6 validates the 3-broker profile.

### 3.5 Drop the `pipeline-metrics` topic

Prometheus already scrapes the processor. Keeping a metrics topic means a second consumer and a
second storage path for data Grafana can read directly. The ksqlDB aggregate output topic
(`service-metrics-1m`) covers the "aggregates as a stream" demo instead.

---

## 4. Repo Layout

```text
log process/
├── docker-compose.yml            # dev: 1 broker, full stack
├── docker-compose.prod.yml       # M6: 3 brokers, RF=3, SASL/SSL profile
├── .env.example
├── Makefile                      # up / down / topics / seed / load / test
├── README.md                     # architecture diagram, setup, runbook (M7)
├── IMPLEMENTATION.md             # this file
│
├── data/                         # gitignored — downloaded datasets
│   └── .gitkeep
├── scripts/
│   ├── download_data.ps1         # NASA + loghub samples
│   ├── create_topics.sh
│   └── smoke_test.py             # M0 round-trip
│
├── producer/
│   ├── main.py                   # replay / flog / scenario modes
│   ├── replay.py                 # file tail + timestamp rewrite + rate limit
│   ├── scenarios.py              # error spikes, malformed injection, 2x bursts
│   ├── config.py
│   └── requirements.txt
│
├── processor/
│   ├── main.py                   # consume loop, offset commit, graceful shutdown
│   ├── parsers/
│   │   ├── nginx_combined.py     # FR1.2 format 1
│   │   └── json_app.py           # FR1.2 format 2
│   ├── enrich.py                 # geo-ip, http fields, log_level derivation
│   ├── redact.py                 # PII hashing (Data privacy NFR)
│   ├── dlq.py                    # failure -> dead-letter-queue with reason
│   ├── metrics.py                # prometheus_client, :8000/metrics
│   ├── health.py                 # FR5.3 /healthz, /readyz
│   └── requirements.txt
│
├── schemas/
│   ├── raw_log_event.schema.json
│   ├── parsed_log_event.schema.json
│   └── dlq_event.schema.json
│
├── ksqldb/
│   └── aggregates.sql            # FR2.4 tumbling windows
│
├── clickhouse/
│   ├── 01_kafka_engine.sql       # Kafka source tables + materialized views
│   ├── 02_tables.sql             # logs_hot (ReplacingMergeTree, TTL)
│   └── 03_cold_archive.sql       # S3 disk + TTL move to MinIO
│
├── grafana/
│   ├── provisioning/             # datasources + dashboards as code
│   └── dashboards/
│       ├── service-health.json   # FR4.1
│       └── pipeline-health.json  # FR5.1 consumer lag
│
├── prometheus/
│   └── prometheus.yml
│
└── tests/
    ├── test_parsers.py           # golden lines: valid, malformed, edge cases
    ├── test_enrich.py
    └── test_e2e.py               # produce -> assert queryable in ClickHouse
```

---

## 5. Data Model (final)

### Raw event → `raw-logs`

```json
{
  "event_id": "a3f5...c1",
  "ingested_at": "2026-07-30T10:15:32.104Z",
  "host": "web-03",
  "service": "checkout-api",
  "source_format": "nginx_combined",
  "raw_message": "10.0.4.12 - - [30/Jul/2026:10:15:32 +0000] \"POST /checkout HTTP/1.1\" 500 342 0.184"
}
```

### Parsed event → `parsed-logs` (and `error-logs` if `status_code >= 500 or log_level == "ERROR"`)

```json
{
  "event_id": "a3f5...c1",
  "timestamp": "2026-07-30T10:15:32.104Z",
  "ingested_at": "2026-07-30T10:15:32.310Z",
  "host": "web-03",
  "service": "checkout-api",
  "client_ip": "10.0.4.12",
  "client_ip_hash": "9c1f...",
  "geo_country": null,
  "http_method": "POST",
  "path": "/checkout",
  "status_code": 500,
  "response_bytes": 342,
  "response_time_ms": 184,
  "log_level": "ERROR",
  "trace_id": null
}
```

### DLQ event → `dead-letter-queue`

```json
{
  "event_id": "a3f5...c1",
  "failed_at": "2026-07-30T10:15:32.310Z",
  "error_reason": "nginx_combined: regex did not match",
  "error_stage": "parse",
  "original": { "...the full raw event..." }
}
```

**Partition key:** `service` on all topics (PRD §3.4). Two derived fields are computed, not parsed:
`log_level` (from `status_code` when the format has no explicit level) and `client_ip_hash`
(SHA-256 + salt, so cold storage can drop `client_ip` entirely).

### Topics

| Topic | Partitions (dev) | Retention | RF (dev) |
| --- | --- | --- | --- |
| `raw-logs` | 6 | 24h | 1 |
| `parsed-logs` | 6 | 7d | 1 |
| `error-logs` | 3 | 14d | 1 |
| `dead-letter-queue` | 1 | 30d | 1 |
| `service-metrics-1m` | 1 | 3d | 1 |

---

## 6. Milestones

Each milestone ends in a demoable state. Don't start the next until the exit criteria pass.

### M0 — Foundations ✅ DONE

**Build:** `docker-compose.yml` with Kafka (KRaft, single broker) + Schema Registry. `create_topics.sh`.
`smoke_test.py` that produces 10 messages and consumes them back.

- [x] Compose file with healthchecks on every service (+ Kafka UI on :8080)
- [x] Topic creation script (idempotent, safe to re-run) — all 5 topics at planned partitions/retention
- [x] `make up` / `make down` / `make topics` / `make smoke` / `make reset`
- [x] Smoke test round-trip

**Exit:** ✅ `make up && make smoke` round-tripped 10/10 messages; topics survived `docker compose restart kafka`.

**Watch for:** Kafka advertised listeners on Windows/WSL2 — you need both an internal
(`kafka:9092`) and host (`localhost:29092`) listener or nothing outside Docker can connect.
*Resolved:* host clients use `localhost:29092`, in-network services use `kafka:9092`.

---

### M1 — Ingestion ✅ DONE

**Build:** `producer/` and the data download script.

- [x] `scripts/download_data.ps1` — NASA Aug95 (160 MB, 1.57M lines) + 4 loghub samples into `data/`
- [x] `replay.py` — rate-limited replay with in-line timestamp rewriting to `now()`
- [x] `event_id` stamping (content hash), `service` partition key, idempotent batched produce with delivery callbacks
- [x] `--stdin` for piping synthetic generators (`flog ... | python producer/main.py --stdin`)
- [x] Producer Prometheus counters on `:8001` — events, failed, bytes, skipped, observed rate, queue depth

**Exit:** ✅ Sustained **1 989 events/sec** against a 2 000 target (0.5 % pacing error); console consumer
shows events with 2026 timestamps and derived `service` keys.

**Measured, unlimited rate: 41 369 events/sec** — 8× the PRD's 5 000/sec success metric, single broker,
single producer process. Recorded here as the M6 starting baseline.

**Watch for:** NASA logs are Latin-1, not UTF-8, and contain malformed lines — read with
`errors="replace"` and let the bad ones flow through to prove the DLQ later.
*Resolved:* `iter_file` decodes leniently; the processor owns rejection, not the producer.

**Synthetic fields (documented, not hidden).** NASA access logs carry no `service`, `host`, or request
time. `replay.py` derives `service` from the request path and `host` from the client identifier, both
deterministically, so per-service dashboards and the `service` partition key are meaningful.
`--synth-latency` appends a modelled `$request_time` (errors slower than successes) so the FR4.1
latency panel has data. Off by default. The README must state all three are synthetic.

**⚠ Finding — partition skew is real with this dataset.** Measured over 400 000 lines:

| Service | Share | Service | Share |
| --- | --- | --- | --- |
| image-service | 41.2 % | software-api | 2.5 % |
| shuttle-api | 27.8 % | facilities-api | 1.0 % |
| history-api | 14.2 % | finance-api | 0.5 % |
| portal-web | 5.4 % | media-service | 0.5 % |
| search-api | 3.4 % | *(5 more)* | < 0.3 % |
| launch-api | 2.8 % | | |

Result: one partition took **55 %** of 200 000 events while another took 0.3 %. This is exactly the
caveat the PRD raises in §3.4 ("reassess if one service dominates volume"), now with numbers.
**Decision: keep `service` as the key** — FR1.3 mandates it and per-service ordering depends on it.
Carry to M6 as a tuning item; the mitigation if it hurts is a composite `service|host` key, which
trades strict per-service ordering for even spread. Document the tradeoff rather than silently fixing it.

---

### M2 — Processing + DLQ ✅ DONE

**Build:** `processor/`, `schemas/`, `tests/`.

- [x] Access-log parser (one regex covering CLF, combined, and optional `$request_time`) and JSON app-log parser with field aliases
- [x] Enrichment: three-tier geo resolution, `path_group`, `is_error`, derived `log_level`
- [x] PII redaction: salted `client_ip_hash`, email/credential/card scrubbing, `REDACT_CLIENT_IP` kill switch
- [x] JSON Schema validation (draft 2020-12) against `parsed_log_event.schema.json`
- [x] Every parse/enrich/validate path wrapped → DLQ with stage + reason; original preserved verbatim for replay
- [x] Branch: all → `parsed-logs`, `is_error` subset *additionally* → `error-logs`
- [x] Manual offset commit only after `producer.flush()` succeeds (at-least-once, FR2.5)
- [x] `/healthz`, `/readyz` (FR5.3) + `/metrics` on `:8000`, one server
- [x] **101 unit tests**, all passing

**Exit:** ✅ 230,829 records consumed → **230,627 parsed + 202 DLQ, zero unaccounted**. Sustained
~3,900 events/sec. Processor never restarted. DLQ entries carry readable reasons:

```text
"error_reason":"does not match access-log format","error_stage":"parse"
  original: ... "GET /shuttle/missions/sts-34/mission-sts-34.html"><IMG images/ssbuv1.gif SRC=..."
```

Verified separately: 5 injected 5xx/FATAL events landed in **both** `parsed-logs` and `error-logs`;
ccTLD enrichment produced `geo_country: "JP"` with `geo_source: "tld"`; redaction turned an email into
`[email]` and a bearer token into `[redacted-credential]`.

**Geo-IP: three tiers, not one.** MaxMind GeoLite2 needs a registered licence key, so it cannot be a
hard dependency of a clone-and-run repo. `enrich.py` uses MaxMind when a `.mmdb` is present, falls
back to **ccTLD inference** (the NASA logs record hostnames like `kgtyk4.kj.yamagata-u.ac.jp`, so this
is the tier that actually fires), and otherwise records `unresolved`. Every event carries `geo_source`
so a dashboard can never present inference as a database lookup.

**Fixed during M2 — packages, not loose scripts.** `producer/config.py` and `processor/config.py` were
both top-level `config`, so whichever landed on `sys.path` first shadowed the other. Both directories
are now packages; run them as `python -m producer.main` / `python -m processor.main`.

**Fixed during M2 — producer ignored JSON identity.** `service`/`host` were always derived from the
request path, which collapsed every JSON app log onto one partition key. `json_identity()` now reads
the declared `service`/`host` from structured logs and only falls back to derivation.

**Fixed during M2 — health probes spammed stderr.** Each `/readyz` probe that hung up produced a
socketserver traceback. `_QuietThreadingHTTPServer` swallows client disconnects only.

**⚠ Finding — the NASA dataset cannot demo error alerting.** Full status distribution over all
1,569,898 lines:

| Status | Count | Share |
| --- | --- | --- |
| 200 | 1,398,988 | 89.113 % |
| 304 | 134,146 | 8.545 % |
| 302 | 26,497 | 1.688 % |
| 404 | 10,056 | 0.641 % |
| 403 | 171 | 0.011 % |
| 501 | 27 | 0.002 % |
| 400 | 10 | 0.001 % |
| 500 | 3 | 0.000 % |

**30 server errors in 1.57 million lines — 0.002 %.** FR4.2's alert threshold is >5 % over 5 minutes,
so real traffic will never fire it and `error-logs` stays essentially empty. `scenarios.py` is
therefore **required**, not optional: it is the only way to satisfy the M4 exit criterion. Build it
first in M4, before the alert rule.

---

### M3 — Storage & Search ✅ DONE

**Build:** `clickhouse/`, plus ClickHouse + MinIO in Compose.

- [x] `logs_hot` as `ReplacingMergeTree(processed_at)` ordered by `(service, timestamp, event_id)`, partitioned by day
- [x] Kafka engine table on `parsed-logs` (3 consumers) + materialized view into `logs_hot`
- [x] Skip indexes on `status_code`, `log_level`, `path_group`, `is_error` for FR3.3 patterns off the sort key
- [x] MinIO S3 disk + `hot_cold` storage policy + `TTL ... TO VOLUME 'cold'` (FR3.2)
- [x] Column-level TTL nulls `client_ip` on archival, keeping `client_ip_hash` (Data privacy NFR)
- [x] Measured end-to-end latency against the <10 s p95 target

**Exit:** ✅ all four criteria met.

**FR3.1 — latency, measured on a live 60,000-event run at 2,000/sec:**

| events | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| 59,907 | 0.79 s | **1.27 s** | 1.35 s | 1.44 s |

The PRD target is <10 s at p95. We are at **1.27 s — about 8× inside budget** — with the dominant
term being ClickHouse's 1 s `kafka_flush_interval_ms`. Dashboard freshness (≤15 s) is comfortable too.

**FR2.5 — dedupe, verified on naturally occurring duplicates.** Replaying the same lines produced 35
byte-identical events (same content hash → same `event_id`):

```text
before OPTIMIZE:  229,181 rows / 229,146 unique  -> 35 duplicates
after  OPTIMIZE:  229,146 rows / 229,146 unique  ->  0 duplicates
```

**FR3.2 — cold tier, verified end to end.** 5,000 rows dated 30 days back moved to `s3_cold`
*without being forced* — ClickHouse's background TTL task did it on its own. The rows stayed
queryable from S3, `client_ip` was nulled on all 5,000 while `client_ip_hash` survived, and MinIO
held **40 objects / 49 KiB**. The synthetic partition was dropped afterwards; reproduce it with the
`INSERT ... FROM numbers(5000)` recipe below.

**Kafka Connect was not needed, as predicted.** ClickHouse's Kafka engine consumed `parsed-logs`
directly: 3 consumers, 229 k messages, **zero exceptions** in `system.kafka_consumers`. One fewer
service to deploy, configure and monitor than the PRD's original architecture.

**Note — `OPTIMIZE TABLE ... PARTITION <expr>` requires a literal**, not an expression like
`toYYYYMMDD(now() - INTERVAL 30 DAY)`. It failed harmlessly here because the background TTL mover had
already done the work, which is the better demonstration anyway.

**⚠ Trap — bind-mounting `config.d` read-only hides the image's own listener config.** The official
ClickHouse image writes `docker_related_config.xml` into `/etc/clickhouse-server/config.d/` at
startup; that file is what sets `listen_host` to `0.0.0.0`. Mounting the directory read-only for our
storage/Prometheus configs masked it, so ClickHouse fell back to the shipped default of
`127.0.0.1`/`::1` — fine via `docker exec`, unreachable from the host.

Symptom is misleading: `ERR_EMPTY_RESPONSE` / "connection closed unexpectedly" on **every** published
ClickHouse port (8123, 9000, 9363), while `netstat` shows Docker's IPv4 and IPv6 forwarders both
listening. They were forwarding correctly to a port nothing was bound to externally.

Fixed by `clickhouse/config.d/listen.xml`. Would otherwise have blocked M4 (Grafana → ClickHouse) and
M5 (Prometheus → `:9363`) with a much harder-to-read failure.

Reproduce the cold-tier test:

```sql
INSERT INTO logpipe.logs_hot
  (event_id, timestamp, ingested_at, processed_at, host, service,
   source_format, client_ip, client_ip_hash, geo_source, log_level, is_error, status_code)
SELECT concat('archive-', toString(number)),
       now64(3) - INTERVAL 30 DAY, now64(3) - INTERVAL 30 DAY, now64(3) - INTERVAL 30 DAY,
       'web-01', 'archive-test', 'nginx_combined', '198.51.100.7', 'abc123',
       'private', 'INFO', false, 200
FROM numbers(5000);
-- wait ~10s for the background mover, then: make parts
```

---

### M4 — Aggregation & Alerting ✅ DONE

**Build:** `producer/scenarios.py`, `ksqldb/`, `grafana/`, plus ksqlDB + Grafana in Compose.

- [x] `scenarios.py` with four scenarios: `baseline`, `error-spike`, `malformed`, `burst`
- [x] ksqlDB stream over `parsed-logs`, 1-minute tumbling window → `service-metrics-1m` (FR2.4)
- [x] `service-health.json`: 4 stat tiles + volume / error-rate / p95-latency timeseries + recent-errors table, service filter, time-range filterable (FR4.1)
- [x] Grafana alert rule: error rate > 5 % over 5 min, `for: 1m`, min-sample guard (FR4.2)
- [x] Slack contact point + notification policy + message templates (FR4.3)
- [x] 16 new unit tests covering the generator's error rates and parse-compatibility

**Exit:** ✅ Alert fired on the injected spike, isolated to the correct service:

```text
rule: Service error rate above 5%   state: firing   health: ok
  -> checkout-api   Alerting        <- the only one
  -> auth-api       Normal
  -> billing-api    Normal
  -> catalog-api    Normal
  -> inventory-api  Normal
  -> media-service  Normal
  -> portal-web     Normal
```

**FR2.4 aggregate output**, straight off `service-metrics-1m` during the spike:

```json
{"SERVICE_NAME":"checkout-api","REQUEST_COUNT":3592,"ERROR_COUNT":1256,"ERROR_RATE_PCT":34.967,"AVG_RESPONSE_MS":489.9,"MAX_RESPONSE_MS":7843}
{"SERVICE_NAME":"catalog-api","REQUEST_COUNT":1272,"ERROR_COUNT":6,"ERROR_RATE_PCT":0.472,"AVG_RESPONSE_MS":104.3,"MAX_RESPONSE_MS":1272}
```

**Decision — the aggregate topic is not round-tripped into ClickHouse.** The PRD left this open.
ksqlDB satisfies FR2.4 by publishing aggregates as a *stream*; dashboards and the alert rule query
ClickHouse directly, which computes the same numbers in milliseconds over raw rows and can slice by
any dimension without a new ksqlDB query. Importing `service-metrics-1m` back into ClickHouse would
duplicate data for no added capability.

**⚠ Bug found and fixed — ksqlDB decimal overflow silently returned NULL.** The natural way to write
the ratio, `ROUND(100.0 * SUM(...) / COUNT(*), 3)`, makes it DECIMAL arithmetic whose scale grows per
operation until `ROUND` overflows. Output looked like this:

```json
{"SERVICE_NAME":"catalog-api","ERROR_COUNT":2,"ERROR_RATE_PCT":null}     <- errors exist, rate NULL
{"SERVICE_NAME":"inventory-api","ERROR_COUNT":0,"ERROR_RATE_PCT":0E-21}  <- decimal zero
```

**A NULL error rate is indistinguishable from a healthy service on a dashboard**, so this would have
produced an alerting system that silently never fires. Fixed by casting to `DOUBLE` before dividing.
This is exactly the class of bug that only shows up when you actually run the query and read the
output — the SQL was valid and ksqlDB reported success.

**⚠ Two ksqlDB topic-attachment traps.** `CREATE TABLE ... AS` refuses to attach to an existing topic
whose retention differs from what it expects. `WINDOW TUMBLING (... RETENTION 3 DAYS)` does **not**
fix this — that governs the state store. The sink topic needs `RETENTION_MS` in the `WITH` clause,
matching `scripts/create_topics.sh` exactly (259200000).

**Slack delivery needs your webhook.** The rule fires, routes, and POSTs; with the placeholder URL
Grafana logs `failed incoming webhook: no_team`. Put a real URL in `.env` as `SLACK_WEBHOOK_URL` and
FR4.3 completes. Everything upstream of the Slack endpoint is verified working.

**Not visually verified:** the dashboard's panel *rendering*. Provisioning, the datasource health
check, and every panel query were confirmed via the Grafana API, but whether each timeseries panel
draws one series per service (rather than a single merged series) depends on the ClickHouse plugin's
long-to-wide handling and needs a human to look at <http://localhost:3000>.

---

### M5 — Pipeline Observability ✅ DONE

**Build:** `prometheus/`, `grafana/dashboards/pipeline-health.json`, kafka-exporter + Prometheus in Compose.

- [x] `kafka-exporter` for per-consumer-group, per-partition lag (FR5.1)
- [x] Prometheus scrapes exporter + ClickHouse + host-side processor and producer
- [x] `pipeline-health.json`: 4 stat tiles + lag-per-group, throughput, ingest-lag percentiles, DLQ-by-stage, processing time, topic write rate
- [x] Alert on sustained consumer lag (>50k for 3m) and on elevated DLQ rate (>5/s for 2m)

**Exit:** ✅ Induced backpressure test — processor stopped, `raw-logs` flooded at 3,000/sec for 3 min:

```text
processor DOWN, load running        processor RESTARTED
  t+15s  lag =  43,541                t+  2s  lag = 501,000
  t+30s  lag =  93,922                t+ 61s  lag = 346,000
  t+45s  lag = 144,272                t+120s  lag = 208,000
  t+60s  lag = 194,055                t+188s  lag = 116,000
                                      final   lag =       0  (all 6 partitions)
```

Peak lag **540,000**, recorded by Prometheus. Every one of the 540,000 backlogged events was
processed — `consumed=540,000 parsed=540,000 dlq=0`. Zero loss across a full consumer outage, which
is the Fault-tolerance NFR demonstrated rather than asserted.

**The lag alert fired**, confirmed in Grafana's notifier log during the backlog:

```text
rule_uid=logpipe-consumer-lag msg="Sending alerts to local notifier" count=1   (10:58 → 11:00, every 30s)
```

It reads `inactive` afterwards because the lag drained — the rule correctly resolved.

**⚠ Finding — processor throughput is unstable under the full stack.** Across 54 samples during the
drain: **peak 4,991/sec, mean 1,884/sec**, swinging between 1,420 and 4,991 sample to sample. M2
measured a steady ~3,900/sec when only Kafka was running. The stack now runs ten containers on one
laptop (ClickHouse alone at 13.6 % CPU, ksqlDB holding 1.36 GB), so the processor is competing for
cores rather than being architecturally slower.

This matters for M6: **the PRD's "<60s to drain a 2x traffic spike" cannot be evaluated from this
test**, which was far harsher — a *complete* 3-minute consumer outage, not a 2x spike. M6 should run
the actual PRD scenario (steady load, then 2x for a bounded window, processor never stopped) and
measure drain separately from this worst-case number.

**Note — `host.docker.internal` is required for host-side scrape targets.** The producer and
processor run on the host, not in Compose. Prometheus reaches them via `extra_hosts:
host.docker.internal:host-gateway`. The producer target shows DOWN whenever no load is running; that
is correct and visible on the dashboard rather than hidden.

---

### M6 — Hardening ✅ DONE

**Build:** `docker-compose.prod.yml`, `security/`, `common/kafka_security.py`,
`scripts/load_test.ps1`, `scripts/prod_smoke.py`.

- [x] Load test harness; throughput numbers recorded below
- [x] Scaled the processor to 3 instances — automatic rebalance, no code changes
- [x] 2x-burst drain measured against the <60 s PRD metric
- [x] `docker-compose.prod.yml`: 3 brokers, RF=3, `min.insync.replicas=2`, SASL auth
- [x] Broker SIGKILLed mid-write → zero loss proven
- [x] Cold-archive scheduling mechanism confirmed

**Exit:** ✅ all criteria met.

#### 2x burst drain (PRD success metric)

Baseline 1,000/s for 45 s, then **2,000/s for 60 s**, processor running throughout:

| metric | value |
| --- | --- |
| peak lag during spike | **1,531** |
| drain to zero | **3 s** (target <60 s) |
| verdict | **PASS**, ~20× inside budget |

This also settles the M5 "throughput instability" concern. M5's 1,884/s mean was a *worst-case
backlog drain* after a total 3-minute outage, not steady-state capacity. At 2x load the processor
never fell more than 1,531 messages behind.

#### Horizontal scaling (Scalability NFR)

Three processor instances, same code, same command, only `--http-port` differing:

```text
partition 0,1 -> consumer ...eb7fb8f19e3e
partition 2,3 -> consumer ...57608f6873af
partition 4,5 -> consumer ...d4b37b279b6d      clean 2/2/2, automatic rebalance
```

Aggregate throughput **4,401/s**, which matched everything the producer could push. But the
per-instance split exposes the M1 partition-skew finding in production form:

| instance | throughput |
| --- | --- |
| :8000 | 298/s |
| :8010 | 2,305/s |
| :8020 | 1,799/s |

**Adding consumers works, but skewed keys mean they do not share load evenly** — one instance did 8×
the work of another. The NFR ("scale horizontally without code changes") holds; the *efficiency* of
that scaling is bounded by the `service` key distribution. Composite `service|host` keying is the
mitigation, at the cost of strict per-service ordering.

#### Security NFR — SASL authentication enforced

```text
[PASS] anonymous / no SASL    expected refused  got refused
[PASS] wrong password         expected refused  got refused   (Invalid username or password)
[PASS] unknown user           expected refused  got refused
[PASS] valid credentials      expected accepted got accepted  (3 brokers visible)
```

`common/kafka_security.py` reads the protocol and credentials from the environment, so the same
producer and processor binaries run against dev (PLAINTEXT) and prod (SASL) with **no code change**.

**Scope, stated plainly:** this is SASL/PLAIN over PLAINTEXT — real *authentication*, no wire
encryption. TLS is a config delta documented at the bottom of `docker-compose.prod.yml`; it is not
exercised here because it needs generated CA and keystore material.

**⚠ Trap — `StandardAuthorizer` deadlocks KRaft bootstrap.** Enabling ACL authorization made every
broker fail to start: the controller's own Raft `VOTE` requests were rejected with
`AuthorizerNotReadyException`, because ACLs live in `__cluster_metadata`, which needs a quorum, which
needs authorization. Chicken-and-egg. Resolution requires `User:ANONYMOUS` in `super.users` for the
internal PLAINTEXT listeners; left disabled with the working config documented inline, since the NFR
asks for authentication, not authorization.

#### Fault tolerance + Durability NFRs — broker killed mid-write

400,000 records at 10,000/s with `acks=all`, `enable.idempotence=true`; **kafka-2 SIGKILLed at t+9 s**
with writes in flight:

```text
t+ 10.0s  produced 100,000  acked  98,112  failed 0     <- broker killed here
t+ 14.0s  produced 140,000  acked  98,112  failed 0     <- acks stalled, leader election
t+ 18.0s  produced 180,000  acked  98,112  failed 0
t+ 19.0s  produced 190,000  acked 128,632  failed 0     <- recovery begins
t+ 20.0s  produced 200,000  acked 199,548  failed 0     <- fully caught up
...
PASS: 400,000 acked, 400,000 readable, zero loss
```

Acks paused for **~9 seconds** during leader election, then the producer's retry path drained the
backlog completely. ISR shrank 3 → 2 on every partition and leadership migrated off the dead broker;
`min.insync.replicas=2` kept writes legal throughout. **Zero delivery failures, zero loss.**

**⚠ Methodology note — the first run of this test proved less than it looked.** Unthrottled, the
produce finished in 3.0 s, so the broker kill landed during the *read-back* phase, after every write
was already durable. It still printed `PASS: zero loss`. The `--rate` flag exists specifically so the
kill happens while writes are in flight; a fault-injection test that cannot fail is not a test.

#### Cold-archive scheduling (FR3.2)

No cron job is needed — the move TTL is attached to every active part and evaluated continuously by a
dedicated background pool:

```text
part 20260731_6_285_16   move_ttl: ['toDateTime(timestamp) + toIntervalDay(7)']
background_move_pool_size: 8
```

M3 verified this actually fires: 5,000 aged rows moved to MinIO unprompted and stayed queryable.

#### Recorded throughput numbers

| Measurement | Result | Conditions |
| --- | --- | --- |
| Producer, unthrottled | **41,369/s** | M1, Kafka only |
| Producer, full stack | 4,133/s | M6, 10 containers + 3 processors |
| Processor, single instance | ~3,900/s | M2, Kafka only |
| Processor, 3 instances | 4,401/s aggregate | M6, full stack, producer-bound |
| Prod cluster, acks=all RF=3 | **133,000/s** | M6, 400k in 3.0 s, unthrottled |
| End-to-end p95 latency | 1.27 s | M3, target <10 s |
| 2x burst drain | 3 s | M6, target <60 s |

The PRD's ≥5,000 events/sec target is met with large margin; the full-stack numbers are bounded by
one laptop running the entire stack, not by the architecture.

---

### M7 — Documentation ✅ DONE

- [x] `README.md`: Mermaid architecture diagram, quickstart, component and topic tables, data model, honest-scope section
- [x] `docs/RUNBOOK.md`: nine incident scenarios — lag, stuck processor, DLQ filling, latency, disk, broker down, alerts not delivering, empty dashboards, reset
- [x] `docs/DECISIONS.md`: twelve decisions with cost and "would change if", plus a deliberately-not-done table
- [x] `scripts/demo.ps1` — a four-act guided walkthrough, verified end to end

**Exit:** ✅ `make up && make topics && make ch-init && make ksql-init && make data`, then
`make process` + `make demo`. Every `make` target referenced in the docs was checked to exist.

**The demo is verified, not just written.** Running it end to end caught two bugs in itself:

1. **PowerShell here-strings broke the script.** Multi-line SQL in `@"..."@` failed to parse at all.
   Rewritten as concatenated single-line strings; all three `.ps1` files are now syntax-checked with
   `[Parser]::ParseFile`.
2. **`kafka-console-consumer` exits non-zero on `--timeout-ms`**, which under
   `$ErrorActionPreference='Stop'` aborted the demo two acts in. Topic reads are informational and
   must never be fatal — wrapped in a helper that swallows the failure and reports "no messages".

**Bonus verification — FR3.2 proved itself on real data.** M3 demonstrated tiered archival with
synthetic rows dated 30 days back. Running the demo on 2026-08-07 showed the genuine pipeline data
from 2026-07-30 had crossed the 7-day TTL and migrated on its own:

```text
partition 20260730   s3_cold   229,146 rows   15.81 MiB    <- on MinIO, unprompted
partition 20260731   default   965,830 rows   61.94 MiB
partition 20260807   default    50,708 rows    3.28 MiB
```

Latency in the same run: **p50 0.70 s, p95 1.23 s, p99 1.62 s** over 50,708 events — consistent with
the M3 measurement across a week and a very different data mix.

---

## 7. Data Sources

| Source | Use | Where |
| --- | --- | --- |
| **NASA-HTTP** (3.46M real requests, Aug 1995) | Realistic baseline, real 404/500 mix, true Common Log Format | <https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html> |
| **logpai/loghub** (19 systems: HDFS, OpenSSH, Linux, Spark…) | Multi-format testing + naturally messy lines for the DLQ | <https://github.com/logpai/loghub> |
| **flog** | Sustained synthetic load at a configurable rate; apache_combined + JSON output | <https://github.com/mingrammer/flog> |
| **`producer/scenarios.py`** (ours) | Scripted error spikes, malformed-line injection, 2x bursts — the milestone exit criteria | this repo |
| **MaxMind GeoLite2 City** (free, registration) | Geo-IP enrichment DB, bundled into the processor image | <https://dev.maxmind.com/geoip/geolite2-free-geolocation-data> |

NASA logs need public-IP substitution if you want geo-IP to produce anything interesting —
many entries are hostnames rather than IPs. `scenarios.py` can map them onto a small set of
real public IPs for demo purposes.

---

## 8. Definition of Done (traceability)

| PRD requirement | Delivered by |
| --- | --- |
| FR1.1 / FR1.2 / FR1.3 | M1 producer, M2 two parsers, `service` partition key |
| FR2.1 / FR2.2 / FR2.3 | M2 parsers, `enrich.py`, `dlq.py` |
| FR2.4 | M4 ksqlDB tumbling windows |
| FR2.5 | M2 commit-after-produce + M3 `ReplacingMergeTree` on `event_id` |
| FR3.1 / FR3.2 / FR3.3 | M3 ClickHouse + MinIO TTL + ordering key |
| FR4.1 / FR4.2 / FR4.3 | M4 dashboard, alert rule, Slack webhook |
| FR5.1 / FR5.2 / FR5.3 | M5 lag dashboard, Compose throughout, `health.py` |
| Scalability / Fault tolerance / Durability | M6 |
| Security (SASL/SSL) | M6 prod profile |
| Data privacy | M2 `redact.py` |
| Observability | M2 `metrics.py` + M5 |
| Portability | Compose-only, MinIO instead of real S3 |
| Documentation | M7 |

---

## 9. Next Action

**All milestones are complete.** The pipeline ingests, parses, enriches, redacts, stores, tiers,
aggregates, visualises, alerts, and observes itself — with every PRD success metric measured rather
than asserted, and 117 unit tests passing.

Open items needing a human:

- Eyeball both dashboards at <http://localhost:3000> and confirm the timeseries panels split per
  series rather than merging into one line.
- Set a real `SLACK_WEBHOOK_URL` in `.env` to close FR4.3 end to end.

Deliberately not done, and why:

- **TLS** on the production profile — needs generated CA/keystore material; config delta documented.
- **ACL authorization** — deadlocks KRaft bootstrap without `User:ANONYMOUS` in `super.users`;
  working config documented inline, separate hardening exercise.
- **Composite partition key** to fix the load-distribution skew — would trade away the strict
  per-service ordering FR1.3 depends on. Documented as a tradeoff, not silently applied.
