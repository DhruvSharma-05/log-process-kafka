# Design decisions

Why this pipeline is built the way it is, including the tradeoffs accepted and the things
deliberately left undone. Each entry states what was decided, what it costs, and what would change
the decision.

---

## ClickHouse over Elasticsearch for hot storage

**Decision:** ClickHouse.

The PRD left this open (§9), suggesting "full-text search favors Elasticsearch; high-cardinality
analytical aggregation favors ClickHouse". Our query patterns are the second kind — count and rate
per service per minute, group by status code, percentile latency. Every dashboard panel is an
aggregation, not a text search.

ClickHouse also collapsed four separate pieces of work into table definitions:

| Requirement | How ClickHouse handles it |
| --- | --- |
| FR3.1 ingest from Kafka | Kafka table engine — **no Kafka Connect deployment at all** |
| FR2.5 dedupe on event ID | `ReplacingMergeTree` collapses on merge |
| FR3.2 tiered archival | `TTL ... TO VOLUME 'cold'` with an S3 disk |
| Privacy NFR | Column-level TTL nulls `client_ip` on the same schedule |

It also fits the Portability NFR better: ~700 MB resident versus Elasticsearch's JVM heap.

**Cost:** no real full-text search. `WHERE message LIKE '%...%'` works but scans. If log-body search
became a primary use case rather than an occasional one, this decision should be revisited.

**Verified:** 229k messages consumed by the Kafka engine with zero consumer exceptions; end-to-end
p95 latency 1.27 s against a 10 s target.

---

## No Kafka Connect

**Decision:** ClickHouse consumes `parsed-logs` directly.

The PRD's architecture has a sink connector between Kafka and storage. ClickHouse's Kafka table
engine plus a materialized view does the same job in ~80 lines of SQL, with one fewer service to
deploy, configure, monitor and version.

**Cost:** the ingestion path is ClickHouse-specific. Swapping storage engines means rewriting the
ingestion, whereas Connect would abstract it. Given the storage choice is already made, that
abstraction has no current buyer.

**Guard rails:** `kafka_skip_broken_messages = 100` so one malformed message cannot wedge the
consumer, with the processor's DLQ as the real defence upstream.

---

## Python + ksqlDB instead of Kafka Streams or Flink

**Decision:** parse/enrich/DLQ in a Python consumer; windowed aggregates in ksqlDB.

The PRD's other open question (§9) was Kafka Streams vs Flink. The split exists because the two jobs
have different shapes:

- **Failure handling wants explicit code.** The DLQ rule — every stage wrapped, stage and reason
  attached, original preserved — is the most important behaviour in the pipeline. It should be
  readable line by line, not expressed as topology configuration.
- **Windowed aggregation wants SQL.** FR2.4's 1-minute tumbling error rate per service is six lines
  of ksqlDB versus roughly 150 of hand-rolled Python windowing.

**Cost:** two runtimes instead of one, and ksqlDB is a 1.4 GB JVM for one query. Kafka Streams would
unify them at the price of a Gradle build and a less readable failure path.

**Would change if:** event-time windowing with out-of-order handling became a requirement. ksqlDB
here windows on Kafka record time — see below.

---

## Aggregates window on record time, not event time

**Decision:** ksqlDB uses the Kafka record timestamp.

The parsed `timestamp` field has variable fractional-second precision — both `...:32Z` and
`...:32.104000Z` occur, depending on source format — and ksqlDB's `TIMESTAMP_FORMAT` takes a single
pattern that cannot express both.

The producer stamps records within milliseconds of ingestion, so record time and event time agree far
more closely than the 1-minute window granularity.

**Cost:** genuinely late-arriving data lands in the wrong window. For a log pipeline where the
shipper writes immediately, this is negligible. For sources buffering minutes of data offline, it
would not be — and that is the case that argues for Flink.

---

## `service` stays the partition key, despite measured skew {#partition-key}

**Decision:** keep `service` as the partition key (FR1.3), and document the consequence rather than
quietly working around it.

The PRD itself flags this: "reassess if one service dominates volume". Measured across 400,000 NASA
log lines:

| Service | Share |
| --- | --- |
| image-service | 41.2% |
| shuttle-api | 27.8% |
| history-api | 14.2% |
| *(11 others)* | 16.8% |

One partition took **55%** of 200,000 events while another took 0.3%. The effect on scaling is real —
three processor instances measured 298/s, 2,305/s and 1,799/s.

**Why not fix it:** the obvious mitigation is a composite `service|host` key, which spreads load
evenly but destroys per-service ordering — exactly what FR1.3 asks for and what makes "replay this
service's logs in order" possible.

**Would change if:** a single service grew to dominate so heavily that one consumer could not keep up
with its partition. At that point ordering per service must be traded for throughput, and that is a
product decision, not a silent implementation detail.

---

## `event_id` is a content hash, not a UUID

**Decision:** `sha256(host | service | raw_message)[:32]`.

FR2.5 requires at-least-once processing with idempotent writes and dedupe on event ID. A `uuid4()`
would be unique per *call*, so a replayed batch after a crash would produce new IDs and duplicate
rows. A content hash makes a replayed event byte-identical, so it sorts to the same position in
`ReplacingMergeTree` and collapses on merge.

**Cost:** two genuinely distinct events with identical content, host, service and timestamp are
treated as one. In access logs that means two identical requests within the same second from the same
client, which is a rounding error against the value of safe replay.

**Verified:** 35 naturally occurring duplicates → 0 after `OPTIMIZE`.

---

## Offsets commit after flush, not after processing

**Decision:** consume batch → produce derived events → `flush()` → **then** commit.

This is what makes at-least-once real rather than nominal. If the produce is not durable and the
offset advances, those events are gone with no trace. A batch that fails to flush is not committed,
is logged, and is reprocessed.

**Cost:** reprocessing after a crash, which is exactly why `event_id` is content-derived.

**Verified:** 540,000 events backlogged during a full consumer outage, all 540,000 processed on
restart — `consumed=540,000 parsed=540,000 dlq=0`.

---

## Three-tier geo resolution instead of requiring MaxMind

**Decision:** MaxMind if a `.mmdb` is present → ccTLD inference → `unresolved`, with the tier always
recorded in `geo_source`.

MaxMind GeoLite2 requires a registered licence key, so making it a hard dependency would mean the
repo cannot be cloned and run. The NASA dataset also logs *hostnames* (`kgtyk4.kj.yamagata-u.ac.jp`),
not IPs, so ccTLD inference is the tier that actually fires on the sample data.

**The `geo_source` field is the point.** Without it, a dashboard showing "JP" cannot distinguish a
database lookup from a guess based on a domain suffix. Inference that cannot be identified as
inference is worse than no inference.

Private, loopback, link-local and RFC 5737 documentation ranges resolve to `null` with
`geo_source: "private"` — never to a country. This corrects the PRD's own example, which mapped
`10.0.4.12` to `"IN"`; RFC1918 space has no geography.

---

## `additionalProperties: false` on the parsed event schema

**Decision:** reject unknown fields outright.

The PRD lists schema drift as a risk with "silent data corruption" as the impact. An unexpected field
is the earliest observable symptom of a producer changing its output. Rejecting it turns a silent
change into a DLQ entry naming the field.

**Cost:** adding a field requires updating the schema. That is the intended friction.

---

## The scenario generator is required infrastructure, not a toy

**Decision:** `producer/scenarios.py` ships as a first-class component with unit tests.

Real traffic cannot test the alerting. The NASA dataset is **0.002% server errors** — 30 in 1,569,898
lines — while FR4.2 fires above 5%. Replaying real logs proves nothing about whether alerting works.

The generator produces error spikes targeting a *named* service while others stay healthy, which is
what lets the demo prove the alert names the right culprit rather than firing globally.

Its own tests assert that generated lines parse with the production parser, that "corrupt" lines
genuinely fail to parse, and that error rates land within tolerance of what was configured. A
generator whose corrupt lines accidentally parse would leave the DLQ empty and the test green.

---

## Prometheus instead of a `pipeline-metrics` Kafka topic

**Decision:** dropped the PRD's `pipeline-metrics` topic; the processor exposes `/metrics` and
Prometheus scrapes it.

Publishing internal metrics to Kafka would need a second consumer and a second storage path for data
Grafana can read directly. The ksqlDB output topic `service-metrics-1m` covers the
"aggregates as a stream" demonstration.

---

## Dev runs RF=1; production profile is separate

**Decision:** dev is one broker at RF=1. `docker-compose.prod.yml` runs 3 brokers at RF=3 with
`min.insync.replicas=2`.

The PRD's Durability NFR says RF ≥ 2 in dev, but its own M0 milestone describes a single broker —
topic creation with RF=2 against one broker fails outright. Rather than silently pick one, dev is
honestly RF=1 and the durability claims are proven on a profile that can actually support them.

**Verified on the production profile:** broker SIGKILLed mid-write, 400,000 records acked with
`acks=all`, all 400,000 readable, zero delivery failures. `unclean.leader.election.enable=false`
means two brokers down refuses writes rather than accepting them unsafely — unavailability preferred
over silent loss.

---

## Deliberately not done

| Item | Why | What it would take |
| --- | --- | --- |
| **TLS on the production profile** | Needs generated CA and keystore material; SASL already proves client authentication | Generate a CA + per-broker keystores; switch `EXTERNAL` to `SASL_SSL`. Config delta documented in `docker-compose.prod.yml`; no code change |
| **ACL authorization** | `StandardAuthorizer` deadlocks KRaft bootstrap — the controller's Raft `VOTE` requests are rejected with `AuthorizerNotReadyException` because ACLs live in a metadata log that needs a quorum, which needs authorization | Add `User:ANONYMOUS` to `super.users` for the internal PLAINTEXT listeners, then define per-topic ACLs. Working config documented inline |
| **Composite partition key** | Would trade away the per-service ordering FR1.3 depends on | A product decision about ordering vs. even load distribution |
| **Distributed tracing, multi-region, ML anomaly detection, custom UI, multi-tenancy** | Explicit PRD non-goals (§1.4, §8) | — |
