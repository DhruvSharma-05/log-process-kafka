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

## The ingest gateway defaults a missing timestamp instead of rejecting it

**Decision:** a JSON log arriving over HTTP with no recognisable timestamp field is stamped with
receipt time, and the substitution is counted in `logpipe_ingest_timestamp_defaulted_total`.

The first version did not do this, and the very first `curl` example dead-lettered:

```json
{"error_reason": "no timestamp field (looked for timestamp, time, @timestamp, ts, eventTime)",
 "error_stage": "parse"}
```

The pipeline was behaving correctly — but an ingestion API that silently drops logs from any client
that forgets a field is hostile to the simplest possible use case. Receipt time is a defensible
approximation for a log arriving over HTTP, and every major log API does the same.

**Cost:** a client whose logs are delayed in transit gets receipt time rather than event time. The
counter makes that measurable rather than invisible.

**Not applied to `/v1/logs/raw`:** access-log lines carry their own timestamps, and a line without
one is genuinely malformed.

---

## The gateway rejects at the boundary; the DLQ handles the rest

**Decision:** malformed input to the HTTP gateway returns 4xx with a reason. It is not dead-lettered.

These are different failure classes and deserve different handling. A client sending invalid JSON can
*fix it* — telling them is more useful than silently absorbing it. An event that entered the pipeline
and failed three stages later has no caller left to inform, which is exactly what the DLQ exists for.

The one exception is `/v1/logs/raw`: the gateway does not parse, so it cannot judge those lines. They
are forwarded and the processor dead-letters them with a stage and reason.

**Cost:** a client that ignores HTTP status codes loses those events. That is the client's bug, and
it is visible in `logpipe_ingest_rejected_total` by reason.

---

## Live external data comes from Wikimedia EventStreams

**Decision:** the live-data demonstration uses <https://stream.wikimedia.org/v2/stream/recentchange>.

It is a genuine real-time firehose with no API key, no rate limit worth worrying about, no expiring
credentials, and it is always on — so a demo works on a laptop with no setup. Roughly 30–50 events
per second across every Wikimedia wiki gives natural multi-service structure for dashboards.

**What is mapped and what is not.** Someone else's schema becomes ours, and the temptation is to fill
every field. `response_time_ms` is left **null** because the source has no latency field; inventing
one would put fiction on a latency dashboard that is otherwise measuring something real.

**The `client_ip` correction.** The mapping was first documented as carrying "a real public IP for
anonymous edits". Measuring it disproved that: a 630-edit live sample contained 609 named accounts,
21 masked temporary accounts (`~2026-43591-61`), and **zero IP addresses** — Wikimedia now masks
anonymous editors. The field carries editor identity, geo resolution correctly reports `unresolved`,
and the documentation was corrected rather than the claim being quietly dropped.

**Cost:** near-zero error rate, so this source cannot exercise alerting. That remains the scenario
generator's job, and the two are complementary: one proves live ingestion, the other proves alerting.

---

## No external alert channel

**Decision:** alerts fire and are visible in Grafana, but are not delivered anywhere. There is no
Slack, email, or webhook contact point.

FR4.3 asks for delivery to at least one external channel. It is **not met**, deliberately.

The earlier Slack integration was fully wired — contact point, notification policy, message
templates — and every leg was verified except the last one: with a placeholder webhook URL, Grafana
logged `failed incoming webhook: no_team` on each fire. Completing it required a live webhook from a
real Slack workspace, which cannot be committed to a repo and cannot be exercised by anyone cloning
it. Carrying configuration whose only working state depends on a secret nobody has is worse than
carrying none: it looks finished and is not.

**What still works:** rule evaluation, the >5% threshold, the `for: 1m` transition, per-service
isolation, and routing to Grafana's default notification policy. The alert is observable via
`make alerts`, the Grafana UI, and the Grafana API. Only the delivery hop is absent.

**Cost:** nobody is paged. For a demonstration pipeline where a human is watching the dashboard, the
alert *state* is the deliverable; in production the delivery hop is mandatory.

**To add one:** Grafana -> Alerting -> Contact points, or re-create
`grafana/provisioning/alerting/contact-points.yml`. The rules and notification policy already exist,
so nothing else changes.

---

## The live web UI is a deliberate scope extension

**Decision:** keep the custom web UI at `http://localhost:8100/`, and document it as an addition
beyond the PRD rather than as a requirement it satisfies.

The PRD lists "Building a custom UI — v1 uses existing tools (Kibana/Grafana) rather than a bespoke
frontend" as an explicit **non-goal** (§1.4), and repeats "Custom-built web UI instead of
Kibana/Grafana" under future work (§8). The UI is therefore out of scope as written.

It earns its place anyway, for one reason Grafana cannot cover: **Grafana shows aggregates on a
refresh interval; this shows individual events as they arrive.** Watching a log line appear the
instant it is POSTed is what makes the pipeline legible to someone seeing it for the first time. It
is a demonstration surface, not an operations tool.

**What it is not:** it does not replace the Grafana dashboards, it has no query capability, no time
range, and no persistence. FR4.1 is still satisfied by Grafana, not by this.

**Cost:** ~1,200 lines of HTML/CSS/JS that the PRD did not ask for, plus a WebSocket path on the
ingest gateway that had to be given real backpressure (below).

---

## The live tail drops events on purpose {#live-tail}

**Decision:** the WebSocket feed is lossy by design — rate-capped, bounded per viewer, and dropping
the oldest queued message under pressure.

Kafka is the durable path. The UI is a window onto it. Once that is settled, dropping is obviously
correct: a viewer that cannot keep up should miss lines, not slow down ingestion or consume unbounded
memory.

The first implementation did neither. It fired one `background_tasks.add_task` per ingested event,
wrote directly to every socket, held an unbounded connection list, and swallowed every exception.
Measured:

| | naive | bounded |
| --- | --- | --- |
| ingest, no viewers | 10,655/s | 12,513/s |
| ingest, 3 idle viewers | 4,960/s (−53%) | **12,948/s (0%)** |
| ingest, 3 fast viewers | 4,297/s (−60%) | **9,190/s (−27%)** |
| ingest, 3 slow viewers | 4,632/s (−57%) | **10,359/s (−17%)** |
| RSS, 40k events, 1 stalled viewer | 68 → 96 MB, never reclaimed | 67 → 68 MB |

The design that produces the right-hand column:

- **`publish()` never touches a socket.** It serialises once and does `put_nowait` into a bounded
  queue per connection. A task per connection drains that queue. The ingest path cannot await a
  browser.
- **Queues are bounded (256) and evict the oldest.** On a live tail the newest line is the
  interesting one; showing a viewer a stale backlog is worse than showing gaps.
- **The broadcast is rate-capped (200/s).** A browser cannot render 12,000 lines a second and nobody
  can read them. Above the cap events are sampled out.
- **Viewers are capped (32)** and refused with WebSocket close code 1013 rather than accepted into a
  connection that will never receive anything.

Everything discarded is counted — `logpipe_ingest_live_sampled_out_total`,
`logpipe_ingest_live_dropped_total`, `logpipe_ingest_ws_rejected_total` — so the lossiness is
measurable rather than assumed.

**Cost that remains:** holding WebSocket connections still costs throughput when viewers are
actively reading (−17% to −27%), because uvicorn services those connections on the same event loop.
That is inherent to serving a live feed from the ingest process; moving the tail to a separate
process would remove it, at the cost of another service. Not worth it for a demonstration surface.

---

## Deliberately not done

| Item | Why | What it would take |
| --- | --- | --- |
| **TLS on the production profile** | Needs generated CA and keystore material; SASL already proves client authentication | Generate a CA + per-broker keystores; switch `EXTERNAL` to `SASL_SSL`. Config delta documented in `docker-compose.prod.yml`; no code change |
| **ACL authorization** | `StandardAuthorizer` deadlocks KRaft bootstrap — the controller's Raft `VOTE` requests are rejected with `AuthorizerNotReadyException` because ACLs live in a metadata log that needs a quorum, which needs authorization | Add `User:ANONYMOUS` to `super.users` for the internal PLAINTEXT listeners, then define per-topic ACLs. Working config documented inline |
| **Composite partition key** | Would trade away the per-service ordering FR1.3 depends on | A product decision about ordering vs. even load distribution |
| **External alert delivery (FR4.3)** | Requires a live webhook or SMTP credential that cannot be committed or exercised from a clone | Add a contact point in Grafana; rules and routing already exist |
| **Distributed tracing, multi-region, ML anomaly detection, custom UI, multi-tenancy** | Explicit PRD non-goals (§1.4, §8) | — |
