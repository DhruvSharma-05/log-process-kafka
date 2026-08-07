# Runbook

What to do when the pipeline misbehaves. Each entry: how you notice, how to confirm, what to do.

**First stop for anything:** Pipeline Health dashboard — <http://localhost:3000> → Logpipe →
Pipeline Health. It answers "is the pipeline keeping up?" before you start guessing.

---

## Consumer lag is growing

**Alert:** `Kafka consumer lag is growing` (>50k sustained for 3 min)

Lag rising is normal during a burst; the pipeline drained a 2x spike in 3 seconds during testing.
Sustained growth means consumption is slower than production.

### Confirm

```bash
make lag        # per-group totals
make targets    # is the processor even being scraped?
```

`make lag` prints `n/a` — not `0` — when a group has no assignment or Kafka is unreachable. Those are
very different situations from "zero lag".

### Diagnose

Read throughput on the Pipeline Health dashboard alongside lag:

| Throughput | Lag | Meaning | Action |
| --- | --- | --- | --- |
| Zero | Rising | Processor is down or stuck | See *Processor is not consuming* below |
| Normal | Rising | Genuinely more load than capacity | Scale out, below |
| Erratic | Rising | Host contention or downstream backpressure | Check ClickHouse and host CPU |

### Fix — scale out

Start more instances. Each needs its own metrics port:

```bash
python -m processor.main --http-port 8010
python -m processor.main --http-port 8020
```

Kafka rebalances automatically; no code or config change. Add the new ports to
`prometheus/prometheus.yml` so they are actually observed.

**Ceiling: 6 instances.** `raw-logs` has 6 partitions and a partition is consumed by exactly one
member of a group. Beyond 6, extra instances idle. To go further, add partitions first:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic raw-logs --partitions 12
```

Partitions can only be increased, never decreased. Increasing them also breaks ordering guarantees
for keys that move to new partitions.

**Expect uneven relief.** Load does not split evenly across instances — during testing three
instances did 298/s, 2,305/s and 1,799/s, because the `service` partition key is skewed. See
[DECISIONS.md](DECISIONS.md#partition-key).

---

## Processor is not consuming

**Symptom:** lag rising, throughput flat at zero.

### Confirm

```bash
curl http://localhost:8000/readyz
```

| Response | Meaning |
| --- | --- |
| `200 {"ready":true,...}` | Consuming normally |
| `503 {"ready":false,...}` | Alive but has not polled within 60s — stuck or rebalancing |
| Connection refused | Process is dead |
| `200` but `assigned_partitions: 0` | In the group, holding no partitions |

`assigned_partitions: 0` on one instance while others hold partitions usually means more instances
than partitions. That is expected, not a fault.

### Windows gotcha

Two instances can bind the same metrics port **without error** — Python's `HTTPServer` sets
`SO_REUSEADDR`, which on Windows permits rebinding a live port. The second silently shadows the
first, so `/metrics` and `/readyz` report only one of them while both consume.

If instance counts look wrong, trust Kafka, not the health endpoint:

```bash
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group log-processor
```

Distinct values in the CONSUMER-ID column are the real instance count.

### Fix

Restart it. Offsets are committed only after downstream produce succeeds, so an uncommitted batch is
reprocessed, not lost. `event_id` is a content hash, so ClickHouse dedupes the replay.

---

## The dead-letter queue is filling

**Alert:** `Dead-letter rate elevated` (>5/s for 2 min)

A steady trickle is healthy — real logs contain malformed lines. A step change means something
upstream changed.

### Diagnose

The DLQ-by-stage panel tells you what kind of problem it is:

| `error_stage` | Meaning | Usual cause |
| --- | --- | --- |
| `decode` | Envelope is not valid JSON or is missing required fields | Producer bug or a foreign writer on `raw-logs` |
| `parse` | Line does not match its declared `source_format` | A service changed its log format |
| `enrich` | Enrichment threw | Corrupt geo database, or a genuine bug |
| `validate` | Parsed fine, failed JSON Schema | **Schema drift** — a field changed type or appeared |

Read the actual failures:

```bash
make dlq
```

Every entry carries `error_reason`, `error_stage`, and the original payload verbatim.

### Fix

1. Identify the format change from the preserved originals.
2. Update the parser in `processor/parsers/` and add a golden-line test for it.
3. Deploy, then replay the DLQ — the originals are intact, so they can be re-produced to `raw-logs`.

**Do not "fix" this by loosening the schema.** `additionalProperties: false` is the drift tripwire; a
DLQ entry is it working, not failing.

---

## Ingestion latency is climbing

**Symptom:** the ingest-lag panel p95 approaching the 10 s SLA line.

Measured baseline is 1.27 s p95, dominated by ClickHouse's 1 s `kafka_flush_interval_ms`.

### Diagnose

```bash
make latency    # ClickHouse's own view: ingested_at -> stored_at
make parts      # is ClickHouse merging or moving parts?
```

Check ClickHouse's Kafka consumer for errors:

```sql
SELECT table, num_messages_read, arrayStringConcat(exceptions.text, ' | ') AS errors
FROM system.kafka_consumers WHERE database = 'logpipe';
```

### Common causes

- **Too many small parts.** Each ClickHouse insert creates a part; merges lag behind. Raise
  `kafka_flush_interval_ms` in `clickhouse/02_kafka_engine.sql` to batch harder — trading latency
  for insert efficiency.
- **Processor lag** feeding through. Fix lag first; latency follows.
- **Disk pressure.** See below.

---

## ClickHouse disk is filling

### Confirm

```bash
make parts   # rows and bytes per partition, hot vs cold
```

```sql
SELECT name, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total
FROM system.disks;
```

### Understand the tiering first

Data is not deleted at 7 days — it **moves** to MinIO and stays queryable. The move is done by a
background pool (8 threads), not a cron job, and is attached to every part as a `move_ttl`.

If cold data is not moving, check that MinIO is reachable; the S3 disk is defined in
`clickhouse/config.d/storage.xml`.

### Fix

Shorten the hot window in `clickhouse/03_cold_archive.sql` and re-apply:

```sql
ALTER TABLE logpipe.logs_hot
  MODIFY TTL toDateTime(timestamp) + INTERVAL 3 DAY TO VOLUME 'cold';
```

Then force existing parts to re-evaluate:

```sql
ALTER TABLE logpipe.logs_hot MATERIALIZE TTL;
```

To reclaim space immediately, drop whole partitions — far cheaper than row-level deletes:

```sql
ALTER TABLE logpipe.logs_hot DROP PARTITION 20260701;
```

`OPTIMIZE TABLE ... PARTITION <expr>` requires a **literal** partition value; an expression like
`toYYYYMMDD(now() - INTERVAL 30 DAY)` is a syntax error.

---

## A broker is down

### Dev (single broker)

Everything stops. Producers buffer and retry; the processor blocks. Restart:

```bash
docker compose restart kafka
```

No data is lost from committed writes — Kafka's log is on a named volume. Verified: topics and
offsets survive a restart.

### Production profile (3 brokers, RF=3)

One broker down is survivable by design and was tested by SIGKILLing a broker mid-write:

- ISR shrinks 3 → 2 on every partition; leadership migrates off the dead broker
- `min.insync.replicas=2` keeps writes legal, so `acks=all` still succeeds
- Acks stall for roughly **9 seconds** during leader election, then catch up
- Result: **400,000 acked, 400,000 readable, zero loss, zero delivery failures**

The stall is expected. Producers are configured with effectively unlimited retries and
`enable.idempotence=true`, so retries do not duplicate.

```bash
docker compose -p logpipe-prod -f docker-compose.prod.yml start kafka-2
```

ISR returns to 3 within about 30 seconds.

**Two brokers down is not survivable** with `min.insync.replicas=2`: writes are refused rather than
accepted unsafely. That is deliberate — `unclean.leader.election.enable=false` means we prefer
unavailability over silent data loss.

---

## Alerts are not reaching Slack

The rule can be firing correctly while delivery fails — check them separately.

### Is the rule firing?

```bash
make alerts
```

Or Grafana → Alerting → Alert rules.

### Is delivery working?

```bash
docker compose logs grafana --since 10m | grep -i slack
```

`failed incoming webhook: no_team` means the placeholder URL is still in place. Put a real one in
`.env`:

```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/REAL/URL
```

Then `docker compose up -d grafana`.

---

## Nothing appears on the dashboards

Work down the pipeline until you find the first empty stage:

```bash
make counts    # 1. Are events reaching Kafka at all?
make lag       # 2. Is the processor consuming?
make dlq       # 3. Is everything being rejected?
make latency   # 4. Is data reaching ClickHouse?
```

Then check Grafana's datasource: Connections → Data sources → ClickHouse → **Test**.

### If the ClickHouse datasource fails

The most likely cause is a listener problem, not a query problem. The official image writes
`docker_related_config.xml` into `/etc/clickhouse-server/config.d/` to bind `0.0.0.0`; bind-mounting
that directory read-only masks it, and ClickHouse falls back to `127.0.0.1` — reachable via
`docker exec`, invisible to everything else.

Symptom is misleading: `ERR_EMPTY_RESPONSE` on **every** published ClickHouse port while `netstat`
shows Docker forwarders listening. `clickhouse/config.d/listen.xml` exists to prevent this; confirm
it is still mounted.

```bash
curl http://localhost:8123/ping    # expect: Ok.
```

### If the time range is the problem

Replayed data has rewritten timestamps, so it lands at "now". If you disabled rewriting
(`--no-rewrite-timestamps`), the NASA data sits in **1995** and no default dashboard range will show
it.

---

## Reset everything

```bash
make reset     # stop and delete all Kafka data
make up
make topics
make ch-init
make ksql-init
```

`make reset` destroys volumes: Kafka logs, ClickHouse tables, Grafana state. Datasets in `data/` are
untouched.
