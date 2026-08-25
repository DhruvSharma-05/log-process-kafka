# Demo guide

How to show this to someone in 10 minutes and have them understand what it solves.

The tooling already exists — `make demo` runs a scripted version. This document is the *narrative*:
what to say, what to click, and what people will ask.

---

## The problem, in 40 seconds

Say this before touching anything:

> "A company runs 20 services across 50 machines. Each one writes its own log file. When checkout
> starts failing at 2am, the on-call engineer has to guess which service broke, SSH into the right
> box, and grep. There is no way to ask *which service is failing right now*, no way to see a problem
> building before customers notice, and logs are either deleted quickly or stored expensively
> forever.
>
> This pipeline turns scattered log files into one queryable, alertable stream — and it has to keep
> working when a service starts emitting garbage, when traffic doubles, and when a machine dies."

That last sentence matters. Anyone can move logs from A to B. The engineering is in the failure
modes, and that is where the demo should spend its time.

---

## Before they arrive (5 minutes)

```bash
make up                                           # 9 containers
make topics && make ch-init && make ksql-init     # first run only
```

Four terminals, or four background processes:

```bash
make process     # the stream processor
make ingest      # HTTP gateway + live UI
make wiki        # live Wikimedia firehose
make baseline    # synthetic 7-service traffic
```

Let it run **at least 10 minutes** before demoing so the dashboards have history. Verify:

```bash
make counts      # topics filling
make latency     # p95 well under 10s
```

Open these tabs, so you can move left to right:

1. <http://localhost:8100> — live tail UI
2. <http://localhost:3000> — Grafana (admin/admin) → Logpipe → **Service Health**
3. <http://localhost:3000> → Logpipe → **Pipeline Health**
4. <http://localhost:8080> — Kafka UI

**Critical:** on Service Health, set the **Service** filter to just the `*-api` services
(`checkout-api`, `catalog-api`, `auth-api`, `billing-api`, `inventory-api`). The Wikimedia feed adds
80+ wiki domains as separate services, and with "All" selected the charts are an unreadable hairball.

---

## The demo

### Act 1 — logs arrive from anywhere (90 seconds)

**Tab: live tail UI (:8100)**

Let them watch real events scroll. Then send one yourself:

```bash
make send
```

> "That line appeared the moment I sent it. Those other events are real Wikipedia edits happening
> right now, anywhere in the world — a live external feed, not a recording."

Then show it takes anything. PowerShell:

```powershell
$body = '{"service":"payments","level":"ERROR","message":"card declined","status_code":502}'
Invoke-RestMethod -Uri http://localhost:8100/v1/logs -Method Post -Body $body -ContentType 'application/json'
```

> "Any language, any tool. It also takes raw nginx access-log lines — you can pipe `tail -f` straight
> at it."

**The point:** one entry point, already live.

### Act 2 — it is structured and searchable (90 seconds)

**Tab: ClickHouse (<http://localhost:8123/play>)**

```sql
SELECT timestamp, service, status_code, path, response_time_ms, geo_country
FROM logpipe.logs_hot
WHERE service = 'checkout-api' AND is_error
ORDER BY timestamp DESC LIMIT 20;
```

> "That log line arrived as unstructured text. It is now typed columns I can filter on — service,
> status, latency, country. This is the query you cannot run when logs are files on 50 machines."

Then the latency claim:

```bash
make latency
```

> "About one second at p95 from the application emitting it to being queryable. The target was ten."

**The point:** unstructured text became a queryable table, fast.

### Act 3 — here is the incident (3 minutes, the centrepiece)

**Tab: Service Health**

Point out the baseline: all services flat, error rate under 1%. Then, in a terminal:

```bash
make spike
```

> "I have just made checkout-api start failing — 35% of its requests now return 5xx. Nothing else
> changed."

Watch the **Error rate % per service** panel. Within a minute `checkout-api` climbs across the red 5%
threshold line while every other service stays flat. Then:

```bash
make alerts
```

```text
Service error rate above 5%    PENDING  ->  FIRING
  checkout-api   Alerting
  catalog-api    Normal
  auth-api       Normal
```

> "The alert names the service. It did not just say 'errors are up' — it isolated the culprit while
> six other services carried on normally. That is the difference between an alert that helps at 2am
> and one that sends you looking."

Also point at **p95 response time**: checkout-api's latency rises with its error rate, because
failures are slower than successes.

**The point:** detection is automatic, specific, and about a minute behind reality.

### Act 4 — it does not fall over (3 minutes)

This is what separates it from a toy. Pick one or two.

**Bad data cannot kill the pipeline:**

```bash
make corrupt     # 5% unparseable lines
make dlq
```

> "Every one of those carries the stage that rejected it, the reason, and the original line kept
> verbatim so it can be replayed after a parser fix. The processor never restarted. One bad log line
> from one bad deploy cannot take down ingestion for everyone else."

**Traffic doubles:**

```bash
make loadtest
```

> "Baseline, then 2x for a minute. Peak backlog 1,531 messages, drained in 3 seconds. The requirement
> was under 60."

**A machine dies** (needs `make prod-up` first — 3 brokers, RF=3):

```bash
make prod-kill
# in another shell, mid-run:
docker compose -p logpipe-prod -f docker-compose.prod.yml kill kafka-2
```

> "I killed a broker while 400,000 records were being written. Acks paused for nine seconds during
> leader election, then caught up. 400,000 acked, 400,000 readable, zero loss."

**Tab: Pipeline Health** — show consumer lag climbing and draining.

> "The pipeline monitors itself. If it falls behind, that shows up here before anyone notices missing
> logs."

### Act 5 — close (30 seconds)

> "Logs from anywhere, structured and queryable in about a second, per-service dashboards, alerting
> that names the culprit, and it survives bad data, traffic spikes and a dead machine. Every number I
> showed is measured, not estimated — they are in IMPLEMENTATION.md along with how they were
> measured."

---

## Questions you will get

**"Why not just use Elasticsearch / Splunk / Datadog?"**
You would, in production. This demonstrates the patterns those products implement internally:
partitioning, at-least-once delivery, dead-lettering, tiered storage, backpressure. Also, ClickHouse
was chosen over Elasticsearch deliberately for analytical query patterns — reasoning in
[DECISIONS.md](DECISIONS.md).

**"What happens if the processor crashes mid-batch?"**
Offsets commit only after downstream writes are durable, so that batch is reprocessed. `event_id` is
a content hash, so ClickHouse's `ReplacingMergeTree` collapses the duplicate. Measured: 35 natural
duplicates collapsed to 0 after merge.

**"How fast is it really?"**
41,369 events/sec producer-side on one laptop; 133,000/sec against the 3-broker cluster. The PRD
asked for 5,000.

**"Is any of this data fake?"**
Yes, and it is labelled. Wikimedia is genuinely live. The NASA dataset is real 1995 traffic, but
`service`, `host` and request time are synthesised because access logs do not carry them. All
`scenarios.py` traffic is synthetic by design. See the README's "Honest scope" section.

**"Why a custom UI when you also have Grafana?"**
Grafana shows aggregates on a refresh interval; the UI shows individual events as they land, which is
what makes the pipeline legible the first time you see it. It is explicitly beyond the PRD's scope
and documented as such.

**"What would you do differently, or what is not finished?"**
Have an answer ready — this is the question that separates people who built something from people who
followed a tutorial. The real ones: no TLS on the production profile; no ACL authorization (it
deadlocks KRaft bootstrap without care); the `service` partition key causes an 8x load imbalance
across consumers; no external alert delivery. All four are documented with reasoning in
[DECISIONS.md](DECISIONS.md).

---

## If something goes wrong mid-demo

**Dashboards are empty.** Almost always the time range. Events are stamped "now" only while a
producer is running; if you stopped it an hour ago, widen the range or restart `make baseline`.

**Charts are an unreadable tangle.** The Service filter is on "All" and the Wikimedia feed is adding
80+ services. Filter to the `*-api` ones.

**The alert will not fire.** It needs >5% errors sustained for a full minute, and it ignores services
with fewer than 20 requests in the window. `make spike` satisfies both; a short manual burst may not.

**Nothing is arriving.** Work down the pipeline until you find the first empty stage:
`make counts` → `make lag` → `make dlq` → `make latency`. Full diagnostic tree in
[RUNBOOK.md](RUNBOOK.md).

**A container is unhealthy.** `make ps`, then `docker compose restart <service>`. Kafka data is on a
named volume and survives.
