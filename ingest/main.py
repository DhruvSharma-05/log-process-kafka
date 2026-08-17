#!/usr/bin/env python3
"""HTTP log-ingestion gateway (FR1.1).

    python -m ingest.main                 # listens on :8100

    curl -X POST localhost:8100/v1/logs -H 'Content-Type: application/json' \
         -d '{"service":"checkout-api","level":"ERROR","message":"payment timeout","status_code":500}'

Three intake shapes, one envelope out:

    POST /v1/logs        one JSON application log
    POST /v1/logs/bulk   newline-delimited JSON, or a JSON array
    POST /v1/logs/raw    plain text, one access-log line per line

Every route produces the same `raw-logs` envelope as `producer/replay.py`, so
the processor, ClickHouse, dashboards and alerts need no changes at all.

Rejection policy: malformed input is refused at the boundary with a 4xx and a
reason, not dead-lettered. The DLQ is for events that got *into* the pipeline
and failed later; a client sending bad JSON should be told so it can fix it.
"""
from __future__ import annotations

import json
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import uvicorn
from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.staticfiles import StaticFiles
import os
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# Imported from the parser so the gateway and the processor can never disagree
# about which field names count as a timestamp.
from processor.parsers.json_app import TIMESTAMP_KEYS
from producer.config import ProducerConfig
from producer.replay import build_event

MAX_BODY_BYTES = 10 * 1024 * 1024   # 10 MB
MAX_BULK_LINES = 10_000
# librdkafka queues locally; past this depth the broker is the bottleneck and
# we shed load with 429 rather than buffering unboundedly and lying to clients.
QUEUE_HIGH_WATER = 100_000

ACCEPTED = Counter("logpipe_ingest_accepted_total", "Events accepted", ["route", "source_format"])
REJECTED = Counter("logpipe_ingest_rejected_total", "Events rejected", ["route", "reason"])
DELIVERED = Counter("logpipe_ingest_delivered_total", "Events acked by Kafka")
DELIVERY_FAILED = Counter("logpipe_ingest_delivery_failed_total", "Kafka delivery failures")
SHED = Counter("logpipe_ingest_shed_total", "Requests rejected with 429 for backpressure")
TIMESTAMP_DEFAULTED = Counter(
    "logpipe_ingest_timestamp_defaulted_total",
    "Logs that arrived with no timestamp and were stamped with receipt time",
)
QUEUE_DEPTH = Gauge("logpipe_ingest_queue_depth", "Messages buffered in librdkafka")
REQUEST_SECONDS = Histogram(
    "logpipe_ingest_request_seconds",
    "Request handling time",
    ["route"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
)

state: dict[str, Any] = {"producer": None, "config": None, "kafka_ok": False}


def _on_delivery(err, _msg) -> None:
    if err is None:
        DELIVERED.inc()
    else:
        DELIVERY_FAILED.inc()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = ProducerConfig()
    producer = Producer(config.kafka_conf())
    state["producer"] = producer
    state["config"] = config
    try:
        producer.list_topics(timeout=10)
        state["kafka_ok"] = True
    except KafkaException as exc:
        print(f"WARNING: Kafka not reachable at startup: {exc}", file=sys.stderr)
        state["kafka_ok"] = False

    print(f"ingest gateway -> {config.bootstrap} topic={config.topic}")
    print("  POST /v1/logs | /v1/logs/bulk | /v1/logs/raw")
    yield

    remaining = producer.flush(timeout=30)
    if remaining:
        print(f"WARNING: {remaining} message(s) unflushed at shutdown", file=sys.stderr)


app = FastAPI(
    title="logpipe ingest gateway",
    description="HTTP entry point into the real-time log processing pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)


def _check_backpressure(route: str) -> None:
    producer: Producer = state["producer"]
    depth = len(producer)
    QUEUE_DEPTH.set(depth)
    if depth >= QUEUE_HIGH_WATER:
        SHED.inc()
        REJECTED.labels(route=route, reason="backpressure").inc()
        raise HTTPException(
            status_code=429,
            detail="ingest queue is full; the pipeline is behind",
            headers={"Retry-After": "1"},
        )


def _publish(event: dict[str, object], route: str, source_format: str) -> None:
    producer: Producer = state["producer"]
    config: ProducerConfig = state["config"]
    try:
        producer.produce(
            config.topic,
            key=str(event["service"]).encode(),
            value=json.dumps(event, separators=(",", ":")).encode(),
            on_delivery=_on_delivery,
        )
    except BufferError:
        SHED.inc()
        REJECTED.labels(route=route, reason="backpressure").inc()
        raise HTTPException(status_code=429, detail="ingest queue is full", headers={"Retry-After": "1"})
    producer.poll(0)
    ACCEPTED.labels(route=route, source_format=source_format).inc()


UTF8_BOM = b"\xef\xbb\xbf"


async def _read_body(request: Request, route: str) -> bytes:
    body = await request.body()
    if not body:
        REJECTED.labels(route=route, reason="empty_body").inc()
        raise HTTPException(status_code=400, detail="empty request body")
    if len(body) > MAX_BODY_BYTES:
        REJECTED.labels(route=route, reason="too_large").inc()
        raise HTTPException(status_code=413, detail=f"body exceeds {MAX_BODY_BYTES} bytes")
    # PowerShell pipelines, Notepad and several Windows tools prepend a UTF-8
    # BOM. json.loads rejects it outright, which would make the gateway look
    # broken to anyone piping data in on Windows. Strip it rather than blaming
    # the caller for their shell's encoding.
    if body.startswith(UTF8_BOM):
        body = body[len(UTF8_BOM):]
    return body


def _envelope_from_json(payload: dict, route: str, service_hint: str | None, host_hint: str | None) -> dict:
    """Wrap a JSON application log in the standard raw-logs envelope.

    A log with no timestamp is stamped with receipt time rather than being
    dead-lettered. Requiring every client to send a timestamp — and silently
    dropping those that don't — makes the API hostile to exactly the simple
    `curl` case it exists to serve. Receipt time is a defensible approximation
    for a log arriving over HTTP, and the substitution is counted in
    `logpipe_ingest_timestamp_defaulted_total` so it is never invisible.
    """
    if not any(payload.get(key) for key in TIMESTAMP_KEYS):
        payload = dict(payload)
        payload["timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        TIMESTAMP_DEFAULTED.inc()

    line = json.dumps(payload, separators=(",", ":"))
    event = build_event(
        line,
        source_format="json_app",
        rewrite=False,          # client timestamps are real; never rewrite them
        service_override=service_hint,
        host_override=host_hint,
    )
    if event is None:
        REJECTED.labels(route=route, reason="empty_event").inc()
        raise HTTPException(status_code=400, detail="log produced an empty event")
    return event


active_connections: list[WebSocket] = []


async def broadcast_event(event: dict):
    if not active_connections:
        return
    disconnected = []
    message = json.dumps(event, separators=(",", ":"))
    for connection in active_connections:
        try:
            await connection.send_text(message)
        except Exception:
            disconnected.append(connection)
    for connection in disconnected:
        if connection in active_connections:
            active_connections.remove(connection)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_connections:
            active_connections.remove(websocket)


# --- Routes ------------------------------------------------------------------


@app.post("/v1/logs", status_code=202)
async def ingest_one(
    request: Request,
    background_tasks: BackgroundTasks,
    service: str | None = Query(None, description="Overrides the service named in the body"),
    host: str | None = Query(None, description="Overrides the host named in the body"),
):
    """Ingest a single JSON application log."""
    with REQUEST_SECONDS.labels(route="single").time():
        _check_backpressure("single")
        body = await _read_body(request, "single")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            REJECTED.labels(route="single", reason="invalid_json").inc()
            raise HTTPException(status_code=400, detail=f"invalid JSON: {exc.msg}")
        if not isinstance(payload, dict):
            REJECTED.labels(route="single", reason="not_an_object").inc()
            raise HTTPException(status_code=400, detail="body must be a JSON object")

        event = _envelope_from_json(payload, "single", service, host)
        _publish(event, "single", "json_app")
        background_tasks.add_task(broadcast_event, event)
        return {"accepted": 1, "event_id": event["event_id"], "service": event["service"]}


@app.post("/v1/logs/bulk", status_code=202)
async def ingest_bulk(
    request: Request,
    background_tasks: BackgroundTasks,
    service: str | None = Query(None),
    host: str | None = Query(None),
):
    """Ingest newline-delimited JSON, or a JSON array of log objects.

    Partial success is real: valid lines are published even when others fail,
    and the response names the failures by line number. Rejecting an entire
    batch because line 4,000 is malformed would lose 9,999 good events.
    """
    with REQUEST_SECONDS.labels(route="bulk").time():
        _check_backpressure("bulk")
        body = await _read_body(request, "bulk")
        text = body.decode("utf-8", "replace").strip()

        if text.startswith("["):
            try:
                items = json.loads(text)
            except json.JSONDecodeError as exc:
                REJECTED.labels(route="bulk", reason="invalid_json").inc()
                raise HTTPException(status_code=400, detail=f"invalid JSON array: {exc.msg}")
            if not isinstance(items, list):
                raise HTTPException(status_code=400, detail="expected a JSON array")
            candidates = list(enumerate(items, start=1))
        else:
            lines = [line for line in text.splitlines() if line.strip()]
            if len(lines) > MAX_BULK_LINES:
                REJECTED.labels(route="bulk", reason="too_many_lines").inc()
                raise HTTPException(status_code=413, detail=f"more than {MAX_BULK_LINES} lines")
            candidates = []
            for number, line in enumerate(lines, start=1):
                try:
                    candidates.append((number, json.loads(line)))
                except json.JSONDecodeError as exc:
                    candidates.append((number, exc))

        accepted, errors = 0, []
        for number, item in candidates:
            if isinstance(item, Exception):
                REJECTED.labels(route="bulk", reason="invalid_json").inc()
                errors.append({"line": number, "error": f"invalid JSON: {item.msg}"})
                continue
            if not isinstance(item, dict):
                REJECTED.labels(route="bulk", reason="not_an_object").inc()
                errors.append({"line": number, "error": "not a JSON object"})
                continue
            try:
                event = _envelope_from_json(item, "bulk", service, host)
            except HTTPException as exc:
                errors.append({"line": number, "error": exc.detail})
                continue
            _publish(event, "bulk", "json_app")
            background_tasks.add_task(broadcast_event, event)
            accepted += 1

        return {"accepted": accepted, "rejected": len(errors), "errors": errors[:20]}


@app.post("/v1/logs/raw", status_code=202)
async def ingest_raw(
    request: Request,
    background_tasks: BackgroundTasks,
    service: str | None = Query(None, description="Service name; derived from the path when absent"),
    host: str | None = Query(None),
    source_format: str = Query("nginx_combined", pattern="^(nginx_combined|json_app)$"),
):
    """Ingest plain-text log lines, one per line.

    Unparseable lines are NOT rejected here — the gateway does not parse. They
    are forwarded and the processor dead-letters them with a reason, which is
    what the DLQ is for.
    """
    with REQUEST_SECONDS.labels(route="raw").time():
        _check_backpressure("raw")
        body = await _read_body(request, "raw")
        lines = [line for line in body.decode("utf-8", "replace").splitlines() if line.strip()]
        if len(lines) > MAX_BULK_LINES:
            REJECTED.labels(route="raw", reason="too_many_lines").inc()
            raise HTTPException(status_code=413, detail=f"more than {MAX_BULK_LINES} lines")

        accepted = 0
        for line in lines:
            event = build_event(
                line,
                source_format=source_format,
                rewrite=False,
                service_override=service,
                host_override=host,
            )
            if event is None:
                continue
            _publish(event, "raw", source_format)
            background_tasks.add_task(broadcast_event, event)
            accepted += 1

        return {"accepted": accepted}


@app.get("/healthz")
async def healthz():
    return {"status": "alive"}


@app.get("/readyz")
async def readyz(response: Response):
    producer: Producer = state["producer"]
    try:
        producer.list_topics(timeout=5)
        state["kafka_ok"] = True
    except KafkaException:
        state["kafka_ok"] = False

    depth = len(producer)
    QUEUE_DEPTH.set(depth)
    ready = state["kafka_ok"] and depth < QUEUE_HIGH_WATER
    if not ready:
        response.status_code = 503
    return {
        "ready": ready,
        "kafka_reachable": state["kafka_ok"],
        "queue_depth": depth,
        "queue_high_water": QUEUE_HIGH_WATER,
    }


@app.get("/metrics")
async def metrics():
    producer: Producer = state["producer"]
    if producer is not None:
        QUEUE_DEPTH.set(len(producer))
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


def main() -> int:
    uvicorn.run(app, host="0.0.0.0", port=8100, log_level="warning", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
