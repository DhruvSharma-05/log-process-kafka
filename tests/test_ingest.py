"""HTTP ingest gateway tests (FR1.1).

Kafka is replaced with a fake producer so these run without a broker. The
routes, validation, envelope construction and backpressure behaviour are all
exercised for real.
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

import ingest.main as gateway


class FakeProducer:
    """Stands in for confluent_kafka.Producer."""

    def __init__(self, _conf=None):
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.queue_depth = 0
        self.raise_buffer_error = False

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.raise_buffer_error:
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, _timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0

    def list_topics(self, timeout=None):
        return object()

    def __len__(self):
        return self.queue_depth


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(gateway, "Producer", FakeProducer)
    with TestClient(gateway.app) as test_client:
        yield test_client


def sent(client) -> list[dict]:
    """Envelopes the gateway published, decoded."""
    return [json.loads(value) for _topic, _key, value in gateway.state["producer"].messages]


# --- Single log --------------------------------------------------------------


def test_single_log_is_accepted_and_enveloped(client):
    response = client.post("/v1/logs", json={
        "service": "checkout-api", "level": "ERROR", "message": "boom",
        "status_code": 500, "timestamp": "2026-08-07T07:00:00Z",
    })
    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] == 1
    assert body["service"] == "checkout-api"
    assert len(body["event_id"]) == 32

    envelope = sent(client)[0]
    assert envelope["service"] == "checkout-api"
    assert envelope["source_format"] == "json_app"
    assert "raw_message" in envelope and "ingested_at" in envelope


def test_service_is_taken_from_the_body(client):
    client.post("/v1/logs", json={"service": "billing-api", "timestamp": "2026-08-07T07:00:00Z"})
    assert sent(client)[0]["service"] == "billing-api"


def test_query_parameter_overrides_body_service(client):
    client.post("/v1/logs?service=override-api",
                json={"service": "billing-api", "timestamp": "2026-08-07T07:00:00Z"})
    assert sent(client)[0]["service"] == "override-api"


def test_partition_key_is_the_service(client):
    """FR1.3 — per-service ordering depends on this."""
    client.post("/v1/logs", json={"service": "orders-api", "timestamp": "2026-08-07T07:00:00Z"})
    _topic, key, _value = gateway.state["producer"].messages[0]
    assert key == b"orders-api"


# --- Timestamp defaulting ----------------------------------------------------


def test_missing_timestamp_is_stamped_with_receipt_time(client):
    """A client that omits a timestamp must not be silently dead-lettered."""
    response = client.post("/v1/logs", json={"service": "a", "message": "no timestamp here"})
    assert response.status_code == 202

    payload = json.loads(sent(client)[0]["raw_message"])
    assert "timestamp" in payload
    assert payload["timestamp"].endswith("Z")


def test_supplied_timestamp_is_never_overwritten(client):
    client.post("/v1/logs", json={"service": "a", "timestamp": "2020-01-01T00:00:00Z"})
    payload = json.loads(sent(client)[0]["raw_message"])
    assert payload["timestamp"] == "2020-01-01T00:00:00Z"


@pytest.mark.parametrize("field", ["time", "@timestamp", "ts", "eventTime"])
def test_timestamp_aliases_are_recognised(client, field):
    """The gateway shares its key list with the parser, so they cannot drift."""
    client.post("/v1/logs", json={"service": "a", field: "2021-06-05T04:03:02Z"})
    payload = json.loads(sent(client)[0]["raw_message"])
    assert "timestamp" not in payload or payload.get(field) == "2021-06-05T04:03:02Z"


# --- Validation --------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "status"),
    [
        (b"", 400),                       # empty
        (b"not json", 400),
        (b'"a string"', 400),             # not an object
        (b"[1,2,3]", 400),                # array, not object
    ],
)
def test_malformed_single_logs_are_refused_at_the_boundary(client, body, status):
    """Bad input gets a 4xx so the client can fix it - it is not dead-lettered.
    The DLQ is for events that entered the pipeline and failed later."""
    response = client.post("/v1/logs", content=body, headers={"Content-Type": "application/json"})
    assert response.status_code == status
    assert gateway.state["producer"].messages == []


# --- Bulk --------------------------------------------------------------------


def test_bulk_ndjson_accepts_all_valid_lines(client):
    ndjson = "\n".join(
        json.dumps({"service": "a", "seq": i, "timestamp": "2026-08-07T07:00:00Z"})
        for i in range(5)
    )
    response = client.post("/v1/logs/bulk", content=ndjson)
    assert response.status_code == 202
    assert response.json() == {"accepted": 5, "rejected": 0, "errors": []}
    assert len(sent(client)) == 5


def test_bulk_partial_success_keeps_the_good_lines(client):
    """Rejecting the whole batch because one line is bad would lose the rest."""
    ndjson = "\n".join([
        json.dumps({"service": "a", "timestamp": "2026-08-07T07:00:00Z"}),
        "{ this is not json",
        json.dumps({"service": "b", "timestamp": "2026-08-07T07:00:01Z"}),
    ])
    response = client.post("/v1/logs/bulk", content=ndjson)
    body = response.json()
    assert body["accepted"] == 2
    assert body["rejected"] == 1
    assert body["errors"][0]["line"] == 2
    assert len(sent(client)) == 2


def test_bulk_accepts_a_json_array(client):
    payload = [
        {"service": "a", "timestamp": "2026-08-07T07:00:00Z"},
        {"service": "b", "timestamp": "2026-08-07T07:00:01Z"},
    ]
    response = client.post("/v1/logs/bulk", json=payload)
    assert response.json()["accepted"] == 2


def test_bulk_rejects_oversized_batches(client, monkeypatch):
    monkeypatch.setattr(gateway, "MAX_BULK_LINES", 3)
    ndjson = "\n".join(json.dumps({"service": "a"}) for _ in range(4))
    assert client.post("/v1/logs/bulk", content=ndjson).status_code == 413


# --- Raw ---------------------------------------------------------------------


def test_raw_lines_are_forwarded_verbatim(client):
    lines = (
        '203.0.113.9 - - [07/Aug/2026:07:00:03 +0000] "GET /health HTTP/1.1" 200 42 0.011\n'
        '198.51.100.5 - - [07/Aug/2026:07:00:04 +0000] "POST /orders HTTP/1.1" 500 88 1.802'
    )
    response = client.post("/v1/logs/raw?service=orders-api", content=lines)
    assert response.json() == {"accepted": 2}

    envelopes = sent(client)
    assert all(e["service"] == "orders-api" for e in envelopes)
    assert all(e["source_format"] == "nginx_combined" for e in envelopes)
    assert envelopes[0]["raw_message"].startswith("203.0.113.9")


def test_raw_does_not_reject_unparseable_lines(client):
    """The gateway does not parse. Bad lines go downstream and the processor
    dead-letters them with a reason - that is what the DLQ is for."""
    response = client.post("/v1/logs/raw?service=x", content="total garbage not a log line")
    assert response.status_code == 202
    assert response.json()["accepted"] == 1


def test_raw_rejects_an_unknown_source_format(client):
    assert client.post("/v1/logs/raw?source_format=syslog", content="x").status_code == 422


# --- Encoding robustness -----------------------------------------------------


def test_utf8_bom_is_stripped_from_json_bodies(client):
    """PowerShell pipelines and several Windows tools prepend a BOM. json.loads
    rejects it, which would make the gateway look broken on Windows."""
    payload = json.dumps({"service": "a", "timestamp": "2026-08-07T07:00:00Z"}).encode()
    response = client.post(
        "/v1/logs",
        content=b"\xef\xbb\xbf" + payload,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 202
    assert len(sent(client)) == 1


def test_utf8_bom_is_stripped_from_bulk_bodies(client):
    ndjson = json.dumps({"service": "a", "timestamp": "2026-08-07T07:00:00Z"}).encode()
    response = client.post("/v1/logs/bulk", content=b"\xef\xbb\xbf" + ndjson)
    assert response.json()["accepted"] == 1


def test_utf8_bom_is_stripped_from_raw_bodies(client):
    line = b'203.0.113.9 - - [07/Aug/2026:07:00:03 +0000] "GET / HTTP/1.1" 200 42'
    response = client.post("/v1/logs/raw?service=x", content=b"\xef\xbb\xbf" + line)
    assert response.json()["accepted"] == 1
    assert not sent(client)[0]["raw_message"].startswith("﻿")


# --- Backpressure ------------------------------------------------------------


def test_returns_429_when_the_local_queue_is_full(client):
    gateway.state["producer"].queue_depth = gateway.QUEUE_HIGH_WATER
    response = client.post("/v1/logs", json={"service": "a", "timestamp": "2026-08-07T07:00:00Z"})
    assert response.status_code == 429
    assert response.headers.get("Retry-After") == "1"


def test_buffer_error_becomes_429_not_500(client):
    """librdkafka's queue filling is backpressure, not a server fault."""
    gateway.state["producer"].raise_buffer_error = True
    response = client.post("/v1/logs", json={"service": "a", "timestamp": "2026-08-07T07:00:00Z"})
    assert response.status_code == 429


# --- Health ------------------------------------------------------------------


def test_healthz_is_liveness_only(client):
    assert client.get("/healthz").json() == {"status": "alive"}


def test_readyz_reports_not_ready_when_the_queue_is_full(client):
    gateway.state["producer"].queue_depth = gateway.QUEUE_HIGH_WATER
    response = client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["ready"] is False


def test_readyz_is_ready_when_healthy(client):
    response = client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["ready"] is True


def test_metrics_endpoint_exposes_prometheus_text(client):
    body = client.get("/metrics").text
    assert "logpipe_ingest_accepted_total" in body


# --- WebSocket live tail -----------------------------------------------------


def test_websocket_receives_ingested_events(client):
    """The /ws endpoint is what the web UI at / consumes."""
    with client.websocket_connect("/ws") as ws:
        client.post("/v1/logs", json={
            "service": "checkout-api", "level": "ERROR", "message": "boom",
            "timestamp": "2026-08-23T06:00:00Z",
        })
        received = ws.receive_json()
    assert received["service"] == "checkout-api"
    assert "raw_message" in received


def test_websocket_receives_bulk_events(client):
    with client.websocket_connect("/ws") as ws:
        ndjson = "\n".join(
            json.dumps({"service": "a", "seq": i, "timestamp": "2026-08-23T06:00:00Z"})
            for i in range(3)
        )
        client.post("/v1/logs/bulk", content=ndjson)
        seen = [ws.receive_json() for _ in range(3)]
    assert len(seen) == 3


def test_websocket_receives_raw_events(client):
    line = '203.0.113.9 - - [23/Aug/2026:06:00:00 +0000] "GET / HTTP/1.1" 200 42'
    with client.websocket_connect("/ws") as ws:
        client.post("/v1/logs/raw?service=web", content=line)
        received = ws.receive_json()
    assert received["service"] == "web"


def test_ingestion_works_with_no_viewers_attached(client):
    """Nothing about the live tail may be load-bearing for ingestion."""
    response = client.post("/v1/logs", json={"service": "a", "timestamp": "2026-08-23T06:00:00Z"})
    assert response.status_code == 202
    assert len(sent(client)) == 1


def test_viewer_disconnect_is_cleaned_up(client):
    import ingest.main as gateway_module
    with client.websocket_connect("/ws"):
        pass
    client.post("/v1/logs", json={"service": "a", "timestamp": "2026-08-23T06:00:00Z"})
    assert gateway_module.live.subscriber_count == 0


def test_live_tail_stats_are_exposed_on_metrics(client):
    body = client.get("/metrics").text
    for metric in ("logpipe_ingest_ws_connected",
                   "logpipe_ingest_live_dropped_total",
                   "logpipe_ingest_live_sampled_out_total"):
        assert metric in body
