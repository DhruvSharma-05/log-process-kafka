"""Producer-side replay tests (FR1.3 and the M1 synthesis rules)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from producer.replay import (
    build_event,
    derive_host,
    derive_service,
    extract_path,
    json_identity,
    make_event_id,
    rewrite_timestamp,
)

ACCESS_LINE = 'uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET /images/logo.gif HTTP/1.0" 200 786'


# --- Timestamp rewriting -----------------------------------------------------


def test_timestamp_is_rewritten_to_now():
    """1995 timestamps would leave every 'last 24 hours' dashboard empty."""
    now = datetime(2026, 7, 30, 18, 40, 55, tzinfo=timezone.utc)
    rewritten = rewrite_timestamp(ACCESS_LINE, now)
    assert "[30/Jul/2026:18:40:55 +0000]" in rewritten
    assert "1995" not in rewritten


def test_rewrite_leaves_the_rest_of_the_line_untouched():
    now = datetime(2026, 7, 30, 18, 40, 55, tzinfo=timezone.utc)
    rewritten = rewrite_timestamp(ACCESS_LINE, now)
    assert '"GET /images/logo.gif HTTP/1.0" 200 786' in rewritten
    assert rewritten.startswith("uplherc.upl.com - - [")


def test_rewrite_is_a_noop_without_a_timestamp():
    line = "some line with no bracketed timestamp"
    assert rewrite_timestamp(line, datetime.now(timezone.utc)) == line


# --- Derivation --------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/images/logo.gif", "image-service"),
        ("/shuttle/missions/sts-68/news.txt", "shuttle-api"),
        ("/history/apollo/", "history-api"),
        ("/", "portal-web"),
    ],
)
def test_service_derivation_is_mapped(path, expected):
    assert derive_service(path) == expected


def test_service_and_host_derivation_are_deterministic():
    """Stable mapping matters: the same request must always key to the same
    partition, or per-service ordering is meaningless."""
    assert derive_service("/unmapped/thing") == derive_service("/unmapped/thing")
    assert derive_host(ACCESS_LINE) == derive_host(ACCESS_LINE)


def test_extract_path_handles_missing_request():
    assert extract_path("garbage with no request line") == "/"


# --- Event identity ----------------------------------------------------------


def test_event_id_is_content_derived_and_stable():
    """FR2.5 dedupe depends on replaying a line producing the same id."""
    first = make_event_id("web-01", "portal-web", ACCESS_LINE)
    assert first == make_event_id("web-01", "portal-web", ACCESS_LINE)
    assert first != make_event_id("web-02", "portal-web", ACCESS_LINE)
    assert len(first) == 32


# --- JSON identity -----------------------------------------------------------


def test_json_identity_prefers_the_declared_service():
    """Without this, every JSON app log derives service from the request path
    and collapses onto a single partition."""
    service, host = json_identity('{"service":"checkout-api","host":"api-07","message":"x"}')
    assert service == "checkout-api"
    assert host == "api-07"


def test_json_identity_accepts_aliases():
    service, host = json_identity('{"app":"billing","hostname":"node-3"}')
    assert (service, host) == ("billing", "node-3")


@pytest.mark.parametrize("line", ["not json", "[1,2,3]", '"scalar"', "", '{"other":"field"}'])
def test_json_identity_returns_none_when_unavailable(line):
    assert json_identity(line) == (None, None)


def test_build_event_uses_json_service_for_json_format():
    event = build_event(
        '{"timestamp":"2026-07-30T10:15:32Z","service":"auth-api","host":"api-09","path":"/login"}',
        source_format="json_app",
    )
    assert event["service"] == "auth-api"
    assert event["host"] == "api-09"


def test_build_event_falls_back_to_path_derivation_for_access_logs():
    event = build_event(ACCESS_LINE, source_format="nginx_combined")
    assert event["service"] == "image-service"
    assert event["source_format"] == "nginx_combined"
    assert set(event) >= {"event_id", "ingested_at", "host", "service", "raw_message"}


def test_build_event_skips_blank_lines():
    assert build_event("   \n", source_format="nginx_combined") is None


def test_synth_latency_is_not_applied_to_json_logs():
    """Appending a float to a JSON line would corrupt it."""
    event = build_event(
        '{"timestamp":"2026-07-30T10:15:32Z","service":"a"}',
        source_format="json_app",
        latency=True,
    )
    assert str(event["raw_message"]).endswith("}")
