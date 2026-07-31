"""Enrichment, redaction and schema-validation tests (FR2.2, FR2.3, privacy NFR)."""
from __future__ import annotations

import pytest
from processor.enrich import GeoResolver, enrich
from processor.redact import hash_client, redact, scrub_text
from processor.validation import ValidationError, validate


@pytest.fixture
def resolver() -> GeoResolver:
    # No .mmdb path: exercises the ccTLD fallback tier, which is what runs
    # against the NASA dataset (hostnames, not IPs).
    return GeoResolver(None)


# --- Geo resolution ----------------------------------------------------------


@pytest.mark.parametrize(
    "address",
    [
        "10.0.4.12",       # RFC1918 — the PRD's own example
        "192.168.1.1",
        "172.16.0.5",
        "127.0.0.1",       # loopback
        "169.254.10.1",    # link-local
        "203.0.113.9",     # RFC5737 documentation range: also non-geographic
        "198.51.100.4",
    ],
)
def test_private_addresses_resolve_to_null_not_a_country(resolver, address):
    """PRD §3.3 correction: the spec's example mapped 10.0.4.12 -> 'IN', which
    is impossible. Private ranges have no geography and must not DLQ either."""
    country, source = resolver.resolve(address)
    assert country is None
    assert source == "private"


@pytest.mark.parametrize(
    ("hostname", "expected"),
    [
        ("kgtyk4.kj.yamagata-u.ac.jp", "JP"),
        ("www.example.co.uk", "GB"),
        ("host.uni-hamburg.de", "DE"),
        ("piweba3y.prodigy.com", None),   # generic TLD: not inferable
        ("nasa.gov", "US"),
        ("mit.edu", "US"),
    ],
)
def test_cctld_inference(resolver, hostname, expected):
    country, source = resolver.resolve(hostname)
    assert country == expected
    assert source == ("tld" if expected else "unresolved")


def test_public_ip_without_maxmind_is_unresolved_not_guessed(resolver):
    """Without a GeoLite2 database there is no honest answer for a public IP,
    so the pipeline records `unresolved` rather than inventing a country."""
    country, source = resolver.resolve("8.8.8.8")
    assert country is None
    assert source == "unresolved"


def test_missing_client_is_unresolved(resolver):
    assert resolver.resolve(None) == (None, "unresolved")
    assert resolver.resolve("") == (None, "unresolved")


def test_geo_source_is_always_recorded(resolver):
    """A dashboard must never present ccTLD inference as a database lookup."""
    event = enrich({"client_ip": "host.jp", "path": "/x", "status_code": 200}, resolver)
    assert event["geo_country"] == "JP"
    assert event["geo_source"] == "tld"


# --- Derived fields ----------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/shuttle/missions/sts-68/news.txt", "/shuttle"),
        ("/images/logo.gif", "/images"),
        ("/", "/"),
        ("/search?q=a/b", "/search"),
        (None, None),
    ],
)
def test_path_group_bucketing(resolver, path, expected):
    event = enrich({"client_ip": None, "path": path, "status_code": 200}, resolver)
    assert event["path_group"] == expected


@pytest.mark.parametrize(
    ("status", "level", "expected"),
    [(200, "INFO", False), (404, "WARN", False), (500, "ERROR", True), (None, "ERROR", True)],
)
def test_is_error_flag(resolver, status, level, expected):
    event = enrich({"client_ip": None, "path": "/", "status_code": status, "log_level": level}, resolver)
    assert event["is_error"] is expected


# --- Redaction ---------------------------------------------------------------


def test_client_hash_is_stable_and_not_reversible():
    first = hash_client("203.0.113.9")
    assert first == hash_client("203.0.113.9")
    assert first != hash_client("203.0.113.10")
    assert "203.0.113.9" not in first
    assert len(first) == 32


def test_redact_adds_hash_and_keeps_raw_ip_by_default():
    event = redact({"client_ip": "203.0.113.9"})
    assert event["client_ip"] == "203.0.113.9"
    assert event["client_ip_hash"] is not None


@pytest.mark.parametrize(
    ("text", "must_not_contain"),
    [
        ("contact alice@example.com now", "alice@example.com"),
        ("Authorization: Bearer sk-abc123def456", "sk-abc123def456"),
        ("card 4111 1111 1111 1111 charged", "4111 1111 1111 1111"),
    ],
)
def test_scrub_text_removes_secrets(text, must_not_contain):
    assert must_not_contain not in scrub_text(text)


def test_scrub_text_passes_through_clean_text():
    assert scrub_text("GET /checkout returned 500") == "GET /checkout returned 500"


# --- Schema validation -------------------------------------------------------


def _valid_event() -> dict:
    return {
        "event_id": "a3f5c1d2e3f4a5b6c7d8e9f0a1b2c3d4",
        "timestamp": "2026-07-30T10:15:32Z",
        "ingested_at": "2026-07-30T10:15:32Z",
        "host": "web-03",
        "service": "checkout-api",
        "log_level": "ERROR",
        "status_code": 500,
        "geo_country": None,
        "geo_source": "private",
    }


def test_valid_event_passes_schema():
    validate(_valid_event(), "parsed_log_event")


@pytest.mark.parametrize(
    ("mutation", "field"),
    [
        ({"log_level": "SEVERE"}, "log_level"),       # not in the enum
        ({"status_code": 999}, "status_code"),        # outside 100-599
        ({"geo_country": "IND"}, "geo_country"),      # must be alpha-2
        ({"geo_source": "guess"}, "geo_source"),      # not a known tier
        ({"response_bytes": -1}, "response_bytes"),
        ({"event_id": "short"}, "event_id"),
    ],
)
def test_schema_rejects_bad_values(mutation, field):
    event = _valid_event() | mutation
    with pytest.raises(ValidationError, match=field):
        validate(event, "parsed_log_event")


@pytest.mark.parametrize("field", ["event_id", "timestamp", "host", "service", "log_level"])
def test_schema_requires_core_fields(field):
    event = _valid_event()
    del event[field]
    with pytest.raises(ValidationError):
        validate(event, "parsed_log_event")


def test_schema_rejects_unknown_fields():
    """additionalProperties:false is the schema-drift tripwire (PRD risk #3)."""
    with pytest.raises(ValidationError):
        validate(_valid_event() | {"surprise_field": 1}, "parsed_log_event")
