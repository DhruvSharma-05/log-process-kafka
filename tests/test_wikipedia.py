"""Wikimedia EventStreams mapping tests.

The mapping turns someone else's schema into ours, so the risk is inventing
data that was never in the source. These tests pin what is real.
"""
from __future__ import annotations

import json

import pytest
from processor.parsers import parse_json_app
from producer.sources.wikipedia import to_log_event

EDIT = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "uri": "https://en.wikipedia.org/wiki/Kafka",
        "dt": "2026-08-07T07:19:20Z",
        "id": "3f2a-1111",
        "domain": "en.wikipedia.org",
    },
    "type": "edit",
    "title": "Apache Kafka",
    "user": "SomeEditor",
    "bot": False,
    "minor": False,
    "server_name": "en.wikipedia.org",
    "wiki": "enwiki",
    "length": {"old": 8000, "new": 8250},
    "comment": "added a citation",
    "timestamp": 1786087160,
}


def make(**overrides) -> dict:
    event = json.loads(json.dumps(EDIT))
    event.update(overrides)
    return event


# --- Core mapping ------------------------------------------------------------


def test_service_and_host_come_from_the_source():
    log = to_log_event(EDIT)
    assert log["service"] == "en.wikipedia.org"
    assert log["host"] == "enwiki"


def test_timestamp_is_the_real_edit_time():
    assert to_log_event(EDIT)["timestamp"] == "2026-08-07T07:19:20Z"


def test_falls_back_to_epoch_timestamp_when_meta_dt_is_absent():
    event = make(meta={"id": "x", "domain": "en.wikipedia.org"})
    log = to_log_event(event)
    assert log["timestamp"].startswith("2026-")


def test_path_is_built_from_the_page_title():
    log = to_log_event(EDIT)
    assert log["path"] == "/wiki/Apache_Kafka"


def test_titles_with_special_characters_are_url_encoded():
    log = to_log_event(make(title="Café & Bar"))
    assert " " not in log["path"]
    assert log["path"].startswith("/wiki/")


def test_response_bytes_is_the_real_new_page_length():
    assert to_log_event(EDIT)["response_bytes"] == 8250


def test_byte_delta_is_computed_from_old_and_new():
    assert to_log_event(EDIT)["wiki_bytes_delta"] == 250


def test_response_time_is_absent_not_invented():
    """The source has no latency field. Fabricating one would put fiction on a
    latency dashboard."""
    assert "response_time_ms" not in to_log_event(EDIT)


@pytest.mark.parametrize(
    ("change_type", "status", "method"),
    [("edit", 200, "POST"), ("new", 201, "POST"), ("log", 204, "GET"), ("categorize", 304, "GET")],
)
def test_status_and_method_by_change_type(change_type, status, method):
    log = to_log_event(make(type=change_type))
    assert log["status_code"] == status
    assert log["http_method"] == method


# --- Level derivation --------------------------------------------------------


@pytest.mark.parametrize(
    "comment",
    ["Reverted edits by X", "Undo revision 123", "rvv", "rollback of vandalism"],
)
def test_reverts_are_flagged_as_warnings(comment):
    assert to_log_event(make(comment=comment))["level"] == "WARN"


def test_bot_edits_are_debug():
    assert to_log_event(make(bot=True, comment="routine update"))["level"] == "DEBUG"


def test_ordinary_edits_are_info():
    assert to_log_event(EDIT)["level"] == "INFO"


def test_revert_beats_bot_for_severity():
    assert to_log_event(make(bot=True, comment="Reverted vandalism"))["level"] == "WARN"


# --- Actor identity ----------------------------------------------------------


def test_editor_identity_is_carried_as_the_client():
    assert to_log_event(EDIT)["client_ip"] == "SomeEditor"


def test_masked_temporary_accounts_pass_through_unchanged():
    """Wikimedia masks anonymous editors behind temporary accounts, so this is
    never an IP. The docstring says so; this pins it."""
    log = to_log_event(make(user="~2026-43591-61"))
    assert log["client_ip"] == "~2026-43591-61"


# --- Robustness --------------------------------------------------------------


def test_events_without_a_server_name_are_skipped():
    assert to_log_event({"meta": {}, "type": "edit"}) is None


def test_events_without_any_timestamp_are_skipped():
    assert to_log_event({"server_name": "x.org", "meta": {}, "type": "edit"}) is None


def test_missing_length_does_not_break_the_mapping():
    log = to_log_event(make(length=None))
    assert log["response_bytes"] is None
    assert log["wiki_bytes_delta"] is None


def test_empty_comment_gets_a_synthesised_message():
    log = to_log_event(make(comment=""))
    assert "Apache Kafka" in log["message"]


def test_very_long_comments_are_truncated():
    assert len(to_log_event(make(comment="x" * 5000))["message"]) <= 500


# --- The mapping must survive the production parser --------------------------


def test_mapped_events_parse_with_the_json_app_parser():
    """If the mapping produced something the pipeline cannot parse, every live
    event would land in the DLQ instead of on a dashboard."""
    for change_type in ("edit", "new", "log", "categorize"):
        log = to_log_event(make(type=change_type))
        parsed = parse_json_app(json.dumps(log))
        assert parsed["status_code"] is not None
        assert parsed["log_level"] in {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
        assert parsed["timestamp"].year >= 2020


def test_source_specific_extras_do_not_reach_the_parsed_schema():
    """`wiki_*` fields are carried for interest but must not leak into the
    validated event, which forbids unknown properties."""
    parsed = parse_json_app(json.dumps(to_log_event(EDIT)))
    assert not any(key.startswith("wiki_") for key in parsed)
