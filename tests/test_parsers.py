"""Golden-line tests for the log parsers (FR1.2, FR2.1).

The negative cases matter as much as the positive ones: each one must raise
ParseError so the record lands in the DLQ instead of becoming a half-event or
crashing the consumer loop (PRD risk #2).
"""
from __future__ import annotations

import pytest
from processor.parsers import ParseError, parse, parse_json_app, parse_nginx_combined

# --- Access-log format -------------------------------------------------------

COMMON = 'uplherc.upl.com - - [30/Jul/2026:18:40:55 +0000] "GET /index.html HTTP/1.0" 200 7280'
COMMON_WITH_TIME = COMMON + " 0.184"
COMBINED = (
    '10.0.4.12 - frank [30/Jul/2026:18:40:55 +0000] "POST /checkout HTTP/1.1" 500 342 '
    '"https://example.com/cart" "Mozilla/5.0 (X11; Linux x86_64)"'
)
COMBINED_WITH_TIME = COMBINED + " 1.507"


def test_common_log_format():
    event = parse_nginx_combined(COMMON)
    assert event["client_ip"] == "uplherc.upl.com"
    assert event["http_method"] == "GET"
    assert event["path"] == "/index.html"
    assert event["status_code"] == 200
    assert event["response_bytes"] == 7280
    assert event["log_level"] == "INFO"
    assert event["response_time_ms"] is None
    assert event["timestamp"].year == 2026


def test_request_time_is_converted_to_milliseconds():
    assert parse_nginx_combined(COMMON_WITH_TIME)["response_time_ms"] == 184
    assert parse_nginx_combined(COMBINED_WITH_TIME)["response_time_ms"] == 1507


def test_combined_format_captures_referer_and_agent():
    event = parse_nginx_combined(COMBINED)
    assert event["referer"] == "https://example.com/cart"
    assert event["user_agent"].startswith("Mozilla/5.0")
    assert event["status_code"] == 500
    assert event["log_level"] == "ERROR"


@pytest.mark.parametrize(
    ("status", "expected"),
    [(200, "INFO"), (304, "INFO"), (404, "WARN"), (403, "WARN"), (500, "ERROR"), (503, "ERROR")],
)
def test_log_level_derived_from_status(status, expected):
    line = f'host - - [30/Jul/2026:18:40:55 +0000] "GET / HTTP/1.0" {status} 10'
    assert parse_nginx_combined(line)["log_level"] == expected


def test_dash_byte_count_is_zero_not_missing():
    line = 'host - - [30/Jul/2026:18:40:55 +0000] "GET / HTTP/1.0" 304 -'
    assert parse_nginx_combined(line)["response_bytes"] == 0


@pytest.mark.parametrize(
    ("name", "line"),
    [
        ("empty", ""),
        ("whitespace", "   \t  "),
        ("truncated", 'uplherc.upl.com - - [30/Jul/2026:18:40:55 +0000] "GET /'),
        ("no_brackets", 'host - - 30/Jul/2026:18:40:55 "GET / HTTP/1.0" 200 10'),
        ("bad_status", 'host - - [30/Jul/2026:18:40:55 +0000] "GET / HTTP/1.0" 20 10'),
        ("bad_month", 'host - - [30/Xxx/2026:18:40:55 +0000] "GET / HTTP/1.0" 200 10'),
        ("empty_request", 'host - - [30/Jul/2026:18:40:55 +0000] "" 200 10'),
        ("path_only", 'host - - [30/Jul/2026:18:40:55 +0000] "/nomethod" 200 10'),
        ("numeric_method", 'host - - [30/Jul/2026:18:40:55 +0000] "123 /x HTTP/1.0" 200 10'),
        ("syslog_line", "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: check pass; user unknown"),
        ("unicode_garbage", "\x00\xff� nonsense �\x00"),
        ("json_line", '{"timestamp":"2026-07-30T10:15:32Z","message":"hi"}'),
    ],
)
def test_malformed_access_log_lines_raise(name, line):
    with pytest.raises(ParseError):
        parse_nginx_combined(line)


# --- JSON application logs ---------------------------------------------------


def test_json_app_canonical_fields():
    event = parse_json_app(
        '{"timestamp":"2026-07-30T10:15:32.104Z","level":"error","message":"boom",'
        '"status_code":500,"path":"/checkout","http_method":"POST",'
        '"client_ip":"203.0.113.9","response_time_ms":184,"trace_id":"abc123"}'
    )
    assert event["log_level"] == "ERROR"
    assert event["status_code"] == 500
    assert event["response_time_ms"] == 184
    assert event["trace_id"] == "abc123"
    assert event["timestamp"].tzinfo is not None


def test_json_app_accepts_field_aliases():
    event = parse_json_app(
        '{"@timestamp":"2026-07-30T10:15:32Z","severity":"WARNING","msg":"slow",'
        '"status":404,"uri":"/missing","remote_addr":"198.51.100.4","duration":0.25}'
    )
    assert event["log_level"] == "WARN"        # WARNING normalised
    assert event["path"] == "/missing"
    assert event["client_ip"] == "198.51.100.4"
    assert event["response_time_ms"] == 250    # seconds -> milliseconds


def test_json_app_epoch_timestamps():
    seconds = parse_json_app('{"time":1785436855,"message":"x"}')["timestamp"]
    millis = parse_json_app('{"time":1785436855000,"message":"x"}')["timestamp"]
    assert seconds.year == 2026
    assert seconds.replace(microsecond=0) == millis.replace(microsecond=0)


def test_json_app_infers_level_from_status_when_absent():
    assert parse_json_app('{"time":1785436855,"status":503}')["log_level"] == "ERROR"
    assert parse_json_app('{"time":1785436855,"status":404}')["log_level"] == "WARN"
    assert parse_json_app('{"time":1785436855,"status":200}')["log_level"] == "INFO"


@pytest.mark.parametrize(
    ("name", "line"),
    [
        ("empty", ""),
        ("not_json", "plain text line"),
        ("truncated_json", '{"timestamp":"2026-07-30T10:15:32Z","message":'),
        ("json_array", '[{"timestamp":"2026-07-30T10:15:32Z"}]'),
        ("json_scalar", '"just a string"'),
        ("no_timestamp", '{"message":"no time here","level":"INFO"}'),
        ("bad_timestamp", '{"timestamp":"not-a-date","message":"x"}'),
        ("null_timestamp", '{"timestamp":null,"message":"x"}'),
    ],
)
def test_malformed_json_lines_raise(name, line):
    with pytest.raises(ParseError):
        parse_json_app(line)


# --- Dispatch ----------------------------------------------------------------


def test_dispatch_selects_parser_by_source_format():
    assert parse(COMMON, "nginx_combined")["status_code"] == 200
    assert parse('{"time":1785436855,"message":"x"}', "json_app")["log_level"] == "INFO"


def test_unknown_source_format_is_a_parse_error():
    with pytest.raises(ParseError, match="unknown source_format"):
        parse(COMMON, "syslog_rfc5424")
