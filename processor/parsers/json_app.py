"""Structured JSON application-log parser (FR1.2 format 2).

Applications disagree about field names, so common aliases are accepted and
normalised onto the canonical schema. Anything that is valid JSON but carries
no recognisable timestamp or message is a parse failure — an object with no
usable content is worse than an explicit DLQ entry.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from . import ParseError

TIMESTAMP_KEYS = ("timestamp", "time", "@timestamp", "ts", "eventTime")
MESSAGE_KEYS = ("message", "msg", "log", "event")
LEVEL_KEYS = ("level", "severity", "loglevel", "log_level")
STATUS_KEYS = ("status_code", "status", "http_status", "statusCode")
METHOD_KEYS = ("http_method", "method", "verb")
PATH_KEYS = ("path", "url", "uri", "request_path")
CLIENT_KEYS = ("client_ip", "ip", "remote_addr", "clientIp")
DURATION_MS_KEYS = ("response_time_ms", "duration_ms", "latency_ms", "elapsed_ms")
DURATION_S_KEYS = ("response_time", "duration", "request_time", "elapsed")
TRACE_KEYS = ("trace_id", "traceId", "traceID", "correlation_id")
BYTES_KEYS = ("response_bytes", "bytes", "bytes_sent", "content_length")

VALID_LEVELS = {"TRACE", "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL", "CRITICAL"}
LEVEL_ALIASES = {"WARNING": "WARN", "CRITICAL": "FATAL", "ERR": "ERROR"}


def _first(payload: dict, keys: tuple[str, ...]):
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def _parse_timestamp(value) -> datetime:
    """Accept ISO-8601 (with Z or offset) and epoch seconds/milliseconds."""
    if isinstance(value, (int, float)):
        # Heuristic: anything past ~2001 in seconds is milliseconds if > 1e11.
        seconds = value / 1000 if value > 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ParseError(f"unparseable timestamp {value!r}") from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise ParseError(f"unsupported timestamp type {type(value).__name__}")


def _coerce_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalise_level(value, status: int | None) -> str:
    if isinstance(value, str):
        level = LEVEL_ALIASES.get(value.strip().upper(), value.strip().upper())
        if level in VALID_LEVELS:
            return LEVEL_ALIASES.get(level, level)
    if status is not None:
        if status >= 500:
            return "ERROR"
        if status >= 400:
            return "WARN"
    return "INFO"


def parse_json_app(raw_message: str) -> dict[str, object]:
    line = raw_message.strip()
    if not line:
        raise ParseError("empty line")

    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ParseError(f"invalid JSON: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise ParseError(f"expected a JSON object, got {type(payload).__name__}")

    raw_timestamp = _first(payload, TIMESTAMP_KEYS)
    if raw_timestamp is None:
        raise ParseError(f"no timestamp field (looked for {', '.join(TIMESTAMP_KEYS)})")
    timestamp = _parse_timestamp(raw_timestamp)

    status = _coerce_int(_first(payload, STATUS_KEYS))
    message = _first(payload, MESSAGE_KEYS)

    response_time_ms = _coerce_int(_first(payload, DURATION_MS_KEYS))
    if response_time_ms is None:
        seconds = _first(payload, DURATION_S_KEYS)
        if seconds is not None:
            try:
                response_time_ms = round(float(seconds) * 1000)
            except (TypeError, ValueError):
                response_time_ms = None

    return {
        "timestamp": timestamp,
        "client_ip": _first(payload, CLIENT_KEYS),
        "http_method": (_first(payload, METHOD_KEYS) or None),
        "path": _first(payload, PATH_KEYS),
        "status_code": status,
        "response_bytes": _coerce_int(_first(payload, BYTES_KEYS)),
        "response_time_ms": response_time_ms,
        "log_level": _normalise_level(_first(payload, LEVEL_KEYS), status),
        "message": message,
        "trace_id": _first(payload, TRACE_KEYS),
        "user_agent": payload.get("user_agent"),
        "referer": payload.get("referer"),
    }
