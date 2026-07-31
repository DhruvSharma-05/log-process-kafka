"""Nginx/Apache access-log parser (FR1.2 format 1).

Handles three shapes with one regex:

  Common Log Format   %h %l %u [%t] "%r" %>s %b
  Combined            ... plus "%{Referer}i" "%{User-agent}i"
  Either + $request_time appended (our own format, and --synth-latency output)

The NASA dataset is Common Log Format, so referer/user-agent are optional and
absent fields become None rather than parse failures.
"""
from __future__ import annotations

import re
from datetime import datetime

from . import ParseError

LINE_RE = re.compile(
    r"^(?P<client>\S+)\s+"
    r"(?P<ident>\S+)\s+"
    r"(?P<user>\S+)\s+"
    r"\[(?P<ts>[^\]]+)\]\s+"
    r'"(?P<request>[^"]*)"\s+'
    r"(?P<status>\d{3})\s+"
    r"(?P<bytes>\d+|-)"
    r'(?:\s+"(?P<referer>[^"]*)"\s+"(?P<agent>[^"]*)")?'   # combined only
    r"(?:\s+(?P<request_time>\d+(?:\.\d+)?))?"             # $request_time only
    r"\s*$"
)

# CLF timestamp: 30/Jul/2026:18:40:55 +0000
TS_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


def _parse_timestamp(value: str) -> datetime:
    try:
        return datetime.strptime(value, TS_FORMAT)
    except ValueError as exc:
        raise ParseError(f"unparseable timestamp {value!r}") from exc


def _parse_request(request: str) -> tuple[str, str]:
    """Split the request line into method and path.

    Real access logs contain junk request lines (empty, truncated, raw binary).
    Anything without at least a method and a path is a parse failure, which is
    the correct outcome: it goes to the DLQ rather than becoming a half-event.
    """
    parts = request.split()
    if len(parts) < 2:
        raise ParseError(f"malformed request line {request!r}")
    method, path = parts[0], parts[1]
    if not method.isalpha():
        raise ParseError(f"invalid HTTP method {method!r}")
    return method.upper(), path


def _derive_log_level(status: int) -> str:
    if status >= 500:
        return "ERROR"
    if status >= 400:
        return "WARN"
    return "INFO"


def parse_nginx_combined(raw_message: str) -> dict[str, object]:
    line = raw_message.strip()
    if not line:
        raise ParseError("empty line")

    match = LINE_RE.match(line)
    if match is None:
        raise ParseError("does not match access-log format")

    fields = match.groupdict()
    timestamp = _parse_timestamp(fields["ts"])
    method, path = _parse_request(fields["request"])
    status = int(fields["status"])

    # "-" means no response body, which is 0 bytes, not missing data.
    raw_bytes = fields["bytes"]
    response_bytes = 0 if raw_bytes == "-" else int(raw_bytes)

    request_time = fields.get("request_time")
    response_time_ms = round(float(request_time) * 1000) if request_time is not None else None

    return {
        "timestamp": timestamp,
        "client_ip": fields["client"],
        "http_method": method,
        "path": path,
        "status_code": status,
        "response_bytes": response_bytes,
        "response_time_ms": response_time_ms,
        "log_level": _derive_log_level(status),
        "user_agent": fields.get("agent"),
        "referer": fields.get("referer"),
        "trace_id": None,
    }
