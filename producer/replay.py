"""Turn raw log-file lines into `raw-logs` events.

Two jobs matter here:

1. **Timestamp rewriting.** The NASA logs are from 1995. Replayed as-is, every
   dashboard "last 24 hours" view would be empty, so the in-line timestamp is
   rewritten to now as each line is emitted.
2. **Synthesising `service` and `host`.** Access logs carry neither, but the PRD
   partitions on `service` and every dashboard groups by it. Both are derived
   deterministically from the line, so a given request always maps to the same
   service and host.
"""
from __future__ import annotations

import hashlib
import json
import random
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from .config import FALLBACK_SERVICES, HOST_POOL, IP_HASH_SALT, SERVICE_MAP

# Common/Combined Log Format timestamp: [01/Aug/1995:00:00:01 -0400]
_TS_RE = re.compile(r"\[(\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2})\s*([+-]\d{4})?\]")

# The request line: "GET /shuttle/countdown/ HTTP/1.0"
_REQUEST_RE = re.compile(r'"(?:[A-Z]+)\s+(\S+)(?:\s+HTTP/[\d.]+)?"')

# Leading client identifier (IP or hostname) in CLF.
_CLIENT_RE = re.compile(r"^(\S+)")

# Trailing status code and byte count, used only for latency synthesis.
_STATUS_RE = re.compile(r'"\s+(\d{3})\s+(\d+|-)\s*$')


def _stable_index(value: str, modulo: int) -> int:
    """Deterministic bucket for a string — same input, same bucket, every run."""
    digest = hashlib.md5(value.encode("utf-8", "replace")).digest()
    return int.from_bytes(digest[:4], "big") % modulo


def extract_path(line: str) -> str:
    match = _REQUEST_RE.search(line)
    return match.group(1) if match else "/"


def derive_service(path: str) -> str:
    """Map a request path onto a synthetic service name."""
    segment = path.lstrip("/").split("/", 1)[0].split("?", 1)[0].lower()
    if segment in SERVICE_MAP:
        return SERVICE_MAP[segment]
    if not segment:
        return "portal-web"
    return FALLBACK_SERVICES[_stable_index(segment, len(FALLBACK_SERVICES))]


def derive_host(line: str) -> str:
    match = _CLIENT_RE.match(line)
    client = match.group(1) if match else "unknown"
    return HOST_POOL[_stable_index(client, len(HOST_POOL))]


def rewrite_timestamp(line: str, now: datetime) -> str:
    """Replace the in-line CLF timestamp with the current time."""
    stamp = now.strftime("%d/%b/%Y:%H:%M:%S +0000")
    return _TS_RE.sub(f"[{stamp}]", line, count=1)


def synth_latency(line: str, rng: random.Random) -> str:
    """Append a synthetic request time (seconds, nginx `$request_time` style).

    Access logs in the wild often omit request time; the NASA logs always do.
    This is clearly synthetic data — enabled only via --synth-latency — and
    exists so the latency panel (FR4.1) has something to show. Errors are
    modelled as slower than successes, which is what makes the panel useful.
    """
    match = _STATUS_RE.search(line)
    status = int(match.group(1)) if match else 200
    if status >= 500:
        seconds = rng.lognormvariate(-0.8, 0.7)   # slow, long tail
    elif status >= 400:
        seconds = rng.lognormvariate(-2.4, 0.5)
    else:
        seconds = rng.lognormvariate(-2.9, 0.6)
    return f"{line} {min(seconds, 30.0):.3f}"


def make_event_id(host: str, service: str, raw_message: str) -> str:
    """Stable ID for dedupe (FR2.5).

    Derived from content, not from a counter or uuid4, so replaying the same
    line twice produces the same id and ClickHouse's ReplacingMergeTree can
    collapse it. This is what makes the dedupe demo in M3 work.
    """
    material = f"{host}|{service}|{raw_message}".encode("utf-8", "replace")
    return hashlib.sha256(material).hexdigest()[:32]


def hash_ip(client: str) -> str:
    """Salted hash of the client identifier (Data privacy NFR)."""
    return hashlib.sha256(f"{IP_HASH_SALT}|{client}".encode("utf-8", "replace")).hexdigest()[:32]


def build_event(
    line: str,
    *,
    source_format: str,
    rewrite: bool = True,
    latency: bool = False,
    rng: random.Random | None = None,
    tags: dict[str, str] | None = None,
    service_override: str | None = None,
    host_override: str | None = None,
) -> dict[str, object] | None:
    """Build one `raw-logs` event. Returns None for blank lines.

    `service_override` / `host_override` let a caller name the service directly
    instead of deriving it from the line — used by scenarios.py, which needs to
    target a specific service to drive an error spike.
    """
    line = line.rstrip("\r\n")
    if not line.strip():
        return None

    now = datetime.now(timezone.utc)
    if rewrite:
        line = rewrite_timestamp(line, now)
    if latency and source_format != "json_app":
        line = synth_latency(line, rng or random.Random())

    if source_format == "json_app":
        # Structured logs name their own service/host; fall back to derivation
        # only when they don't.
        service, host = json_identity(line)
        service = service or derive_service(extract_path(line))
        host = host or derive_host(line)
    else:
        service = derive_service(extract_path(line))
        host = derive_host(line)

    service = service_override or service
    host = host_override or host

    event: dict[str, object] = {
        "event_id": make_event_id(host, service, line),
        "ingested_at": now.isoformat().replace("+00:00", "Z"),
        "host": host,
        "service": service,
        "source_format": source_format,
        "raw_message": line,
    }
    if tags:
        event.update(tags)
    return event


def json_identity(line: str) -> tuple[str | None, str | None]:
    """Pull `service` and `host` out of a JSON application log line.

    Structured logs usually name their own service, and that name must win:
    deriving it from the request path instead would key every JSON event
    identically and collapse them onto one partition. Returns (None, None) for
    anything unreadable — the processor is what rejects bad JSON, not this.
    """
    try:
        payload = json.loads(line)
    except (ValueError, TypeError):
        return None, None
    if not isinstance(payload, dict):
        return None, None

    service = payload.get("service") or payload.get("service_name") or payload.get("app")
    host = payload.get("host") or payload.get("hostname") or payload.get("node")
    return (
        str(service) if isinstance(service, (str, int)) else None,
        str(host) if isinstance(host, (str, int)) else None,
    )


def iter_file(path: Path, *, loop: bool = False) -> Iterator[str]:
    """Yield lines from a log file.

    NASA logs are Latin-1 with occasional broken bytes, so decoding is lenient:
    a bad byte must never stop ingestion. Genuinely unparseable content is the
    processor's problem (it goes to the DLQ), not the producer's.
    """
    while True:
        with path.open("r", encoding="latin-1", errors="replace") as handle:
            yield from handle
        if not loop:
            return


def iter_stream(stream: TextIO) -> Iterator[str]:
    """Yield lines from stdin — used for `flog ... | python producer/main.py --stdin`."""
    yield from stream
