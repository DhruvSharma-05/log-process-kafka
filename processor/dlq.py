"""Dead-letter-queue construction (FR2.3).

The rule the whole pipeline rests on: a single bad event must never stop the
stream. Anything that cannot be parsed, enriched, or validated is wrapped with
the reason it failed and forwarded to `dead-letter-queue`, and processing
continues with the next record.

The original payload is preserved verbatim so an operator can replay it after
fixing the parser.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

MAX_ORIGINAL_BYTES = 16384  # keep one poison pill from blowing up the DLQ topic


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_dlq_event(
    original: dict | bytes | str | None,
    *,
    reason: str,
    stage: str,
    event_id: str | None = None,
    service: str | None = None,
) -> dict[str, object]:
    """Wrap a failed record with its failure reason.

    `stage` is one of decode | parse | enrich | validate | produce, so the DLQ
    can be grouped by where in the pipeline things broke.
    """
    if isinstance(original, bytes):
        payload: object = original.decode("utf-8", "replace")[:MAX_ORIGINAL_BYTES]
    elif isinstance(original, str):
        payload = original[:MAX_ORIGINAL_BYTES]
    elif isinstance(original, dict):
        payload = original
        event_id = event_id or original.get("event_id")
        service = service or original.get("service")
    else:
        payload = None

    return {
        "event_id": event_id,
        "failed_at": _now(),
        "error_reason": reason[:512],
        "error_stage": stage,
        "service": service,
        "original": payload,
    }


def serialise(event: dict[str, object]) -> bytes:
    return json.dumps(event, separators=(",", ":"), default=str).encode()
