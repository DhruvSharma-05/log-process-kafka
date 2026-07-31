"""PII redaction (Data privacy NFR).

The client address is the PII that matters in access logs. Every event gets a
salted `client_ip_hash`, which is what long-term storage keeps; the raw value is
retained only in the hot tier, and `REDACT_CLIENT_IP=1` drops it everywhere.

The salt must match the producer's, or hashes won't join across the pipeline.
"""
from __future__ import annotations

import hashlib
import os
import re

from .config import IP_HASH_SALT

REDACT_CLIENT_IP = os.getenv("REDACT_CLIENT_IP", "0") == "1"

# Patterns scrubbed from free-text message bodies before storage.
EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
BEARER_RE = re.compile(r"\b(?:Bearer|token|api[_-]?key)\s*[=:]?\s*\S{8,}", re.IGNORECASE)
CARD_RE = re.compile(r"\b(?:\d[ -]?){13,19}\b")


def hash_client(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(f"{IP_HASH_SALT}|{value}".encode("utf-8", "replace")).hexdigest()[:32]


def scrub_text(text: str | None) -> str | None:
    """Remove obvious secrets and identifiers from a free-text field."""
    if not text:
        return text
    text = EMAIL_RE.sub("[email]", text)
    text = BEARER_RE.sub("[redacted-credential]", text)
    text = CARD_RE.sub("[redacted-number]", text)
    return text


def redact(event: dict[str, object]) -> dict[str, object]:
    """Apply redaction to an enriched event, in place."""
    client = event.get("client_ip")
    event["client_ip_hash"] = hash_client(client if isinstance(client, str) else None)

    if REDACT_CLIENT_IP:
        event["client_ip"] = None

    if isinstance(event.get("message"), str):
        event["message"] = scrub_text(event["message"])

    return event
