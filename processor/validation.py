"""JSON Schema validation for outbound events.

Validation is the last gate before an event reaches `parsed-logs`. A failure
here is a schema-drift signal (the PRD's "silent data corruption" risk), so it
goes to the DLQ with the offending field named rather than being written out.
"""
from __future__ import annotations

import functools
import json
from pathlib import Path

from jsonschema import Draft202012Validator

from .config import SCHEMA_DIR


@functools.lru_cache(maxsize=None)
def load_validator(name: str) -> Draft202012Validator:
    path = Path(SCHEMA_DIR) / f"{name}.schema.json"
    with path.open(encoding="utf-8") as handle:
        schema = json.load(handle)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


class ValidationError(ValueError):
    """Raised when an event does not satisfy its schema."""


def validate(event: dict, schema_name: str = "parsed_log_event") -> None:
    """Raise ValidationError with the first (deepest) problem found."""
    validator = load_validator(schema_name)
    errors = sorted(validator.iter_errors(event), key=lambda e: list(e.absolute_path))
    if not errors:
        return
    first = errors[0]
    location = ".".join(str(part) for part in first.absolute_path) or "<root>"
    raise ValidationError(f"{location}: {first.message}")
