"""Log-line parsers (FR1.2 / FR2.1).

Every parser raises `ParseError` on failure — never returns partial data and
never raises anything else. The caller turns a `ParseError` into a DLQ event.
"""
from __future__ import annotations


class ParseError(ValueError):
    """Raised when a line cannot be parsed into a structured event."""


from .json_app import parse_json_app  # noqa: E402
from .nginx_combined import parse_nginx_combined  # noqa: E402

# Dispatch table keyed by the producer's `source_format` hint.
PARSERS = {
    "nginx_combined": parse_nginx_combined,
    "json_app": parse_json_app,
}


def parse(raw_message: str, source_format: str) -> dict[str, object]:
    """Parse a raw line using the declared format.

    An unknown format is a configuration error on the producer side, so it is
    itself a parse failure and lands in the DLQ with a clear reason.
    """
    parser = PARSERS.get(source_format)
    if parser is None:
        raise ParseError(f"unknown source_format {source_format!r}")
    return parser(raw_message)


__all__ = ["PARSERS", "ParseError", "parse", "parse_json_app", "parse_nginx_combined"]
