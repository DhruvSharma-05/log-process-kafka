"""Event enrichment (FR2.2): geo lookup and derived fields.

Geo resolution has three tiers, in order:

1. **MaxMind GeoLite2**, if a `.mmdb` is present and `geoip2` is installed.
2. **ccTLD inference** for hostname clients — the NASA dataset logs hostnames
   (`kgtyk4.kj.yamagata-u.ac.jp`), not IPs, so this is the tier that actually
   fires on our sample data.
3. **None**, recorded with a reason.

Every event carries `geo_source` so a dashboard never presents an inferred
country as if it were a database lookup. Per PRD correction §3.3, a private or
unresolvable address yields `geo_country: null` and is *not* a DLQ condition.
"""
from __future__ import annotations

import ipaddress
import os
from typing import Literal

GeoSource = Literal["maxmind", "tld", "private", "unresolved"]

# ccTLD -> ISO 3166-1 alpha-2. Not exhaustive; covers the bulk of the dataset
# plus common cases. Unlisted suffixes fall through to "unresolved".
CCTLD_COUNTRY: dict[str, str] = {
    "au": "AU", "at": "AT", "be": "BE", "br": "BR", "ca": "CA", "ch": "CH",
    "cl": "CL", "cn": "CN", "cz": "CZ", "de": "DE", "dk": "DK", "ee": "EE",
    "es": "ES", "fi": "FI", "fr": "FR", "gr": "GR", "hk": "HK", "hu": "HU",
    "id": "ID", "ie": "IE", "il": "IL", "in": "IN", "is": "IS", "it": "IT",
    "jp": "JP", "kr": "KR", "lu": "LU", "mx": "MX", "my": "MY", "nl": "NL",
    "no": "NO", "nz": "NZ", "pl": "PL", "pt": "PT", "ro": "RO", "ru": "RU",
    "se": "SE", "sg": "SG", "si": "SI", "sk": "SK", "th": "TH", "tr": "TR",
    "tw": "TW", "ua": "UA", "uk": "GB", "us": "US", "za": "ZA",
}

# US-restricted generic TLDs. .com/.net/.org are global and stay unresolved.
US_TLDS = {"gov", "mil", "edu"}


class GeoResolver:
    """Resolves a client identifier to a country code."""

    def __init__(self, db_path: str | None = None) -> None:
        self._reader = None
        self.backend = "tld-only"
        if db_path and os.path.exists(db_path):
            try:
                import geoip2.database  # imported lazily: optional dependency

                self._reader = geoip2.database.Reader(db_path)
                self.backend = "maxmind"
            except Exception:  # noqa: BLE001 - a missing/broken DB must not stop the pipeline
                self._reader = None

    def close(self) -> None:
        if self._reader is not None:
            self._reader.close()

    def resolve(self, client: str | None) -> tuple[str | None, GeoSource]:
        if not client:
            return None, "unresolved"

        try:
            address = ipaddress.ip_address(client)
        except ValueError:
            return self._resolve_hostname(client)

        # PRD correction §3.3: RFC1918 and friends have no geography.
        if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
            return None, "private"

        if self._reader is not None:
            try:
                response = self._reader.country(client)
                return response.country.iso_code, "maxmind"
            except Exception:  # noqa: BLE001 - address simply not in the DB
                return None, "unresolved"
        return None, "unresolved"

    @staticmethod
    def _resolve_hostname(hostname: str) -> tuple[str | None, GeoSource]:
        suffix = hostname.rsplit(".", 1)[-1].lower()
        if suffix in CCTLD_COUNTRY:
            return CCTLD_COUNTRY[suffix], "tld"
        if suffix in US_TLDS:
            return "US", "tld"
        return None, "unresolved"


def enrich(parsed: dict[str, object], resolver: GeoResolver) -> dict[str, object]:
    """Add derived fields to a parsed event, in place."""
    client = parsed.get("client_ip")
    country, source = resolver.resolve(client if isinstance(client, str) else None)
    parsed["geo_country"] = country
    parsed["geo_source"] = source

    # Coarse path bucket: /shuttle/missions/sts-68/... -> /shuttle
    # Keeps dashboard group-by cardinality sane; full path stays queryable.
    path = parsed.get("path")
    if isinstance(path, str) and path.startswith("/"):
        head = path.lstrip("/").split("/", 1)[0].split("?", 1)[0]
        parsed["path_group"] = f"/{head}" if head else "/"
    else:
        parsed["path_group"] = None

    status = parsed.get("status_code")
    parsed["is_error"] = bool(isinstance(status, int) and status >= 500) or parsed.get("log_level") == "ERROR"
    return parsed
