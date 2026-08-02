"""Kafka client security settings (Security NFR).

The dev profile runs PLAINTEXT; the production profile
(`docker-compose.prod.yml`) runs SASL. Switching between them is environment
configuration, not a code change — which is the property the NFR actually
asks for:

    # dev (default)
    python -m producer.main --file ...

    # production profile
    KAFKA_BOOTSTRAP=localhost:39092 \
    KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT \
    KAFKA_SASL_USERNAME=logpipe \
    KAFKA_SASL_PASSWORD=logpipe-secret \
    python -m producer.main --file ...
"""
from __future__ import annotations

import os

VALID_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}


def security_conf() -> dict[str, object]:
    """Build librdkafka security settings from the environment.

    Returns an empty dict for plain local development, so the dev path stays
    exactly as it was before this existed.
    """
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if protocol not in VALID_PROTOCOLS:
        raise ValueError(
            f"KAFKA_SECURITY_PROTOCOL={protocol!r} is not one of {sorted(VALID_PROTOCOLS)}"
        )
    if protocol == "PLAINTEXT":
        return {}

    conf: dict[str, object] = {"security.protocol": protocol}

    if protocol.startswith("SASL"):
        username = os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_SASL_PASSWORD")
        if not username or not password:
            raise ValueError(
                f"{protocol} requires KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"
            )
        conf["sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        conf["sasl.username"] = username
        conf["sasl.password"] = password

    if protocol.endswith("SSL"):
        ca_location = os.getenv("KAFKA_SSL_CA_LOCATION")
        if ca_location:
            conf["ssl.ca.location"] = ca_location
        # Only for self-signed certificates in a test environment.
        if os.getenv("KAFKA_SSL_SKIP_VERIFY", "0") == "1":
            conf["enable.ssl.certificate.verification"] = False

    return conf
