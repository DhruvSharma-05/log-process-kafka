"""Producer configuration. Environment variables override the defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

# Host-side clients use the PLAINTEXT_HOST listener. Anything running *inside*
# Docker Compose must use kafka:9092 instead. See docker-compose.yml.
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DEFAULT_TOPIC = os.getenv("RAW_TOPIC", "raw-logs")
DEFAULT_METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", "8001"))

# Salt for the client-IP hash. Overridden per environment; a fixed default keeps
# event_ids reproducible across local runs.
IP_HASH_SALT = os.getenv("IP_HASH_SALT", "logpipe-dev-salt")

# NASA logs carry no service or host field, so the replayer synthesises them
# from the request path. Keeps per-service dashboards and the `service`
# partition key meaningful. Documented as synthetic in the README.
SERVICE_MAP: dict[str, str] = {
    "shuttle": "shuttle-api",
    "images": "image-service",
    "icons": "image-service",
    "history": "history-api",
    "software": "software-api",
    "facilities": "facilities-api",
    "payloads": "payload-api",
    "biomed": "biomed-api",
    "elv": "launch-api",
    "procurement": "procurement-api",
    "persons": "directory-api",
    "htbin": "search-api",
    "cgi-bin": "search-api",
    "finance": "finance-api",
    "msfc": "portal-web",
    "ksc.html": "portal-web",
}

# Fallback pool for paths with no mapping — hashed, so a given path always
# lands on the same service.
FALLBACK_SERVICES: tuple[str, ...] = ("portal-web", "search-api", "media-service")

HOST_POOL: tuple[str, ...] = ("web-01", "web-02", "web-03", "web-04", "web-05", "web-06")


@dataclass
class ProducerConfig:
    bootstrap: str = DEFAULT_BOOTSTRAP
    topic: str = DEFAULT_TOPIC
    metrics_port: int = DEFAULT_METRICS_PORT

    def kafka_conf(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.bootstrap,
            # Durability: wait for all in-sync replicas. Idempotence removes
            # duplicates introduced by internal retries.
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            # Throughput: batch aggressively. linger.ms is the single biggest
            # lever for hitting the 5k events/sec target.
            "linger.ms": 20,
            "batch.size": 262144,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 512000,
            "message.timeout.ms": 30000,
            "client.id": "logpipe-producer",
        }


@dataclass
class ReplayOptions:
    """Knobs for how a source file is turned into events."""

    rate: int = 1000              # events/sec; 0 = as fast as possible
    limit: int | None = None      # stop after N events
    loop: bool = False            # restart the file when it ends
    rewrite_timestamps: bool = True
    synth_latency: bool = False   # NASA logs have no request time; opt-in synthetic
    tags: dict[str, str] = field(default_factory=dict)
