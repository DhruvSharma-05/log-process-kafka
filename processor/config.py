"""Stream-processor configuration. Environment variables override the defaults."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from common.kafka_security import security_conf

REPO_ROOT = Path(__file__).resolve().parent.parent

# Inside Docker Compose this becomes kafka:9092. See docker-compose.yml.
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

IN_TOPIC = os.getenv("RAW_TOPIC", "raw-logs")
PARSED_TOPIC = os.getenv("PARSED_TOPIC", "parsed-logs")
ERROR_TOPIC = os.getenv("ERROR_TOPIC", "error-logs")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead-letter-queue")

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "log-processor")
HTTP_PORT = int(os.getenv("PROCESSOR_HTTP_PORT", "8000"))

# Must match the producer so client_ip_hash is consistent across the pipeline.
IP_HASH_SALT = os.getenv("IP_HASH_SALT", "logpipe-dev-salt")

# Optional MaxMind GeoLite2 City/Country database. When absent, enrichment
# falls back to ccTLD inference — see enrich.py.
GEOIP_DB_PATH = os.getenv("GEOIP_DB_PATH", str(REPO_ROOT / "data" / "GeoLite2-Country.mmdb"))

SCHEMA_DIR = REPO_ROOT / "schemas"


@dataclass
class ProcessorConfig:
    bootstrap: str = DEFAULT_BOOTSTRAP
    in_topic: str = IN_TOPIC
    parsed_topic: str = PARSED_TOPIC
    error_topic: str = ERROR_TOPIC
    dlq_topic: str = DLQ_TOPIC
    group_id: str = CONSUMER_GROUP
    http_port: int = HTTP_PORT

    # How many records to pull per poll. Bigger batches amortise the produce +
    # flush + commit cycle, at the cost of more reprocessing after a crash.
    batch_size: int = 500
    poll_timeout: float = 1.0

    def consumer_conf(self) -> dict[str, object]:
        return {
            **security_conf(),
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            # At-least-once (FR2.5): offsets are committed by hand only after
            # the derived events are durably produced.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
            "fetch.min.bytes": 1,
            "client.id": "logpipe-processor",
        }

    def producer_conf(self) -> dict[str, object]:
        return {
            **security_conf(),
            "bootstrap.servers": self.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 262144,
            "queue.buffering.max.messages": 500000,
            "message.timeout.ms": 30000,
            "client.id": "logpipe-processor-out",
        }
