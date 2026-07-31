#!/usr/bin/env python3
"""M0 smoke test: produce N messages to Kafka and read them back.

Proves the host-facing listener works, that a produce is durably acked, and
that a fresh consumer group can read from the beginning of a topic.

    python scripts/smoke_test.py
"""
from __future__ import annotations

import json
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:29092"
TOPIC = "smoke-test"
MESSAGE_COUNT = 10
CONSUME_TIMEOUT_S = 30


def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC in admin.list_topics(timeout=10).topics:
        return
    future = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])[TOPIC]
    future.result(timeout=15)
    print(f"created topic {TOPIC}")


def produce() -> list[dict]:
    delivered: list[dict] = []
    failures: list[str] = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))
        else:
            delivered.append({"partition": msg.partition(), "offset": msg.offset()})

    # acks=all is the durability setting the whole pipeline will use.
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    run_id = str(uuid.uuid4())[:8]

    for i in range(MESSAGE_COUNT):
        payload = json.dumps({"run_id": run_id, "seq": i, "sent_at": time.time()})
        producer.produce(TOPIC, key=f"key-{i}".encode(), value=payload.encode(), on_delivery=on_delivery)

    remaining = producer.flush(timeout=15)
    if remaining:
        raise RuntimeError(f"{remaining} message(s) never left the producer queue")
    if failures:
        raise RuntimeError(f"delivery failures: {failures}")

    print(f"produced {len(delivered)} messages (run_id={run_id})")
    return delivered


def consume() -> int:
    # Unique group id so every run reads from the beginning.
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"smoke-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    seen = 0
    deadline = time.time() + CONSUME_TIMEOUT_S
    try:
        while seen < MESSAGE_COUNT and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            body = json.loads(msg.value())
            print(f"  <- partition={msg.partition()} offset={msg.offset()} seq={body['seq']}")
            seen += 1
    finally:
        consumer.close()
    return seen


def main() -> int:
    print(f"bootstrap: {BOOTSTRAP}\n")
    try:
        ensure_topic()
        produce()
        print("consuming...")
        seen = consume()
    except Exception as exc:  # noqa: BLE001 - smoke test reports any failure plainly
        print(f"\nFAIL: {exc}", file=sys.stderr)
        return 1

    if seen < MESSAGE_COUNT:
        print(f"\nFAIL: consumed {seen}/{MESSAGE_COUNT} within {CONSUME_TIMEOUT_S}s", file=sys.stderr)
        return 1

    print(f"\nPASS: round-tripped {seen}/{MESSAGE_COUNT} messages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
