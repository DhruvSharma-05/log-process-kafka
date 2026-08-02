#!/usr/bin/env python3
"""Production-profile verification (M6).

Checks the two NFRs the dev stack cannot demonstrate:

  Security    SASL client authentication is enforced — anonymous and
              wrong-password clients are refused, valid credentials work.
  Durability  With RF=3 / min.insync.replicas=2 and acks=all, every acked
              record survives; a broker can be killed mid-write without loss.

    # Auth checks only
    python scripts/prod_smoke.py --auth

    # Durability: produce continuously so a broker can be killed mid-run
    python scripts/prod_smoke.py --durability --count 60000

Requires the production profile:
    docker compose -p logpipe-prod -f docker-compose.prod.yml up -d --wait
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP = "localhost:39092"
TOPIC = "raw-logs"
VALID = {"username": "logpipe", "password": "logpipe-secret"}


def _sasl_conf(username: str | None, password: str | None) -> dict[str, object]:
    conf: dict[str, object] = {"bootstrap.servers": BOOTSTRAP}
    if username is not None:
        conf.update(
            {
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": username,
                "sasl.password": password,
            }
        )
    return conf


def _can_connect(conf: dict[str, object], timeout: float = 8.0) -> tuple[bool, str]:
    """True if the client can fetch cluster metadata."""
    try:
        admin = AdminClient({**conf, "socket.timeout.ms": 5000})
        metadata = admin.list_topics(timeout=timeout)
        return True, f"{len(metadata.brokers)} broker(s) visible"
    except KafkaException as exc:
        return False, str(exc.args[0])
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def check_auth() -> int:
    print("=== SASL authentication (Security NFR) ===\n")
    cases = [
        ("anonymous / no SASL", _sasl_conf(None, None), False),
        ("wrong password", _sasl_conf("logpipe", "wrong-password"), False),
        ("unknown user", _sasl_conf("nobody", "whatever"), False),
        ("valid credentials", _sasl_conf(VALID["username"], VALID["password"]), True),
    ]

    failures = 0
    for name, conf, should_connect in cases:
        ok, detail = _can_connect(conf)
        passed = ok == should_connect
        verdict = "PASS" if passed else "FAIL"
        expectation = "accepted" if should_connect else "refused"
        actual = "accepted" if ok else "refused"
        print(f"  [{verdict}] {name:<22} expected {expectation:<8} got {actual:<8} ({detail[:60]})")
        if not passed:
            failures += 1

    print()
    print("auth checks:", "all passed" if failures == 0 else f"{failures} FAILED")
    return 1 if failures else 0


def check_durability(count: int, rate: int = 0) -> int:
    """Produce `count` records with acks=all, then read them all back.

    Use --rate to stretch the produce phase out. Without it the run finishes in
    a couple of seconds and any broker you kill will die *after* the writes are
    done, which proves far less than it appears to.

    Kill a broker while this runs:
        docker compose -p logpipe-prod -f docker-compose.prod.yml stop kafka-2
    """
    pacing = f" at {rate:,}/s (~{count / rate:.0f}s)" if rate > 0 else " unthrottled"
    print(f"=== Durability: producing {count:,} records with acks=all{pacing} ===\n")
    run_id = str(uuid.uuid4())[:8]

    acked = 0
    failed: list[str] = []

    def on_delivery(err, _msg) -> None:
        nonlocal acked
        if err is not None:
            failed.append(str(err))
        else:
            acked += 1

    producer = Producer(
        {
            **_sasl_conf(VALID["username"], VALID["password"]),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "message.timeout.ms": 60000,
            # Keep retrying through a leader election rather than giving up.
            "retries": 2147483647,
            "retry.backoff.ms": 100,
        }
    )

    started = time.perf_counter()
    chunk = max(1, rate // 50) if rate > 0 else 0
    for i in range(count):
        payload = json.dumps({"run_id": run_id, "seq": i}).encode()
        while True:
            try:
                producer.produce(TOPIC, key=str(i % 6).encode(), value=payload, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

        if chunk and (i + 1) % chunk == 0:
            drift = started + (i + 1) / rate - time.perf_counter()
            if drift > 0:
                time.sleep(drift)

        if (i + 1) % 10000 == 0:
            print(
                f"  t+{time.perf_counter() - started:5.1f}s  produced {i + 1:,}  "
                f"acked {acked:,}  failed {len(failed)}",
                flush=True,
            )

    remaining = producer.flush(timeout=120)
    elapsed = time.perf_counter() - started

    print()
    print(f"produced {count:,} in {elapsed:.1f}s")
    print(f"acked    {acked:,}")
    print(f"failed   {len(failed):,}")
    print(f"unflushed {remaining:,}")
    if failed:
        print(f"first error: {failed[0]}")

    # Read back and count only this run's records.
    print(f"\nreading back records for run_id={run_id} ...")
    consumer = Consumer(
        {
            **_sasl_conf(VALID["username"], VALID["password"]),
            "group.id": f"durability-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    seen: set[int] = set()
    idle_since = time.time()
    try:
        while time.time() - idle_since < 20:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"  consumer error: {msg.error()}", file=sys.stderr)
                continue
            try:
                body = json.loads(msg.value())
            except (ValueError, TypeError):
                continue
            if body.get("run_id") == run_id:
                seen.add(body["seq"])
                idle_since = time.time()
    finally:
        consumer.close()

    print(f"read back {len(seen):,} distinct records of {acked:,} acked")
    missing = acked - len(seen)
    print()
    if len(failed) == 0 and remaining == 0 and missing <= 0:
        print(f"PASS: {acked:,} acked, {len(seen):,} readable, zero loss")
        return 0
    print(f"FAIL: {len(failed)} delivery failures, {remaining} unflushed, {missing} acked-but-missing")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the production Kafka profile.")
    parser.add_argument("--auth", action="store_true", help="Run SASL authentication checks")
    parser.add_argument("--durability", action="store_true", help="Run the acks=all durability test")
    parser.add_argument("--count", type=int, default=60000, help="Records for the durability test")
    parser.add_argument(
        "--rate",
        type=int,
        default=0,
        help="Records/sec during the durability test; 0 = unthrottled. "
        "Throttle it so a broker can be killed while writes are still in flight.",
    )
    args = parser.parse_args()

    if not args.auth and not args.durability:
        args.auth = args.durability = True

    status = 0
    if args.auth:
        status |= check_auth()
        print()
    if args.durability:
        status |= check_durability(args.count, args.rate)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
