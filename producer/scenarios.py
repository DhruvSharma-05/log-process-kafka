#!/usr/bin/env python3
"""Scenario generator — synthetic traffic with controllable failure modes.

This exists because the NASA dataset cannot exercise the alerting path. Across
all 1,569,898 lines it contains **30 server errors (0.002 %)**, while FR4.2's
alert fires above 5 % over 5 minutes. Replaying real traffic therefore proves
nothing about alerting; the spike has to be manufactured, on demand, at a known
time, against a named service.

    # Steady multi-service background traffic (~0.4 % errors)
    python -m producer.scenarios --scenario baseline --rate 200 --duration 600

    # Trip the FR4.2 alert: 35 % errors on one service for 5 minutes
    python -m producer.scenarios --scenario error-spike --service checkout-api \
        --error-rate 0.35 --rate 150 --duration 300

    # Prove the DLQ under load: 5 % unparseable lines
    python -m producer.scenarios --scenario malformed --corrupt-rate 0.05 --duration 120

    # Consumer-lag drain test (M5/M6): 2x traffic for 60s
    python -m producer.scenarios --scenario burst --rate 4000 --duration 60

All output is clearly synthetic: reserved-documentation IPs (RFC 5737) and
hostnames under example.* domains.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from collections.abc import Iterator
from datetime import datetime, timezone

from confluent_kafka import Producer

from .config import ProducerConfig
from .replay import build_event

# Services and the paths they serve. Weights approximate a real traffic mix:
# a couple of high-volume services and a long tail.
SERVICE_PROFILE: dict[str, tuple[int, tuple[str, ...]]] = {
    "checkout-api": (10, ("/checkout", "/checkout/cart", "/checkout/pay", "/checkout/confirm")),
    "catalog-api": (30, ("/catalog", "/catalog/search", "/catalog/item/8891", "/catalog/browse")),
    "auth-api": (12, ("/auth/login", "/auth/token", "/auth/refresh")),
    "inventory-api": (8, ("/inventory/stock", "/inventory/reserve")),
    "media-service": (25, ("/media/img/hero.jpg", "/media/img/thumb.png", "/media/video/promo.mp4")),
    "billing-api": (5, ("/billing/invoice", "/billing/charge")),
    "portal-web": (10, ("/", "/about", "/help")),
}

METHODS_BY_PATH = {"/checkout/pay": "POST", "/auth/login": "POST", "/checkout": "POST"}

# RFC 5737 documentation ranges + example.* hostnames: unmistakably synthetic.
CLIENTS: tuple[str, ...] = (
    "192.0.2.14", "192.0.2.55", "198.51.100.23", "203.0.113.87",
    "client-a.example.jp", "client-b.example.co.uk", "client-c.example.de",
    "crawler.example.com", "mobile.example.fr",
)

USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    "curl/8.4.0",
    "checkout-client/2.1",
)

SUCCESS_STATUSES = ((200, 88), (304, 7), (302, 3), (404, 2))
ERROR_STATUSES = ((500, 60), (503, 25), (502, 12), (504, 3))

# Lines that must fail parsing, one per failure mode the DLQ should distinguish.
CORRUPT_LINES: tuple[str, ...] = (
    '192.0.2.14 - - [{ts}] "GET /truncated',
    '192.0.2.14 - - {ts} "GET / HTTP/1.1" 200 10',
    '192.0.2.14 - - [{ts}] "GET / HTTP/1.1" 20 10',
    '192.0.2.14 - - [{ts}] "" 200 10',
    "Jun 14 15:16:01 combo sshd(pam_unix)[19939]: check pass; user unknown",
    "\x00\xff garbage \xfe\x00",
    '{"partial": "json without a timestamp"}',
    "",
)


def _weighted(rng: random.Random, options: tuple[tuple[object, int], ...]):
    values = [value for value, _ in options]
    weights = [weight for _, weight in options]
    return rng.choices(values, weights=weights, k=1)[0]


def _pick_service(rng: random.Random) -> str:
    names = list(SERVICE_PROFILE)
    weights = [SERVICE_PROFILE[name][0] for name in names]
    return rng.choices(names, weights=weights, k=1)[0]


def _latency_ms(rng: random.Random, status: int) -> float:
    """Errors are slower than successes — that is what makes a latency panel useful."""
    if status >= 500:
        return min(rng.lognormvariate(6.9, 0.6), 30000)   # ~1s median, long tail
    if status >= 400:
        return min(rng.lognormvariate(4.0, 0.5), 5000)
    return min(rng.lognormvariate(4.4, 0.7), 8000)


def make_line(rng: random.Random, service: str, *, is_error: bool) -> str:
    """Render one Combined Log Format line with a trailing $request_time."""
    _, paths = SERVICE_PROFILE[service]
    path = rng.choice(paths)
    method = METHODS_BY_PATH.get(path, "GET")
    status = _weighted(rng, ERROR_STATUSES if is_error else SUCCESS_STATUSES)
    latency_ms = _latency_ms(rng, status)
    size = 0 if status in (304, 500, 503) else rng.randint(120, 48000)
    stamp = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
    return (
        f"{rng.choice(CLIENTS)} - - [{stamp}] "
        f'"{method} {path} HTTP/1.1" {status} {size} '
        f'"https://shop.example.com{path}" "{rng.choice(USER_AGENTS)}" '
        f"{latency_ms / 1000:.3f}"
    )


class Scenario:
    """Yields (line, service, is_error) triples until its duration elapses."""

    def __init__(self, args: argparse.Namespace, rng: random.Random) -> None:
        self.args = args
        self.rng = rng

    def lines(self) -> Iterator[tuple[str, str, bool]]:
        raise NotImplementedError


class Baseline(Scenario):
    """Healthy multi-service traffic with a low, realistic error rate."""

    def lines(self):
        while True:
            service = _pick_service(self.rng)
            is_error = self.rng.random() < self.args.error_rate
            yield make_line(self.rng, service, is_error=is_error), service, is_error


class ErrorSpike(Scenario):
    """One service degrades; everything else stays healthy.

    Realism matters for the demo: if *every* service errors at once, the
    dashboard cannot show the per-service isolation that FR4.1 is about, and
    the alert cannot demonstrate that it names the right culprit.
    """

    def lines(self):
        target = self.args.service
        while True:
            if self.rng.random() < self.args.target_share:
                is_error = self.rng.random() < self.args.error_rate
                yield make_line(self.rng, target, is_error=is_error), target, is_error
            else:
                other = _pick_service(self.rng)
                while other == target:
                    other = _pick_service(self.rng)
                is_error = self.rng.random() < self.args.background_error_rate
                yield make_line(self.rng, other, is_error=is_error), other, is_error


class Malformed(Scenario):
    """Healthy traffic with a measured fraction of unparseable lines."""

    def lines(self):
        while True:
            if self.rng.random() < self.args.corrupt_rate:
                stamp = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
                yield self.rng.choice(CORRUPT_LINES).replace("{ts}", stamp), "corrupt-source", False
            else:
                service = _pick_service(self.rng)
                yield make_line(self.rng, service, is_error=False), service, False


class Burst(Scenario):
    """Sustained high-rate healthy traffic, to build and then drain consumer lag."""

    def lines(self):
        while True:
            service = _pick_service(self.rng)
            yield make_line(self.rng, service, is_error=False), service, False


SCENARIOS = {
    "baseline": Baseline,
    "error-spike": ErrorSpike,
    "malformed": Malformed,
    "burst": Burst,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic traffic with controllable failure modes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    parser.add_argument("--rate", type=int, default=200, help="Events/sec (default: 200)")
    parser.add_argument("--duration", type=int, default=300, help="Seconds to run (default: 300)")
    parser.add_argument("--service", default="checkout-api", help="Target service for error-spike")
    parser.add_argument(
        "--error-rate",
        type=float,
        default=None,
        help="Error fraction. Default: 0.004 for baseline, 0.35 for error-spike",
    )
    parser.add_argument(
        "--background-error-rate",
        type=float,
        default=0.004,
        help="Error fraction for non-target services during a spike (default: 0.004)",
    )
    parser.add_argument(
        "--target-share",
        type=float,
        default=0.5,
        help="Fraction of spike traffic aimed at the target service (default: 0.5)",
    )
    parser.add_argument("--corrupt-rate", type=float, default=0.05, help="Unparseable fraction")
    parser.add_argument("--seed", type=int, default=None, help="Seed for reproducible runs")
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--topic", default=None)

    args = parser.parse_args(argv)
    if args.error_rate is None:
        args.error_rate = 0.35 if args.scenario == "error-spike" else 0.004
    return args


def run(args: argparse.Namespace) -> int:
    config = ProducerConfig()
    if args.bootstrap:
        config.bootstrap = args.bootstrap
    if args.topic:
        config.topic = args.topic

    rng = random.Random(args.seed)
    scenario = SCENARIOS[args.scenario](args, rng)
    producer = Producer(config.kafka_conf())

    sent = 0
    errors_sent = 0
    failed = 0

    def on_delivery(err, _msg) -> None:
        nonlocal failed
        if err is not None:
            failed += 1

    print(f"scenario:  {args.scenario}")
    print(f"topic:     {config.topic} @ {config.bootstrap}")
    print(f"rate:      {args.rate}/s for {args.duration}s")
    if args.scenario == "error-spike":
        print(f"target:    {args.service} at {args.error_rate:.0%} errors "
              f"({args.target_share:.0%} of traffic)")
    elif args.scenario == "malformed":
        print(f"corrupt:   {args.corrupt_rate:.1%} of lines")
    print()

    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False
        print("\nstopping...", flush=True)

    signal.signal(signal.SIGINT, stop)

    started = time.perf_counter()
    deadline = started + args.duration
    chunk = max(1, args.rate // 50)
    last_report = started

    for line, service, is_error in scenario.lines():
        now = time.perf_counter()
        if not running or now >= deadline:
            break

        event = build_event(
            line,
            source_format="nginx_combined",
            rewrite=False,          # scenario lines are already stamped with now
            service_override=service,
        )
        if event is None:
            # Deliberately blank corrupt line: send it anyway so the DLQ sees it.
            event = {
                "event_id": f"blank-{sent}",
                "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "host": "synthetic",
                "service": service or "corrupt-source",
                "source_format": "nginx_combined",
                "raw_message": " ",
            }

        while True:
            try:
                producer.produce(
                    config.topic,
                    key=str(event["service"]).encode(),
                    value=json.dumps(event, separators=(",", ":")).encode(),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)

        sent += 1
        errors_sent += is_error
        producer.poll(0)

        if sent % chunk == 0:
            target_time = started + sent / args.rate
            drift = target_time - time.perf_counter()
            if drift > 0:
                time.sleep(drift)

        if now - last_report >= 5.0:
            elapsed = now - started
            print(
                f"  {elapsed:6.1f}s  sent={sent:>8,}  errors={errors_sent:>7,}  "
                f"remaining={max(0, deadline - now):5.0f}s",
                flush=True,
            )
            last_report = now

    remaining = producer.flush(timeout=30)
    elapsed = time.perf_counter() - started
    print()
    print(f"sent {sent:,} events in {elapsed:.1f}s ({sent / elapsed:,.0f}/s)")
    if args.scenario in ("baseline", "error-spike"):
        print(f"of which ~{errors_sent:,} were 5xx ({100 * errors_sent / max(sent, 1):.1f}%)")
    if failed or remaining:
        print(f"WARNING: {failed} delivery failures, {remaining} unflushed", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
