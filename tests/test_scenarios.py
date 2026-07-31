"""Scenario-generator tests (M4).

These matter because the generator is the *only* way the FR4.2 alert can be
exercised — the real dataset is 0.002 % server errors. If the generator's error
rate is wrong, the alert demo silently proves nothing.
"""
from __future__ import annotations

import argparse
import random
import re

import pytest
from processor.parsers import parse_nginx_combined
from producer.replay import build_event
from producer.scenarios import SERVICE_PROFILE, Baseline, ErrorSpike, Malformed, make_line

STATUS_RE = re.compile(r'"\s+(\d{3})\s')


def _args(**overrides) -> argparse.Namespace:
    base = {
        "service": "checkout-api",
        "error_rate": 0.35,
        "background_error_rate": 0.004,
        "target_share": 0.5,
        "corrupt_rate": 0.05,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _take(scenario, n):
    out = []
    for item in scenario.lines():
        out.append(item)
        if len(out) >= n:
            break
    return out


# --- Generated lines must survive the real parser ---------------------------


@pytest.mark.parametrize("service", sorted(SERVICE_PROFILE))
def test_generated_lines_parse_with_the_production_parser(service):
    """A generator that emits lines the pipeline can't parse would send the
    whole demo to the DLQ instead of the dashboard."""
    rng = random.Random(7)
    for is_error in (False, True):
        event = parse_nginx_combined(make_line(rng, service, is_error=is_error))
        assert event["status_code"] is not None
        assert event["response_time_ms"] is not None
        assert event["path"].startswith("/")


def test_error_lines_carry_5xx_and_success_lines_do_not():
    rng = random.Random(11)
    for _ in range(50):
        assert int(STATUS_RE.search(make_line(rng, "checkout-api", is_error=True)).group(1)) >= 500
        assert int(STATUS_RE.search(make_line(rng, "checkout-api", is_error=False)).group(1)) < 500


def test_errors_are_modelled_as_slower_than_successes():
    """The latency panel is only meaningful if failure looks different."""
    rng = random.Random(3)
    errors = [parse_nginx_combined(make_line(rng, "checkout-api", is_error=True))["response_time_ms"]
              for _ in range(300)]
    ok = [parse_nginx_combined(make_line(rng, "checkout-api", is_error=False))["response_time_ms"]
          for _ in range(300)]
    assert sum(errors) / len(errors) > 3 * (sum(ok) / len(ok))


# --- Scenario error rates ----------------------------------------------------


def test_baseline_error_rate_is_low_and_close_to_configured():
    scenario = Baseline(_args(error_rate=0.004), random.Random(5))
    sample = _take(scenario, 20000)
    rate = sum(1 for _, _, is_error in sample if is_error) / len(sample)
    assert 0.001 < rate < 0.010, f"baseline error rate {rate:.4%} outside expected band"


def test_error_spike_targets_only_the_named_service():
    scenario = ErrorSpike(_args(error_rate=0.35, target_share=0.5), random.Random(5))
    sample = _take(scenario, 20000)

    target = [is_error for _, service, is_error in sample if service == "checkout-api"]
    others = [is_error for _, service, is_error in sample if service != "checkout-api"]

    target_rate = sum(target) / len(target)
    other_rate = sum(others) / len(others)

    # The point of the spike: one service far above the 5% threshold, the rest
    # far below it, so the alert can prove it names the right culprit.
    assert 0.30 < target_rate < 0.40, f"target rate {target_rate:.2%}"
    assert other_rate < 0.02, f"background rate {other_rate:.2%} would also trip the alert"


def test_error_spike_respects_target_share():
    scenario = ErrorSpike(_args(target_share=0.5), random.Random(5))
    sample = _take(scenario, 10000)
    share = sum(1 for _, service, _ in sample if service == "checkout-api") / len(sample)
    assert 0.45 < share < 0.55


# --- Malformed scenario ------------------------------------------------------


def test_malformed_scenario_emits_the_configured_corrupt_fraction():
    scenario = Malformed(_args(corrupt_rate=0.05), random.Random(5))
    sample = _take(scenario, 10000)
    corrupt = sum(1 for _, service, _ in sample if service == "corrupt-source")
    assert 0.04 < corrupt / len(sample) < 0.06


def test_every_corrupt_line_actually_fails_to_parse():
    """A 'corrupt' line the parser happily accepts would inflate parsed-logs
    and leave the DLQ empty — the opposite of what the scenario claims."""
    from producer.scenarios import CORRUPT_LINES

    for template in CORRUPT_LINES:
        line = template.replace("{ts}", "31/Jul/2026:10:00:00 +0000")
        with pytest.raises(Exception):
            parse_nginx_combined(line)


# --- Service override --------------------------------------------------------


def test_service_override_wins_over_path_derivation():
    """Scenario traffic must land on the service it names, not on whatever the
    path happens to map to, or the spike would be attributed to the wrong one."""
    line = make_line(random.Random(1), "checkout-api", is_error=True)
    event = build_event(line, source_format="nginx_combined", service_override="billing-api")
    assert event["service"] == "billing-api"


def test_no_override_falls_back_to_derivation():
    line = make_line(random.Random(1), "media-service", is_error=False)
    event = build_event(line, source_format="nginx_combined")
    assert event["service"] == "media-service"
