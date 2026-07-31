"""Health and metrics endpoints (FR5.3, Observability NFR).

One HTTP server on a single port serves all three:

  GET /healthz   process is alive           -> 200 always (liveness)
  GET /readyz    consuming recently         -> 200 / 503   (readiness)
  GET /metrics   Prometheus exposition

`/readyz` distinguishes "alive" from "actually working": a processor that is
running but has not completed a poll within `stale_after` seconds is reporting
itself unready, which is what a container orchestrator needs to act on.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


class HealthState:
    """Shared, thread-safe view of processor liveness."""

    def __init__(self, stale_after: float = 60.0) -> None:
        self.stale_after = stale_after
        self._lock = threading.Lock()
        self._started = time.time()
        self._last_poll = 0.0
        self._assigned = 0
        self._shutting_down = False

    def mark_poll(self) -> None:
        with self._lock:
            self._last_poll = time.time()

    def set_assigned(self, count: int) -> None:
        with self._lock:
            self._assigned = count

    def begin_shutdown(self) -> None:
        with self._lock:
            self._shutting_down = True

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            last_poll, assigned, shutting_down = self._last_poll, self._assigned, self._shutting_down
            uptime = time.time() - self._started
        age = time.time() - last_poll if last_poll else None
        ready = (
            not shutting_down
            and last_poll > 0
            and age is not None
            and age < self.stale_after
        )
        return {
            "ready": ready,
            "uptime_seconds": round(uptime, 1),
            "seconds_since_last_poll": round(age, 1) if age is not None else None,
            "assigned_partitions": assigned,
            "shutting_down": shutting_down,
        }


def _make_handler(state: HealthState):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _respond(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            path = self.path.split("?", 1)[0].rstrip("/") or "/"
            if path == "/metrics":
                self._respond(200, generate_latest(), CONTENT_TYPE_LATEST)
            elif path == "/healthz":
                self._respond(200, b'{"status":"alive"}', "application/json")
            elif path == "/readyz":
                snapshot = state.snapshot()
                body = json.dumps(snapshot).encode()
                self._respond(200 if snapshot["ready"] else 503, body, "application/json")
            elif path == "/":
                self._respond(200, b"logpipe processor: /healthz /readyz /metrics\n", "text/plain")
            else:
                self._respond(404, b"not found\n", "text/plain")

        def log_message(self, *_args) -> None:
            """Silence per-request logging — health probes would flood stdout."""

    return Handler


class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that does not log client disconnects.

    Health probes routinely open a keep-alive connection and hang up without
    reading the response. socketserver treats that as an unhandled error and
    dumps a traceback per probe, which would bury real errors in the log.
    """

    daemon_threads = True

    def handle_error(self, request, client_address) -> None:
        exception = sys.exc_info()[1]
        if isinstance(exception, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def start_http_server(port: int, state: HealthState) -> ThreadingHTTPServer:
    server = _QuietThreadingHTTPServer(("0.0.0.0", port), _make_handler(state))
    thread = threading.Thread(target=server.serve_forever, name="health-http", daemon=True)
    thread.start()
    return server
