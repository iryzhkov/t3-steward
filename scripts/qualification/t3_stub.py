#!/usr/bin/env python3
"""Synthetic T3 provider for the disposable qualification harness.

The harness never runs a real agent turn. The fleet's Claude and Codex quota is
live and shared, and the qualification contract calls for synthetic provider
observations, so the worker processes are pointed at this stub instead of a real
T3 server.

The stub answers the few read-shaped requests a worker makes while it is being
constructed, and it appends every request it receives to a JSONL journal. That
journal is evidence rather than decoration: a case that claims no provider turn
was started is checked against it, so an accidental dispatch would be visible.

Usage: t3_stub.py PORT JOURNAL
"""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The version the pinned compatibility range accepts. The stub reports it so a
# worker's version gate behaves as it would against the deployed server.
SERVER_VERSION = "0.0.38"


class Journal:
    """Append-only record of every request, written under one lock."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()

    def record(self, entry):
        with self.lock:
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, sort_keys=True) + "\n")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        # The journal is the record; the default stderr line would duplicate it.
        return

    def _record(self, method, body):
        self.server.journal.record(
            {
                "method": method,
                "path": self.path,
                "body": body.decode("utf-8", "replace") if body else "",
            }
        )

    def _reply(self, status, payload):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):  # noqa: N802 - the base class fixes this name
        self._record("GET", b"")
        if self.path.startswith("/health"):
            self._reply(200, {"status": "ok", "version": SERVER_VERSION})
            return
        if "version" in self.path:
            self._reply(200, {"version": SERVER_VERSION})
            return
        if self.path.startswith("/.well-known/t3/environment"):
            self._reply(
                200,
                {
                    "version": SERVER_VERSION,
                    "serverVersion": SERVER_VERSION,
                    "projects": [],
                    "providers": [],
                },
            )
            return
        self._reply(200, {"threads": [], "sessions": [], "version": SERVER_VERSION})

    def do_POST(self):  # noqa: N802 - the base class fixes this name
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self._record("POST", body)
        # Nothing in the qualified cases dispatches a turn. A stub that invented
        # a thread identity would let an accidental dispatch look successful, so
        # every write-shaped request is refused instead.
        self._reply(
            501,
            {
                "error": "the synthetic qualification provider executes no turns",
                "version": SERVER_VERSION,
            },
        )


def main():
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    port = int(sys.argv[1])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.journal = Journal(sys.argv[2])
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
