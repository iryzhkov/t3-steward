#!/usr/bin/env python3
"""Synthetic T3 provider for the disposable qualification harness.

The harness never runs a real agent turn. The fleet's Claude and Codex quota is
live and shared, and the qualification contract calls for synthetic provider
observations, so the worker processes are pointed at this stub instead of a real
T3 server.

What makes the lifecycle cases testable is that the turn behaviour is scripted.
When a turn starts, the stub looks for an executable at

    <turns-root>/<projectId>/turn<N>.sh

and runs it with the prepared workspace as its working directory. Turn 1 of a
parking scenario registers a task-bound wait and stops; turn 2, started by the
steward's own wake message, writes the declared outputs. Nothing about the
steward is simulated: the scripts run the real CLI against the real coordinator.

The stub answers the endpoints the worker and the watchdog actually use, and
appends every request to a JSONL journal so that a case which claims no turn was
dispatched can be checked rather than believed.

Usage: t3_stub.py PORT JOURNAL TURNS_ROOT
"""

import json
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The version the pinned compatibility range accepts.
SERVER_VERSION = "0.0.38"


def stamp():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Fleet:
    """Every thread the synthetic provider has been asked to run."""

    def __init__(self, journal_path, turns_root):
        self.lock = threading.Lock()
        self.threads = {}
        self.projects = {}
        self.journal_path = journal_path
        self.turns_root = turns_root
        self.sequence = 0

    def record(self, entry):
        with open(self.journal_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")

    def project(self, project_id):
        if project_id not in self.projects:
            self.projects[project_id] = {
                "id": project_id,
                "title": project_id,
                "workspaceRoot": "/",
                "defaultModelSelection": {},
            }
        return self.projects[project_id]

    def create(self, command):
        thread_id = command.get("threadId") or str(uuid.uuid4())
        project_id = command.get("projectId") or "unknown"
        self.project(project_id)
        self.threads[thread_id] = {
            "id": thread_id,
            "projectId": project_id,
            "title": command.get("title") or thread_id,
            "modelSelection": command.get("modelSelection") or {},
            "runtimeMode": command.get("runtimeMode") or "full-access",
            "interactionMode": command.get("interactionMode") or "default",
            "worktreePath": command.get("worktreePath"),
            "createdAt": stamp(),
            "updatedAt": stamp(),
            "archivedAt": None,
            "settledAt": None,
            "settledOverride": None,
            "deletedAt": None,
            "latestTurn": None,
            "session": {
                "threadId": thread_id,
                "status": "ready",
                "providerName": "synthetic",
                "providerInstanceId": "synthetic",
                "runtimeMode": "full-access",
                "activeTurnId": None,
                "lastError": None,
                "updatedAt": stamp(),
            },
            "latestUserMessageAt": None,
            "hasPendingApprovals": False,
            "hasPendingUserInput": False,
            "hasActionableProposedPlan": False,
            "backgroundLiveness": None,
            "messages": [],
            "turns": 0,
        }
        return thread_id

    def start_turn(self, command):
        thread_id = command.get("threadId")
        thread = self.threads.get(thread_id)
        if thread is None:
            return None
        message = command.get("message") or {}
        thread["turns"] += 1
        turn_number = thread["turns"]
        turn_id = "turn-%s-%d" % (thread_id, turn_number)
        started = stamp()
        thread["latestTurn"] = {
            "turnId": turn_id,
            "state": "running",
            "requestedAt": started,
            "startedAt": started,
            "completedAt": None,
        }
        thread["session"]["status"] = "running"
        thread["session"]["activeTurnId"] = turn_id
        thread["latestUserMessageAt"] = started
        thread["updatedAt"] = started
        thread["messages"].append(
            {
                "id": message.get("messageId") or str(uuid.uuid4()),
                "role": "user",
                "text": message.get("text") or "",
                "createdAt": started,
            }
        )
        # A settled thread that is asked for another turn is live again. This is
        # what a delivered wake does to a parked attempt's thread.
        thread["settledAt"] = None
        thread["settledOverride"] = None
        return thread_id, turn_number, turn_id

    def finish_turn(self, thread_id, turn_id, text):
        thread = self.threads.get(thread_id)
        if thread is None:
            return
        completed = stamp()
        if thread["latestTurn"] and thread["latestTurn"]["turnId"] == turn_id:
            thread["latestTurn"]["state"] = "completed"
            thread["latestTurn"]["completedAt"] = completed
        thread["session"]["status"] = "ready"
        thread["session"]["activeTurnId"] = None
        thread["updatedAt"] = completed
        thread["messages"].append(
            {
                "id": str(uuid.uuid4()),
                "role": "assistant",
                "text": text,
                "createdAt": completed,
            }
        )

    def settle(self, thread_id):
        thread = self.threads.get(thread_id)
        if thread is None:
            return False
        thread["settledAt"] = stamp()
        thread["settledOverride"] = "settled"
        thread["updatedAt"] = thread["settledAt"]
        return True

    def shell(self):
        threads = []
        for thread in self.threads.values():
            shell = {key: value for key, value in thread.items() if key not in ("messages", "turns", "worktreePath")}
            threads.append(shell)
        return {
            "snapshotSequence": self.sequence,
            "projects": list(self.projects.values()),
            "threads": threads,
            "updatedAt": stamp(),
        }

    def detail(self, thread_id):
        thread = self.threads.get(thread_id)
        if thread is None:
            return None
        body = {key: value for key, value in thread.items() if key not in ("turns", "worktreePath")}
        return {"thread": body}


def run_turn(fleet, thread_id, turn_number, turn_id):
    """Run the scripted turn, then complete it.

    The script is the agent. It runs with the prepared workspace as its working
    directory, exactly where a real agent would be, and whatever it leaves
    behind is what the worker will find.
    """
    with fleet.lock:
        thread = fleet.threads.get(thread_id)
        worktree = thread.get("worktreePath") if thread else None
        project = thread.get("projectId") if thread else None
    script = os.path.join(fleet.turns_root, project or "", "turn%d.sh" % turn_number)
    summary = "synthetic turn %d completed" % turn_number
    if os.path.isfile(script) and worktree and os.path.isdir(worktree):
        try:
            completed = subprocess.run(
                ["/bin/sh", script],
                cwd=worktree,
                env={
                    "PATH": "/usr/bin:/bin",
                    "T3_QUAL_WORKSPACE": worktree,
                    "T3_QUAL_THREAD": thread_id,
                    "T3_QUAL_TURN": str(turn_number),
                },
                capture_output=True,
                text=True,
                timeout=300,
            )
            summary = (completed.stdout or "").strip() or summary
            fleet.record(
                {
                    "event": "turn-script",
                    "thread": thread_id,
                    "turn": turn_number,
                    "script": script,
                    "exit": completed.returncode,
                    "stdout": (completed.stdout or "")[-4000:],
                    "stderr": (completed.stderr or "")[-4000:],
                }
            )
        except Exception as failure:  # noqa: BLE001 - the stub must not die
            fleet.record({"event": "turn-script-error", "thread": thread_id, "error": str(failure)})
            summary = "backlog status: failed synthetic turn script error"
    else:
        fleet.record({"event": "turn-script-absent", "thread": thread_id, "turn": turn_number, "script": script})
    with fleet.lock:
        fleet.finish_turn(thread_id, turn_id, summary)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args, **_kwargs):
        return

    @property
    def fleet(self):
        return self.server.fleet

    def _reply(self, status, payload):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):  # noqa: N802 - the base class fixes this name
        path = self.path.split("?", 1)[0]
        self.fleet.record({"method": "GET", "path": self.path})
        if path.startswith("/health"):
            self._reply(200, {"status": "ok", "version": SERVER_VERSION})
            return
        if path.startswith("/.well-known/t3/environment"):
            self._reply(
                200,
                {
                    "environmentId": "qualification",
                    "label": "disposable qualification",
                    "platform": {"os": "linux", "arch": "amd64"},
                    "serverVersion": SERVER_VERSION,
                    "capabilities": {},
                },
            )
            return
        if path.startswith("/api/auth/session"):
            self._reply(200, {"authenticated": True, "scopes": ["orchestration:read", "orchestration:operate"]})
            return
        if path.startswith("/api/orchestration/shell") or path.startswith("/api/orchestration/snapshot"):
            with self.fleet.lock:
                self._reply(200, self.fleet.shell())
            return
        if path.startswith("/api/orchestration/threads/"):
            thread_id = path[len("/api/orchestration/threads/") :]
            with self.fleet.lock:
                detail = self.fleet.detail(thread_id)
            if detail is None:
                self._reply(404, {"error": "no such thread"})
                return
            self._reply(200, detail)
            return
        self._reply(404, {"error": "the synthetic qualification provider does not serve " + path})

    def do_POST(self):  # noqa: N802 - the base class fixes this name
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        try:
            command = json.loads(body or b"{}")
        except Exception:  # noqa: BLE001
            command = {}
        self.fleet.record({"method": "POST", "path": self.path, "command": command})
        if not self.path.startswith("/api/orchestration/dispatch"):
            self._reply(404, {"error": "the synthetic qualification provider does not serve " + self.path})
            return
        kind = command.get("type")
        started = None
        with self.fleet.lock:
            self.fleet.sequence += 1
            sequence = self.fleet.sequence
            if kind == "thread.create":
                self.fleet.create(command)
            elif kind == "thread.turn.start":
                started = self.fleet.start_turn(command)
                if started is None:
                    self._reply(404, {"error": "no such thread"})
                    return
            elif kind == "thread.settle":
                if not self.fleet.settle(command.get("threadId")):
                    self._reply(404, {"error": "no such thread"})
                    return
            elif kind in ("thread.turn.interrupt", "thread.session.stop"):
                thread = self.fleet.threads.get(command.get("threadId"))
                if thread and thread["latestTurn"]:
                    thread["latestTurn"]["state"] = "interrupted"
                    thread["latestTurn"]["completedAt"] = stamp()
                    thread["session"]["status"] = "ready"
                    thread["session"]["activeTurnId"] = None
            elif kind == "thread.delete":
                self.fleet.threads.pop(command.get("threadId"), None)
        if started is not None:
            thread_id, turn_number, turn_id = started
            threading.Thread(
                target=run_turn, args=(self.fleet, thread_id, turn_number, turn_id), daemon=True
            ).start()
        self._reply(200, {"sequence": sequence})


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    port = int(sys.argv[1])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.fleet = Fleet(sys.argv[2], sys.argv[3])
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
