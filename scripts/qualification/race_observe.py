#!/usr/bin/env python3
"""Atomic race samples and strict, identity-bound case 12 verdicts."""
import datetime
import json
from pathlib import Path
import sqlite3
import sys

def sample(database, run, output):
    with sqlite3.connect("file:" + database + "?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        attempts = [json.loads(row[0]) for row in db.execute(
            "SELECT record FROM coordinator_attempts")]
        attempts = [a for a in attempts if a["workflowRunId"] == run]
        waits = [json.loads(row[0]) for row in db.execute(
            "SELECT record FROM coordinator_task_waits")]
        waits = [w for w in waits if w["workflowRunId"] == run]
    row = {"observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "run": run, "attempts": attempts, "waits": waits}
    log = Path(output).parent / "t3-stub.jsonl"
    ended = False
    if len(attempts) == 1 and log.exists():
        for line in log.read_text().splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") == "turn-script" and event.get("thread") == attempts[0].get("threadId") and event.get("turn") == 1:
                ended = True
    row["firstTurnEnded"] = ended
    with open(output, "a") as target:
        target.write(json.dumps(row) + "\n")
    print(attempts[0]["progress"] if len(attempts) == 1 else "unobserved", "true" if ended else "false")

def verdict(evidence, signal, bias, run):
    root = Path(evidence)
    rows = [json.loads(line) for line in (root / (signal + "-samples.jsonl")).read_text().splitlines()]
    registration = (root / (signal + "-register.txt")).read_text()
    check_started = root / (signal + "-check-started.txt")
    expected_id = (root / (signal + "-attempt.txt")).read_text().strip()
    errors = []
    if not check_started.exists():
        errors.append("registration first check was not observed")
    attempts = [a for row in rows for a in row["attempts"]]
    identities = {(a["id"], a.get("threadId")) for a in attempts if a.get("threadId")}
    if len(identities) != 1 or next(iter(identities))[0] != expected_id:
        errors.append("missing or changing attempt/thread identity")
    for row in rows:
        for a in row["attempts"]:
            bound = [w for w in row["waits"] if w["attemptId"] == a["id"]]
            if a["progress"] == "waiting-external" and (a.get("completedAt") or a["control"] != "waiting-external"):
                errors.append("parked attempt carries terminal evidence")
            if a["progress"] in ("succeeded", "failed") and any(not w.get("settledAt") for w in bound):
                errors.append("terminal attempt still owns an unsettled park")
    final = rows[-1]
    if len(final["attempts"]) != 1 or final["attempts"][0]["progress"] != "succeeded":
        errors.append("iteration did not succeed")
    parked = "This task is now parked" in registration and "exit=0" in registration
    refused = "attempt is terminal (succeeded); task-bound waits are refused" in registration and "exit=1" in registration
    ordering = "unproven"
    if bias == "registration-first":
        if not parked or not any(row["attempts"] and row["attempts"][0]["progress"] == "waiting-external" and row["firstTurnEnded"] for row in rows):
            errors.append("registration-before-completion not observed")
        elif len(final["waits"]) != 1 or final["waits"][0].get("delivery") != "delivered" or final["waits"][0].get("result", {}).get("outcome") != "met":
            errors.append("registered wait did not settle and deliver")
        else:
            ordering = "registration-before-completion"
    else:
        gate = root / (signal + "-release.json")
        if not gate.exists() or json.loads(gate.read_text()).get("progress") != "succeeded" or not refused or final["waits"]:
            errors.append("completion-before-registration not observed")
        else:
            ordering = "completion-before-registration"
    detail = json.loads((root / (signal + "-final.json")).read_text())
    tasks = [t for t in detail.get("workflow", {}).get("tasks", []) if not t.get("sink")]
    if len(tasks) != 1 or tasks[0].get("attempt", {}).get("id") != expected_id:
        errors.append("final admin evidence names wrong attempt")
    else:
        artifacts = [a.get("metadata", a) for a in tasks[0].get("artifacts", [])]
        if not any(a.get("kind") == "output" for a in artifacts):
            errors.append("successful task has no collected output")
        if not any(a.get("kind") == "verification" for a in artifacts):
            errors.append("successful task has no verification evidence")
    result = {"run": run, "signal": signal, "bias": bias, "attemptId": expected_id,
              "threadId": next(iter(identities))[1] if len(identities) == 1 else None,
              "ordering": ordering, "registration": "parked" if parked else "refused" if refused else "unproven",
              "errors": sorted(set(errors)), "passed": not errors}
    (root / (signal + "-verdict.json")).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0 if not errors else 1

if sys.argv[1] == "sample":
    sample(*sys.argv[2:])
else:
    sys.exit(verdict(*sys.argv[2:]))
