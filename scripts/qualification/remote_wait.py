#!/usr/bin/env python3
"""Disposable setup, host-bound condition, and strict remote-wait evidence."""
import datetime
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys

def configure(root, ports):
    worker = root / "worker-a"
    config = worker / "config.yaml"
    with config.open("a") as out:
        out.write("""
  coordinator_client:
    coordinator_id: qual-coordinator
    address: qual-admin
    connection: ssh
    remote_command: """ + str(root / "bin/t3-steward") + """
    credential: secretref:f03-admin/qual-client
    request_timeout: 30s
    message_limits:
      max_bytes: 4194304
      max_files: 1000
      max_artifact_bytes: 1073741824
""")
    with (worker / "ssh_config").open("a") as out:
        # Reuse only this disposable host's known-host/key stanza.
        client = (root / "client/ssh_config").read_text()
        out.write("\n" + client)
    credential = Path(".config/upkeeper/secrets/f03-admin/qual-client")
    (worker / "home" / credential).parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(root / "client/home" / credential, worker / "home" / credential)
    (worker / "home" / credential).chmod(0o600)
    (worker / "home/remote-host-token").write_text("worker-a\n")
    assert len(set(ports)) == 3
    (root / "evidence/endpoint-identities.json").write_text(json.dumps(dict(zip(
        ["coordinator", "worker-a", "worker-b"], ports)), indent=2) + "\n")
    # No ambient client JSON can override these explicit private fixture paths.
    for host in ["coordinator", "worker-a", "worker-b", "client"]:
        assert not (root / host / "home/.config/t3-steward/coordinator-client.json").exists()

def condition(root):
    expected = root / "worker-a/home"
    actual = Path(os.environ["HOME"])
    okay = actual == expected and (actual / "remote-host-token").read_text() == "worker-a\n"
    row = {"observedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "home": str(actual), "cwd": os.getcwd(), "workerOnly": okay,
           "ready": (actual / "remote-condition-ready").exists()}
    with (root / "evidence/condition-checks.jsonl").open("a") as out:
        out.write(json.dumps(row) + "\n")
    if not okay:
        return 2
    return 0 if row["ready"] else 1

def records(path, table, column="record"):
    with sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True) as db:
        return [json.loads(row[0]) for row in db.execute("SELECT " + column + " FROM " + table)]

def snapshot(root, run):
    with sqlite3.connect("file:" + str(root / "coordinator/state.db") + "?mode=ro", uri=True) as db:
        db.execute("BEGIN")
        waits = [json.loads(r[0]) for r in db.execute("SELECT record FROM coordinator_task_waits")]
        attempts = [json.loads(r[0]) for r in db.execute("SELECT record FROM coordinator_attempts")]
    return {"waits": [w for w in waits if w["workflowRunId"] == run],
            "attempts": [a for a in attempts if a["workflowRunId"] == run]}

def events(root, host):
    path = root / ("evidence/t3-" + host + ".jsonl")
    result = []
    for line in path.read_text().splitlines() if path.exists() else []:
        try:
            result.append(json.loads(line))
        except ValueError:
            continue
    return result

def starts(root, host, thread):
    return [e for e in events(root, host) if e.get("command", {}).get("type") == "thread.turn.start"
            and e["command"].get("threadId") == thread]

def parked(root, run):
    value = snapshot(root, run)
    assert len(value["attempts"]) == 1 and len(value["waits"]) == 1, value
    attempt, wait = value["attempts"][0], value["waits"][0]
    assert attempt["progress"] == attempt["control"] == "waiting-external", attempt
    assert not attempt.get("completedAt") and not wait.get("settledAt"), value
    assert len(starts(root, "worker-a", wait["threadId"])) == 1
    assert any(e.get("event") == "turn-script" and e.get("turn") == 1
               and e.get("thread") == wait["threadId"] for e in events(root, "worker-a"))
    assert not starts(root, "coordinator", wait["threadId"])
    with (root / "evidence/parked-samples.jsonl").open("a") as out:
        out.write(json.dumps(value) + "\n")
    print("PASS parked after provider turn ended", wait["threadId"])

def verdict(root, run):
    final = snapshot(root, run)
    assert len(final["waits"]) == len(final["attempts"]) == 1, final
    wait, attempt = final["waits"][0], final["attempts"][0]
    assert attempt["progress"] == "succeeded", attempt
    assert wait["delivery"] == "delivered" and wait["result"]["outcome"] == "met", wait
    assert wait.get("wokenAt") and wait.get("deliveredAt") and wait.get("resumption"), wait
    thread = wait["threadId"]
    assert len(starts(root, "worker-a", thread)) == 2
    assert not starts(root, "coordinator", thread) and not starts(root, "worker-b", thread)
    local = records(root / "worker-a/state.db", "waits", "state")
    local = [w for w in local if w.get("taskWaitId") == wait["id"]]
    assert len(local) == 1 and local[0]["threadId"] == thread, local
    coordinator_local = records(root / "coordinator/state.db", "waits", "state")
    assert not [w for w in coordinator_local if w.get("taskWaitId") == wait["id"]]
    checks = [json.loads(line) for line in (root / "evidence/condition-checks.jsonl").read_text().splitlines()]
    assert checks and all(c["workerOnly"] for c in checks)
    assert any(not c["ready"] for c in checks) and any(c["ready"] for c in checks)
    assert all(str(root / "worker-a/storage/workspaces") in c["cwd"] for c in checks), checks
    sshlog = (root / "evidence/sshd.log").read_text()
    assert any("Starting session: forced-command" in line and str(root / "forced/admin") in line for line in sshlog.splitlines())
    assert not (root / "worker-a/state.db.admin.sock").exists()
    detail = json.loads((root / "evidence/final.json").read_text())
    tasks = [t for t in detail["workflow"]["tasks"] if not t.get("sink")]
    artifacts = [a.get("metadata", a) for a in tasks[0].get("artifacts", [])]
    assert any(a.get("kind") == "output" for a in artifacts)
    assert any(a.get("kind") == "verification" for a in artifacts)
    report = {"passed": True, "run": run, "attemptId": attempt["id"], "threadId": thread,
              "waitId": wait["id"], "deliveryId": wait["deliveryId"], "workerStarts": 2,
              "coordinatorStarts": 0, "otherWorkerStarts": 0, "checks": checks,
              "finalWait": wait, "workerLocalWait": local[0]}
    (root / "evidence/remote-wait-verdict.json").write_text(json.dumps(report, indent=2) + "\n")
    print("PASS remote task registration, local condition, coordinator settlement, same worker thread wake")

if __name__ == "__main__":
    action, directory, *args = sys.argv[1:]
    root = Path(directory)
    if action == "configure":
        configure(root, args)
    elif action == "condition":
        sys.exit(condition(root))
    elif action == "parked":
        parked(root, *args)
    elif action == "verdict":
        verdict(root, *args)
    else:
        raise SystemExit("unknown action")
