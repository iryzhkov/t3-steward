#!/usr/bin/env python3
"""Bounded wire attacks and fresh quota fixture against a disposable fleet."""
import base64
import datetime
import hashlib
import hmac
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
from typing import Any

root = pathlib.Path(sys.argv[1])
evidence = root / "evidence"
env = dict(PATH=f"{root}/bin:/usr/bin:/bin", HOME=f"{root}/client/home",
           XDG_CONFIG_HOME=f"{root}/client/home/.config",
           XDG_STATE_HOME=f"{root}/client/home/.local/state",
           T3_QUAL_SSH_CONFIG=f"{root}/client/ssh_config")
failed = 0


def call(name, args, data=None):
    r = subprocess.run(args, input=data, text=True, capture_output=True, env=env, timeout=45)
    (evidence / (name + ".out")).write_text(r.stdout)
    (evidence / (name + ".err")).write_text(r.stderr)
    (evidence / (name + ".exit")).write_text(str(r.returncode))
    return r


def verdict(name, passed, detail):
    global failed
    failed += not passed
    line = f"[case] {'PASS' if passed else 'FAIL'} {name} {detail}"
    print(line, flush=True)
    with (evidence / "security-verdicts.txt").open("a") as out:
        out.write(line + "\n")


def compact(value):
    return json.dumps(value, separators=(",", ":"))


ssh = [f"{root}/bin/ssh", "-oBatchMode=yes", "qual-admin"]
# Sign a well-formed admin-domain frame with a WORKER credential. It travels
# through the real admin SSH forced command and must fail authentication there.
credential = json.loads((root / "worker-a/home/.config/upkeeper/secrets/f02-protocol/worker-a").read_text())
now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
stamp = lambda t: t.isoformat().replace("+00:00", "Z")
payload = dict(version="backlog.admin/v1", operation="query", query=dict(kind="identity"))
frame: dict[str, Any] = dict(version="backlog.admin.remote/v1", operation="query",
             sessionId="qualification-worker-role-session", requestId="qualification-worker-role-request",
             sender=credential["workerPrincipal"], recipient="qual-coordinator",
             sequence=1, sentAt=stamp(now), deadline=stamp(now + datetime.timedelta(seconds=30)),
             payloadSha256=hashlib.sha256(compact(payload).encode()).hexdigest(),
             authentication=dict(principal=credential["workerPrincipal"], keyId=credential["workerKeyId"], signature=""),
             payload=payload)
signature_input = b"t3-steward/coordinator-admin-remote/v1\n" + compact(frame).encode()
frame["authentication"]["signature"] = hmac.new(base64.b64decode(credential["workerSecret"]),
                                              signature_input, hashlib.sha256).hexdigest()
r = call("case17-wire-worker-role", ssh, compact(frame) + "\n")
verdict("case17-wire-worker-role", r.returncode != 0 and "authentication" in (r.stdout + r.stderr),
        "worker-signed frame refused by real admin forced command")

# The server must bound bytes before trying to decode a JSON document. No
# credential is involved and the attack body is never recorded in the evidence.
r = call("case17-wire-oversize", ssh, "x" * (4194304 + 2) + "\n")
verdict("case17-wire-oversize", r.returncode != 0 and "limit" in (r.stdout + r.stderr),
        "4 MiB+2-byte frame refused by server before JSON decoding")

# Unlike quota_checks=false's synthesized open pool, a durable admission row is
# the source used by the live query. Insert and remove only our disposable row.
db = sqlite3.connect(root / "coordinator/state.db", timeout=10)
previous = db.execute("SELECT revision,record FROM coordinator_quota_admissions WHERE id=?",
                      ("synthetic-pool",)).fetchone()
record = dict(quotaPoolId="synthetic-pool", revision=1, admission="closed",
              observedAt=stamp(now), appliedAt=stamp(now), reason="disposable qualification",
              bucketEpochs=[])
prior_pool = db.execute("SELECT record FROM coordinator_quota_pools WHERE id=?", ("synthetic-pool",)).fetchone()[0]
pool = json.loads(prior_pool)
pool["checksDisabled"] = False
db.execute("UPDATE coordinator_quota_pools SET record=? WHERE id=?", (compact(pool), "synthetic-pool"))
db.execute("INSERT OR REPLACE INTO coordinator_quota_admissions(id,revision,record) VALUES(?,?,?)",
           ("synthetic-pool", 1, compact(record)))
db.commit()
try:
    r = call("case17-closed-admission", [f"{root}/bin/t3-steward", "campaign", "--config",
             f"{root}/client/config-main.yaml", "check", f"{root}/campaigns/case6", "--json"])
    matrix = json.loads(r.stdout)["matrix"]
    reasons = [reason for task in matrix["tasks"] for candidate in task["candidates"]
               for reason in candidate.get("reasons", [])]
    verdict("case17-quota-closure", matrix["outcome"] == "accepted_waiting"
            and any(x["code"] == "quota-closed" and not x["permanent"] for x in reasons),
            "fresh durable closed admission reports temporary quota-closed")
finally:
    db.execute("UPDATE coordinator_quota_pools SET record=? WHERE id=?", (prior_pool, "synthetic-pool"))
    if previous:
        db.execute("UPDATE coordinator_quota_admissions SET revision=?,record=? WHERE id=?",
                   (*previous, "synthetic-pool"))
    else:
        db.execute("DELETE FROM coordinator_quota_admissions WHERE id=?", ("synthetic-pool",))
    db.commit()
    db.close()

# Validate an actual output artifact round-trip first, then lower the bound.
run = json.loads((evidence / "case6-run.json").read_text())
artifact = next(x["metadata"] for x in run["workflow"]["artifacts"] if x["metadata"]["name"] == "result.txt")
args = [f"{root}/bin/t3-steward", "backlog", "--config", f"{root}/client/config-main.yaml",
        "artifact", "get", artifact["id"]]
r = call("case17-artifact-baseline", args)
baseline = r.returncode == 0 and len(r.stdout.encode()) == artifact["size"]
config = root / "client/artifact-cap.yaml"
config.write_text((root / "client/config-main.yaml").read_text().replace(
    "max_artifact_bytes: 1073741824", "max_artifact_bytes: 1"))
args[3] = str(config)
r = call("case17-artifact-refused", args)
verdict("case17-artifact-bound", baseline and r.returncode == 7 and not r.stdout
        and "invalid coordinator artifact response" in r.stderr,
        "25-byte declared output downloaded normally, then refused with 1-byte bound and no leaked payload")
sys.exit(int(failed > 0))
