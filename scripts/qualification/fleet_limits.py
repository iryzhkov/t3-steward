#!/usr/bin/env python3
"""Real subprocess probes for UpKeeper projection, rollback and bounded admin clients.

Inputs are disposable fleet root and UpKeeper source. Files copied from source are
test fixtures; all writes and target-side application stay beneath the temp root.
No claim of live provider observation is made.
"""
import copy
import json
import os
import pathlib
import shutil
import sqlite3
import time
import subprocess
import sys

root, source = map(pathlib.Path, sys.argv[1:])
evidence = root / "evidence"
failures = 0


def verdict(name, passed, detail):
    global failures
    failures += not passed
    line = f"[case] {'PASS' if passed else 'FAIL'} {name} {detail}"
    print(line, flush=True)
    with (evidence / "supplemental-verdicts.txt").open("a") as out:
        out.write(line + "\n")


def environment(home):
    return dict(PATH=f"{root}/bin:/usr/bin:/bin", HOME=str(home),
                XDG_CONFIG_HOME=str(home / ".config"),
                XDG_STATE_HOME=str(home / ".local/state"),
                XDG_DATA_HOME=str(home / ".local/share"),
                XDG_CACHE_HOME=str(home / ".cache"),
                XDG_RUNTIME_DIR=str(home / ".runtime"))


def command(name, argv, env=None, data=None, cwd=None):
    result = subprocess.run(argv, input=data, text=True, capture_output=True,
                            env=env, cwd=cwd, timeout=120)
    (evidence / (name + ".out")).write_text(result.stdout)
    (evidence / (name + ".err")).write_text(result.stderr)
    (evidence / (name + ".exit")).write_text(str(result.returncode))
    return result


upkeeper = root / "bin/upkeeper"
build = command("upkeeper-build", ["go", "build", "-o", str(upkeeper), "./cmd/upkeeper"], cwd=source)
if build.returncode:
    verdict("case5", False, "UpKeeper build failed")
    sys.exit(1)
config = root / "upkeeper-config"
for filename in ("inventory/hosts.yml", "desired/components.yml", "desired/release.json"):
    destination = config / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source / filename, destination)
intent = json.loads((source / "testdata/fleet/fleet-intent.json").read_text())
manifest = json.loads((config / "desired/release.json").read_text())
manifest["components"]["steward-fleet-configuration"] = intent
(config / "desired/release.json").write_text(json.dumps(manifest))
env = environment(root / "upkeeper-controller")
env["DEV_FLEET_CONFIG_ROOT"] = str(config)
plan = command("case5-plan", [str(upkeeper), "fleet-plan", "--json"], env)
if plan.returncode:
    verdict("case5-plan", False, "plan invocation failed")
else:
    report = json.loads(plan.stdout)
    good = report["enrollment_plan"]["applied"] is False
    for host in ("homelab", "omarchy-pc"):
        good &= "claude-opus-4-5" in report["hosts"][host]["desired_models"]["claudeAgent"]
        good &= report["hosts"][host]["observed_models"] is None
    verdict("case5-plan", good, "Opus desired on both hosts; absent observation remains null; enrollment remains explicit")

# Apply through the compiled target-side protocol, not direct adapter calls.
# The homes contain only throwaway credential markers. The adapter checks metadata,
# never reads their values, and does not contact a service or enroll a worker.
for host in ("homelab", "omarchy-pc"):
    home = root / ("upkeeper-" + host)
    for namespace in ("f02-protocol", "f03-admin"):
        secret = home / ".config/upkeeper/secrets" / namespace / host
        secret.parent.mkdir(parents=True, exist_ok=True)
        secret.write_text("disposable-metadata-only")
        secret.chmod(0o600)
    original = copy.deepcopy(intent)
    changed = copy.deepcopy(intent)
    changed["profiles"][changed["hosts"][host]["profile"]]["provider_instances"].append("qualification-extra")
    changed["profiles"][changed["hosts"][host]["profile"]]["models"]["qualification-extra"] = ["extra"]
    changed["coordinator"]["client_endpoint"]["request_timeout"] = "45s"
    before = None
    for phase, spec in (("initial", original), ("change", changed), ("rollback", original)):
        run = f"qualification-{host}-{phase}"
        payload = dict(run_id=run, host=dict(name=host, os_id="linux"),
                       components=["steward-fleet-configuration"],
                       manifest=dict(components={"steward-fleet-configuration": spec}),
                       lock=dict(run_id=run), stored_files={})
        result = command(f"case5-{host}-{phase}", [str(upkeeper), "__agent"],
                         environment(home), json.dumps(payload))
        try:
            document = json.loads(result.stdout)
            ok = document["result"] in ("updated", "converged")
        except (ValueError, KeyError):
            ok = False
        verdict(f"case5-{host}-{phase}", ok, f"target-side process exit={result.returncode}")
        paths = [home / ".config/t3-steward" / f for f in
                 ("worker-bootstrap.json", "coordinator-client.json")]
        content = [p.read_bytes() if p.exists() else None for p in paths]
        if phase == "initial":
            before = content
        elif phase == "change":
            verdict(f"case17-{host}-change", before is not None and all(a != b for a, b in zip(content, before)) and all(content),
                    "both owned documents changed before rollback")
        else:
            verdict(f"case17-{host}-rollback", content == before and all(content),
                    "earlier intent restored both owned documents byte-for-byte")

# This test cannot substitute desired allowlists for accepted runtime authorization.
verdict("case5-effective", False,
        "profiles applied and plan inspected, but Opus observed/effective sets not yet proven in Steward; coordinator projection requires explicit catalog application")

client_home = root / "client/home"
env = environment(client_home)
env["T3_QUAL_SSH_CONFIG"] = str(root / "client/ssh_config")
base = (root / "client/config-main.yaml").read_text()
steward = str(root / "bin/t3-steward")
for name, content in (
    ("wrong-role", base.replace("secretref:f03-admin/qual-client", "secretref:f02-protocol/worker-a")),
    ("message-bound", base.replace("max_bytes: 4194304", "max_bytes: 128")),
    ("artifact-bound", base.replace("max_artifact_bytes: 1073741824", "max_artifact_bytes: 1")),
):
    config_path = root / "client" / (name + ".yaml")
    config_path.write_text(content)
    if name == "artifact-bound":
        run_document = json.loads((evidence / "case6-run.json").read_text())
        artifacts = run_document["workflow"]["artifacts"]
        artifact = next(a["metadata"]["id"] for a in artifacts if a["metadata"]["name"] == "result.txt")
        args = ["backlog", "--config", str(config_path), "artifact", "get", artifact]
    else:
        args = ["coordinator", "--config", str(config_path), "identity", "--json"]
    result = command("case17-" + name, [steward] + args, env)
    expected = {"wrong-role": "credential", "message-bound": "limit",
                "artifact-bound": "invalid coordinator artifact response"}[name]
    verdict("case17-" + name, result.returncode != 0 and expected in (result.stderr + result.stdout).lower(),
            f"remote client refusal exit={result.returncode}; inspect exact diagnostic")

# Check drift details and dispatch multiplicity from actual runtime evidence.
drift = json.loads((evidence / "case6-check.json").read_text())
reasons = [r for t in drift["matrix"]["tasks"] for c in t["candidates"] for r in c.get("reasons", [])]
match = [r for r in reasons if r["code"] == "catalog-digest-mismatch"]
verdict("case6-diagnostics", len(match) == 1 and match[0].get("desired") != match[0].get("observed")
        and match[0].get("revision") == 1, "drift includes desired/observed digests and enrollment revision")
events = [json.loads(line) for line in (evidence / "t3-stub.jsonl").read_text().splitlines()]
starts = [e for e in events if e.get("command", {}).get("type") == "thread.turn.start"]
scripts = [e for e in events if e.get("event") == "turn-script"]
verdict("case6-no-duplicate-dispatch", len(starts) == len(scripts) == 1,
        f"provider journal records {len(starts)} starts, {len(scripts)} scripted turns")
logs = [p for p in evidence.glob("coordinator*.log")]
drift_lines = [line for p in logs for line in p.read_text().splitlines()
               if "catalog" in line.lower() and ("mismatch" in line.lower() or "drift" in line.lower())]
verdict("case6-bounded-logs", len(drift_lines) <= 6,
        f"{len(drift_lines)} drift-related log lines across coordinator lifetimes")

# Inject stale durable observations and quota closure into ONLY this disposable
# database. CLI queries and classification still cross the real SSH/admin boundary.
def check(name, directory):
    response = command(name, [steward, "campaign", "--config",
                       str(root / "client/config-main.yaml"), "check", str(directory), "--json"], env)
    return json.loads(response.stdout)["matrix"]


def has_reason(matrix, code):
    return any(r["code"] == code and not r["permanent"] for t in matrix["tasks"]
               for c in t["candidates"] for r in c.get("reasons", []))


db = sqlite3.connect(root / "coordinator/state.db", timeout=10)
rows = db.execute("SELECT worker_id,record,valid_until FROM coordinator_worker_snapshots").fetchall()
for worker, raw, valid_until in rows:
    doc = json.loads(raw)
    doc["validUntil"] = "2000-01-01T00:00:00Z"
    doc["observedAt"] = "2000-01-01T00:00:00Z"
    db.execute("UPDATE coordinator_worker_snapshots SET record=?,valid_until=? WHERE worker_id=?",
               (json.dumps(doc), doc["validUntil"], worker))
db.commit()
matrix = check("case17-stale-check", root / "campaigns/case6")
verdict("case17-stale-snapshot", has_reason(matrix, "worker-stale")
        and matrix["outcome"] == "accepted_waiting", "expired disposable worker snapshots queried through SSH")
for worker, raw, valid_until in rows:
    db.execute("UPDATE coordinator_worker_snapshots SET record=?,valid_until=? WHERE worker_id=?",
               (raw, valid_until, worker))
db.commit()
db.close()
security = subprocess.run([sys.executable, str(pathlib.Path(__file__).with_name("fleet_security.py")),
                           str(root)], check=False)
if security.returncode:
    failures += 1

# Fresh probe key prevents earlier good evidence masking the injected network failure.
network = root / "campaigns/case17-network"
shutil.copytree(root / "campaigns/case6", network, dirs_exist_ok=True)
workflow = network / "workflow.yaml"
workflow.write_text(workflow.read_text().replace("  scope: task", "  scope: task\n  ref: refs/heads/main"))
forced = root / "forced/repo-good"
saved = forced.read_bytes()
primed = check("case17-expiry-prime", network)
prime_ok = any(c.get("repository", {}).get("class") == "authenticated-ok"
               for t in primed["tasks"] for c in t["candidates"] if c["worker"] == "worker-b")
forced.write_text("#!/bin/sh\necho 'Connection reset by peer' >&2\nexit 255\n")
try:
    cached = check("case17-expiry-cached", network)
    cached_ok = any(c.get("repository", {}).get("class") == "authenticated-ok"
                    for t in cached["tasks"] for c in t["candidates"] if c["worker"] == "worker-b")
    # The product cache lasts ten minutes. Exercise actual wall-clock expiry,
    # not a rewritten production constant or a simulated observer.
    print("[harness] waiting 605s for real repository evidence expiry", flush=True)
    time.sleep(605)
    matrix = check("case17-expiry-after", network)
    observed = has_reason(matrix, "network-unavailable")
    verdict("case17-expired-evidence", prime_ok and cached_ok and observed,
            "good evidence cached while endpoint failed, then re-probed after actual 10-minute TTL")
    verdict("case17-temporary-network", observed and matrix["outcome"] == "accepted_waiting",
            f"forced Git disconnect; matrix={matrix['outcome']}")
finally:
    forced.write_bytes(saved)
sys.exit(int(failures > 0))
