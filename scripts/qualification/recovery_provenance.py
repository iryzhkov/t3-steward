#!/usr/bin/env python3
"""Real Steward processes with synthetic turns; never accesses live state or SQLite."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT = Path(sys.argv[1]).resolve()
PORT = int(sys.argv[2])
assert ROOT.parent == Path("/tmp") and ROOT.name.startswith("t3qual.")
EVIDENCE = ROOT / "evidence"
RESULTS = []

def save(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, indent=2) + "\n")

def cli(*args, codex=None, check=True):
    env = {"PATH": str(ROOT / "bin") + ":/usr/bin:/bin",
           "HOME": str(ROOT / "client/home"),
           "XDG_CONFIG_HOME": str(ROOT / "client/home/.config"),
           "XDG_STATE_HOME": str(ROOT / "client/home/.local/state"),
           "T3_QUAL_SSH_CONFIG": str(ROOT / "client/ssh_config")}
    if codex:
        env["CODEX_THREAD_ID"] = codex
    command = [str(ROOT / "bin/t3-steward"), args[0], "--config",
               str(ROOT / "client/config-main.yaml"), *args[1:]]
    result = subprocess.run(command, env=env, cwd=ROOT / "client", text=True,
                            capture_output=True, timeout=90)
    with (EVIDENCE / "supplemental-cli.jsonl").open("a") as out:
        out.write(json.dumps({"args": args, "exit": result.returncode,
                              "stdout": result.stdout, "stderr": result.stderr}) + "\n")
    if check and result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    raw = result.stdout
    for i, char in enumerate(raw):
        if char == "{":
            try:
                return json.JSONDecoder().raw_decode(raw[i:])[0]
            except ValueError:
                pass
    if check:
        raise RuntimeError("no JSON: " + raw)
    return {"exit": result.returncode, "stdout": raw, "stderr": result.stderr}

def show(run):
    return cli("backlog", "show", run, "--json")

def tasks(doc):
    return {x["task"]["name"]: x for x in doc["workflow"]["tasks"] if not x.get("sink")}

def wait_for(label, predicate, seconds=240):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(1)
    raise RuntimeError("timeout: " + label)

def terminal(run):
    def read():
        doc = show(run)
        state = doc["workflow"]["summary"]["run"]["progress"]
        return doc if state in ("succeeded", "failed") else None
    return wait_for("terminal " + run, read, 300)

def dispatch(command):
    req = urllib.request.Request("http://127.0.0.1:%d/api/orchestration/dispatch" % PORT,
                                  json.dumps(command).encode(),
                                  {"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=10))

def turn_script(project, mode):
    directory = ROOT / "turns" / project
    directory.mkdir(exist_ok=True)
    script = directory / "turn1.sh"
    script.write_text("#!/bin/sh\nexec /usr/bin/python3 " + str(ROOT / "scripted-agent.py") + " " + mode + "\n")
    script.chmod(0o755)

AGENT = r'''
import json, os, pathlib, subprocess, sys, time
root = pathlib.Path(ROOT_LITERAL)
mode = sys.argv[1]
deps = pathlib.Path(".t3/dependencies")
files = list(deps.rglob("*")) if deps.exists() else []
seed = next((p for p in files if p.name == "seed.txt"), None)
repair = next((p for p in files if p.name == "repair.txt"), None)
def write(name, value):
    pathlib.Path(name).write_text(value)
def git(*args):
    env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", HOME=str(root/"worker-a/home"),
               PATH=str(root/"bin")+":/usr/bin:/bin",
               T3_QUAL_SSH_CONFIG=str(root/"worker-a/ssh_config"))
    return subprocess.check_output(["git", *args], env=env, text=True).strip()
if mode == "notify":
    write("result.txt", "terminal notification fixture\n")
elif repair:
    assert repair.read_text() == "repaired\n"
    if mode == "commit":
        assert (root/"signals/pruned").exists()
        record = next(p for p in files if p.name == "implementation")
        provenance = json.loads(record.read_text())
        assert git("rev-parse", provenance["ref"]) == provenance["commit"]
        assert git("show", provenance["ref"]+":payload.txt") == "durable payload"
        assert seed.read_text() == "ancestor artifact\n"
        (root/"evidence/case16-consumer.json").write_text(json.dumps(provenance))
    write("result.txt", "descendant verified\n")
    with (root/"evidence/descendant-turns").open("a") as out:
        out.write(mode+"\n")
elif seed:
    assert seed.read_text() == "ancestor artifact\n"
    if mode == "rerun" and not (root/"signals/repair").exists():
        print("backlog status: failed intentional failed root")
        sys.exit(0)
    if mode == "commit":
        (root/"signals/gate-started").touch()
        deadline = time.monotonic()+180
        while not (root/"signals/pruned").exists():
            assert time.monotonic() < deadline
            time.sleep(.2)
    write("repair.txt", "repaired\n")
else:
    write("seed.txt", "ancestor artifact\n")
    with (root/"evidence/ancestor-turns").open("a") as out:
        out.write(mode+"\n")
    if mode == "commit":
        write("payload.txt", "durable payload\n")
        git("add", "payload.txt")
        git("-c", "user.name=qualification", "-c", "user.email=qualification@example.invalid",
            "commit", "-m", "disposable durable commit")
        (root/"evidence/case16-producer-commit").write_text(git("rev-parse", "HEAD"))
        (root/"evidence/case16-producer-workspace").write_text(str(pathlib.Path.cwd()))
print("scripted "+mode+" task completed")
'''.replace("ROOT_LITERAL", repr(str(ROOT)))
(ROOT / "scripted-agent.py").write_text(AGENT)

def campaign(name, commit=False):
    directory = ROOT / "campaigns" / name
    (directory / "prompts").mkdir(parents=True)
    (directory / "prompts/task.md").write_text("Synthetic qualification turn only.\n")
    extra = """    commits:
      - name: implementation
        revision: HEAD
""" if commit else ""
    inputs = """      seed: [seed.txt, implementation]
""" if commit else ""
    need = "[seed, repair]" if commit else "[repair]"
    (directory / "workflow.yaml").write_text("""version: 2
name: %s
class: required
environment:
  project: plain
  type: git
  scope: task
routes:
  - instance: synthetic
    model: synthetic-model
    quota_pool: synthetic-pool
tasks:
  seed:
    prompt_file: prompts/task.md
    outputs: [seed.txt]
    verify: ["test -s seed.txt"]
%s  repair:
    prompt_file: prompts/task.md
    needs: [seed]
    inputs_from:
      seed: [seed.txt]
    outputs: [repair.txt]
    verify: ["test -s repair.txt"]
  descendant:
    prompt_file: prompts/task.md
    needs: %s
    inputs_from:
      repair: [repair.txt]
%s    outputs: [result.txt]
    verify: ["test -s result.txt"]
""" % (name, extra, need, inputs))
    return directory

def case9():
    turn_script("qual-plain", "rerun")
    original = cli("campaign", "submit", str(campaign("qual-rerun")),
                   "--idempotency-key", "qual-rerun-source", "--json")["runId"]
    before = terminal(original)
    save("case9-original.json", before)
    ts = tasks(before)
    assert ts["seed"]["attempt"]["progress"] == "succeeded"
    assert ts["repair"]["attempt"]["progress"] == "failed"
    assert not (ROOT/"evidence/descendant-turns").exists()
    (ROOT/"signals/repair").touch()
    result = cli("campaign", "rerun", original, "--from", "repair",
                 "--idempotency-key", "qual-rerun-new", "--reason", "disposable repair",
                 "--json")
    save("case9-rerun-response.json", result)
    # The API response carries the new run under its graph result.
    def run_ids(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in ("id", "runId") and isinstance(child, str) and child.startswith("run:rerun:"):
                    yield child
                yield from run_ids(child)
        elif isinstance(value, list):
            for child in value:
                yield from run_ids(child)
    new = next(run_ids(result))
    assert new != original
    after = terminal(new)
    save("case9-new.json", after)
    assert after["workflow"]["summary"]["run"]["progress"] == "succeeded"
    assert set(tasks(after)) == {"repair", "descendant"}
    assert (ROOT/"evidence/ancestor-turns").read_text().splitlines() == ["rerun"]
    assert (ROOT/"evidence/descendant-turns").read_text().splitlines() == ["rerun"]
    provenance = after["workflow"]["summary"]["run"]["graph"]["rerunOf"]
    assert provenance["sourceRunId"] == original
    unchanged = show(original)
    save("case9-original-after.json", unchanged)
    assert before["workflow"]["summary"]["run"] == unchanged["workflow"]["summary"]["run"]
    assert ts == tasks(unchanged)
    return {"source": original, "new": new, "provenance": provenance}

def case10():
    turn_script("qual-plain", "notify")
    canonical, provider = "qual-interactive-canonical", "qual-codex-provider-session"
    dispatch({"type":"thread.create", "threadId":canonical, "projectId":"qual-good"})
    logs = ROOT / "client/t3/userdata/logs/provider"
    logs.mkdir(parents=True, exist_ok=True)
    # This is the native provider-event projection written by a synthetic Codex
    # session. No SQLite is opened and the CLI must resolve distinct identities.
    (logs / ("events."+canonical+".log")).write_text(
        '[2026-09-14T00:00:00Z] NTIVE: '+json.dumps(
        {"provider":"codex", "threadId":canonical,
         "payload":{"threadId":provider}})+"\n")
    directory = ROOT / "campaigns/qual-notify"
    (directory/"prompts").mkdir(parents=True)
    (directory/"prompts/task.md").write_text("synthetic turn")
    (directory/"workflow.yaml").write_text("""version: 2
name: qual-notify
class: required
environment:
  project: plain
  type: git
  scope: task
routes:
  - instance: synthetic
    model: synthetic-model
    quota_pool: synthetic-pool
tasks:
  work:
    prompt_file: prompts/task.md
    outputs: [result.txt]
    verify: ["test -s result.txt"]
""")
    result = cli("campaign", "submit", str(directory), "--idempotency-key",
                 "qual-notify", "--notify-thread", "current", "--json", codex=provider)
    save("case10-submit.json", result)
    assert result["notify"]["threadId"] == canonical
    run = result["runId"]
    save("case10-run.json", terminal(run))
    def delivered():
        journal = [json.loads(x) for x in (EVIDENCE/"t3-stub.jsonl").read_text().splitlines()]
        starts = [x for x in journal if x.get("command",{}).get("type") == "thread.turn.start"
                  and x["command"].get("threadId") == canonical]
        return starts if starts else None
    starts = wait_for("canonical terminal notification", delivered, 90)
    assert len(starts) == 1
    assert run in starts[0]["command"]["message"]["text"]
    save("case10-delivery.json", starts)
    return {"run":run, "canonical":canonical, "providerSession":provider, "deliveries":len(starts)}

def case16():
    turn_script("qual-plain", "commit")
    run = cli("campaign", "submit", str(campaign("qual-commit", True)),
              "--idempotency-key", "qual-commit", "--json")["runId"]
    wait_for("gate after seed finalization", lambda:(ROOT/"signals/gate-started").exists())
    before = show(run)
    save("case16-before-prune.json", before)
    assert tasks(before)["seed"]["attempt"]["progress"] == "succeeded"
    commit = (EVIDENCE/"case16-producer-commit").read_text()
    # Prune only the disposable repository mirrors. Publish a temporary mirror
    # branch first, prove remote update removes it, then force object pruning.
    mirrors = [p.parent for p in (ROOT/"worker-a").rglob("HEAD")
               if p.parent.name.endswith(".git") and "repository-cache" in p.parts]
    assert mirrors, "no repository mirrors located"
    env = dict(os.environ, HOME=str(ROOT/"worker-a/home"), PATH=str(ROOT/"bin")+":/usr/bin:/bin",
               T3_QUAL_SSH_CONFIG=str(ROOT/"worker-a/ssh_config"), GIT_CONFIG_NOSYSTEM="1")
    evidence = []
    for mirror in mirrors:
        def git(*args, check=True):
            p = subprocess.run(["git","--git-dir",str(mirror),*args],
                               env=env, capture_output=True, text=True)
            evidence.append({"mirror":str(mirror), "args":args, "exit":p.returncode,
                             "stdout":p.stdout, "stderr":p.stderr})
            if check and p.returncode:
                raise RuntimeError(str(evidence[-1]))
            return p
        source = (EVIDENCE/"case16-producer-workspace").read_text()
        assert Path(source).is_relative_to(ROOT/"worker-a")
        git("fetch",source,commit+":refs/heads/qualification-ephemeral")
        assert git("rev-parse","refs/heads/qualification-ephemeral").stdout.strip() == commit
        git("remote","update","--prune")
        assert git("show-ref","--verify","refs/heads/qualification-ephemeral",check=False).returncode != 0
        git("reflog","expire","--expire=now","--all")
        git("gc","--prune=now")
        assert git("cat-file","-e",commit,check=False).returncode != 0
    save("case16-pruning.json", evidence)
    (ROOT/"signals/pruned").touch()
    after = terminal(run)
    save("case16-after.json", after)
    assert after["workflow"]["summary"]["run"]["progress"] == "succeeded"
    provenance = json.loads((EVIDENCE/"case16-consumer.json").read_text())
    assert provenance["commit"] == commit and provenance["workflowRunId"] == run
    return {"run":run, "commit":commit, "ref":provenance["ref"], "prunedMirrors":len(mirrors)}

for name, case in [("case9-rerun",case9), ("case10-codex-notify",case10), ("case16-artifact-commit",case16)]:
    try:
        detail = case()
        result = {"case":name,"verdict":"PASS","evidence":detail}
    except Exception as error:
        result = {"case":name,"verdict":"FAIL","error":repr(error)}
    RESULTS.append(result)
    save("supplemental-results.json",RESULTS)
    print(json.dumps(result),flush=True)
sys.exit(any(x["verdict"] != "PASS" for x in RESULTS))
