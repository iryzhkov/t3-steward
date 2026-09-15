#!/usr/bin/env python3
"""Opt-in private GitHub read-only qualification; key bytes are never read."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import sys

mode, raw_root, *args = sys.argv[1:]
root = Path(raw_root).resolve()
assert root.parent == Path("/tmp") and root.name.startswith("t3qual.")
key = Path(os.environ["QUAL_GITHUB_KEY_PATH"]).resolve()
known = Path(os.environ["QUAL_GITHUB_KNOWN_HOSTS_PATH"]).resolve()
assert key.is_file() and known.is_file()
assert "\n" not in str(key) and "\n" not in str(known)

def ssh_config(worker, authorized):
    path = root / worker / "ssh_config"
    raw = path.read_text().split("# private GitHub qualification")[0]
    identity = key if authorized else root / "keys/repo-unauthorized"
    raw += f"""# private GitHub qualification
Host github.com
  HostName github.com
  User git
  IdentityFile "{identity}"
  IdentityAgent none
  IdentitiesOnly yes
  BatchMode yes
  StrictHostKeyChecking yes
  UserKnownHostsFile "{known}"
  PasswordAuthentication no
  ConnectTimeout 10
"""
    path.write_text(raw)

if mode == "setup":
    for host in ("coordinator", "worker-a", "worker-b"):
        path = root / host / "config.yaml"
        text = path.read_text()
        old = "repository: ssh://qual-repo-private/private.git\n      default_ref: main"
        assert text.count(old) == 1
        text = text.replace(old, "repository: git@github.com:iryzhkov/UpKeeper.git\n      default_ref: HEAD\n      credentials: [qual-github-ssh]")
        path.write_text(text)
    path = root / "forced/worker-a-exchange"
    text = path.read_text()
    needle = "set -eu\n"
    assert text.count(needle) == 1
    text = text.replace(needle, needle + "export T3_STEWARD_CREDENTIAL_QUAL_GITHUB_SSH=" + shlex.quote(str(key)) + "\n")
    path.write_text(text)
    ssh_config("worker-a", True)
    ssh_config("worker-b", False)
    hashes = {h: hashlib.sha256((root/h/"config.yaml").read_bytes()).hexdigest()
              for h in ("coordinator", "worker-a", "worker-b")}
    (root/"evidence/private-github-config-hashes.json").write_text(json.dumps(hashes, indent=2)+"\n")
elif mode == "both":
    ssh_config("worker-b", True)
    expected = json.loads((root/"evidence/private-github-config-hashes.json").read_text())
    assert all(hashlib.sha256((root/h/"config.yaml").read_bytes()).hexdigest() == digest for h,digest in expected.items())
elif mode == "verify":
    phase = args[0]
    doc = json.loads((root/f"evidence/private-github-{phase}.json").read_text())
    candidates = {c["worker"]: c for task in doc["matrix"]["tasks"] for c in task["candidates"]}
    assert set(candidates) == {"worker-a","worker-b"}, candidates
    a, b = candidates["worker-a"], candidates["worker-b"]
    def authenticated(candidate):
        repository = candidate.get("repository") or {}
        return repository.get("observed") is True and repository.get("class") == "authenticated-ok"
    assert authenticated(a), a
    if phase == "one":
        assert not authenticated(b), b
        assert b.get("outcome") != "ready_now", b
        assert (b.get("repository") or {}).get("observed") is False, b
        assert any("qual-github-ssh" in r.get("detail","") and "unavailable" in r.get("detail","") for r in b.get("reasons",[])), b
    else:
        assert authenticated(b), b
        assert not any(r.get("permanent") for c in candidates.values() for r in c.get("reasons",[])), candidates
    print(json.dumps({"phase":phase, "workers": {name: {"outcome": c["outcome"], "repository":c.get("repository"), "reasonCodes":[r["code"] for r in c.get("reasons",[])]} for name,c in candidates.items()}}))
else:
    raise SystemExit("unknown mode")
