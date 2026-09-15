#!/usr/bin/env python3
"""Case 17: real UpKeeper agent and release rollback, disposable subprocess transport."""
import copy
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

source = pathlib.Path(sys.argv[1]).resolve()
root = pathlib.Path(tempfile.mkdtemp(prefix="t3-coordinator-rollback."))
evidence = root / "evidence"
evidence.mkdir()
binary = root / "bin/upkeeper"
binary.parent.mkdir()
component = "steward-fleet-configuration"
destination = ".config/t3-steward/coordinator-fleet.json"
failures = []


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def run(name, argv, env=None, data=None, cwd=None):
    result = subprocess.run(argv, env=env, input=data, cwd=cwd,
                            text=True, capture_output=True, timeout=120)
    for suffix, value in (("out", result.stdout), ("err", result.stderr), ("exit", str(result.returncode))):
        (evidence / (name + "." + suffix)).write_text(value)
    return result


def check(name, condition):
    print(f"{'PASS' if condition else 'FAIL'} {name}", flush=True)
    if not condition:
        failures.append(name)


def environment(home):
    return dict(PATH=f"{root}/bin:/usr/bin:/bin", HOME=str(home),
                XDG_CONFIG_HOME=str(home / ".config"), XDG_STATE_HOME=str(home / ".local/state"),
                XDG_DATA_HOME=str(home / ".local/share"), XDG_CACHE_HOME=str(home / ".cache"),
                XDG_RUNTIME_DIR=str(home / ".runtime"))


built = run("build", ["go", "build", "-o", str(binary), "./cmd/upkeeper"], cwd=source)
assert built.returncode == 0
run("build-metadata", ["go", "version", "-m", str(binary)])
# The production CLI and target agent run in separate processes. Only the SSH
# network boundary is replaced with an explicit local subprocess adapter.
proxy = root / "bin/ssh"
proxy.write_text("""#!/usr/bin/python3
import json,os,subprocess,sys
from pathlib import Path
args=sys.argv[1:]; remote=args[args.index('--')+1:]
if remote == ['true']: sys.exit(0)
if remote != ['.local/bin/upkeeper','__agent']: sys.exit(99)
payload=sys.stdin.read()
Path(os.environ['QUAL_PAYLOAD']).write_text(payload)
home=Path(os.environ['QUAL_TARGET_HOME'])
env=dict(PATH=str(Path(os.environ['QUAL_BINARY']).parent)+':/usr/bin:/bin',HOME=str(home),XDG_CONFIG_HOME=str(home/'.config'),
 XDG_STATE_HOME=str(home/'.local/state'),XDG_DATA_HOME=str(home/'.local/share'),
 XDG_CACHE_HOME=str(home/'.cache'),XDG_RUNTIME_DIR=str(home/'.runtime'))
sys.exit(subprocess.run([os.environ['QUAL_BINARY'],'__agent'],input=payload,text=True,env=env).returncode)
""")
proxy.chmod(0o700)
systemctl = root / "bin/systemctl"
systemctl.write_text("#!/bin/sh\nprintf 'LoadState=not-found\\n'\n")
systemctl.chmod(0o700)
intent = json.loads((source / "testdata/fleet/fleet-intent.json").read_text())
assert intent["coordinator"]["host"] == "normandy" and "normandy" not in intent["hosts"]
changed = copy.deepcopy(intent)
for profile in changed["profiles"].values():
    profile["executor_slots"] += 1

for scenario in ("existing", "absent", "moved"):
    home = root / scenario / "target"
    env = environment(home)
    config = root / scenario / "release"
    for name in ("inventory/hosts.yml", "desired/components.yml", "desired/release.json"):
        target = config / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    inventory = json.loads((config / "inventory/hosts.yml").read_text())
    spec = inventory["all"]["hosts"]["normandy"]
    spec.update(ansible_host="qualification.invalid", machine_id_sha256="0"*64, self_managed=False)
    dump(config / "inventory/hosts.yml", inventory)
    manifest = json.loads((config / "desired/release.json").read_text())
    baseline = copy.deepcopy(intent)
    if scenario == "absent":
        manifest["components"].pop(component, None)
    else:
        if scenario == "moved":
            baseline["coordinator"]["host"] = "old-controller"
            inventory["all"]["hosts"]["old-controller"] = copy.deepcopy(spec)
            dump(config / "inventory/hosts.yml", inventory)
        manifest["components"][component] = baseline
    dump(config / "desired/release.json", manifest)
    controller = environment(root / scenario / "controller")
    controller.update(DEV_FLEET_CONFIG_ROOT=str(config), QUAL_TARGET_HOME=str(home),
                      QUAL_BINARY=str(binary), QUAL_PAYLOAD=str(evidence / f"{scenario}-dispatch.json"))
    assert run(scenario+"-git-init", ["git","init","-q",str(config)],controller).returncode == 0
    assert run(scenario+"-git-add", ["git","-C",str(config),"add","."],controller).returncode == 0
    assert run(scenario+"-git-commit", ["git","-C",str(config),"-c","user.name=Qualification",
        "-c","user.email=qualification@invalid","commit","-qm","Historical release"],controller).returncode == 0
    previous = run(scenario+"-git-head", ["git","-C",str(config),"rev-parse","HEAD"],controller).stdout.strip()

    def apply(name, desired):
        payload = dict(run_id=name, host=dict(name="normandy",os_id="linux"),
            components=[component],manifest=dict(components={component:desired}),
            lock=dict(run_id=name),stored_files={})
        result = run(name, [str(binary),"__agent"],env,json.dumps(payload))
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["result"] in ("updated","converged")

    path = home / destination
    sibling = home / ".config/t3-steward/worker-bootstrap.json"
    sibling.parent.mkdir(parents=True, exist_ok=True)
    sibling.write_bytes(b"unrelated worker sentinel\n")
    if scenario == "existing":
        apply(scenario+"-baseline",intent)
        prior = path.read_bytes()
    else:
        prior = None
    apply(scenario+"-forward",changed)
    forward = path.read_bytes()
    check(scenario+"-forward-changes", forward != prior and path.stat().st_mode & 0o777 == 0o600)
    siblings = [sibling, home / ".config/t3-steward/coordinator-client.json"]
    if scenario != "existing":
        siblings[1].write_bytes(b"unrelated client sentinel\n")
    sibling_bytes = {str(p): p.read_bytes() if p.exists() else None for p in siblings}
    result = run(scenario+"-rollback", [str(binary),"rollback","--to",previous,"--hosts","normandy",
        "--components",component,"--json"],controller,cwd=config)
    actual = path.read_bytes() if path.exists() else None
    check(scenario+"-rollback-command",result.returncode == 0)
    check(scenario+"-restored",actual == prior)
    check(scenario+"-siblings-preserved",all((p.read_bytes() if p.exists() else None)==sibling_bytes[str(p)] for p in siblings))
    retained = []
    for record in home.glob(".local/state/dev-fleet/fleet-config/*/prior.json"):
        contents = json.loads(record.read_text())
        for entry in contents["documents"]:
            if entry["destination"] == destination and entry.get("content") == forward.decode():
                retained.append(str(record))
    check(scenario+"-forward-retained-for-rollback",bool(retained))
    dump(evidence / (scenario+"-digests.json"),dict(
        before=hashlib.sha256(prior).hexdigest() if prior is not None else None,
        forward=hashlib.sha256(forward).hexdigest(),
        after=hashlib.sha256(actual).hexdigest() if actual is not None else None,
        retained=retained))
print(f"evidence: {evidence}",flush=True)
dump(evidence / "verdict.json",dict(failures=failures))
sys.exit(bool(failures))
