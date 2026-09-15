#!/usr/bin/env python3
"""Case 17: receipt-proven client rollback through real CLI and agent processes."""
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
root = pathlib.Path(tempfile.mkdtemp(prefix="t3-client-rollback."))
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
worker = json.loads((source / "testdata/fleet/projection-worker-homelab.json").read_text())
client_fixture = json.loads((source / "testdata/fleet/projection-client-homelab.json").read_text())
for scenario in ("absent", "existing", "drift", "missing-proof"):
    home = root / scenario / "target"
    config = root / scenario / "release"
    for name in ("inventory/hosts.yml", "desired/components.yml", "desired/release.json"):
        target = config / name; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
    inventory = json.loads((config / "inventory/hosts.yml").read_text())
    inventory["all"]["hosts"]["homelab"].update(ansible_host="qualification.invalid", machine_id_sha256="0"*64, self_managed=False)
    dump(config / "inventory/hosts.yml", inventory)
    manifest = json.loads((config / "desired/release.json").read_text())
    manifest["components"].pop(component, None)
    dump(config / "desired/release.json", manifest)
    controller = environment(root / scenario / "controller")
    controller.update(DEV_FLEET_CONFIG_ROOT=str(config), QUAL_TARGET_HOME=str(home),
                      QUAL_BINARY=str(binary), QUAL_PAYLOAD=str(evidence / f"{scenario}-dispatch.json"))
    for label,args in [
        ("init",["git","init","-q",str(config)]),
        ("add",["git","-C",str(config),"add","."]),
        ("commit",["git","-C",str(config),"-c","user.name=Qualification","-c","user.email=qualification@invalid","commit","-qm","Historical release"])]:
        assert run(scenario+"-"+label,args,controller).returncode==0
    previous=run(scenario+"-head",["git","-C",str(config),"rev-parse","HEAD"],controller).stdout.strip()
    for namespace in ("f02-protocol","f03-admin"):
        p=home/".config/upkeeper/secrets"/namespace/"homelab";p.parent.mkdir(parents=True,exist_ok=True)
        p.write_bytes(b"disposable-qualification-only");p.chmod(0o600)
    bootstrap=home/".config/t3-steward/worker-bootstrap.json"
    dump(bootstrap,worker);bootstrap.chmod(0o600)
    bootstrap_before=bootstrap.read_bytes()
    client=home/".config/t3-steward/coordinator-client.json"
    prior=None
    if scenario=="existing":
        value=copy.deepcopy(client_fixture);value["address"]="prior-admin"
        dump(client,value);client.chmod(0o600);prior=client.read_bytes()
    for index in (1,2):
        desired=copy.deepcopy(intent);desired["coordinator"]["client_endpoint"]["address"]=f"candidate-admin-{index}"
        payload=dict(run_id=f"{scenario}-apply-{index}",host=dict(name="homelab",os_id="linux"),
            components=[component],manifest=dict(components={component:desired,
            "worker-configuration":dict(kind="worker-configuration",hosts={"homelab":worker})}),
            lock=dict(run_id=f"{scenario}-apply-{index}"),stored_files={})
        result=run(scenario+f"-apply-{index}",[str(binary),"__agent"],environment(home),json.dumps(payload))
        assert result.returncode==0 and json.loads(result.stdout)["result"] in ("updated","converged"),result.stdout
    check(scenario+"-client-changed",client.read_bytes()!=prior)
    refuse=scenario in ("drift","missing-proof")
    if scenario=="drift": client.write_bytes(b"unexplained local change\\n")
    if scenario=="missing-proof": (home/".local/state/dev-fleet/fleet-config/client-ownership.json").unlink()
    if refuse: prior=client.read_bytes()
    for repeat in (1,2):
        result=run(scenario+f"-rollback-{repeat}",[str(binary),"rollback","--to",previous,"--hosts","homelab",
            "--components",component,"--json"],controller,cwd=config)
        check(scenario+f"-rollback-{repeat}-command",(result.returncode!=0) if refuse else (result.returncode==0))
        check(scenario+f"-rollback-{repeat}-prior", (client.read_bytes() if client.exists() else None)==prior)
        check(scenario+f"-rollback-{repeat}-bootstrap",bootstrap.read_bytes()==bootstrap_before)
print(f"evidence: {evidence}",flush=True)
dump(evidence/"verdict.json",dict(failures=failures))
sys.exit(bool(failures))
