#!/usr/bin/env python3
"""Case 5 support: real UpKeeper application, separate provider observation, oracles."""
import copy
import json
import os
import pathlib
import shutil
import subprocess
import sys
from typing import Any

phase = sys.argv[1]
root = pathlib.Path(sys.argv[2])
evidence = root / "evidence"
model = "claude-opus-4-5"


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def env(home):
    return dict(PATH=f"{root}/bin:/usr/bin:/bin", HOME=str(home),
                XDG_CONFIG_HOME=str(home / ".config"),
                XDG_STATE_HOME=str(home / ".local/state"),
                XDG_DATA_HOME=str(home / ".local/share"),
                XDG_CACHE_HOME=str(home / ".cache"),
                XDG_RUNTIME_DIR=str(home / ".runtime"))


def command(name, args, environment=None, data=None, cwd=None):
    result = subprocess.run(args, capture_output=True, text=True, input=data,
                            env=environment, cwd=cwd, timeout=120)
    (evidence / (name + ".out")).write_text(result.stdout)
    (evidence / (name + ".err")).write_text(result.stderr)
    (evidence / (name + ".exit")).write_text(str(result.returncode))
    if result.returncode:
        raise RuntimeError(f"{name} failed; see {evidence / (name + '.err')}")
    return result


if phase == "prepare":
    source = pathlib.Path(sys.argv[3])
    binary = root / "bin/upkeeper"
    command("case5-upkeeper-build", ["go", "build", "-o", str(binary), "./cmd/upkeeper"], cwd=source)
    command("case5-upkeeper-build-metadata", ["go", "version", "-m", str(binary)])
    command("case5-steward-build-metadata", ["go", "version", "-m", str(root / "bin/t3-steward")])
    profile = dict(cpu_class="medium", executor_slots=1, capabilities=["git", "huyang"],
                   provider_instances=["synthetic"],
                   models={"synthetic": [model, "synthetic-model"]},
                   quota_pools=["synthetic-pool"])
    intent: dict[str, Any] = dict(kind="steward-fleet-configuration", schema_version=1,
                  coordinator=dict(coordinator_id="qual-coordinator", host="qual-controller",
                      client_endpoint=dict(address="qual-admin", connection="ssh",
                                           remote_command="t3-steward", request_timeout="30s")),
                  profiles={"standard": profile, "build": copy.deepcopy(profile)},
                  hosts={}, projects={})
    for worker, profile_name in (("worker-a", "standard"), ("worker-b", "build")):
        intent["hosts"][worker] = dict(profile=profile_name, worker_id=worker, transport="ssh",
            credential_ref=f"secretref:f02-protocol/{worker}",
            admin_credential_ref=f"secretref:f03-admin/{worker}")
    for project, eligible in (("good", ["worker-a", "worker-b"]),
                             ("plain", ["worker-a"]), ("beside", ["worker-b"])):
        intent["projects"][project] = dict(repository="ssh://qual-repo-good/good.git",
            default_ref="main", setup_profile="quick", eligible_workers=eligible)
    dump(evidence / "case5-authored-intent.json", intent)

    config = root / "upkeeper-config"
    for name in ("inventory/hosts.yml", "desired/components.yml", "desired/release.json"):
        destination = config / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, destination)
    inventory = json.loads((config / "inventory/hosts.yml").read_text())
    prototype = inventory["all"]["hosts"]["homelab"]
    for host in ("worker-a", "worker-b", "qual-controller"):
        host_spec = copy.deepcopy(prototype)
        host_spec.update(ansible_host="127.0.0.1", canonical_hostname=host)
        inventory["all"]["hosts"][host] = host_spec
    dump(config / "inventory/hosts.yml", inventory)
    manifest = json.loads((config / "desired/release.json").read_text())
    baseline = copy.deepcopy(intent)
    for item in baseline["profiles"].values():
        item["models"]["synthetic"] = ["synthetic-model"]
    manifest["components"]["steward-fleet-configuration"] = baseline
    dump(config / "desired/release.json", manifest)
    # Previous authorization is immutable Git history in a disposable release checkout.
    environment = env(root / "upkeeper-review-home")
    command("case5-release-init", ["git", "init", "-q", str(config)], environment)
    command("case5-release-add", ["git", "-C", str(config), "add", "."], environment)
    command("case5-release-commit", ["git", "-C", str(config), "-c", "user.name=Qualification",
            "-c", "user.email=qualification@invalid", "commit", "-qm", "Baseline synthetic authorization"], environment)
    previous = command("case5-previous-release", ["git", "-C", str(config), "rev-parse", "HEAD"], environment).stdout.strip()
    (evidence / "case5-previous-release.txt").write_text(previous)

    payload = dict(run_id="qualification-case5-baseline", host=dict(name="qual-controller", os_id="linux"),
                   components=["steward-fleet-configuration"],
                   manifest=dict(components={"steward-fleet-configuration": baseline}),
                   lock=dict(run_id="qualification-case5-baseline"), stored_files={})
    command("case5-upkeeper-baseline-apply", [str(binary), "__agent"],
            env(root / "coordinator/home"), json.dumps(payload))
    projected = root / "coordinator/home/.config/t3-steward/coordinator-fleet.json"
    assert projected.is_file(), "UpKeeper did not distribute the coordinator catalog"
    assert projected.stat().st_mode & 0o777 == 0o600, "catalog is not owner-only"
    shutil.copyfile(projected, evidence / "case5-baseline-coordinator-fleet.json")
    for worker in ("worker-a", "worker-b"):
        shutil.copyfile(root / worker / "t3/caches/synthetic.json",
                        evidence / f"case5-observation-before-{worker}.json")

elif phase == "enable":
    binary = root / "bin/upkeeper"
    config = root / "upkeeper-config"
    intent = json.loads((evidence / "case5-authored-intent.json").read_text())
    manifest = json.loads((config / "desired/release.json").read_text())
    manifest["components"]["steward-fleet-configuration"] = intent
    dump(config / "desired/release.json", manifest)
    environment = env(root / "upkeeper-review-home")
    environment["DEV_FLEET_CONFIG_ROOT"] = str(config)
    previous = (evidence / "case5-previous-release.txt").read_text()
    command("case5-enrollment-plan", [str(binary), "fleet-plan", "--previous", previous, "--json"], environment)
    payload = dict(run_id="qualification-case5-catalog", host=dict(name="qual-controller", os_id="linux"),
                   components=["steward-fleet-configuration"],
                   manifest=dict(components={"steward-fleet-configuration": intent}),
                   lock=dict(run_id="qualification-case5-catalog"), stored_files={})
    command("case5-upkeeper-coordinator-apply", [str(binary), "__agent"],
            env(root / "coordinator/home"), json.dumps(payload))
    projected = root / "coordinator/home/.config/t3-steward/coordinator-fleet.json"
    assert projected.stat().st_mode & 0o777 == 0o600
    shutil.copyfile(projected, evidence / "case5-applied-coordinator-fleet.json")

elif phase == "advertise":
    for worker in ("worker-a", "worker-b"):
        cache = root / worker / "t3/caches/synthetic.json"
        observation = json.loads(cache.read_text())
        observation["models"] += [{"slug": model}, {"slug": "unapproved-observation"}]
        dump(cache, observation)
        dump(evidence / f"case5-observation-after-{worker}.json", observation)

elif phase == "verify":
    failures = 0

    def verdict(name, passed, detail):
        global failures
        failures += not passed
        line = f"[case] {'PASS' if passed else 'FAIL'} {name} {detail}"
        print(line, flush=True)
        with (evidence / "case5-verdicts.txt").open("a") as out:
            out.write(line + "\n")

    def workers(name):
        return {w["snapshot"]["workerId"]: w for w in
                json.loads((evidence / name).read_text())["workers"]}

    initial = workers("case5-initial-workers.json")
    applied = workers("case5-after-apply-before-restart.json")
    missing = workers("case5-missing-observation.json")
    observed = workers("case5-observed-before-enroll.json")
    effective = workers("case5-effective-workers.json")
    plan = json.loads((evidence / "case5-enrollment-plan.out").read_text())
    verdict("case5-plan-explicit", plan["enrollment_plan"]["applied"] is False,
            "real UpKeeper plan never applies enrollment")
    sets = {}
    for worker in ("worker-a", "worker-b"):
        desired = plan["hosts"][worker]["desired_models"]["synthetic"]
        before = missing[worker]
        accepted = effective[worker]
        before_models = next(p["models"] for p in before["snapshot"]["inventory"]["providers"]
                             if p["instanceId"] == "synthetic")
        after_models = next(p["models"] for p in accepted["snapshot"]["inventory"]["providers"]
                            if p["instanceId"] == "synthetic")
        prior_revision = initial[worker]["enrollment"]["revision"]
        verdict(f"case5-no-auto-enroll-{worker}",
                applied[worker]["enrollment"]["revision"] == prior_revision
                and before["enrollment"]["revision"] == prior_revision and not before["enrolled"]
                and observed[worker]["enrollment"]["revision"] == prior_revision and not observed[worker]["enrolled"],
                "UpKeeper application, restart and new provider observation did not enroll")
        refusal = (evidence / f"case5-missing-enroll-{worker}.out").read_text()
        verdict(f"case5-missing-observation-{worker}",
                (evidence / f"case5-missing-enroll-{worker}.exit").read_text() == "8"
                and "provider route is unavailable" in refusal and model not in before_models,
                "desired Opus absent from observation; correct-fenced enrollment refused")
        verdict(f"case5-effective-opus-{worker}", model in desired and model in after_models
                and "unapproved-observation" not in after_models and accepted["enrolled"]
                and accepted["enrollment"]["revision"] == prior_revision + 1
                and (evidence / f"case5-opus-enroll-{worker}.exit").read_text() == "0",
                "explicit enrollment admits observed authorized Opus and excludes unapproved observed model")
        observed_models = [m["slug"] for m in json.loads(
            (evidence / f"case5-observation-after-{worker}.json").read_text())["models"]]
        sets[worker] = dict(desired=desired, observed=observed_models,
                            effective=after_models, enrolled=accepted["enrolled"],
                            catalog_revision=accepted["snapshot"]["inventory"]["catalogRevision"])
    dump(evidence / "case5-model-sets.json", sets)
    sys.exit(int(failures > 0))
else:
    raise SystemExit(f"unknown phase {phase}")
